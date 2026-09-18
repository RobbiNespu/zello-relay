#!/usr/bin/env python3
"""
Author : Robbi Nespu <robbinespu@gmail.com>
Url: https://github.com/RobbiNespu/zello-relay

zello-relay - a bidirectional bridge between Zello channels.

Opus payloads are forwarded verbatim from one channel to the other. Nothing is
decoded, re-encoded, or routed through a soundcard, so there is no generational
audio loss and no VOX timing to tune.

Loop protection is explicit rather than timing-based: each side knows both of
the relay's own account names and never forwards a stream that originated from
one of them.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import os
import signal
import struct
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import websockets
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15

LOG = logging.getLogger("zello-relay")

CONSUMER_WS = "wss://zello.io/ws"
WORK_WS = "wss://zellowork.io/ws/{network}"

AUDIO_PACKET = 0x01
# Byte 0: packet type. Bytes 1-4: stream id. Bytes 5-8: packet id. Then Opus.
HEADER = struct.Struct(">BII")

# A stream's packets can arrive before the peer's start_stream is acknowledged.
# Buffer them rather than dropping the first half-second of every transmission.
MAX_BUFFERED_PACKETS = 120  # ~7s at 60ms packets
COMMAND_TIMEOUT = 5.0
RESERVED = -1  # placeholder in outbound_active while start_stream is in flight


# --------------------------------------------------------------------------
# auth
# --------------------------------------------------------------------------

@dataclass
class Auth:
    """Credentials for the consumer network. Zello Work needs neither."""
    issuer: Optional[str] = None
    private_key: Optional[str] = None

    @property
    def enabled(self) -> bool:
        return bool(self.issuer and self.private_key)

    def token(self, ttl: int = 60) -> str:
        """
        Build a Zello developer JWT.

        Deliberately hand-rolled: Zello rejects the URL-safe base64 that PyJWT
        and most other libraries emit, so every segment uses standard base64.
        """
        header = {"typ": "JWT", "alg": "RS256"}
        payload = {"iss": self.issuer, "exp": round(time.time() + ttl)}
        segments = [
            base64.b64encode(json.dumps(header, separators=(",", ":")).encode()),
            base64.b64encode(json.dumps(payload, separators=(",", ":")).encode()),
        ]
        signing_input = b".".join(segments)
        key = RSA.import_key(self.private_key)
        signature = pkcs1_15.new(key).sign(SHA256.new(signing_input))
        segments.append(base64.b64encode(signature))
        return b".".join(segments).decode()


# --------------------------------------------------------------------------
# relay bookkeeping
# --------------------------------------------------------------------------

@dataclass
class RelayStream:
    """One inbound stream on this side, mapped onto an outbound stream on the peer."""
    sender: str
    peer_stream_id: Optional[int] = None
    buffer: List[bytes] = field(default_factory=list)
    dropped: bool = False
    opener: Optional[asyncio.Task] = None


class Side:
    """One end of the bridge: a websocket connection logged on to one channel."""

    def __init__(self, cfg: Dict[str, Any], auth: Auth, half_duplex: bool = True):
        self.name: str = cfg.get("name") or cfg["channel"]
        self.username: str = cfg["username"]
        self.password: str = cfg["password"]
        self.channel: str = cfg["channel"]
        self.url: str = cfg.get("url") or CONSUMER_WS
        self.auth = auth
        self.half_duplex = half_duplex

        self.peer: "Side" = None  # type: ignore[assignment]
        self.ignore_senders: Set[str] = set()

        self.ws: Optional[Any] = None
        self.online = False
        self.outbound_active: Optional[int] = None
        self.inbound: Dict[int, RelayStream] = {}

        self._seq = 0
        self._pending: Dict[int, asyncio.Future] = {}

    # -- plumbing ---------------------------------------------------------

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    async def _command(self, payload: Dict[str, Any], timeout: float = COMMAND_TIMEOUT) -> Dict[str, Any]:
        ws = self.ws
        if ws is None:
            raise ConnectionError(f"{self.name}: not connected")
        seq = self._next_seq()
        payload["seq"] = seq
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[seq] = fut
        try:
            await ws.send(json.dumps(payload))
            return await asyncio.wait_for(fut, timeout)
        finally:
            self._pending.pop(seq, None)

    async def _send_audio(self, stream_id: int, payload: bytes) -> None:
        ws = self.ws
        if ws is None:
            return
        # packet_id is always 0 on packets sent to the server
        await ws.send(HEADER.pack(AUDIO_PACKET, stream_id, 0) + payload)

    # -- connection lifecycle --------------------------------------------

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                await self._session()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.warning("%s: connection lost (%s); reconnecting in %.0fs",
                            self.name, exc, backoff)
            finally:
                self._teardown()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    async def _session(self) -> None:
        LOG.info("%s: connecting to %s", self.name, self.url)
        async with websockets.connect(
            self.url, ping_interval=20, ping_timeout=40, max_size=2 ** 20
        ) as ws:
            self.ws = ws
            # The reader must already be running when logon is sent: the logon
            # reply comes back through it, so awaiting the command with no
            # reader would just sit there until the command timeout.
            reader = asyncio.create_task(self._read_loop(ws))
            logon = asyncio.create_task(self._logon())
            try:
                done, _ = await asyncio.wait(
                    {reader, logon}, return_when=asyncio.FIRST_COMPLETED
                )
                if logon in done:
                    logon.result()  # re-raise a rejected logon
                    await reader    # stay on until the socket closes
                else:
                    reader.result()  # the socket died first; re-raise why
            finally:
                for task in (reader, logon):
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task

    async def _read_loop(self, ws: Any) -> None:
        async for message in ws:
            if isinstance(message, (bytes, bytearray)):
                await self._on_binary(bytes(message))
            else:
                await self._on_json(json.loads(message))

    async def _logon(self) -> None:
        cmd: Dict[str, Any] = {
            "command": "logon",
            "username": self.username,
            "password": self.password,
            "channel": self.channel,
        }
        if self.auth.enabled:
            cmd["auth_token"] = self.auth.token()
        resp = await self._command(cmd, timeout=15.0)
        if not resp.get("success"):
            raise ConnectionError(f"{self.name}: logon rejected: {resp.get('error')}")
        LOG.info("%s: logged on as %s, joined '%s'", self.name, self.username, self.channel)

    def _teardown(self) -> None:
        """Drop all per-connection state so a reconnect starts clean."""
        self.ws = None
        self.online = False
        for relay in self.inbound.values():
            if relay.opener and not relay.opener.done():
                relay.opener.cancel()
        self.inbound.clear()
        for fut in self._pending.values():
            if not fut.done():
                fut.cancel()
        self._pending.clear()
        # our streams on the peer are dead too
        if self.peer is not None:
            self.peer.outbound_active = None

    # -- inbound handling -------------------------------------------------

    async def _on_json(self, msg: Dict[str, Any]) -> None:
        seq = msg.get("seq")
        if seq is not None and seq in self._pending:
            fut = self._pending[seq]
            if not fut.done():
                fut.set_result(msg)
            return

        cmd = msg.get("command")
        if cmd == "on_channel_status":
            self.online = msg.get("status") == "online"
            LOG.info("%s: channel '%s' is %s (%s users)", self.name, self.channel,
                     msg.get("status"), msg.get("users_online", "?"))
        elif cmd == "on_stream_start":
            await self._on_stream_start(msg)
        elif cmd == "on_stream_stop":
            await self._on_stream_stop(msg)
        elif cmd == "on_error":
            LOG.error("%s: server error: %s", self.name, msg.get("error"))

    async def _on_stream_start(self, msg: Dict[str, Any]) -> None:
        sid = msg["stream_id"]
        sender = msg.get("from", "")

        if msg.get("for"):
            return  # private message, not channel traffic
        if sender in self.ignore_senders:
            LOG.debug("%s: ignoring stream %s from own account %s", self.name, sid, sender)
            return

        peer = self.peer
        reason = None
        if peer.ws is None or not peer.online:
            reason = "peer channel not online"
        elif peer.outbound_active is not None:
            reason = "peer already transmitting"
        elif self.half_duplex and self.outbound_active is not None:
            reason = "half-duplex: this channel is already carrying a relay"

        if reason:
            LOG.warning("%s: dropping stream %s from %s (%s)", self.name, sid, sender, reason)
            return

        relay = RelayStream(sender=sender)
        self.inbound[sid] = relay
        peer.outbound_active = RESERVED  # claim the peer before awaiting anything
        relay.opener = asyncio.create_task(self._open_peer_stream(sid, relay, msg))

    async def _open_peer_stream(self, sid: int, relay: RelayStream, msg: Dict[str, Any]) -> None:
        peer = self.peer
        try:
            resp = await peer._command({
                "command": "start_stream",
                "channel": peer.channel,
                "type": "audio",
                "codec": msg.get("codec", "opus"),
                # pass the sender's own encoding parameters straight through
                "codec_header": msg["codec_header"],
                "packet_duration": msg.get("packet_duration", 60),
            })
        except asyncio.CancelledError:
            if peer.outbound_active == RESERVED:
                peer.outbound_active = None
            raise
        except Exception as exc:
            LOG.error("%s: start_stream on %s failed: %s", self.name, peer.name, exc)
            relay.dropped = True
            relay.buffer.clear()
            if peer.outbound_active == RESERVED:
                peer.outbound_active = None
            return

        if not resp.get("success"):
            LOG.error("%s: %s refused start_stream: %s", self.name, peer.name, resp.get("error"))
            relay.dropped = True
            relay.buffer.clear()
            if peer.outbound_active == RESERVED:
                peer.outbound_active = None
            return

        out_id = resp["stream_id"]
        relay.peer_stream_id = out_id
        peer.outbound_active = out_id
        LOG.info("%s -> %s: relaying %s (stream %s -> %s, %d packets buffered)",
                 self.name, peer.name, relay.sender, sid, out_id, len(relay.buffer))

        for payload in relay.buffer:
            await peer._send_audio(out_id, payload)
        relay.buffer.clear()

    async def _on_binary(self, data: bytes) -> None:
        if len(data) < HEADER.size:
            return
        kind, sid, _packet_id = HEADER.unpack_from(data)
        if kind != AUDIO_PACKET:
            return
        relay = self.inbound.get(sid)
        if relay is None or relay.dropped:
            return

        payload = data[HEADER.size:]
        if relay.peer_stream_id is None:
            if len(relay.buffer) < MAX_BUFFERED_PACKETS:
                relay.buffer.append(payload)
            else:
                LOG.warning("%s: stream %s buffer full, dropping packet", self.name, sid)
            return
        await self.peer._send_audio(relay.peer_stream_id, payload)

    async def _on_stream_stop(self, msg: Dict[str, Any]) -> None:
        sid = msg["stream_id"]
        relay = self.inbound.pop(sid, None)
        if relay is None:
            return
        if relay.opener and not relay.opener.done():
            relay.opener.cancel()

        peer = self.peer
        out_id = relay.peer_stream_id
        if out_id is not None:
            try:
                await peer._command({
                    "command": "stop_stream",
                    "stream_id": out_id,
                    "channel": peer.channel,
                })
            except Exception as exc:
                LOG.warning("%s: stop_stream on %s failed: %s", self.name, peer.name, exc)
            LOG.info("%s -> %s: stream %s ended", self.name, peer.name, sid)

        if peer.outbound_active in (out_id, RESERVED):
            peer.outbound_active = None


# --------------------------------------------------------------------------
# config + entrypoint
# --------------------------------------------------------------------------

def load_config(path: Path) -> Dict[str, Any]:
    with path.open() as fh:
        cfg = json.load(fh)

    for key in ("channel_a", "channel_b"):
        if key not in cfg:
            raise SystemExit(f"config: missing '{key}'")
        for field_name in ("username", "password", "channel"):
            if not cfg[key].get(field_name):
                raise SystemExit(f"config: {key}.{field_name} is required")

    if cfg["channel_a"]["username"] == cfg["channel_b"]["username"]:
        raise SystemExit(
            "config: each side needs its own Zello account - a single account "
            "cannot hold two connections"
        )
    if cfg["channel_a"]["channel"] == cfg["channel_b"]["channel"]:
        raise SystemExit("config: both sides point at the same channel")
    return cfg


def build_auth(cfg: Dict[str, Any]) -> Auth:
    issuer = os.environ.get("ZELLO_ISSUER") or cfg.get("issuer")
    key = os.environ.get("ZELLO_PRIVATE_KEY")
    if not key:
        key_path = cfg.get("private_key_path")
        if key_path:
            key = Path(key_path).read_text()
    if not issuer or not key:
        LOG.warning(
            "no issuer/private key configured - this only works against Zello Work. "
            "For free Zello, get an Issuer and Private Key from developers.zello.com."
        )
        return Auth()
    return Auth(issuer=issuer, private_key=key)


async def amain() -> None:
    cfg_path = Path(os.environ.get("ZELLO_RELAY_CONFIG", "config.json"))
    if not cfg_path.exists():
        raise SystemExit(f"config not found: {cfg_path}")
    cfg = load_config(cfg_path)

    logging.basicConfig(
        level=getattr(logging, str(cfg.get("log_level", "INFO")).upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(message)s",
        stream=sys.stdout,
    )

    auth = build_auth(cfg)
    half_duplex = cfg.get("half_duplex", True)

    a = Side(cfg["channel_a"], auth, half_duplex)
    b = Side(cfg["channel_b"], auth, half_duplex)
    a.peer, b.peer = b, a

    # the loop guard: neither side ever forwards audio sent by the relay itself
    own = {a.username, b.username}
    a.ignore_senders = own
    b.ignore_senders = own

    LOG.info("bridging '%s' <-> '%s' (half_duplex=%s)", a.channel, b.channel, half_duplex)

    tasks = [asyncio.create_task(a.run()), asyncio.create_task(b.run())]
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    await stop.wait()
    LOG.info("shutting down")
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
