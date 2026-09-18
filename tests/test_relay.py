#
# Copyright 2026 Robbi Nespu <robbinespu@gmail.com>
# SPDX-License-Identifier: Apache-2.0
"""
Offline protocol tests: a fake websocket on each side drives real stream
start/packet/stop sequences through the relay logic. No network, no Zello
account needed.
"""
import asyncio
import base64
import json
import struct
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from zello_relay import AUDIO_PACKET, HEADER, Auth, Side  # noqa: E402

CODEC_HEADER = base64.b64encode(
    (16000).to_bytes(2, "little") + (1).to_bytes(1, "big") + (60).to_bytes(1, "big")
).decode()

PASSED = []
FAILED = []


def check(name, cond, detail=""):
    (PASSED if cond else FAILED).append(name)
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'' if cond else '  -> ' + detail}")


class FakeWS:
    """Stands in for a live Zello websocket and answers commands."""

    def __init__(self):
        self.side = None
        self.sent_json = []
        self.sent_binary = []
        self._next_stream_id = 5000
        self.hold = None  # set to an asyncio.Event to stall responses

    async def send(self, data):
        if isinstance(data, (bytes, bytearray)):
            self.sent_binary.append(bytes(data))
            return
        msg = json.loads(data)
        self.sent_json.append(msg)
        asyncio.create_task(self._respond(msg))

    async def _respond(self, msg):
        if self.hold is not None:
            await self.hold.wait()
        cmd = msg.get("command")
        if cmd == "start_stream":
            self._next_stream_id += 1
            await self.side._on_json(
                {"seq": msg["seq"], "success": True, "stream_id": self._next_stream_id}
            )
        else:
            await self.side._on_json({"seq": msg["seq"], "success": True})

    def commands(self, name):
        return [m for m in self.sent_json if m.get("command") == name]


def build_bridge(half_duplex=True):
    cfg_a = {"name": "A", "username": "bot_a", "password": "x", "channel": "CH-A"}
    cfg_b = {"name": "B", "username": "bot_b", "password": "x", "channel": "CH-B"}
    a = Side(cfg_a, Auth(), half_duplex)
    b = Side(cfg_b, Auth(), half_duplex)
    a.peer, b.peer = b, a
    own = {"bot_a", "bot_b"}
    a.ignore_senders = own
    b.ignore_senders = own
    for side in (a, b):
        ws = FakeWS()
        ws.side = side
        side.ws = ws
        side.online = True
    return a, b


def stream_start(sid, sender):
    return {
        "command": "on_stream_start",
        "stream_id": sid,
        "type": "audio",
        "codec": "opus",
        "codec_header": CODEC_HEADER,
        "packet_duration": 60,
        "channel": "CH-A",
        "from": sender,
    }


def audio(sid, packet_id, payload):
    return HEADER.pack(AUDIO_PACKET, sid, packet_id) + payload


async def settle(n=6):
    for _ in range(n):
        await asyncio.sleep(0)


async def test_forward_path():
    print("\ntest: audio forwarded A -> B")
    a, b = build_bridge()
    await a._on_stream_start(stream_start(1, "alice"))
    await settle()

    starts = b.ws.commands("start_stream")
    check("start_stream issued on B", len(starts) == 1, f"got {len(starts)}")
    check("codec_header passed through unchanged",
          starts and starts[0]["codec_header"] == CODEC_HEADER)
    check("target channel is B's channel", starts and starts[0]["channel"] == "CH-B")

    for i in range(3):
        await a._on_binary(audio(1, i, b"opus%d" % i))
    await settle()

    out_id = b.outbound_active
    check("B has an active outbound stream", isinstance(out_id, int) and out_id > 0, str(out_id))
    check("3 audio packets reached B", len(b.ws.sent_binary) == 3, str(len(b.ws.sent_binary)))

    if b.ws.sent_binary:
        kind, sid, pid = HEADER.unpack_from(b.ws.sent_binary[0])
        payload = b.ws.sent_binary[0][HEADER.size:]
        check("packet type preserved", kind == AUDIO_PACKET)
        check("stream id rewritten to B's stream", sid == out_id, f"{sid} != {out_id}")
        check("packet_id zeroed for server", pid == 0, str(pid))
        check("payload forwarded verbatim", payload == b"opus0", repr(payload))

    await a._on_stream_stop({"command": "on_stream_stop", "stream_id": 1})
    await settle()
    check("stop_stream issued on B", len(b.ws.commands("stop_stream")) == 1)
    check("B outbound released", b.outbound_active is None, str(b.outbound_active))
    check("inbound map cleaned up", a.inbound == {}, str(a.inbound))


async def test_loop_guard():
    print("\ntest: relay never forwards its own audio (loop guard)")
    a, b = build_bridge()
    # B's bot transmits into channel A; A must not bounce it back to B
    await a._on_stream_start(stream_start(2, "bot_b"))
    await settle()
    check("no start_stream for own account", b.ws.commands("start_stream") == [])
    check("stream not tracked", a.inbound == {})

    await a._on_stream_start(stream_start(3, "bot_a"))
    await settle()
    check("no start_stream for this side's own account", b.ws.commands("start_stream") == [])

    await a._on_binary(audio(2, 0, b"should-not-forward"))
    await settle()
    check("no audio leaked to B", b.ws.sent_binary == [])


async def test_buffering():
    print("\ntest: packets arriving before start_stream ack are buffered, not lost")
    a, b = build_bridge()
    b.ws.hold = asyncio.Event()  # stall B's start_stream response

    await a._on_stream_start(stream_start(4, "alice"))
    await settle()
    for i in range(5):
        await a._on_binary(audio(4, i, bytes([i])))
    await settle()
    check("nothing sent while awaiting ack", b.ws.sent_binary == [])
    check("packets held in buffer", len(a.inbound[4].buffer) == 5, str(len(a.inbound[4].buffer)))

    b.ws.hold.set()
    await settle(10)
    check("buffer flushed after ack", len(b.ws.sent_binary) == 5, str(len(b.ws.sent_binary)))
    order = [p[HEADER.size:] for p in b.ws.sent_binary]
    check("flushed in original order", order == [bytes([i]) for i in range(5)], str(order))


async def test_half_duplex():
    print("\ntest: half-duplex blocks a simultaneous reverse relay")
    a, b = build_bridge(half_duplex=True)
    await a._on_stream_start(stream_start(6, "alice"))
    await settle()
    check("A->B relay running", b.outbound_active is not None)

    # someone keys up on B while B is playing the relayed audio
    msg = stream_start(7, "bob")
    msg["channel"] = "CH-B"
    await b._on_stream_start(msg)
    await settle()
    check("reverse stream dropped", a.ws.commands("start_stream") == [])
    check("reverse stream not tracked", b.inbound == {})

    await a._on_stream_stop({"command": "on_stream_stop", "stream_id": 6})
    await settle()

    # once the first relay ends, the reverse direction works
    await b._on_stream_start(msg)
    await settle()
    check("reverse relay allowed after release", len(a.ws.commands("start_stream")) == 1)
    check("B->A audio path live", a.outbound_active is not None)


async def test_peer_offline():
    print("\ntest: stream dropped cleanly when peer is offline")
    a, b = build_bridge()
    b.online = False
    await a._on_stream_start(stream_start(8, "alice"))
    await settle()
    check("no start_stream attempted", b.ws.commands("start_stream") == [])
    check("no stale outbound reservation", b.outbound_active is None, str(b.outbound_active))


async def test_jwt_shape():
    print("\ntest: JWT uses standard (not URL-safe) base64")
    from Crypto.PublicKey import RSA

    key = RSA.generate(2048).export_key().decode()
    token = Auth(issuer="test-issuer", private_key=key).token()
    parts = token.split(".")
    check("three JWT segments", len(parts) == 3, str(len(parts)))
    header = json.loads(base64.b64decode(parts[0]))
    payload = json.loads(base64.b64decode(parts[1]))
    check("alg is RS256", header.get("alg") == "RS256", str(header))
    check("iss claim set", payload.get("iss") == "test-issuer", str(payload))
    check("exp claim set", isinstance(payload.get("exp"), int), str(payload))
    check("signature decodes with standard base64", len(base64.b64decode(parts[2])) == 256)


async def main():
    for test in (test_forward_path, test_loop_guard, test_buffering,
                 test_half_duplex, test_peer_offline, test_jwt_shape):
        await test()
    print(f"\n{len(PASSED)} passed, {len(FAILED)} failed")
    if FAILED:
        print("failures: " + ", ".join(FAILED))
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
