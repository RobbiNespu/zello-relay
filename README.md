# zello-relay

Bidirectional bridge between Zello channels.

The relay holds two WebSocket connections to the Zello Channel API, one per
channel. When someone transmits on channel A, it opens a matching stream on
channel B and forwards the Opus packets **verbatim** — no decode, no re-encode,
no soundcard, no VOX. The sender's own codec parameters are passed straight
through, so what comes out of B is bit-for-bit what went into A.

Loop protection is explicit rather than timing-based: each side knows both of
the relay's account names and never forwards a stream that came from one of
them.

## Demo

A handheld keyed up on one channel, landing on the other. The log scrolling on
the monitor behind it is the relay forwarding that stream in real time.

[![zello-relay in use — a handheld transmitting while the relay log scrolls](docs/demo-thumb.jpg)](docs/demo.mp4)

## What you need first

Two things, and the first one gates everything else.

### 1. Developer credentials

Free (consumer) Zello requires a signed JWT on logon. Zello Work does not.

1. Sign in at <https://developers.zello.com/> with your Zello account.
2. Complete the developer profile.
3. Copy the **Issuer** and **Private Key**.

Save the private key as `config/zello_private_key.pem` — the full PEM block,
`-----BEGIN PRIVATE KEY-----` header included.

### 2. Two dedicated Zello accounts

One per side. They cannot be shared with your handheld, and they cannot be the
same account twice — one account cannot hold two connections.

- Create `relaybot_a` and `relaybot_b` (any names).
- Add `relaybot_a` to channel A and `relaybot_b` to channel B.
- If either channel is private/moderated, its owner has to approve the bot.

**Before you do this:** a relay puts every transmission from channel A in front
of channel B's users, who never agreed to that. If you don't own both channels,
ask the other owner first.

## Setup

```bash
mkdir -p config
cp config.example.json config/config.json
$EDITOR config/config.json          # issuer, both accounts, both channel names
cp /path/to/key.pem config/zello_private_key.pem
chmod 600 config/zello_private_key.pem
```

```bash
docker compose up -d --build
docker compose logs -f
```

Healthy startup looks like:

```
bridging 'My First Channel' <-> 'My Second Channel' (half_duplex=True)
A: connecting to wss://zello.io/ws
A: logged on as relaybot_a, joined 'My First Channel'
A: channel 'My First Channel' is online (4 users)
```

Key up on channel A and you should see:

```
A -> B: relaying alice (stream 1234 -> 5678, 2 packets buffered)
A -> B: stream 1234 ended
```

A real build and the traffic that follows it:

![docker compose up --build, then docker compose logs -f showing both channels online and streams relaying in both directions](docs/startup-log.png)

## Configuration

| Key | Meaning |
|---|---|
| `issuer` | Issuer from developers.zello.com. Omit for Zello Work. |
| `private_key_path` | Path to the PEM inside the container (`/config/...`). |
| `half_duplex` | `true` blocks a reverse relay while one is in flight — radio-like behaviour. `false` allows both directions at once. |
| `channel_a` / `channel_b` | `username`, `password`, `channel`, optional `name` for logs, optional `url`. |
| `log_level` | `DEBUG` shows ignored-own-stream decisions. |

`ZELLO_ISSUER` and `ZELLO_PRIVATE_KEY` environment variables override the config
file, if you'd rather keep secrets in Docker secrets or your shell.

### Zello Work

Set `url` on each side to `wss://zellowork.io/ws/<network-name>` and drop
`issuer` / `private_key_path` entirely — Zello Work authenticates on
username/password alone.

## Behaviour worth knowing

- **One stream at a time per direction.** A second talker while a relay is
  running is dropped with a logged reason, not queued. This matches how a
  half-duplex radio link behaves.
- **Startup buffering.** Packets that arrive before the peer acknowledges
  `start_stream` are buffered (up to ~7s) and flushed in order, so the first
  syllable isn't clipped.
- **Reconnect.** Each side reconnects independently with exponential backoff to
  60s. A dropped connection clears that side's stream state so nothing leaks
  into the next session.
- **Channel traffic only.** Private (one-to-one) messages are ignored by design.

## Tests

No network or Zello account needed — a fake websocket drives real
start/packet/stop sequences through the relay logic:

```bash
pip install -r requirements.txt
python tests/test_relay.py
```

Covers the forward path and stream-id rewrite, the loop guard, pre-ack
buffering and flush order, half-duplex blocking and release, peer-offline
handling, and the JWT encoding.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `logon rejected: not authorized` | Bad JWT (check the PEM is complete), wrong password, or issuer mismatch. |
| `logon rejected: not enough params` | Missing `auth_token` on the consumer network. |
| `dropping stream ... peer channel not online` | The bot isn't in the far channel yet, or is awaiting moderator approval. |
| `refused start_stream: listen only connection` | The bot account has listen-only rights on that channel. |
| Audio one way only | Usually only one bot got added to its channel. Check both `channel ... is online` lines appear. |

## Layout

```
zello_relay.py        relay (auth, protocol, forwarding) — single file, no framework
config.example.json   copy to config/config.json
tests/test_relay.py   offline protocol tests
Dockerfile            python:3.12-slim, runs as uid 1000
docker-compose.yml    restart: unless-stopped, ./config mounted read-only
docs/                 README screenshots and demo clip
```
