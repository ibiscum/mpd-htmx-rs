# mpd-htmx-rs

Small Rust web UI for controlling an MPD (Music Player Daemon) instance with HTMX-style partial updates.

## Purpose

This app provides:

- A responsive browser player view (`/`) usable on desktop and phone
- Live player updates over Server-Sent Events (`/events`)
- Playback controls (play/pause/next/prev/stop)
- Queue view with click-to-play
- Volume controls and playback option toggles (repeat/random/single/consume)
- Cover art for current song (`/cover/current`)
- Tabbed screens for:
  - Library browser (directory navigation + sortable columns + pagination + queue/add-to-playlist)
  - Playlist management (list/load/save/clear/delete)
  - Search tab (full-library text search)

It bridges browser actions to MPD commands and keeps the UI synchronized with MPD state.

## Application flow

1. The app starts an HTTP server (default bind: `0.0.0.0:3000`).
2. In parallel, it starts an MPD bridge worker.
3. The worker connects to MPD, fetches initial player state, and watches MPD subsystem events.
4. On MPD player/queue changes, the worker refreshes state and pushes it to a `watch` channel.
5. `/events` streams rendered HTML partials to the browser whenever state changes.
6. `/cmd/{action}` and `/cmd/play/{position}` receive control commands and forward them to MPD via a command channel.
7. If MPD is temporarily unavailable, the worker logs the error and reconnects automatically.

## MPD connection configuration

Set one of:

- `MPD_ADDR` (full socket address, preferred when already known)
- or `MPD_HOST` and optional `MPD_PORT` (default port: `6600`)

Examples:

```bash
MPD_ADDR=192.168.1.50:6600 cargo run
```

```bash
MPD_HOST=192.168.1.50 MPD_PORT=6600 cargo run
```

IPv6 example:

```bash
MPD_ADDR='[2001:db8::10]:6600' cargo run
```

## Quick start

### Local MPD (same machine)

```bash
MPD_HOST=127.0.0.1 MPD_PORT=6600 cargo run
```

Open: <http://localhost:3000>

### Remote MPD (different host)

```bash
MPD_HOST=192.168.1.50 MPD_PORT=6600 cargo run
```

Open: <http://localhost:3000>

### Remote MPD via full socket address

```bash
MPD_ADDR=192.168.1.50:6600 cargo run
```

## Recommendations for remote MPD hosts

For best practice when MPD runs on another machine:

1. **Do not expose MPD directly to the public internet.** Keep it on a trusted LAN/VPN/private subnet.
2. **Restrict network access** to only the web app host using firewall rules.
3. **Bind MPD intentionally** (for example in `mpd.conf`) to the required interface only, not `0.0.0.0` unless you truly need it.
4. **Use authentication in MPD** (`password` configuration) if remote clients are allowed.
5. **Prefer encrypted transport** (VPN, WireGuard, Tailscale, or SSH tunnel) when crossing untrusted networks.
6. **Monitor logs** on both sides; this app will retry on connection loss, which helps with transient outages but should not hide persistent network misconfiguration.
