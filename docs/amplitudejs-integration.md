# AmplitudeJS Integration Plan (MPD-Safe)

## Goal

Use AmplitudeJS for richer browser UI interactions while **preserving MPD as the only playback engine**.

## Current UI Status

The player templates now use an Amplitude-style visual skin (buttons, progress bar styling, and queue current-row highlighting) while keeping the existing HTMX + MPD command flow unchanged.

## Non-Goals

- Do **not** move playback into the browser.
- Do **not** replace MPD queue/state with client-side audio state.
- Do **not** make AmplitudeJS the source of truth.

## Core Rule

`MPD state (server) > web UI state (client)`

All play/pause/next/seek/volume actions must continue to call backend MPD endpoints.  
SSE updates from MPD must continue to drive displayed state.

---

## Phase 1 — Safe UI-Only Trial

1. Add AmplitudeJS script behind a feature flag (env var or template toggle).
2. Initialize it in “UI mode” only:
   - no real audio source URLs
   - no autoplay
   - no local playlist authority
3. Keep existing buttons wired to `/cmd/*` endpoints.
4. Continue rendering player state from SSE partials.

**Exit criteria**
- Controls still operate MPD exactly as before.
- Disconnect/reconnect behavior unchanged.

---

## Phase 2 — Event Bridge (Client -> MPD)

Map UI interactions to backend commands:

- Play/Pause -> `POST /cmd/toggle`
- Next -> `POST /cmd/next`
- Prev -> `POST /cmd/prev`
- Stop -> `POST /cmd/stop`
- Volume -> existing volume command endpoints
- Queue row click -> `POST /cmd/play/:position`

If AmplitudeJS emits local state transitions, intercept them and convert to backend calls; do not accept them as final state.

**Exit criteria**
- Every interaction produces an MPD command request.
- UI state always settles to server-pushed SSE state.

---

## Detailed Implementation Steps (UI Patterns Only, MPD-Controlled)

### 1) Scope AmplitudeJS to presentation widgets only

- Use AmplitudeJS classes/components for:
  - button styling and active states
  - progress bar visuals
  - playlist row highlighting and selection UX
- Do not configure browser audio playback sources.

### 2) Keep server-rendered structure and IDs stable

- Keep existing backend-rendered player/queue HTML from Askama templates.
- Add stable element hooks (ids/data attributes) so frontend glue code can target:
  - play/pause/next/prev/stop controls
  - progress bar container/fill
  - queue rows with MPD queue position

### 3) Route all user actions to backend endpoints

- On button click, call backend endpoints directly:
  - `POST /cmd/toggle`
  - `POST /cmd/next`
  - `POST /cmd/prev`
  - `POST /cmd/stop`
  - `POST /cmd/vol-up` / `POST /cmd/vol-down`
  - `POST /cmd/play/:position`
- Do not commit UI state as “final” on click; only show temporary pending/pressed state.

### 4) Drive final state from SSE updates

- On each `player-update` SSE payload:
  - update track metadata text
  - update queue “current row” highlight
  - update play/pause visual state
  - set progress bar to authoritative `elapsed/duration`
  - update volume/mode toggle indicators
- If SSE state disagrees with optimistic UI, SSE wins immediately.

### 5) Implement progress animation as cosmetic only

- Between SSE ticks, animate progress locally for smoothness.
- On next SSE frame, snap/rebase animation to authoritative elapsed time.
- Stop animation when MPD reports paused/stopped or when SSE disconnects.

### 6) Preserve HTMX/no-JS fallback behavior

- Keep existing HTMX attributes and fallback handlers intact.
- If AmplitudeJS fails to load:
  - controls must still issue backend requests
  - tab navigation and queue actions must still work

### 7) Add explicit command error handling

- For non-2xx `/cmd/*` responses:
  - show a visible error toast/banner
  - clear pending visual state for affected control
  - do not fake success or mutate final state locally

### 8) Add guardrails in code review

- Reject any change that:
  - adds real audio playback URLs to AmplitudeJS
  - treats client-side playlist/progress as source of truth
  - bypasses `/cmd/*` for playback control

### 9) Validation sequence before enabling feature flag

1. Trigger every transport/mode command and confirm backend endpoint hit.
2. Confirm UI state always converges to SSE after each action.
3. Simulate SSE disconnect/reconnect and verify resync.
4. Block AmplitudeJS script and verify app still works with fallback.
5. Run existing regression tests plus manual checklist from this document.

---

## Phase 3 — Optional Progress UX Improvements

Use AmplitudeJS visuals for smoother local progress animation between SSE ticks, but:

- reset from SSE on every update
- pause animation when MPD reports paused/stopped
- never “invent” track changes locally

**Exit criteria**
- No drift after 5+ minutes playback.
- Track/time remains consistent with MPD.

---

## Data Contract

Keep this separation:

- **Backend authoritative fields**: title, artist, album, queue, play state, elapsed, duration, volume, modes.
- **Frontend transient fields**: animation state, hover/selection, expanded panels.

---

## Failure Handling

- If AmplitudeJS fails to load, UI must fall back to current HTMX flow.
- If MPD command fails, show explicit error state and keep previous known state.
- If SSE drops, reconnect and refresh authoritative player partial.

---

## Security / Networking

- Do not expose direct media URLs unless intentionally designed.
- Keep MPD control path as backend-only.
- Keep existing LAN/VPN security recommendations.

---

## Regression Test Checklist

1. Play/Pause/Next/Prev/Stop still control MPD.
2. Queue click starts correct MPD queue position.
3. Volume and mode toggles still match MPD status.
4. Progress updates continue after long playback.
5. App still works when AmplitudeJS script is blocked/unavailable.

---

## Rollout Strategy

1. Ship behind feature flag disabled by default.
2. Enable in dev first.
3. Validate checklist above.
4. Enable in production only after parity confirmation.
