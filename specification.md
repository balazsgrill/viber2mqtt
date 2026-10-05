# viber2mqtt — Specification

## 1. Overview

`viber2mqtt` is a bridge between a Viber chat bot and an MQTT broker, used to integrate
home automation events and commands into Viber conversations.

The bot's behavior is entirely driven by a static configuration file that defines two
kinds of entries, each identified by a unique text identifier:

- **Notifications** — forward a message to subscribed Viber users whenever a payload is
  published to a configured MQTT topic.
- **Actions** — publish a fixed, configured payload to a configured MQTT topic whenever a
  user sends a matching trigger text to the bot.

The application itself defines no home-automation logic; it only maps between MQTT topics
and Viber chat messages according to configuration.

## 2. Configuration

A single configuration file (format: YAML, loaded at startup; not reloaded at runtime)
defines the broker connection and the list of notifications and actions.

```yaml
mqtt:
  host: localhost
  port: 1883
  username: ...        # optional
  password: ...         # optional

viber:
  auth_token: ...
  bot_name: "Home Bot"
  webhook_url: "https://example.com/viber/webhook"

storage:
  backend: sqlite        # one of: json, sqlite, postgres
  path: ./data/viber2mqtt.db    # json/sqlite: file path
  # dsn: postgres://user:pass@host:5432/dbname   # postgres only

notifications:
  - id: front-door
    topic: home/sensors/front-door
    message: "Front door {state}"      # template, see 4.2
    description: "Front door open/closed sensor"   # shown in the bot's notification list

  - id: temperature
    topic: home/sensors/living-room/temperature
    message: "Living room temperature: {value} °C"

actions:
  - id: lights-on
    trigger: "lights on"               # exact text the user must send (case-insensitive)
    topic: home/actions/lights
    payload: "ON"
    description: "Turn on living room lights"

  - id: lights-off
    trigger: "lights off"
    topic: home/actions/lights
    payload: "OFF"
```

Each notification and action `id` must be unique within its own list (an id may be reused
between a notification and an action, since they are addressed differently — by
subscription id vs. by trigger text).

### 2.1 Validation

On startup the application validates the configuration and refuses to start if:

- any `id` is duplicated within `notifications` or within `actions`,
- any `trigger` text is duplicated within `actions` (case-insensitively),
- a referenced field is missing (`topic`, `message`/`payload`, etc.).

## 3. MQTT Behavior

- The application maintains a single persistent connection to the configured broker,
  reconnecting automatically with backoff on disconnect.
- On startup, it subscribes to the MQTT topic of every configured notification.
- Action topics are publish-only; the application does not subscribe to them.
- Quality of service: notifications are consumed at QoS 1. Action payloads are published
  at QoS 1. (Configurable per-topic later if needed; not required for v1.)

## 4. Notifications

### 4.1 Subscribing

Viber users interact with the bot via chat commands:

| Command | Behavior |
|---|---|
| `/list` | Lists all configured notification ids and descriptions, marking which ones the sender is currently subscribed to. |
| `/subscribe <id>` | Subscribes the sender to the notification `<id>`. |
| `/unsubscribe <id>` | Unsubscribes the sender from notification `<id>`. |
| `/status` | Lists the notification ids the sender is currently subscribed to. |
| `/help` | Shows available commands. |

Unknown commands or an unknown `<id>` get a short error reply; they are not treated as
action triggers.

### 4.2 Message templating

The `message` field of a notification is a template. When a payload arrives on the
configured topic:

- If the payload parses as JSON, template placeholders of the form `{field}` or
  `{nested.field}` are substituted with the corresponding value from the parsed payload
  (dot notation for nested objects; arrays are not addressed in v1).
- If the payload is not valid JSON, the single placeholder `{payload}` is substituted
  with the raw payload text; any other named placeholder is left as a literal `{field}`
  (and a warning is logged) since there is no structured data to resolve it against.
- If a referenced field is missing from the JSON payload, the placeholder is replaced
  with an empty string and a warning is logged.

The rendered message is sent to every Viber user currently subscribed to that
notification id. Delivery failures for an individual user (e.g. user blocked the bot) are
logged and do not affect delivery to other subscribers.

### 4.3 Persistence

Subscriptions and the user registry (§6.1) are persisted so they survive restarts. The
storage backend is selected via `storage.backend` in configuration; the application
supports three interchangeable backends implementing the same logical schema:

- `json` — a single JSON file at `storage.path`, rewritten atomically on every change.
  Simplest option, suitable for small/single-instance deployments; not safe for
  concurrent writers outside the application itself.
- `sqlite` — a SQLite database file at `storage.path`. Recommended default: still
  file-based and dependency-free, but with transactional writes and better durability
  under concurrent access within the process.
- `postgres` — an external PostgreSQL database reached via `storage.dsn`. Useful when
  viber2mqtt needs to share this state with other services, or when running multiple
  replicas of the application against shared state.

All backends store the same two logical collections:

- `users`: user id, display name, first-contact timestamp.
- `subscriptions`: user id, notification id.

The configured backend is read once at startup and updated synchronously on every
subscribe/unsubscribe (§4.1) and on every new user registration (§6.1).

## 5. Actions

- Any Viber user who messages the bot can trigger an action — there is no authorization
  allowlist in v1.
- When an incoming message's trimmed text matches an action's `trigger` text
  (case-insensitive, exact match — not a substring match), the application publishes that
  action's configured, fixed `payload` to its configured `topic`.
- The bot replies to the sender with a short confirmation (e.g. "OK: lights on").
- If a message matches neither a command nor any action trigger, the bot replies with a
  brief "unrecognized" message and suggests `/help`.
- Trigger matching happens after checking for `/`-prefixed commands, so action triggers
  must not start with `/`.

## 6. User Registration & Monitoring

In the absence of any authentication scheme, the bot provides passive, open monitoring so
that anyone using it can see who else is using it.

### 6.1 User registry

The application maintains a registry of all Viber users who have ever interacted with the
bot ("known users"), persisted in the same local store as subscriptions. A user is added
to this registry the first time either:

- Viber delivers a `subscribed` / `conversation_started` callback for that user, or
- the first inbound message from that user id is received,

whichever happens first. Each registry entry stores at least: Viber user id, display name
(if provided by Viber), and the timestamp of first contact.

### 6.2 New-user announcement

When a previously-unknown user id is added to the registry, the bot broadcasts an
announcement message (e.g. `"New user joined: <name> (<id>)"`) to every other user
already in the registry at that point. The new user does not receive this announcement
about their own registration. Broadcast failures to an individual existing user (e.g.
blocked bot) are logged and do not prevent delivery to the others.

### 6.3 `/users` command

Any user can run `/users` to list every known user id together with the notification ids
they are currently subscribed to, e.g.:

```
U1a2b3 (Alice): front-door, temperature
U4d5e6 (Bob): (none)
```

This is the basic monitoring mechanism provided in place of authentication: since anyone
can subscribe to notifications or trigger actions, anyone can also audit who else is using
the bot and what they are subscribed to.

## 7. Non-goals (v1)

- No per-user authorization for actions.
- No runtime reload of configuration (requires restart to pick up changes).
- No templating of action payloads from user message content.
- No group-chat support — notifications and actions operate on 1:1 bot conversations.

## 8. Error Handling

- MQTT broker unavailable at startup or during operation: log and retry with
  exponential backoff; Viber bot remains responsive to commands (subscriptions still
  work) but notifications stop until reconnected, and actions fail with an error reply to
  the user ("Could not reach home automation system, try again later").
- Viber API errors on send: logged; do not crash the process.
- Malformed configuration: fail fast at startup with a descriptive error.
