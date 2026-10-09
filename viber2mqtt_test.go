package main

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// ---------- config validation ----------

func validYAML() string {
	return `
mqtt:
  host: localhost
  port: 1883
viber:
  auth_token: tok
  bot_name: Bot
  webhook_url: https://example.com/viber/webhook
storage:
  backend: json
  path: ./data/v2m.json
notifications:
  - id: front-door
    topic: home/sensors/front-door
    message: "Front door {state}"
    description: sensor
actions:
  - id: lights-on
    trigger: "lights on"
    topic: home/actions/lights
    payload: "ON"
`
}

func writeCfg(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "cfg.yaml")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestConfigValid(t *testing.T) {
	c, err := LoadConfig(writeCfg(t, validYAML()))
	if err != nil {
		t.Fatalf("expected valid: %v", err)
	}
	if c.MQTT.Port != 1883 || len(c.Notifications) != 1 || len(c.Actions) != 1 {
		t.Fatalf("unexpected config: %+v", c)
	}
}

func TestConfigValidation(t *testing.T) {
	cases := map[string]string{
		"trigger slash": strings.Replace(validYAML(), `trigger: "lights on"`, `trigger: "/lights on"`, 1),
		"missing topic": strings.Replace(validYAML(), "    topic: home/actions/lights\n", "", 1),
		"bad backend":   strings.Replace(validYAML(), `backend: json`, `backend: redis`, 1),
	}
	for name, y := range cases {
		if _, err := LoadConfig(writeCfg(t, y)); err == nil {
			t.Errorf("%s: expected error, got nil", name)
		}
	}

	// duplicate notification ids
	dup := `
mqtt:
  host: h
viber:
  auth_token: t
  webhook_url: https://x
storage:
  backend: json
  path: ./d
notifications:
  - id: a
    topic: t
    message: m
  - id: a
    topic: t2
    message: m2
`
	if _, err := LoadConfig(writeCfg(t, dup)); err == nil {
		t.Error("duplicate notification id: expected error")
	}

	// duplicate trigger (case-insensitive)
	dupT := `
mqtt:
  host: h
viber:
  auth_token: t
  webhook_url: https://x
storage:
  backend: json
  path: ./d
actions:
  - id: a
    trigger: "lights on"
    topic: t
    payload: "ON"
  - id: b
    trigger: "LIGHTS ON"
    topic: t
    payload: "OFF"
`
	if _, err := LoadConfig(writeCfg(t, dupT)); err == nil {
		t.Error("duplicate trigger: expected error")
	}
}

func TestConfigPostgresDSN(t *testing.T) {
	y := `
mqtt:
  host: h
viber:
  auth_token: t
  webhook_url: https://x
storage:
  backend: postgres
  dsn: postgres://u:p@host:5432/db
`
	c, err := LoadConfig(writeCfg(t, y))
	if err != nil {
		t.Fatalf("expected valid: %v", err)
	}
	d := postgresDSN(c.Storage.DSN)
	if !strings.Contains(d, "sslmode=disable") {
		t.Errorf("expected sslmode default, got %q", d)
	}
	// explicit sslmode wins
	c2, _ := LoadConfig(writeCfg(t, strings.Replace(y, "postgres://u:p@host:5432/db", "postgres://u:p@h:5432/db?sslmode=verify-full", 1)))
	if !strings.Contains(postgresDSN(c2.Storage.DSN), "sslmode=verify-full") {
		t.Errorf("explicit sslmode should win: %q", postgresDSN(c2.Storage.DSN))
	}
}

// ---------- templating ----------

func TestRenderJSON(t *testing.T) {
	got := renderMessage(`Front door {state}`, []byte(`{"state":"open"}`), "n")
	if got != "Front door open" {
		t.Errorf("got %q", got)
	}
	got = renderMessage(`Room: {room.name} {value}C`, []byte(`{"room":{"name":"Kitchen"},"value":21.5}`), "n")
	if got != "Room: Kitchen 21.5C" {
		t.Errorf("got %q", got)
	}
	// missing field -> empty
	got = renderMessage(`x={missing}`, []byte(`{}`), "n")
	if got != "x=" {
		t.Errorf("got %q", got)
	}
	// missing field on JSON payload -> empty string (spec 4.2)
	got = renderMessage(`a {nope} b`, []byte(`{}`), "n")
	if got != "a  b" {
		t.Errorf("got %q", got)
	}
	// non-numeric json scalar -> {payload}
	got = renderMessage(`p={payload}`, []byte(`"hello"`), "n")
	if got != "p=hello" {
		t.Errorf("got %q", got)
	}
}

func TestRenderRaw(t *testing.T) {
	got := renderMessage(`Door: {payload}`, []byte(`OPENED`), "n")
	if got != "Door: OPENED" {
		t.Errorf("got %q", got)
	}
	// raw payload with a named placeholder: left literal + warning
	got = renderMessage(`x {field}`, []byte(`OPENED`), "n")
	if got != "x {field}" {
		t.Errorf("got %q", got)
	}
	// empty payload
	got = renderMessage(`x {payload}`, []byte(``), "n")
	if got != "x " {
		t.Errorf("got %q", got)
	}
}

// ---------- stores ----------

func checkStore(t *testing.T, st Store) {
	t.Helper()
	new1, err := st.UserFirstSeen("U1", "Alice")
	if err != nil || !new1 {
		t.Fatalf("first seen U1: new=%v err=%v", new1, err)
	}
	new2, _ := st.UserFirstSeen("U1", "Alice")
	if new2 {
		t.Fatal("U1 should not be new on second call")
	}
	new3, _ := st.UserFirstSeen("U2", "Bob")
	if !new3 {
		t.Fatal("U2 should be new")
	}

	if err := st.Subscribe("U1", "front-door"); err != nil {
		t.Fatal(err)
	}
	if err := st.Subscribe("U1", "front-door"); err != nil { // idempotent
		t.Fatalf("re-subscribe should be no-op: %v", err)
	}
	if err := st.Subscribe("U1", "temperature"); err != nil {
		t.Fatal(err)
	}
	if err := st.Subscribe("U2", "temperature"); err != nil {
		t.Fatal(err)
	}

	subs, _ := st.SubscriptionsOf("U1")
	if len(subs) != 2 {
		t.Fatalf("U1 subs: %v", subs)
	}
	subs, _ = st.SubscriptionsOf("U2")
	if len(subs) != 1 || subs[0] != "temperature" {
		t.Fatalf("U2 subs: %v", subs)
	}
	subs, _ = st.SubscriptionsOf("nobody")
	if len(subs) != 0 {
		t.Fatalf("nobody subs: %v", subs)
	}
	who, _ := st.SubscribersOf("temperature")
	if len(who) != 2 {
		t.Fatalf("temperature subscribers: %v", who)
	}
	who, _ = st.SubscribersOf("front-door")
	if len(who) != 1 || who[0] != "U1" {
		t.Fatalf("front-door subscribers: %v", who)
	}

	if err := st.Unsubscribe("U1", "temperature"); err != nil {
		t.Fatal(err)
	}
	if err := st.Unsubscribe("U1", "temperature"); err == nil {
		t.Fatal("unsubscribing twice should error")
	}
	if err := st.Unsubscribe("U1", "never"); err == nil {
		t.Fatal("unsubscribing unknown should error")
	}

	users, _ := st.ListUsers()
	if len(users) != 2 {
		t.Fatalf("users: %v", users)
	}
	found := map[string]string{}
	for _, u := range users {
		found[u.ID] = u.DisplayName
	}
	if found["U1"] != "Alice" || found["U2"] != "Bob" {
		t.Fatalf("users: %v", found)
	}
}

func TestJSONStore(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.json")
	st, err := OpenStore(StorageConfig{Backend: "json", Path: p})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	checkStore(t, st)

	// persistence across reopen
	st2, err := OpenStore(StorageConfig{Backend: "json", Path: p})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	new, _ := st2.UserFirstSeen("U1", "Alice")
	if new {
		t.Fatal("U1 should persist")
	}
	subs, _ := st2.SubscriptionsOf("U1")
	if len(subs) != 1 || subs[0] != "front-door" {
		t.Fatalf("persisted subs: %v", subs)
	}
}

func TestSQLiteStore(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.db")
	st, err := OpenStore(StorageConfig{Backend: "sqlite", Path: p})
	if err != nil {
		t.Fatal(err)
	}
	checkStore(t, st)

	// persistence across reopen
	st.Close()
	st2, err := OpenStore(StorageConfig{Backend: "sqlite", Path: p})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	new, _ := st2.UserFirstSeen("U1", "Alice")
	if new {
		t.Fatal("U1 should persist")
	}
	subs, _ := st2.SubscriptionsOf("U1")
	if len(subs) != 1 || subs[0] != "front-door" {
		t.Fatalf("persisted subs: %v", subs)
	}
}

// ---------- webhook end-to-end (fake send, no network) ----------

func TestWebhookFlow(t *testing.T) {
	st, err := OpenStore(StorageConfig{Backend: "json", Path: filepath.Join(t.TempDir(), "s.json")})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cfg := &Config{
		Notifications: []Notification{{ID: "front-door", Topic: "home/sensors/front-door", Message: "Front door {state}", Description: "Front door sensor"}},
		Actions:       []Action{{ID: "lights-on", Trigger: "lights on", Topic: "home/actions/lights", Payload: "ON"}},
	}
	a := newApp(cfg, st, nil)

	var mu sync.Mutex
	sent := map[string][]string{}
	a.send = func(uid, text string) error {
		mu.Lock()
		defer mu.Unlock()
		sent[uid] = append(sent[uid], text)
		return nil
	}
	msg := func(uid, name, body string) {
		t.Helper()
		ev := `{"type":"message","contact_id":"` + uid + `","event":{"type":"message","contact_name":"` + name + `","message":"` + body + `"}}`
		postEvent(t, a, ev)
	}
	last := func(uid string) string {
		t.Helper()
		mu.Lock()
		defer mu.Unlock()
		m := sent[uid]
		if len(m) == 0 {
			t.Fatalf("no messages sent to %s", uid)
		}
		return m[len(m)-1]
	}

	// First contact from U1: registered, help reply, no broadcast.
	msg("U1", "Alice", "/help")
	if got := last("U1"); !strings.Contains(got, "/list") {
		t.Fatalf("help reply: %q", got)
	}
	// U2 first contact: broadcast to U1, U2 gets unrecognized.
	msg("U2", "Bob", "hello")
	if got := last("U1"); !strings.Contains(got, "New user joined: Bob (U2)") {
		t.Fatalf("U1 should get new-user broadcast, got %q", got)
	}
	if got := last("U2"); !strings.Contains(got, "Unrecognized") {
		t.Fatalf("U2 unrecognized: %q", got)
	}

	// Subscribe + list + users.
	msg("U1", "Alice", "/subscribe front-door")
	if got := last("U1"); !strings.Contains(got, "Subscribed to front-door") {
		t.Fatalf("subscribe reply: %q", got)
	}
	msg("U1", "Alice", "/list")
	if got := last("U1"); !strings.Contains(got, "* front-door") {
		t.Fatalf("list reply: %q", got)
	}
	msg("U1", "Alice", "/users")
	if got := last("U1"); !strings.Contains(got, "U1") || !strings.Contains(got, "front-door") || !strings.Contains(got, "(none)") {
		t.Fatalf("users reply: %q", got)
	}

	// Unknown notification id.
	msg("U1", "Alice", "/subscribe nope")
	if got := last("U1"); !strings.Contains(got, "Unknown notification") {
		t.Fatalf("unknown id reply: %q", got)
	}

	// Action trigger exact match: MQTT down -> error reply.
	msg("U2", "Bob", "lights on")
	if got := last("U2"); !strings.Contains(got, "Could not reach home automation system") {
		t.Fatalf("action reply: %q", got)
	}
	// Not a substring match.
	msg("U2", "Bob", "lights on now")
	if got := last("U2"); !strings.Contains(got, "Unrecognized") {
		t.Fatalf("substring should not trigger: %q", got)
	}
}

func postEvent(t *testing.T, a *app, body string) {
	t.Helper()
	req := httptest.NewRequest("POST", "/viber/webhook", strings.NewReader(body))
	w := httptest.NewRecorder()
	a.handler(w, req)
	if w.Code != 200 {
		t.Fatalf("webhook status: %d", w.Code)
	}
}

// ---------- timestamp parsing ----------

func TestParseTS(t *testing.T) {
	// formats returned by sqlite and postgres TIMESTAMP columns
	for _, s := range []string{
		"2026-10-09T12:45:00Z",      // RFC3339
		"2026-10-09 12:45:00",       // postgres timestamp (no fraction)
		"2026-10-09 12:45:00.123456", // postgres timestamp (microseconds)
	} {
		if got := parseTS(s); got.IsZero() {
			t.Errorf("parseTS(%q) = zero", s)
		}
	}
}
