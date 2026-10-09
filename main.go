package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type app struct {
	cfg   *Config
	store Store
	viber *viberClient
	mqtt  mqtt.Client

	// notifByID maps notification id -> config entry
	notifByID map[string]Notification
	// triggerToAction maps lowercased trigger -> action
	triggerToAction map[string]Action
	// actionByID maps action id -> config entry (for confirmations)
	actionByID map[string]Action

	// send delivers a text message to a user. Set to viber.SendMessage by newApp;
	// tests override it.
	send func(userID, text string) error

	mu      sync.Mutex
	lastErr error
}

func newApp(cfg *Config, store Store, viber *viberClient) *app {
	a := &app{cfg: cfg, store: store, viber: viber}
	a.send = viber.SendMessage
	a.notifByID = make(map[string]Notification, len(cfg.Notifications))
	for _, n := range cfg.Notifications {
		a.notifByID[n.ID] = n
	}
	a.triggerToAction = make(map[string]Action, len(cfg.Actions))
	a.actionByID = make(map[string]Action, len(cfg.Actions))
	for _, act := range cfg.Actions {
		a.actionByID[act.ID] = act
		a.triggerToAction[strings.ToLower(strings.TrimSpace(act.Trigger))] = act
	}
	return a
}

// ---------- MQTT ----------

func (a *app) connectMQTT() error {
	addr := fmt.Sprintf("tcp://%s:%d", a.cfg.MQTT.Host, a.cfg.MQTT.Port)
	opts := mqtt.NewClientOptions().
		AddBroker(addr).
		SetClientID(fmt.Sprintf("viber2mqtt-%d", time.Now().UnixNano()%100000)).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(30 * time.Second).
		SetOnConnectHandler(func(c mqtt.Client) {
			a.mu.Lock()
			a.lastErr = nil
			a.mu.Unlock()
			log.Printf("mqtt: connected to %s", addr)
			// (re)subscribe on every connect so a clean-session reconnect
			// restores the notification subscriptions.
			for _, n := range a.cfg.Notifications {
				if t := c.Subscribe(n.Topic, 1, a.onMessage(n.ID)); !t.WaitTimeout(10*time.Second) || t.Error() != nil {
					log.Printf("mqtt: WARN subscribe %s: %v", n.Topic, t.Error())
				}
			}
			log.Printf("mqtt: subscribed to %d notification topics", len(a.cfg.Notifications))
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			a.mu.Lock()
			a.lastErr = err
			a.mu.Unlock()
			log.Printf("mqtt: connection lost: %v", err)
		})
	if a.cfg.MQTT.Username != "" {
		opts.SetUsername(a.cfg.MQTT.Username)
		opts.SetPassword(a.cfg.MQTT.Password)
	}
	client := mqtt.NewClient(opts)
	tok := client.Connect()
	if !tok.WaitTimeout(15 * time.Second) {
		a.mu.Lock()
		a.lastErr = fmt.Errorf("connect timeout to %s", addr)
		a.mu.Unlock()
		log.Printf("mqtt: connect timeout to %s; will keep retrying in background", addr)
		a.mqtt = client
		return nil
	}
	if err := tok.Error(); err != nil {
		a.mu.Lock()
		a.lastErr = err
		a.mu.Unlock()
		// Broker unavailable at startup is not fatal: the Viber bot must keep
		// responding (spec 8). AutoReconnect keeps trying in the background.
		log.Printf("mqtt: connect to %s failed: %v; will keep retrying in background", addr, err)
		a.mqtt = client
		return nil
	}
	a.mqtt = client
	return nil
}

func (a *app) onMessage(notifID string) func(mqtt.Client, mqtt.Message) {
	return func(_ mqtt.Client, msg mqtt.Message) {
		subIDs, err := a.store.SubscribersOf(notifID)
		if err != nil {
			log.Printf("mqtt: %v", err)
			return
		}
		if len(subIDs) == 0 {
			return
		}
		n, ok := a.notifByID[notifID]
		if !ok {
			return
		}
		text := renderMessage(n.Message, msg.Payload(), notifID)
		for _, uid := range subIDs {
			if err := a.send(uid, text); err != nil {
				log.Printf("viber: failed to notify %s of %s: %v", uid, notifID, err)
			}
		}
	}
}

func (a *app) publish(topic, payload string) error {
	if a.mqtt == nil || !a.mqtt.IsConnected() {
		a.mu.Lock()
		err := a.lastErr
		a.mu.Unlock()
		if err != nil {
			return fmt.Errorf("mqtt not connected: %w", err)
		}
		return fmt.Errorf("mqtt not connected")
	}
	tok := a.mqtt.Publish(topic, 1, false, payload)
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		return fmt.Errorf("mqtt publish %s: %v", topic, tok.Error())
	}
	return nil
}

// ---------- webhook ----------

func (a *app) handler(w http.ResponseWriter, r *http.Request) {
	var ev event
	if err := decodeJSON(r, &ev); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Printf("viber: callback %q from %s (path %s)", ev.Event, r.RemoteAddr, r.URL.Path)
	switch ev.Event {
	case "webhook":
		// set_webhook verification callback: nothing to do, just 200.
	case "subscribed", "conversation_started":
		u := ev.User
		if u != nil && u.ID != "" {
			a.registerUser(u.ID, u.Name)
		}
	case "unsubscribed":
		if ev.UserID != "" {
			_ = ev.UserID // user left; subscriptions stay, just no active conversation
		}
	case "message":
		if ev.Message != nil && ev.Message.Type == "text" {
			u := ev.Sender
			if u != nil && u.ID != "" {
				a.registerUser(u.ID, u.Name)
				text := strings.TrimSpace(ev.Message.Text)
				if strings.HasPrefix(text, "/") {
					a.handleCommand(u.ID, text)
				} else if act, ok := a.triggerToAction[strings.ToLower(text)]; ok {
					a.handleAction(u.ID, act)
				} else {
					_ = a.send(u.ID, "Unrecognized message. Send /help for a list of commands.")
				}
			}
		}
	default:
		// delivered/seen/failed and anything else: acknowledge.
	}
	w.WriteHeader(http.StatusOK)
}

// registerUser adds the user to the registry if new and broadcasts.
func (a *app) registerUser(userID, displayName string) {
	if isNew, err := a.store.UserFirstSeen(userID, displayName); err == nil && isNew {
		a.broadcastNewUser(userID, displayName)
	}
}

// decodeJSON wraps r.Body decoder to return a 400-friendly error.
func decodeJSON(r *http.Request, v any) error {
	if r.Body == nil {
		return fmt.Errorf("no body")
	}
	return json.NewDecoder(r.Body).Decode(v)
}

// broadcastNewUser announces a newly-registered user to all other known users.
func (a *app) broadcastNewUser(newUserID, displayName string) {
	users, err := a.store.ListUsers()
	if err != nil {
		log.Printf("users: %v", err)
		return
	}
	msg := fmt.Sprintf("New user joined: %s (%s)", displayName, newUserID)
	for _, u := range users {
		if u.ID == newUserID {
			continue
		}
		if err := a.send(u.ID, msg); err != nil {
			log.Printf("viber: failed to announce new user to %s: %v", u.ID, err)
		}
	}
}

// ---------- commands ----------

func (a *app) handleCommand(userID, text string) {
	parts := strings.SplitN(strings.TrimPrefix(text, "/"), " ", 2)
	cmd := strings.ToLower(parts[0])
	arg := ""
	if len(parts) > 1 {
		arg = strings.TrimSpace(parts[1])
	}
	switch cmd {
	case "help":
		_ = a.send(userID,
			"Commands:\n"+
				"  /list - list all notifications\n"+
				"  /subscribe <id> - subscribe to a notification\n"+
				"  /unsubscribe <id> - unsubscribe\n"+
				"  /status - list your subscriptions\n"+
				"  /users - list known users\n"+
				"  /help - this help")
	case "list":
		a.cmdList(userID)
	case "subscribe":
		a.cmdSubscribe(userID, arg)
	case "unsubscribe":
		a.cmdUnsubscribe(userID, arg)
	case "status":
		a.cmdStatus(userID)
	case "users":
		a.cmdUsers(userID)
	default:
		_ = a.send(userID, "Unknown command. Send /help for a list of commands.")
	}
}

func (a *app) cmdList(userID string) {
	mine, _ := a.store.SubscriptionsOf(userID)
	set := map[string]bool{}
	for _, m := range mine {
		set[m] = true
	}
	lines := []string{"Notifications:"}
	for _, n := range a.cfg.Notifications {
		mark := "  "
		if set[n.ID] {
			mark = "* "
		}
		line := fmt.Sprintf("%s%s - %s", mark, n.ID, n.Description)
		lines = append(lines, line)
	}
	_ = a.send(userID, strings.Join(lines, "\n"))
}

func (a *app) cmdSubscribe(userID, id string) {
	if id == "" {
		_ = a.send(userID, "Usage: /subscribe <id>. See /list.")
		return
	}
	if _, ok := a.notifByID[id]; !ok {
		_ = a.send(userID, fmt.Sprintf("Unknown notification %q. See /list.", id))
		return
	}
	if err := a.store.Subscribe(userID, id); err != nil {
		log.Printf("store: %v", err)
		_ = a.send(userID, "Could not subscribe, try again later.")
		return
	}
	_ = a.send(userID, "Subscribed to "+id)
}

func (a *app) cmdUnsubscribe(userID, id string) {
	if id == "" {
		_ = a.send(userID, "Usage: /unsubscribe <id>. See /list.")
		return
	}
	if err := a.store.Unsubscribe(userID, id); err != nil {
		_ = a.send(userID, "Not subscribed to "+id)
		return
	}
	_ = a.send(userID, "Unsubscribed from "+id)
}

func (a *app) cmdStatus(userID string) {
	ids, err := a.store.SubscriptionsOf(userID)
	if err != nil {
		_ = a.send(userID, "Could not read subscriptions.")
		return
	}
	if len(ids) == 0 {
		_ = a.send(userID, "You are not subscribed to anything. See /list.")
		return
	}
	_ = a.send(userID, "Subscribed to:\n  "+strings.Join(ids, "\n  "))
}

func (a *app) cmdUsers(userID string) {
	users, err := a.store.ListUsers()
	if err != nil {
		_ = a.send(userID, "Could not read user list.")
		return
	}
	lines := []string{"Known users:"}
	for _, u := range users {
		subIDs, _ := a.store.SubscriptionsOf(u.ID)
		name := u.DisplayName
		if name == "" {
			name = u.ID
		}
		if len(subIDs) == 0 {
			lines = append(lines, fmt.Sprintf("  %s (%s): (none)", name, u.ID))
		} else {
			lines = append(lines, fmt.Sprintf("  %s (%s): %s", name, u.ID, strings.Join(subIDs, ", ")))
		}
	}
	_ = a.send(userID, strings.Join(lines, "\n"))
}

func (a *app) handleAction(userID string, act Action) {
	if err := a.publish(act.Topic, act.Payload); err != nil {
		log.Printf("action %s: %v", act.ID, err)
		_ = a.send(userID, "Could not reach home automation system, try again later.")
		return
	}
	_ = a.send(userID, "OK: "+strings.TrimSpace(act.Trigger))
}

// ---------- server ----------

// webhookServer wraps app.handler with optional bearer-token auth.
type webhookServer struct {
	app   *app
	token string // if empty, no auth
}

func (s *webhookServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.token != "" {
		// constant-time compare to avoid timing attacks
		got := r.Header.Get("Authorization")
		want := "Bearer " + s.token
		if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}
	s.app.handler(w, r)
}

func main() {
	var flagConfig string
	flag.StringVar(&flagConfig, "config", "config.yaml", "path to the YAML config file")
	flag.Parse()

	cfg, err := LoadConfig(flagConfig)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		log.Fatalf("viber2mqtt: %v", err)
	}
}

func run(ctx context.Context, cfg *Config) error {
	store, err := OpenStore(cfg.Storage)
	if err != nil {
		return err
	}
	defer store.Close()

	viber := newViberClient(cfg.Viber.AuthToken, cfg.Viber.BotName)
	a := newApp(cfg, store, viber)

	// Start the webhook server before registering the webhook with Viber:
	// Viber immediately fires a verification callback to the URL, so the
	// listener must be up (or Viber rejects the registration).
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:      &webhookServer{app: a},
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return fmt.Errorf("webhook: %w", err)
	}
	errCh := make(chan error, 1)
	go func() {
		log.Printf("viber2mqtt: webhook listening on %s", srv.Addr)
		errCh <- srv.Serve(ln)
	}()

	if err := viber.RegisterWebhook(cfg.Viber.Webhook); err != nil {
		// Non-fatal: keep the webhook server up so the URL can be debugged
		// (e.g. proxy routing) and Viber retries the callback; the app also
		// re-registers on every restart.
		log.Printf("viber: webhook registration FAILED: %v — server staying up, will retry on restart", err)
	}

	if err := a.connectMQTT(); err != nil {
		ln.Close()
		return err
	}
	defer a.mqtt.Disconnect(250)

	// Block until the process is stopped or the server fails.
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}
