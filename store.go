package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

type User struct {
	ID           string
	DisplayName  string
	FirstContact time.Time
}

// Store persists the user registry and subscriptions.
type Store interface {
	// UserFirstSeen registers the user if unknown and reports whether they are new.
	UserFirstSeen(userID, displayName string) (bool, error)
	ListUsers() ([]User, error)
	Subscribe(userID, notifID string) error
	Unsubscribe(userID, notifID string) error
	SubscriptionsOf(userID string) ([]string, error)
	SubscribersOf(notifID string) ([]string, error)
	Close() error
}

func OpenStore(cfg StorageConfig) (Store, error) {
	switch cfg.Backend {
	case "json":
		return openJSONStore(cfg.Path)
	case "sqlite":
		if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o755); err != nil {
			return nil, fmt.Errorf("storage: %w", err)
		}
		return openSQLStore("sqlite3", "file:"+cfg.Path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	case "postgres":
		return openSQLStore("postgres", postgresDSN(cfg.DSN))
	default:
		return nil, fmt.Errorf("unknown storage backend %q", cfg.Backend)
	}
}

// postgresDSN defaults to sslmode=disable for local brokers; set it explicitly in the DSN to change.
func postgresDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	q := u.Query()
	if q.Get("sslmode") == "" {
		q.Set("sslmode", "disable")
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// ---------- JSON backend ----------

type jsonStore struct {
	path string
	mu   sync.Mutex
	data jsonData
}

type jsonUser struct {
	DisplayName  string `json:"display_name"`
	FirstContact int64  `json:"first_contact"`
}

type jsonSub struct {
	UserID  string `json:"user_id"`
	NotifID string `json:"notification_id"`
}

type jsonData struct {
	Users map[string]jsonUser `json:"users"`
	Subs  []jsonSub           `json:"subscriptions"`
}

func openJSONStore(path string) (*jsonStore, error) {
	s := &jsonStore{path: path}
	if data, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(data, &s.data); err != nil {
			return nil, fmt.Errorf("storage: %s: %w", path, err)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("storage: %s: %w", path, err)
	}
	if s.data.Users == nil {
		s.data.Users = map[string]jsonUser{}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}
	return s, nil
}

func (s *jsonStore) save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked()
}

func (s *jsonStore) saveLocked() error {
	data, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *jsonStore) UserFirstSeen(userID, displayName string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data.Users[userID]; ok {
		return false, nil
	}
	s.data.Users[userID] = jsonUser{DisplayName: displayName, FirstContact: time.Now().Unix()}
	return true, s.saveLocked()
}

func (s *jsonStore) ListUsers() ([]User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	users := make([]User, 0, len(s.data.Users))
	for id, u := range s.data.Users {
		users = append(users, User{ID: id, DisplayName: u.DisplayName, FirstContact: time.Unix(u.FirstContact, 0)})
	}
	sort.Slice(users, func(i, j int) bool {
		if users[i].FirstContact.Equal(users[j].FirstContact) {
			return users[i].ID < users[j].ID
		}
		return users[i].FirstContact.Before(users[j].FirstContact)
	})
	return users, nil
}

func (s *jsonStore) Subscribe(userID, notifID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sub := range s.data.Subs {
		if sub.UserID == userID && sub.NotifID == notifID {
			return nil
		}
	}
	s.data.Subs = append(s.data.Subs, jsonSub{UserID: userID, NotifID: notifID})
	return s.saveLocked()
}

func (s *jsonStore) Unsubscribe(userID, notifID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, sub := range s.data.Subs {
		if sub.UserID == userID && sub.NotifID == notifID {
			s.data.Subs = append(s.data.Subs[:i], s.data.Subs[i+1:]...)
			return s.saveLocked()
		}
	}
	return fmt.Errorf("not subscribed to %q", notifID)
}

func (s *jsonStore) SubscriptionsOf(userID string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for _, sub := range s.data.Subs {
		if sub.UserID == userID {
			ids = append(ids, sub.NotifID)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

func (s *jsonStore) SubscribersOf(notifID string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := map[string]bool{}
	var ids []string
	for _, sub := range s.data.Subs {
		if sub.NotifID == notifID && !seen[sub.UserID] {
			seen[sub.UserID] = true
			ids = append(ids, sub.UserID)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

func (s *jsonStore) Close() error { return nil }

// ---------- SQL backend (sqlite / postgres) ----------

type sqlStore struct {
	db *sql.DB
	// ph returns the bind placeholder for argument n (1-based): "?" for sqlite, "$n" for postgres.
	ph func(n int) string
	// insertUser is the dialect-specific INSERT returning via RowsAffected.
	insertUser string
}

func openSQLStore(driver, dsn string) (*sqlStore, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("storage: %w", err)
	}
	s := &sqlStore{db: db, ph: func(n int) string { return "?" }}
	if driver == "postgres" {
		s.ph = func(n int) string { return fmt.Sprintf("$%d", n) }
		s.insertUser = "INSERT INTO users (id, display_name, first_contact) VALUES (" + s.ph(1) + ", " + s.ph(2) + ", NOW()) ON CONFLICT (id) DO NOTHING"
	} else {
		s.insertUser = "INSERT OR IGNORE INTO users (id, display_name, first_contact) VALUES (" + s.ph(1) + ", " + s.ph(2) + ", CURRENT_TIMESTAMP)"
	}
	schema := "CREATE TABLE IF NOT EXISTS users (\n" +
		"  id TEXT PRIMARY KEY,\n" +
		"  display_name TEXT NOT NULL DEFAULT '',\n" +
		"  first_contact DATETIME NOT NULL\n" +
		");\n" +
		"CREATE TABLE IF NOT EXISTS subscriptions (\n" +
		"  user_id TEXT NOT NULL,\n" +
		"  notification_id TEXT NOT NULL,\n" +
		"  PRIMARY KEY (user_id, notification_id)\n" +
		");"
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("storage: %w", err)
	}
	return s, nil
}

var tsLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05",
}

func parseTS(v string) time.Time {
	for _, layout := range tsLayouts {
		if t, err := time.Parse(layout, v); err == nil {
			return t
		}
	}
	return time.Time{}
}

func (s *sqlStore) UserFirstSeen(userID, displayName string) (bool, error) {
	res, err := s.db.Exec(s.insertUser, userID, displayName)
	if err != nil {
		return false, fmt.Errorf("storage: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *sqlStore) ListUsers() ([]User, error) {
	rows, err := s.db.Query("SELECT id, display_name, first_contact FROM users ORDER BY first_contact, id")
	if err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}
	defer rows.Close()
	var users []User
	for rows.Next() {
		var u User
		var ts string
		if err := rows.Scan(&u.ID, &u.DisplayName, &ts); err != nil {
			return nil, fmt.Errorf("storage: %w", err)
		}
		u.FirstContact = parseTS(ts)
		users = append(users, u)
	}
	return users, rows.Err()
}

func (s *sqlStore) Subscribe(userID, notifID string) error {
	var stmt string
	if s.ph(1) == "$1" {
		stmt = "INSERT INTO subscriptions (user_id, notification_id) VALUES (" + s.ph(1) + ", " + s.ph(2) + ") ON CONFLICT DO NOTHING"
	} else {
		stmt = "INSERT OR IGNORE INTO subscriptions (user_id, notification_id) VALUES (" + s.ph(1) + ", " + s.ph(2) + ")"
	}
	if _, err := s.db.Exec(stmt, userID, notifID); err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	return nil
}

func (s *sqlStore) Unsubscribe(userID, notifID string) error {
	res, err := s.db.Exec("DELETE FROM subscriptions WHERE user_id = "+s.ph(1)+" AND notification_id = "+s.ph(2), userID, notifID)
	if err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("not subscribed to %q", notifID)
	}
	return nil
}

func (s *sqlStore) SubscriptionsOf(userID string) ([]string, error) {
	rows, err := s.db.Query("SELECT notification_id FROM subscriptions WHERE user_id = "+s.ph(1)+" ORDER BY notification_id", userID)
	if err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("storage: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *sqlStore) SubscribersOf(notifID string) ([]string, error) {
	rows, err := s.db.Query("SELECT user_id FROM subscriptions WHERE notification_id = "+s.ph(1)+" ORDER BY user_id", notifID)
	if err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("storage: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *sqlStore) Close() error { return s.db.Close() }
