package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

const viberAPI = "https://chatapi.viber.com/pa/send_message"

// ---------- callback payloads ----------

type viberUser struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type inboundMessage struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// event is a Viber webhook callback. The user id location depends on the
// event: "user" for subscribed/conversation_started, "sender" for message,
// "user_id" for delivered/seen/failed/unsubscribed.
type event struct {
	Event   string          `json:"event"`
	User    *viberUser      `json:"user"`
	Sender  *viberUser      `json:"sender"`
	UserID  string          `json:"user_id"`
	Message *inboundMessage `json:"message"`
}

// apiResponse is the structured response of the Viber REST API.
type apiResponse struct {
	Status        int      `json:"status"`
	StatusMessage string   `json:"status_message"`
	EventTypes    []string `json:"event_types"`
}

func (r apiResponse) errHint(op string) error {
	switch r.StatusMessage {
	case "invalidAuthToken":
		return fmt.Errorf("viber: %s: invalid auth token — check viber.auth_token", op)
	case "invalidUrl":
		return fmt.Errorf("viber: %s: webhook URL invalid — it must be a public HTTPS URL with a trusted CA certificate, reachable from the internet, and answering 200 on the verification callback", op)
	case "badData", "missingData":
		return fmt.Errorf("viber: %s: request rejected (%s)", op, r.StatusMessage)
	default:
		return fmt.Errorf("viber: %s: status %d (%s)", op, r.Status, r.StatusMessage)
	}
}

// ---------- API client ----------

type viberClient struct {
	token   string
	botName string
	http    *http.Client
}

func newViberClient(token, botName string) *viberClient {
	return &viberClient{token: token, botName: botName, http: &http.Client{Timeout: 15 * time.Second}}
}

// RegisterWebhook registers (or re-registers) the webhook with Viber.
func (c *viberClient) RegisterWebhook(webhookURL string) error {
	body, _ := json.Marshal(map[string]string{
		"auth_token": c.token,
		"url":        webhookURL,
	})
	log.Printf("viber: set_webhook: registering url=%q", webhookURL)
	res, err := c.post("https://chatapi.viber.com/pa/set_webhook", body)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	data, _ := io.ReadAll(res.Body)
	log.Printf("viber: set_webhook: response HTTP %d, headers=%v, body=%s", res.StatusCode, res.Header, data)
	var out apiResponse
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("viber: set_webhook: HTTP %d: %s", res.StatusCode, data)
	}
	if err := json.Unmarshal(data, &out); err == nil && out.Status != 0 {
		return out.errHint("set_webhook")
	}
	log.Printf("viber: webhook registered at %s (event_types=%v)", webhookURL, out.EventTypes)
	return nil
}

// SendMessage sends a plain text message to a single user.
func (c *viberClient) SendMessage(userID, text string) error {
	body, _ := json.Marshal(map[string]any{
		"auth_token": c.token,
		"receiver":   userID,
		"type":       "text",
		"text":       text,
		"sender":     map[string]string{"name": c.botName},
	})
	res, err := c.post(viberAPI, body)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	data, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("viber: send to %s: HTTP %d: %s", userID, res.StatusCode, data)
	}
	var out apiResponse
	if err := json.Unmarshal(data, &out); err == nil && out.Status != 0 {
		return fmt.Errorf("viber: send to %s: %s", userID, out.errHint("send_message"))
	}
	return nil
}

// post sends a JSON request authenticated with the bot's auth token header.
func (c *viberClient) post(url string, body []byte) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Viber-Auth-Token", c.token)
	return c.http.Do(req)
}
