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

type event struct {
	Type           string `json:"type"`
	Timestamp      int64  `json:"timestamp"`
	SubscriberID   string `json:"subscriber_id"`
	RegistrationID string `json:"registration_id"`
	ContactID      string `json:"contact_id"`
	Event          *eventBody
}

type eventBody struct {
	Type          string         `json:"type"`
	ContactName   string         `json:"contact_name"`
	Conversation  string         `json:"conversation"`
	Message       string         `json:"message"`
	MessageToken  string         `json:"message_token"`
	ForwardedFrom *forwardedFrom `json:"forwarded_from"`
}

type forwardedFrom struct {
	Name string `json:"name"`
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
		"auth_token":  c.token,
		"url":         webhookURL,
		"name":        c.botName,
		"event_types": "subscription,conversation_start",
	})
	res, err := c.post("https://chatapi.viber.com/pa/set_webhook", body)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(res.Body)
		return fmt.Errorf("viber: set_webhook: HTTP %d: %s", res.StatusCode, data)
	}
	log.Printf("viber: webhook registered at %s", webhookURL)
	return nil
}

// SendMessage sends a plain text message to a single user.
func (c *viberClient) SendMessage(userID, text string) error {
	body, _ := json.Marshal(map[string]any{
		"receiver":           userID,
		"message":            text,
		"sender":             map[string]string{"name": c.botName},
		"validation_message": "viber2mqtt",
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
	var out struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(data, &out); err == nil && out.Status != "scheduled" && out.Status != "" && out.Status != "ok" {
		return fmt.Errorf("viber: send to %s: status %q", userID, out.Status)
	}
	return nil
}

func (c *viberClient) post(url string, body []byte) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.http.Do(req)
}
