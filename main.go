package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
	_ "modernc.org/sqlite"
)

const (
	statusAPI = "https://status.claude.com/api/v2/summary.json"
)

var postRelays = []string{
	"wss://nos.lol",
	"wss://yabu.me",
	"wss://relay.nostr.wirednet.jp",
}

var targetComponents = []string{
	"claude.ai",
	"platform.claude.com",
	"Claude API",
	"Claude Code",
}

// ── Nostr ──────────────────────────────────────────────

func decodeNsec(nsec string) string {
	_, data, err := nip19.Decode(strings.TrimSpace(nsec))
	if err != nil {
		log.Fatalf("Failed to decode nsec: %v", err)
	}
	skHex, ok := data.(string)
	if !ok {
		log.Fatalf("Unexpected type from nip19.Decode: %T", data)
	}
	return skHex
}

func publishEvent(ctx context.Context, ev nostr.Event) {
	var wg sync.WaitGroup
	for _, url := range postRelays {
		url := url
		wg.Add(1)
		go func() {
			defer wg.Done()
			relay, err := nostr.RelayConnect(ctx, url)
			if err != nil {
				log.Printf("❌ Connect failed (%s): %v", url, err)
				return
			}
			defer relay.Close()
			if err := relay.Publish(ctx, ev); err != nil {
				log.Printf("❌ Publish failed (%s): %v", url, err)
				return
			}
			log.Printf("✅ Published (%s): %s", url, ev.ID)
		}()
	}
	wg.Wait()
}

// ── Status ─────────────────────────────────────────────

type StatusSummary struct {
	Components []struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	} `json:"components"`
	Incidents []struct {
		Name      string `json:"name"`
		Status    string `json:"status"`
		Shortlink string `json:"shortlink"`
	} `json:"incidents"`
}

func fetchStatus() (*StatusSummary, error) {
	resp, err := http.Get(statusAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var s StatusSummary
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

func componentEmoji(status string) string {
	switch status {
	case "operational":
		return "✅"
	case "degraded_performance", "partial_outage":
		return "⚠️"
	default:
		return "🔴"
	}
}

func incidentEmoji(status string) string {
	switch status {
	case "resolved":
		return "✅"
	case "investigating", "identified", "monitoring":
		return "🟡"
	default:
		return "🔴"
	}
}

func buildStatusMessage() (string, error) {
	s, err := fetchStatus()
	if err != nil {
		return "", err
	}

	lines := []string{"最新のステータスだよ！", "", "【コンポーネント】"}

	for _, name := range targetComponents {
		status := "unknown"
		for _, c := range s.Components {
			if strings.HasPrefix(c.Name, name) {
				status = c.Status
				break
			}
		}
		lines = append(lines, fmt.Sprintf("%s %s - %s",
			componentEmoji(status),
			name,
			strings.ReplaceAll(status, "_", " "),
		))
	}

	if len(s.Incidents) > 0 {
		lines = append(lines, "", "【インシデント】")
		for _, inc := range s.Incidents {
			lines = append(lines, fmt.Sprintf("%s %s", incidentEmoji(inc.Status), inc.Name))
			lines = append(lines, fmt.Sprintf("　- %s", strings.ToUpper(inc.Status[:1])+inc.Status[1:]))
			if inc.Shortlink != "" {
				lines = append(lines, fmt.Sprintf("　🖇️ %s", inc.Shortlink))
			}
		}
	}

	return strings.Join(lines, "\n"), nil
}

// ── Webhook投稿フォーマット ────────────────────────────

func formatStatus(status string) string {
	label := strings.ToUpper(strings.ReplaceAll(status, "_", " "))
	switch status {
	case "resolved", "operational":
		return fmt.Sprintf("✧ %s ✧", label)
	case "investigating", "identified", "monitoring":
		return fmt.Sprintf("💡 %s 💡", label)
	default:
		return fmt.Sprintf("⚠️ %s ⚠️", label)
	}
}

func cleanComponentName(name string) string {
	idx := strings.Index(name, " (")
	if idx >= 0 {
		return name[:idx]
	}
	return name
}

type ComponentItem struct {
	Name string `json:"name"`
}

func formatComponents(components []ComponentItem) string {
	if len(components) == 0 {
		return ""
	}
	names := make([]string, 0, len(components))
	for _, c := range components {
		names = append(names, cleanComponentName(c.Name))
	}
	var chunks []string
	for i := 0; i < len(names); i += 3 {
		end := i + 3
		if end > len(names) {
			end = len(names)
		}
		chunks = append(chunks, strings.Join(names[i:end], "・"))
	}
	return strings.Join(chunks, "\n\u3000\u3000")
}

// ── SQLite 重複チェック ────────────────────────────────

type DB struct {
	db *sql.DB
	mu sync.Mutex
}

func newDB(path string) (*DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS webhook_logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		incident_key TEXT NOT NULL UNIQUE,
		received_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)
	if err != nil {
		return nil, err
	}
	return &DB{db: db}, nil
}

func (d *DB) isDuplicate(key string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var count int
	err := d.db.QueryRow(`SELECT COUNT(*) FROM webhook_logs WHERE incident_key = ?`, key).Scan(&count)
	return count > 0, err
}

func (d *DB) record(key string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec(`INSERT OR IGNORE INTO webhook_logs (incident_key) VALUES (?)`, key)
	d.db.Exec(`DELETE FROM webhook_logs WHERE id NOT IN (SELECT id FROM webhook_logs ORDER BY id DESC LIMIT 100)`)
	return err
}

// ── メンション購読 ─────────────────────────────────────

func subscribeMentions(ctx context.Context, skHex string, myPubkey string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		log.Printf("Connecting to %s ...", postRelays[0])
		relay, err := nostr.RelayConnect(ctx, postRelays[0])
		if err != nil {
			log.Printf("❌ Connect failed: %v, retrying in 10s", err)
			time.Sleep(10 * time.Second)
			continue
		}
		log.Printf("✅ Connected to %s", postRelays[0])

		since := nostr.Timestamp(time.Now().Unix())
		filters := []nostr.Filter{{
			Kinds: []int{nostr.KindTextNote},
			Tags:  nostr.TagMap{"p": []string{myPubkey}},
			Since: &since,
		}}

		sub, err := relay.Subscribe(ctx, filters)
		if err != nil {
			log.Printf("❌ Subscribe failed: %v", err)
			relay.Close()
			time.Sleep(10 * time.Second)
			continue
		}

		log.Printf("📡 Subscribed to mentions for %s", myPubkey)

		for ev := range sub.Events {
			log.Printf("📨 Mention from %s: %s", ev.PubKey, ev.Content)
			lower := strings.ToLower(ev.Content)
			go func(ev *nostr.Event) {
				var msg string
				if strings.Contains(lower, "ステータス") || strings.Contains(lower, "status") {
					var err error
					msg, err = buildStatusMessage()
					if err != nil {
						log.Printf("❌ buildStatusMessage: %v", err)
						return
					}
				} else {
					cries := []string{"うにー！", "うににー！", "うにちゃん！"}
					msg = cries[time.Now().UnixNano()%3]
				}
				reply := nostr.Event{
					Kind:      nostr.KindTextNote,
					CreatedAt: nostr.Timestamp(time.Now().Unix()),
					Tags: nostr.Tags{
						{"e", ev.ID, "", "reply"},
						{"p", ev.PubKey},
					},
					Content: msg,
				}
				if err := reply.Sign(skHex); err != nil {
					log.Printf("❌ Sign failed: %v", err)
					return
				}
				publishEvent(ctx, reply)
			}(ev)
		}

		log.Printf("⚠️ Subscription ended, reconnecting in 5s")
		relay.Close()
		time.Sleep(5 * time.Second)
	}
}

// ── Webhook HTTPサーバー ───────────────────────────────

type WebhookPayload struct {
	ComponentUpdate *struct{} `json:"component_update"`
	Incident        *struct {
		ID              string          `json:"id"`
		UpdatedAt       string          `json:"updated_at"`
		Name            string          `json:"name"`
		Status          string          `json:"status"`
		Shortlink       string          `json:"shortlink"`
		Components      []ComponentItem `json:"components"`
		IncidentUpdates []struct {
			Body string `json:"body"`
		} `json:"incident_updates"`
	} `json:"incident"`
	Page *struct {
		StatusIndicator   string `json:"status_indicator"`
		StatusDescription string `json:"status_description"`
	} `json:"page"`
}

func webhookHandler(db *DB, skHex string, ctx context.Context) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "OK")
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read error", http.StatusInternalServerError)
			return
		}

		var payload WebhookPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			http.Error(w, "parse error", http.StatusBadRequest)
			return
		}

		if payload.ComponentUpdate != nil {
			fmt.Fprint(w, "Ignored (component_update)")
			return
		}

		var lines []string

		if payload.Incident != nil {
			inc := payload.Incident
			key := fmt.Sprintf("%s:%s", inc.ID, inc.UpdatedAt)
			dup, err := db.isDuplicate(key)
			if err != nil {
				log.Printf("❌ DB error: %v", err)
			}
			if dup {
				fmt.Fprint(w, "Ignored (duplicate)")
				return
			}
			db.record(key)

			lines = append(lines, "ステータスが更新されたよ！", "")
			lines = append(lines, fmt.Sprintf("📡 %s", inc.Name), "")
			lines = append(lines, formatStatus(inc.Status))
			if len(inc.IncidentUpdates) > 0 {
				lines = append(lines, "", fmt.Sprintf("💬 %s", inc.IncidentUpdates[0].Body))
			}
			if cs := formatComponents(inc.Components); cs != "" {
				lines = append(lines, fmt.Sprintf("🖇️ %s", cs))
			}
			if inc.Shortlink != "" {
				lines = append(lines, fmt.Sprintf("🔗 %s", inc.Shortlink))
			}
		} else if payload.Page != nil {
			lines = append(lines, "ステータスが更新されたよ！", "")
			lines = append(lines, formatStatus(payload.Page.StatusIndicator))
			if payload.Page.StatusDescription != "" {
				lines = append(lines, "", fmt.Sprintf("💬 %s", payload.Page.StatusDescription))
			}
		} else {
			fmt.Fprint(w, "Ignored (unknown payload)")
			return
		}

		content := strings.Join(lines, "\n")
		ev := nostr.Event{
			Kind:      nostr.KindTextNote,
			CreatedAt: nostr.Timestamp(time.Now().Unix()),
			Tags:      nostr.Tags{},
			Content:   content,
		}
		if err := ev.Sign(skHex); err != nil {
			log.Printf("❌ Sign failed: %v", err)
			http.Error(w, "sign error", http.StatusInternalServerError)
			return
		}

		go publishEvent(ctx, ev)
		fmt.Fprint(w, "Posted!")
	}
}

// ── main ───────────────────────────────────────────────

func main() {
	nsec := os.Getenv("MY_NSEC")
	if nsec == "" {
		log.Fatal("MY_NSEC is required")
	}

	skHex := decodeNsec(nsec)
	pubkey, err := nostr.GetPublicKey(skHex)
	if err != nil {
		log.Fatalf("Failed to get pubkey: %v", err)
	}
	log.Printf("My pubkey: %s", pubkey)

	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "/data/unychan.db"
	}
	db, err := newDB(dbPath)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go subscribeMentions(ctx, skHex, pubkey)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	http.HandleFunc("/", webhookHandler(db, skHex, ctx))
	srv := &http.Server{Addr: ":" + port}

	go func() {
		log.Printf("HTTP server listening on :%s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down...")
	srv.Shutdown(context.Background())
}
