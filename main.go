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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
	_ "modernc.org/sqlite"
)

var postRelays = []string{
	"wss://nos.lol",
	"wss://relay.nostr.wirednet.jp",
	"wss://relay-jp.nostr.wirednet.jp",
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

func publishEvent(ctx context.Context, ev nostr.Event) bool {
	var wg sync.WaitGroup
	var successCount int32
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
			atomic.AddInt32(&successCount, 1)
		}()
	}
	wg.Wait()
	return atomic.LoadInt32(&successCount) > 0
}

// ── Status (Webhook由来の最新状態を保持) ────────────────

// status.claude.com への直接アクセス（API/HTML/Atom）は Fly.io のデータセンターIPから
// bot判定(405)で弾かれる。そのため、Webhookで受け取った最新のインシデント本文を
// メモリに保持し、メンション応答でもそれを返す。
type statusStore struct {
	mu        sync.RWMutex
	message   string    // 最後にWebhookで構築した本文
	updatedAt time.Time // 最後に更新した時刻
}

var latestStatus = &statusStore{}

func (s *statusStore) set(msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.message = msg
	s.updatedAt = time.Now()
}

func (s *statusStore) get() (string, time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.message, s.updatedAt
}

func buildStatusMessage() (string, error) {
	msg, updatedAt := latestStatus.get()
	if msg == "" {
		return "まだステータスの更新を受け取ってないよ！\n何か動きがあったらお知らせするね！", nil
	}
	footer := fmt.Sprintf("\n\n（%s前に受け取った情報だよ）", humanizeDuration(time.Since(updatedAt)))
	return msg + footer, nil
}

func humanizeDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return "さっき"
	case d < time.Hour:
		return fmt.Sprintf("%d分", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%d時間", int(d.Hours()))
	default:
		return fmt.Sprintf("%d日", int(d.Hours()/24))
	}
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
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS polled_incidents (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		incident_key TEXT UNIQUE NOT NULL,
		posted_at DATETIME DEFAULT CURRENT_TIMESTAMP
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
	if _, delErr := d.db.Exec(`DELETE FROM webhook_logs WHERE id NOT IN (SELECT id FROM webhook_logs ORDER BY id DESC LIMIT 100)`); delErr != nil {
		log.Printf("⚠️ webhook_logs cleanup failed: %v", delErr)
	}
	return err
}

func (d *DB) isPolledDuplicate(key string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var count int
	err := d.db.QueryRow(`SELECT COUNT(*) FROM polled_incidents WHERE incident_key = ?`, key).Scan(&count)
	return count > 0, err
}

func (d *DB) recordPolled(key string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec(`INSERT OR IGNORE INTO polled_incidents (incident_key) VALUES (?)`, key)
	if _, delErr := d.db.Exec(`DELETE FROM polled_incidents WHERE id NOT IN (SELECT id FROM polled_incidents ORDER BY id DESC LIMIT 200)`); delErr != nil {
		log.Printf("⚠️ polled_incidents cleanup failed: %v", delErr)
	}
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

		log.Printf("📡 Subscribing to mentions via SimplePool...")
		pool := nostr.NewSimplePool(ctx)
		since := nostr.Timestamp(time.Now().Unix())
		filters := []nostr.Filter{{
			Kinds: []int{nostr.KindTextNote},
			Tags:  nostr.TagMap{"p": []string{myPubkey}},
			Since: &since,
		}}

		subRelays := []string{"wss://nos.lol", "wss://relay.nostr.wirednet.jp", "wss://yabu.me"}
		events := pool.SubMany(ctx, subRelays, filters)
		log.Printf("📡 Subscribed to mentions for %s", myPubkey)

		for ie := range events {
			ev := ie.Event
			log.Printf("📨 Mention from %s: %s", ev.PubKey, ev.Content)
			lower := strings.ToLower(ev.Content)
			go func(ev *nostr.Event, lower string) {
				var msg string
				if strings.Contains(lower, "ステータス") || strings.Contains(lower, "status") {
					var err error
					msg, err = buildStatusMessage()
					if err != nil {
						log.Printf("❌ buildStatusMessage: %v", err)
						return
					}
				} else {
					cries := []string{"うにー！", "うににー！", "うにちゃんだよ！", "うにゅ！", "うにゅう！", "うにぃ！", "うにうに！", "よんだ？", "はーい！", "「ステータス」っていってみて！", "Claudeを崇めよ"}
					msg = cries[time.Now().UnixNano()%int64(len(cries))]
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
			}(ev, lower)
		}

		log.Printf("⚠️ SubMany ended, reconnecting in 5s")
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

func webhookHandler(ctx context.Context, db *DB, skHex string) http.HandlerFunc {
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

		if payload.ComponentUpdate != nil && payload.Incident == nil {
			fmt.Fprint(w, "Ignored (component_update only)")
			return
		}

		var lines []string
		var dedupKey string

		if payload.Incident != nil {
			inc := payload.Incident
			dedupKey = fmt.Sprintf("%s:%s", inc.ID, inc.UpdatedAt)
			dup, err := db.isDuplicate(dedupKey)
			if err != nil {
				log.Printf("❌ DB error: %v", err)
			}
			if dup {
				fmt.Fprint(w, "Ignored (duplicate)")
				return
			}

			log.Printf("📋 Webhook incident: %s (%s)", inc.Name, inc.Status)

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

		if publishEvent(ctx, ev) {
			if dedupKey != "" {
				db.record(dedupKey)
			}
			// メンション応答用に最新状態を保持する
			latestStatus.set(content)
			fmt.Fprint(w, "Posted!")
		} else {
			log.Printf("⚠️ All relays failed for webhook")
			http.Error(w, "publish failed", http.StatusInternalServerError)
		}
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

	// インシデントポーリング（現在CAPTCHA保護により無効化、Webhook併用中）
	// go pollIncidents(ctx, db, skHex)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	http.HandleFunc("/", webhookHandler(ctx, db, skHex))
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
