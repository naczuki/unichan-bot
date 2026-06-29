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

// ── インシデント整形（webhook/poll共通）─────────────────

// Incident は incidents.json / webhook の両方から組み立てる共通の中間表現。
type Incident struct {
	ID         string
	UpdatedAt  string
	Name       string
	Status     string
	Shortlink  string
	Components []ComponentItem
	LatestBody string
}

func (inc Incident) dedupKey() string {
	return fmt.Sprintf("%s:%s", inc.ID, inc.UpdatedAt)
}

// formatIncidentLines は Nostr 投稿本文を組み立てる。webhookとpollで共通利用。
func formatIncidentLines(inc Incident) string {
	var lines []string
	lines = append(lines, "ステータスが更新されたよ！", "")
	lines = append(lines, fmt.Sprintf("📡 %s", inc.Name), "")
	lines = append(lines, formatStatus(inc.Status))
	if inc.LatestBody != "" {
		lines = append(lines, "", fmt.Sprintf("💬 %s", inc.LatestBody))
	}
	if cs := formatComponents(inc.Components); cs != "" {
		lines = append(lines, fmt.Sprintf("🖇️ %s", cs))
	}
	if inc.Shortlink != "" {
		lines = append(lines, fmt.Sprintf("🔗 %s", inc.Shortlink))
	}
	return strings.Join(lines, "\n")
}

// postIncident は重複チェック→Nostr投稿→記録→latestStatus更新を一括で行う。
// 投稿したら true、重複や失敗で投稿しなかったら false を返す。
func postIncident(ctx context.Context, db *DB, skHex string, inc Incident, source string) bool {
	key := inc.dedupKey()
	dup, err := db.seen(key)
	if err != nil {
		log.Printf("❌ DB error (%s): %v", source, err)
	}
	if dup {
		return false
	}
	content := formatIncidentLines(inc)
	ev := nostr.Event{
		Kind:      nostr.KindTextNote,
		CreatedAt: nostr.Timestamp(time.Now().Unix()),
		Tags:      nostr.Tags{},
		Content:   content,
	}
	if err := ev.Sign(skHex); err != nil {
		log.Printf("❌ Sign failed (%s): %v", source, err)
		return false
	}
	if publishEvent(ctx, ev) {
		db.markSeen(key)
		latestStatus.set(content)
		log.Printf("✅ Posted incident via %s: %s (%s)", source, inc.Name, inc.Status)
		return true
	}
	log.Printf("⚠️ All relays failed (%s)", source)
	return false
}

// ── インシデントポーリング ─────────────────────────────

const browserUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

type incidentsResponse struct {
	Incidents []struct {
		ID              string          `json:"id"`
		Name            string          `json:"name"`
		Status          string          `json:"status"`
		UpdatedAt       string          `json:"updated_at"`
		Shortlink       string          `json:"shortlink"`
		Components      []ComponentItem `json:"components"`
		IncidentUpdates []struct {
			Body string `json:"body"`
		} `json:"incident_updates"`
	} `json:"incidents"`
}

func fetchIncidents(ctx context.Context, client *http.Client) (*incidentsResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://status.claude.com/api/v2/incidents.json", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var parsed incidentsResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, err
	}
	return &parsed, nil
}

func pollIncidents(ctx context.Context, db *DB, skHex string) {
	const interval = 90 * time.Second
	client := &http.Client{Timeout: 15 * time.Second}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// 起動直後の既存インシデントは「既読」として記録だけして投稿しない
	// （再起動のたびに過去インシデントを蒸し返さないため）。
	primeSeen(ctx, db, client)

	log.Printf("🔁 Incident polling started (interval=%s)", interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			data, err := fetchIncidents(ctx, client)
			if err != nil {
				log.Printf("⚠️ poll fetch failed: %v", err)
				continue
			}
			for _, raw := range data.Incidents {
				inc := Incident{
					ID:         raw.ID,
					UpdatedAt:  raw.UpdatedAt,
					Name:       raw.Name,
					Status:     raw.Status,
					Shortlink:  raw.Shortlink,
					Components: raw.Components,
				}
				if len(raw.IncidentUpdates) > 0 {
					inc.LatestBody = raw.IncidentUpdates[0].Body
				}
				postIncident(ctx, db, skHex, inc, "poll")
			}
		}
	}
}

// primeSeen は起動時に現在のインシデント群を既読として記録するだけ（投稿しない）。
func primeSeen(ctx context.Context, db *DB, client *http.Client) {
	data, err := fetchIncidents(ctx, client)
	if err != nil {
		log.Printf("⚠️ primeSeen fetch failed (起動時の既読登録をスキップ): %v", err)
		return
	}
	n := 0
	for _, raw := range data.Incidents {
		key := fmt.Sprintf("%s:%s", raw.ID, raw.UpdatedAt)
		if seen, _ := db.seen(key); !seen {
			db.markSeen(key)
			n++
		}
	}
	log.Printf("🔖 primeSeen: %d 件の既存インシデントを既読登録", n)
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

// seen / markSeen は webhook とポーリングで共通のキー空間（webhook_logs）を使い、
// 同じインシデント更新が両経路から二重投稿されるのを防ぐ。
func (d *DB) seen(key string) (bool, error) {
	return d.isDuplicate(key)
}

func (d *DB) markSeen(key string) error {
	return d.record(key)
}

func (d *DB) record(key string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec(`INSERT OR IGNORE INTO webhook_logs (incident_key) VALUES (?)`, key)
	if _, delErr := d.db.Exec(`DELETE FROM webhook_logs WHERE id NOT IN (SELECT id FROM webhook_logs ORDER BY id DESC LIMIT 300)`); delErr != nil {
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

		if payload.ComponentUpdate != nil {
			fmt.Fprint(w, "Ignored (component_update)")
			return
		}

		if payload.Incident != nil {
			raw := payload.Incident
			inc := Incident{
				ID:         raw.ID,
				UpdatedAt:  raw.UpdatedAt,
				Name:       raw.Name,
				Status:     raw.Status,
				Shortlink:  raw.Shortlink,
				Components: raw.Components,
			}
			if len(raw.IncidentUpdates) > 0 {
				inc.LatestBody = raw.IncidentUpdates[0].Body
			}
			if dup, _ := db.seen(inc.dedupKey()); dup {
				fmt.Fprint(w, "Ignored (duplicate)")
				return
			}
			log.Printf("📋 Webhook incident: %s (%s)", inc.Name, inc.Status)
			if postIncident(ctx, db, skHex, inc, "webhook") {
				fmt.Fprint(w, "Posted!")
			} else {
				http.Error(w, "publish failed", http.StatusInternalServerError)
			}
			return
		}

		if payload.Page != nil {
			var lines []string
			lines = append(lines, "ステータスが更新されたよ！", "")
			lines = append(lines, formatStatus(payload.Page.StatusIndicator))
			if payload.Page.StatusDescription != "" {
				lines = append(lines, "", fmt.Sprintf("💬 %s", payload.Page.StatusDescription))
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
				latestStatus.set(content)
				fmt.Fprint(w, "Posted!")
			} else {
				log.Printf("⚠️ All relays failed for webhook")
				http.Error(w, "publish failed", http.StatusInternalServerError)
			}
			return
		}

		fmt.Fprint(w, "Ignored (unknown payload)")
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

	// インシデントポーリング（webhookと併用。webhookの遅延をポーリングで補う）
	go pollIncidents(ctx, db, skHex)

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
