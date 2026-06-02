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

	"regexp"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
	_ "modernc.org/sqlite"
)

const (
	incidentAPI   = "https://status.claude.com/api/v2/incidents.json"
	statusFeedURL = "https://status.claude.com/history.atom"
	browserUA     = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
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

// ── Status ─────────────────────────────────────────────

type StatusSummary struct {
	Components []struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	} `json:"components"`
	Incidents []struct {
		ID        string `json:"id"`
		UpdatedAt string `json:"updated_at"`
		Name      string `json:"name"`
		Status    string `json:"status"`
		Shortlink string `json:"shortlink"`
		IncidentUpdates []struct {
			ID   string `json:"id"`
			Body string `json:"body"`
		} `json:"incident_updates"`
	} `json:"incidents"`
}

type IncidentSummary struct {
	Incidents []struct {
		ID        string `json:"id"`
		UpdatedAt string `json:"updated_at"`
		Name      string `json:"name"`
		Status    string `json:"status"`
		Shortlink string `json:"shortlink"`
		IncidentUpdates []struct {
			ID   string `json:"id"`
			Body string `json:"body"`
		} `json:"incident_updates"`
	} `json:"incidents"`
}

func fetchIncidents() (*IncidentSummary, error) {
	resp, err := http.Get(incidentAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("incidents API returned %d: %s", resp.StatusCode, string(body))
	}
	var s IncidentSummary
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
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

// インシデントのステータス文字列に絵文字を付ける
func feedStatusEmoji(status string) string {
	switch strings.ToLower(status) {
	case "resolved", "completed":
		return "✅"
	case "investigating", "identified", "monitoring", "update", "in progress", "scheduled", "verifying":
		return "🟡"
	default:
		return "🔴"
	}
}

type feedIncident struct {
	Title   string
	Status  string // 最新の更新状態 (Resolved/Investigating/...)
	Updated string // ISO8601
	Link    string
}

// status.claude.com の Atom フィードを取得して最新インシデントを返す。
// API(summary.json)やトップページHTMLはデータセンターIPからbot判定(405)で弾かれるが、
// Atomフィードは取得できるため、こちらを使う。
func fetchStatusFeed() ([]feedIncident, error) {
	req, err := http.NewRequest("GET", statusFeedURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Accept", "application/atom+xml,application/xml;q=0.9,*/*;q=0.8")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return nil, fmt.Errorf("status feed returned %d: %s", resp.StatusCode, string(body))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	xmlText := string(body)
	var incidents []feedIncident
	for _, e := range entryRe.FindAllStringSubmatch(xmlText, -1) {
		entry := e[1]
		title := htmlUnescape(strings.TrimSpace(submatch(entryTitleRe, entry)))
		updated := strings.TrimSpace(submatch(entryUpdatedRe, entry))
		link := ""
		if lm := entryLinkRe.FindStringSubmatch(entry); lm != nil {
			link = lm[1]
		}
		// content内の最初の <strong>状態</strong> が最新の更新状態
		status := "unknown"
		if cm := entryContentRe.FindStringSubmatch(entry); cm != nil {
			if sm := strongStateRe.FindStringSubmatch(cm[1]); sm != nil {
				status = strings.TrimSpace(sm[1])
			}
		}
		if title == "" {
			continue
		}
		incidents = append(incidents, feedIncident{Title: title, Status: status, Updated: updated, Link: link})
	}
	if len(incidents) == 0 {
		return nil, fmt.Errorf("no entries parsed from status feed")
	}
	return incidents, nil
}

func submatch(re *regexp.Regexp, s string) string {
	if m := re.FindStringSubmatch(s); m != nil {
		return m[1]
	}
	return ""
}

var (
	entryRe         = regexp.MustCompile(`(?s)<entry>(.*?)</entry>`)
	entryTitleRe    = regexp.MustCompile(`(?s)<title>(.*?)</title>`)
	entryUpdatedRe  = regexp.MustCompile(`(?s)<updated>(.*?)</updated>`)
	entryLinkRe     = regexp.MustCompile(`<link[^>]*href="([^"]+)"`)
	entryContentRe  = regexp.MustCompile(`(?s)<content[^>]*>(.*?)</content>`)
	strongStateRe   = regexp.MustCompile(`(?s)(?:&lt;|<)strong(?:&gt;|>)\s*(.*?)\s*(?:&lt;|<)/strong`)
)

func htmlUnescape(s string) string {
	r := strings.NewReplacer(
		"&amp;", "&",
		"&#39;", "'",
		"&quot;", `"`,
		"&lt;", "<",
		"&gt;", ">",
	)
	return r.Replace(s)
}

// 状態が解決済みかどうか
func isResolvedStatus(s string) bool {
	switch strings.ToLower(s) {
	case "resolved", "completed":
		return true
	}
	return false
}

func buildStatusMessage() (string, error) {
	incidents, err := fetchStatusFeed()
	if err != nil {
		return "", err
	}

	// 進行中（未解決）のインシデントを抽出
	var ongoing []feedIncident
	for _, inc := range incidents {
		if !isResolvedStatus(inc.Status) {
			ongoing = append(ongoing, inc)
		}
	}

	lines := []string{"最新のステータスだよ！", ""}

	if len(ongoing) == 0 {
		lines = append(lines, "✅ 進行中のインシデントはないみたい！")
		// 直近の解決済みインシデントを1件だけ参考表示
		latest := incidents[0]
		lines = append(lines, "", "【直近のインシデント】")
		lines = append(lines, fmt.Sprintf("%s %s", feedStatusEmoji(latest.Status), latest.Title))
		lines = append(lines, fmt.Sprintf("　- %s", latest.Status))
	} else {
		lines = append(lines, "【進行中のインシデント】")
		for _, inc := range ongoing {
			lines = append(lines, fmt.Sprintf("%s %s", feedStatusEmoji(inc.Status), inc.Title))
			lines = append(lines, fmt.Sprintf("　- %s", inc.Status))
			if inc.Link != "" {
				lines = append(lines, fmt.Sprintf("　🖇️ %s", inc.Link))
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

// ── インシデントポーリング ────────────────────────────

func pollIncidents(ctx context.Context, db *DB, skHex string) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkIncidents(ctx, db, skHex)
		}
	}
}

func checkIncidents(ctx context.Context, db *DB, skHex string) {
	s, err := fetchIncidents()
	if err != nil {
		log.Printf("❌ pollIncidents fetchIncidents: %v", err)
		return
	}

	cutoff := time.Now().UTC().Add(-24 * time.Hour)

	for _, inc := range s.Incidents {
		if len(inc.IncidentUpdates) == 0 {
			continue
		}
		latestUpdate := inc.IncidentUpdates[0]
		// 24時間以上前のインシデントはスキップ
		updatedAt, err := time.Parse(time.RFC3339, inc.UpdatedAt)
		if err != nil || updatedAt.Before(cutoff) {
			continue
		}
		key := fmt.Sprintf("poll:%s:%s", inc.ID, latestUpdate.ID)
		dup, err := db.isPolledDuplicate(key)
		if err != nil {
			log.Printf("❌ pollIncidents DB: %v", err)
			continue
		}
		if dup {
			continue
		}

		log.Printf("📋 New incident via polling: %s (%s)", inc.Name, inc.Status)

		lines := []string{"ステータスが更新されたよ！", ""}
		lines = append(lines, fmt.Sprintf("📡 %s", inc.Name), "")
		lines = append(lines, formatStatus(inc.Status))
		if latestUpdate.Body != "" {
			lines = append(lines, "", fmt.Sprintf("💬 %s", latestUpdate.Body))
		}
		if inc.Shortlink != "" {
			lines = append(lines, fmt.Sprintf("🔗 %s", inc.Shortlink))
		}

		content := strings.Join(lines, "\n")
		ev := nostr.Event{
			Kind:      nostr.KindTextNote,
			CreatedAt: nostr.Timestamp(time.Now().Unix()),
			Tags:      nostr.Tags{},
			Content:   content,
		}
		if err := ev.Sign(skHex); err != nil {
			log.Printf("❌ pollIncidents Sign: %v", err)
			continue
		}
		if publishEvent(ctx, ev) {
			db.recordPolled(key)
		} else {
			log.Printf("⚠️ All relays failed for %s, will retry next poll", inc.ID)
		}
	}
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
