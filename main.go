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

// postRelays は投稿・返信・通知の書き込み先。Fly の外向きIPは US 扱いになる
// ことがあり、書き込みで US を拒否するリレー（yabu.me の "Country US not allowed"）
// には publish できない。そのため書き込みは US ジオブロックのないリレーに限る。
// （yabu.me は読み取りは通るので subRelays には残す。下記 subscribeMentions 参照）
var postRelays = []string{
	"wss://nos.lol",
	"wss://relay.nostr.wirednet.jp",
	"wss://relay-jp.nostr.wirednet.jp",
	"wss://relay.damus.io",
	"wss://r.kojira.io",
}

// subRelays はメンション購読（読み取り）先。読み取りは US でも通るので、JPユーザーの
// 投稿先になりやすいリレー（yabu.me / r.kojira.io 等）を広めに購読してメンションの
// 取りこぼしを防ぐ。書き込み不可の yabu.me もここには含める。
var subRelays = []string{
	"wss://nos.lol",
	"wss://relay.nostr.wirednet.jp",
	"wss://relay-jp.nostr.wirednet.jp",
	"wss://relay.damus.io",
	"wss://yabu.me",
	"wss://r.kojira.io",
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

// ── Status (進行中インシデントのスナップショットを保持) ──

// incidentSnapshot は poll が取得した最新のインシデント一覧をメモリに保持する。
// メンション（「ステータス」等）への応答は、毎回 status.claude.com を叩かず
// このスナップショットから「現在進行中の障害」を組み立てる。
type incidentSnapshot struct {
	mu        sync.RWMutex
	incidents []incidentPayload
	updatedAt time.Time
	primed    bool // 一度でも取得に成功したか
}

var incidentList = &incidentSnapshot{}

func (s *incidentSnapshot) set(list []incidentPayload) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.incidents = list
	s.updatedAt = time.Now()
	s.primed = true
}

func (s *incidentSnapshot) get() ([]incidentPayload, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.incidents, s.primed
}

// ongoingStatuses は「対応中」とみなすインシデントステータス。
var ongoingStatuses = map[string]bool{
	"investigating": true,
	"identified":    true,
	"monitoring":    true,
}

// buildStatusMessage は現在進行中の障害を時刻付きの一覧で返す。
// 進行中の障害がなければ「いま対応中の障害はないよ」を返す。
func buildStatusMessage() string {
	incs, primed := incidentList.get()
	if !primed {
		return "まだステータスを確認できてないよ！\nちょっと待ってね！"
	}
	var lines []string
	for _, inc := range incs {
		if !ongoingStatuses[inc.Status] {
			continue
		}
		lines = append(lines, fmt.Sprintf("📡 %s", inc.Name))
		lines = append(lines, fmt.Sprintf("　🕐 %s（%s）", incidentTimeJST(inc), inc.Status))
	}
	if len(lines) == 0 {
		return "いま対応中の障害はないよ"
	}
	return "いま対応中の障害だよ！\n\n" + strings.Join(lines, "\n")
}

// incidentTimeJST はインシデントの最終更新時刻を JST の "M/D HH:MM JST" で返す。
// updated_at → created_at → started_at の順に利用できるものを使う。
func incidentTimeJST(inc incidentPayload) string {
	for _, iso := range []string{inc.UpdatedAt, inc.CreatedAt, inc.StartedAt} {
		if iso == "" {
			continue
		}
		if t, err := time.Parse(time.RFC3339, iso); err == nil {
			return t.In(time.FixedZone("JST", 9*3600)).Format("1/2 15:04") + " JST"
		}
	}
	return "時刻不明"
}

// notifyOwner はオーナー宛に kind:1 ノート（p タグ付き）を投稿して通知する。
// ステータス取得エラー／復旧の連絡に使う。ownerPubkey が空なら何もしない。
func notifyOwner(ctx context.Context, skHex, ownerPubkey, msg string) {
	if ownerPubkey == "" {
		return
	}
	ev := nostr.Event{
		Kind:      nostr.KindTextNote,
		CreatedAt: nostr.Timestamp(time.Now().Unix()),
		Tags:      nostr.Tags{{"p", ownerPubkey}},
		Content:   msg,
	}
	if err := ev.Sign(skHex); err != nil {
		log.Printf("❌ notifyOwner sign failed: %v", err)
		return
	}
	publishEvent(ctx, ev)
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

// postResult は postIncident の結果を表す（webhook の応答コード分岐に使う）。
type postResult int

const (
	postFailed    postResult = iota // 投稿失敗 or DBエラー（リトライ可）
	postPosted                      // 新規に投稿した
	postDuplicate                   // 既に処理済みで投稿しなかった
)

// postIncident は重複チェック→Nostr投稿を一括で行う。
//
// webhook と poll は併走するため、投稿の「前」に db.claim でキーを原子的に確保し、
// 確保できた経路だけが投稿する。これにより同じインシデント更新が両経路から
// 二重投稿されるレースを防ぐ。投稿に失敗した場合は unclaim でキーを戻し、
// 次回（webhook再送やポーリング）でリトライできるようにする。
func postIncident(ctx context.Context, db *DB, skHex string, inc Incident, source string) postResult {
	key := inc.dedupKey()
	claimed, err := db.claim(key)
	if err != nil {
		// DBエラー時は安全側に倒して投稿しない（誤った二重投稿を避ける）。
		log.Printf("❌ DB error (%s): %v", source, err)
		return postFailed
	}
	if !claimed {
		return postDuplicate // 既に他経路が処理済み（重複）
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
		db.unclaim(key)
		return postFailed
	}
	if publishEvent(ctx, ev) {
		log.Printf("✅ Posted incident via %s: %s (%s)", source, inc.Name, inc.Status)
		return postPosted
	}
	log.Printf("⚠️ All relays failed (%s)", source)
	db.unclaim(key)
	return postFailed
}

// ── インシデントポーリング ─────────────────────────────

const browserUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

// incidentPayload は incidents.json と webhook の incident オブジェクトに共通の
// JSON 形。両経路でこの型をパースし、toIncident で投稿用の Incident に変換する。
type incidentPayload struct {
	ID              string          `json:"id"`
	Name            string          `json:"name"`
	Status          string          `json:"status"`
	CreatedAt       string          `json:"created_at"`
	UpdatedAt       string          `json:"updated_at"`
	StartedAt       string          `json:"started_at"`
	Shortlink       string          `json:"shortlink"`
	Components      []ComponentItem `json:"components"`
	IncidentUpdates []struct {
		Body string `json:"body"`
	} `json:"incident_updates"`
}

func (p incidentPayload) toIncident() Incident {
	inc := Incident{
		ID:         p.ID,
		UpdatedAt:  p.UpdatedAt,
		Name:       p.Name,
		Status:     p.Status,
		Shortlink:  p.Shortlink,
		Components: p.Components,
	}
	if len(p.IncidentUpdates) > 0 {
		inc.LatestBody = p.IncidentUpdates[0].Body
	}
	return inc
}

type incidentsResponse struct {
	Incidents []incidentPayload `json:"incidents"`
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

func pollIncidents(ctx context.Context, db *DB, skHex, ownerPubkey string) {
	const interval = 90 * time.Second
	client := &http.Client{Timeout: 15 * time.Second}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// 起動直後の既存インシデントは「既読」として記録だけして投稿しない
	// （再起動のたびに過去インシデントを蒸し返さないため）。
	primeSeen(ctx, db, client)

	// failing: 直近の取得が失敗状態か。健全↔失敗の遷移時にだけオーナーへ通知する
	// （失敗が続いても通知は最初の一回、復旧時にも一回だけ）。
	failing := false

	log.Printf("🔁 Incident polling started (interval=%s)", interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			data, err := fetchIncidents(ctx, client)
			if err != nil {
				log.Printf("⚠️ poll fetch failed: %v", err)
				if !failing {
					notifyOwner(ctx, skHex, ownerPubkey,
						fmt.Sprintf("⚠️ ステータスの取得に失敗してるみたい…\nエラー: %v\n\n直ったらまた知らせるね！", err))
					failing = true
				}
				continue
			}
			if failing {
				notifyOwner(ctx, skHex, ownerPubkey, "✅ ステータスの取得が復活したよ！")
				failing = false
			}
			incidentList.set(data.Incidents)
			for _, raw := range data.Incidents {
				postIncident(ctx, db, skHex, raw.toIncident(), "poll")
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
	incidentList.set(data.Incidents)
	n := 0
	for _, raw := range data.Incidents {
		if claimed, _ := db.claim(raw.toIncident().dedupKey()); claimed {
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
	return &DB{db: db}, nil
}

// claim は key を webhook_logs に登録し、今回新規に登録できたら true を返す。
// INSERT OR IGNORE の RowsAffected を見ることで「確認」と「登録」を1つの
// 操作で原子的に行い、webhook と poll が同じインシデントを同時処理しても
// 二重投稿しないようにする。webhook とポーリングは共通のキー空間を使う。
func (d *DB) claim(key string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	res, err := d.db.Exec(`INSERT OR IGNORE INTO webhook_logs (incident_key) VALUES (?)`, key)
	if err != nil {
		return false, err
	}
	if _, delErr := d.db.Exec(`DELETE FROM webhook_logs WHERE id NOT IN (SELECT id FROM webhook_logs ORDER BY id DESC LIMIT 300)`); delErr != nil {
		log.Printf("⚠️ webhook_logs cleanup failed: %v", delErr)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// unclaim は claim 済みの key を取り消す。投稿に失敗したときに呼び、
// 次回（webhook再送やポーリング）でリトライできるようにする。
func (d *DB) unclaim(key string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.db.Exec(`DELETE FROM webhook_logs WHERE incident_key = ?`, key)
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
		// 接続ごとに子コンテキストを作り、再接続前に cancel して
		// このpoolが張ったリレー接続を確実に閉じる（張りっぱなしのリーク防止）。
		subCtx, cancel := context.WithCancel(ctx)
		pool := nostr.NewSimplePool(subCtx)
		since := nostr.Timestamp(time.Now().Unix())
		filters := []nostr.Filter{{
			Kinds: []int{nostr.KindTextNote},
			Tags:  nostr.TagMap{"p": []string{myPubkey}},
			Since: &since,
		}}

		events := pool.SubMany(subCtx, subRelays, filters)
		log.Printf("📡 Subscribed to mentions for %s", myPubkey)

		for ie := range events {
			ev := ie.Event
			log.Printf("📨 Mention from %s: %s", ev.PubKey, ev.Content)
			lower := strings.ToLower(ev.Content)
			go func(ev *nostr.Event, lower string) {
				var msg string
				if strings.Contains(lower, "ステータス") || strings.Contains(lower, "status") {
					msg = buildStatusMessage()
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

		cancel() // このpoolのリレー接続を閉じてから張り直す
		log.Printf("⚠️ SubMany ended, reconnecting in 5s")
		time.Sleep(5 * time.Second)
	}
}

// ── Webhook HTTPサーバー ───────────────────────────────

type WebhookPayload struct {
	ComponentUpdate *struct{}        `json:"component_update"`
	Incident        *incidentPayload `json:"incident"`
	Page            *struct {
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

		// component_update 単独（incident を伴わない）のときだけ無視する。
		// incident が同居するペイロードでは下のインシデント処理に進める。
		if payload.ComponentUpdate != nil && payload.Incident == nil {
			fmt.Fprint(w, "Ignored (component_update only)")
			return
		}

		if payload.Incident != nil {
			inc := payload.Incident.toIncident()
			log.Printf("📋 Webhook incident: %s (%s)", inc.Name, inc.Status)
			switch postIncident(ctx, db, skHex, inc, "webhook") {
			case postPosted:
				fmt.Fprint(w, "Posted!")
			case postDuplicate:
				fmt.Fprint(w, "Ignored (duplicate)")
			default:
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

// defaultOwnerNpub はステータス取得エラー／復旧の通知先（オーナー）。
// 環境変数 OWNER_NPUB で上書きできる。
const defaultOwnerNpub = "npub1s2es6vzyg9cwd2xgr85yqm3k9gmf2322gctcjn8zwphnssxxcqpsw829jv"

// ownerPubkeyHex は通知先 npub を hex pubkey に変換する。
// 不正な値なら空文字を返し、通知を無効化する（bot 自体は止めない）。
func ownerPubkeyHex() string {
	npub := os.Getenv("OWNER_NPUB")
	if npub == "" {
		npub = defaultOwnerNpub
	}
	_, data, err := nip19.Decode(strings.TrimSpace(npub))
	if err != nil {
		log.Printf("⚠️ OWNER_NPUB decode失敗、オーナー通知を無効化: %v", err)
		return ""
	}
	hex, ok := data.(string)
	if !ok {
		log.Printf("⚠️ OWNER_NPUB 想定外の型 %T、オーナー通知を無効化", data)
		return ""
	}
	return hex
}

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

	ownerPubkey := ownerPubkeyHex()
	if ownerPubkey != "" {
		log.Printf("Owner pubkey (通知先): %s", ownerPubkey)
	}

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
	go pollIncidents(ctx, db, skHex, ownerPubkey)

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
