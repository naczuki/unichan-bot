//go:build ignore

// statuscheck.go — Fly.io実機からstatus.claude.comへのアクセス可否を検証する単発ツール。
//
// 使い方（Fly.ioのうにちゃんマシン上で）:
//   fly ssh console
//   # リポジトリ内で:
//   go run statuscheck.go
//
// ローカルでも `go run statuscheck.go` で動くが、判定はIP依存なので
// 必ずFly.io上で実行した結果を見ること。
package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var endpoints = []string{
	"https://status.claude.com/api/v2/status.json",
	"https://status.claude.com/api/v2/summary.json",
	"https://status.claude.com/api/v2/incidents.json",
	"https://status.claude.com/api/v2/incidents/unresolved.json",
	"https://status.claude.com/history.atom",
	"https://status.claude.com/history.rss",
}

const browserUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

type variant struct {
	name    string
	headers map[string]string
}

var variants = []variant{
	{
		name:    "A: 素のGoデフォルト（ヘッダなし）",
		headers: map[string]string{},
	},
	{
		name: "B: UAのみ偽装",
		headers: map[string]string{
			"User-Agent": browserUA,
		},
	},
	{
		name: "C: UA＋ブラウザ風ヘッダ一式",
		headers: map[string]string{
			"User-Agent":      browserUA,
			"Accept":          "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
			"Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
			"Accept-Encoding": "gzip, deflate, br",
			"Referer":         "https://status.claude.com/",
		},
	},
}

func main() {
	client := &http.Client{Timeout: 15 * time.Second}

	// まず自分のegress IPを確認（どのIPから出ているか把握用）
	if ip := fetchIP(client); ip != "" {
		fmt.Printf("egress IP: %s\n", ip)
	}
	fmt.Println(strings.Repeat("=", 60))

	for _, v := range variants {
		fmt.Printf("\n### %s\n", v.name)
		for _, ep := range endpoints {
			req, _ := http.NewRequest("GET", ep, nil)
			for k, val := range v.headers {
				req.Header.Set(k, val)
			}
			start := time.Now()
			resp, err := client.Do(req)
			elapsed := time.Since(start)
			if err != nil {
				fmt.Printf("  %-55s ERR  %v\n", short(ep), err)
				continue
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			fmt.Printf("  %-55s %d  %6dB  %v\n", short(ep), resp.StatusCode, len(body), elapsed.Round(time.Millisecond))
		}
	}
}

func short(u string) string {
	return strings.TrimPrefix(u, "https://status.claude.com/")
}

func fetchIP(c *http.Client) string {
	resp, err := c.Get("https://api.ipify.org")
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return strings.TrimSpace(string(b))
}
