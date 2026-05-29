# unichan-bot 🐚

Claude のステータスを監視して [Nostr](https://nostr.com/) に投稿するボット「うにちゃん」のメモ。

## なにするやつ

- Claude のステータスページの Webhook を受けて、インシデント更新を Nostr に投稿する
- メンションに反応する
  - 本文に「ステータス」or「status」が含まれていたら最新ステータスをリプライ
  - それ以外は適当に「うにー！」とか返す
- SQLite で重複投稿を防止（webhook と polling それぞれ用のテーブル）

> ポーリング（`pollIncidents`）は CAPTCHA 保護のため現在無効化中。Webhook 併用で運用してる（`main.go` 参照）。

## 環境変数

| 変数 | 必須 | 説明 |
|------|------|------|
| `MY_NSEC` | ✅ | ボットの秘密鍵（nsec 形式） |
| `DB_PATH` | | SQLite のパス（デフォルト `/data/unychan.db`） |
| `PORT` | | HTTP サーバーのポート（デフォルト `8080`） |

## ローカルで動かす

```sh
export MY_NSEC=nsec1...
go run .
```

`/` への POST が Webhook エンドポイント。

## デプロイ

[Fly.io](https://fly.io/) に置いてる。

```sh
fly deploy
```

- リージョン: `nrt`（東京）
- `/data` に volume をマウントして SQLite を永続化
- 設定は `fly.toml` 参照

## 投稿先リレー

- `wss://nos.lol`
- `wss://relay.nostr.wirednet.jp`
- `wss://relay-jp.nostr.wirednet.jp`

メンション購読は上記＋ `wss://yabu.me`。

## 監視対象コンポーネント

- claude.ai
- platform.claude.com
- Claude API
- Claude Code

## 構成

| ファイル | 中身 |
|----------|------|
| `main.go` | 全部 |
| `Dockerfile` | ビルド用 |
| `fly.toml` | Fly.io 設定 |

## 使ってるもの

- Go 1.23
- [go-nostr](https://github.com/nbd-wtf/go-nostr)
- modernc.org/sqlite（cgo なし）
