package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var (
	db  *gorm.DB
	rdb *redis.Client
	ctx = context.Background()
)

type WalletTransactionHistory struct {
	ID              int64     `gorm:"column:id;primaryKey;autoIncrement"`
	WalletAddress   string    `gorm:"column:address;size:191;uniqueIndex:idx_wallet_tx_to;index:idx_address;index:idx_address_chain,priority:1"`
	TxHash          string    `gorm:"column:tx_hash;size:66;uniqueIndex:idx_wallet_tx_to;index:idx_tx_hash"`
	ToAddress       string    `gorm:"column:to_address;size:191;uniqueIndex:idx_wallet_tx_to;index:idx_to_address"`
	FromAddress     string    `gorm:"column:from_address;size:191;index:idx_from_address"`
	Chain           string    `gorm:"column:chain;size:20;primaryKey;index:idx_chain;index:idx_address_chain,priority:2;index:idx_chain_tx_hash,priority:1"`
	Value           string    `gorm:"column:value"`
	BlockNumber     int64     `gorm:"column:block_number"`
	BlockTime       int64     `gorm:"column:block_time;index:idx_block_time"`
	Standard        string    `gorm:"column:standard;index:idx_standard"`
	ContractAddress string    `gorm:"column:contract_address;index:idx_contract_address"`
	TokenName       string    `gorm:"column:token_name"`
	TokenSymbol     string    `gorm:"column:token_symbol"`
	TokenDecimal    uint8     `gorm:"column:token_decimal"`
	CreatedAt       time.Time `gorm:"column:created_at"`
}

// QuickNodePayload for incoming JSON
type QuickNodePayload struct {
	Metadata  Metadata   `json:"metadata"`
	Transfers []Transfer `json:"transfers"`
}
type Metadata struct {
	Network string `json:"network"` // "tron-mainnet" etc
}

type Transfer struct {
	BlockNumber int    `json:"blockNumber"`
	BlockTime   int64  `json:"blockTime"`
	From        string `json:"from"`
	To          string `json:"to"`
	Standard    string `json:"standard"`
	TxHash      string `json:"txHash"`
	Value       string `json:"value"`
}

func main() {
	initDatabase()
	initRedis()

	http.HandleFunc("/quicknode-webhook", webhookHandler)

	log.Println("Listening on :4444")
	log.Fatal(http.ListenAndServe(":4444", nil))
}

func initDatabase() {
	DbUrl := os.Getenv("DATABASE_URL")
	if DbUrl == "" {
		DbUrl = "root:Password@tcp(127.0.0.1:3306)/token13_app?parseTime=True"
		log.Printf("DATABASE_URL not set, using default local : %s", DbUrl)
	}

	log.Println("Connecting to db: ", DbUrl)

	var err error
	db, err = gorm.Open(mysql.Open(DbUrl), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to db: %v", err)
	}

	err = db.AutoMigrate(&WalletTransactionHistory{})
	if err != nil {
		log.Fatalf("AutoMigrate failed: %v", err)
	}
	log.Println("Connected to db")
}

func initRedis() {
	RedisURL := os.Getenv("REDIS_URL")
	password := os.Getenv("REDIS_PASSWORD")

	if RedisURL == "" || password == "" {
		log.Fatal("Missing required Redis environment variables")
	}

	rdb = redis.NewClient(&redis.Options{
		Addr:     RedisURL,
		Password: password,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Connected to Redis")
}

// Handler for QuickNode webhook
func webhookHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload QuickNodePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		log.Printf(" JSON decode error: %v", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	parts := strings.Split(payload.Metadata.Network, "-")
	chain := parts[0] // "tron", "eth", etc

	for _, tx := range payload.Transfers {
		processTransaction(tx, chain)
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Received"))
}

// Save to db & Cache
func processTransaction(tx Transfer, chain string) {
	wallets := []string{tx.From, tx.To}
	// Extract chain (e.g., "tron" from "tron-mainnet")

	for _, walletAddress := range wallets {
		dbTx := WalletTransactionHistory{
			WalletAddress: walletAddress,
			TxHash:        tx.TxHash,
			FromAddress:   tx.From,
			ToAddress:     tx.To,
			Value:         tx.Value,
			BlockNumber:   int64(tx.BlockNumber),
			BlockTime:     tx.BlockTime,
			Standard:      tx.Standard,
			Chain:         chain,
		}

		if err := db.Create(&dbTx).Error; err != nil {
			log.Printf(" DB insert failed: %v", err)
			continue
		}

		// Cache in Redis
		cacheKey := fmt.Sprintf("wallet:txHistory:%s:%s", chain, walletAddress)

		// Marshal struct to JSON
		jsonTx, err := json.Marshal(dbTx)
		if err != nil {
			log.Printf("JSON marshal failed: %v", err)
			continue
		}

		// Push to Redis
		if err := rdb.LPush(ctx, cacheKey, jsonTx).Err(); err != nil {
			log.Printf("Redis insert failed: %v", err)
		}

		//Trim Redis list to last 200 txs
		if err := rdb.LTrim(ctx, cacheKey, 0, 100).Err(); err != nil {
			log.Printf("Redis LTRIM failed: %v", err)
		}
	}

	log.Printf("Saved tx %s for %s", tx.TxHash, tx.From)
}
