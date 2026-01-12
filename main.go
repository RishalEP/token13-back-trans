package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	rdb *redis.Client // Will be nil if Redis is not configured
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
	initRedis() // Safe to fail

	http.HandleFunc("/quicknode-webhook", webhookHandler)
	// Health Endpoint for Monitoring
	http.HandleFunc("/quicknode-webhook/health", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Logging Health Endpoint Hit!")
		w.WriteHeader(http.StatusOK)
	})

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

	if RedisURL == "" {
		log.Println("Notice: REDIS_URL not found. Running in DB-only mode.")
		return
	}

	client := redis.NewClient(&redis.Options{
		Addr:     RedisURL,
		Password: password,
	})

	// Try to connect with a short timeout
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		log.Printf("Warning: Failed to connect to Redis at %s: %v. Running without cache.", RedisURL, err)
		return
	}

	// Only assign to global variable if connection succeeded
	rdb = client
	log.Println("Connected to Redis")
}

// Handler for QuickNode webhook
func webhookHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	// 1. Read the raw body bytes
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading body: %v", err)
		http.Error(w, "Read Error", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	// 2. Log the exact payload received (Great for debugging)
	log.Printf("--- New Webhook Received ---\nPayload: %s\n----------------------------", string(bodyBytes))

	// 3. Unmarshal the bytes into the struct
	var payload QuickNodePayload
	if err := json.Unmarshal(bodyBytes, &payload); err != nil {
		log.Printf("JSON decode error: %v", err)
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

		// --- DB INSERT (Primary Storage) ---
		if err := db.Create(&dbTx).Error; err != nil {
			log.Printf("DB insert failed: %v", err)
			continue
		}

		// --- REDIS CACHE (Optional / Enhancement) ---
		// We only run this block if rdb was successfully initialized
		if rdb != nil {
			cacheKey := fmt.Sprintf("wallet:txHistory:%s:%s", chain, walletAddress)

			jsonTx, err := json.Marshal(dbTx)
			if err == nil {
				if err := rdb.LPush(ctx, cacheKey, jsonTx).Err(); err != nil {
					log.Printf("Redis LPush failed (non-fatal): %v", err)
				} else {
					// Trim to last 100
					rdb.LTrim(ctx, cacheKey, 0, 100)
				}
			}
		}
	}

	log.Printf("Processed tx %s for chain %s", tx.TxHash, chain)
}
