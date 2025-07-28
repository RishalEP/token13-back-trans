package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	db  *gorm.DB
	rdb *redis.Client
	ctx = context.Background()
)

// WalletTransactionHistory model for MySQL
type WalletTransactionHistory struct {
	ID            uint64 `gorm:"primaryKey"`
	WalletAddress string `gorm:"index:idx_wallet_txhash,unique"` //  unique index of txhash+wallet addr
	TxHash        string `gorm:"index:idx_wallet_txhash,unique"`
	Chain         string `gorm:"index"`
	FromAddress   string `gorm:"size:191"`
	ToAddress     string `gorm:"size:191"`
	Value         string
	BlockNumber   int64
	BlockTime     int64
	Standard      string
	CreatedAt     time.Time
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

	log.Println("Listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func initDatabase() {
	user := os.Getenv("MYSQL_USER")
	//pass := os.Getenv("MYSQL_PASS")
	host := os.Getenv("MYSQL_HOST")
	port := os.Getenv("MYSQL_PORT")
	dbname := os.Getenv("MYSQL_DBNAME")

	if user == "" || host == "" || port == "" || dbname == "" {
		log.Fatal("Missing required MySQL environment variables")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", user, "", host, port, dbname)
	log.Println("Connecting to db: ", dsn)

	var err error
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
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
	host := os.Getenv("REDIS_HOST")
	port := os.Getenv("REDIS_PORT")

	if host == "" || port == "" {
		log.Fatal("Missing required Redis environment variables")
	}

	redisAddr := fmt.Sprintf("%s:%s", host, port)

	rdb = redis.NewClient(&redis.Options{
		Addr: redisAddr,
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
