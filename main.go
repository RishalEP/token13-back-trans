package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/btcsuite/btcutil/base58"
	"github.com/go-redis/redis/v8"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	db  *gorm.DB
	rdb *redis.Client
	ctx = context.Background()
)

// ---------------------------------------------------------
// 1. DATABASE MODELS (UNCHANGED)
// ---------------------------------------------------------

// TRON Schema
type WalletTransactionHistory struct {
	ID              int64     `gorm:"column:id;primaryKey;autoIncrement"`
	WalletAddress   string    `gorm:"column:address;size:191;uniqueIndex:idx_wallet_tx_to;index:idx_address;index:idx_address_chain,priority:1"`
	TxHash          string    `gorm:"column:tx_hash;size:66;uniqueIndex:idx_wallet_tx_to;index:idx_tx_hash"`
	Chain           string    `gorm:"column:chain;size:20;primaryKey;index:idx_chain"`
	BlockNumber     int64     `gorm:"column:block_number"`
	BlockTime       int64     `gorm:"column:block_time;index:idx_block_time"`
	FromAddress     string    `gorm:"column:from_address;size:191;index:idx_from_address"`
	ToAddress       string    `gorm:"column:to_address;size:191;uniqueIndex:idx_wallet_tx_to;index:idx_to_address"`
	Amount          string    `gorm:"column:token_amount"`
	FiatValue       float64   `gorm:"column:fiat_value;type:decimal(20,8);default:0"`
	BandwidthUsed   int64     `gorm:"column:bandwidth_used"`
	EnergyUsed      int64     `gorm:"column:energy_used"`
	CostInTrx       float64   `gorm:"column:cost_in_trx;type:decimal(20,8);default:0"`
	CostInUsd       float64   `gorm:"column:cost_in_usd;type:decimal(20,8);default:0"`
	Direction       string    `gorm:"column:direction;size:10"`
	Status          string    `json:"status" gorm:"type:varchar(20);default:'success'"`
	TransactionType string    `json:"transaction_type" gorm:"type:varchar(50);default:'transfer'"`
	Standard        string    `gorm:"column:standard;index:idx_standard"`
	ContractAddress string    `gorm:"column:contract_address;index:idx_contract_address"`
	TokenName       string    `gorm:"column:token_name"`
	TokenSymbol     string    `gorm:"column:token_symbol"`
	TokenDecimal    uint8     `gorm:"column:token_decimal"`
	CreatedAt       time.Time `gorm:"column:created_at"`
}

// EVM Schema
type EvmTransactionHistory struct {
	ID              int64     `gorm:"column:id;primaryKey;autoIncrement"`
	WalletAddress   string    `gorm:"column:address;size:191;uniqueIndex:idx_evm_tx_to;index:idx_address;index:idx_address_chain,priority:1"`
	TxHash          string    `gorm:"column:tx_hash;size:66;uniqueIndex:idx_evm_tx_to;index:idx_tx_hash"`
	Chain           string    `gorm:"column:chain;size:20;primaryKey;index:idx_chain"`
	BlockNumber     int64     `gorm:"column:block_number"`
	BlockTime       int64     `gorm:"column:block_time;index:idx_block_time"`
	FromAddress     string    `gorm:"column:from_address;size:191;index:idx_from_address"`
	ToAddress       string    `gorm:"column:to_address;size:191;index:idx_to_address"`
	Amount          string    `gorm:"column:token_amount"`
	GasUsed         int64     `gorm:"column:gas_used"`
	GasPrice        string    `gorm:"column:gas_price"`
	NetworkFee      string    `gorm:"column:network_fee"`
	Status          string    `gorm:"column:status;type:varchar(20);default:'success'"`
	TransactionType string    `gorm:"column:transaction_type;type:varchar(50);default:'transfer'"`
	MethodId        string    `gorm:"column:method_id;size:100"`
	Standard        string    `gorm:"column:standard;index:idx_standard"`
	ContractAddress string    `gorm:"column:contract_address;index:idx_contract_address"`
	TokenName       string    `gorm:"column:token_name"`
	TokenSymbol     string    `gorm:"column:token_symbol"`
	TokenDecimal    uint8     `gorm:"column:token_decimal"`
	Direction       string    `gorm:"column:direction;size:10"`
	CreatedAt       time.Time `gorm:"column:created_at"`
}

func (t *WalletTransactionHistory) TableName() string { return "tron_transaction_histories" }
func (t *EvmTransactionHistory) TableName() string    { return "evm_transaction_histories" }

// ---------------------------------------------------------
// 2. QUICKNODE PAYLOAD STRUCTURES (MAPPED TO JS OUTPUT)
// ---------------------------------------------------------

type QuickNodePayload struct {
	Metadata  Metadata   `json:"metadata"`
	Transfers []Transfer `json:"transfers"`
}
type Metadata struct {
	Network string `json:"network"`
}

// Transfer Struct - Matches the JS snake_case output
type Transfer struct {
	// Common Fields
	BlockNumber int    `json:"block_number"`
	BlockTime   int64  `json:"block_time"`
	From        string `json:"from_address"`
	To          string `json:"to_address"`
	Standard    string `json:"standard"`
	TxHash      string `json:"tx_hash"`
	Value       string `json:"token_amount"`
	Contract    string `json:"contract_address"`

	// TRON Specific (Mapped from JS snake_case)
	NetUsage   int64  `json:"net_usage,omitempty"` // Maps to BandwidthUsed
	EnergyUsed int64  `json:"energy_used,omitempty"`
	CostInTrx  string `json:"cost_in_trx,omitempty"` // String decimal

	// EVM Specific
	GasUsed           string `json:"gasUsed,omitempty"`
	EffectiveGasPrice string `json:"effectiveGasPrice,omitempty"`

	// Token Info
	TokenName     string `json:"tokenName,omitempty"`
	TokenSymbol   string `json:"tokenSymbol,omitempty"`
	TokenDecimals int    `json:"decimals,omitempty"`
}

const (
	WatchedWalletKey = "watched_wallets"
)

//Load Addresses From DB into Redis

func LoadWatchedAddresses() {
	log.Println("Loading Watched Addresses from DB")

	var addresses []string

	results := db.Table("wallet_addresses").Pluck("address", &addresses)
	if results.Error != nil {
		log.Printf("Error loading watched addresses: %v", results.Error)
	}

	if len(addresses) > 0 {
		rdb.Del(ctx, WatchedWalletKey)
		formatted := make([]interface{}, len(addresses))
		for i, v := range addresses {
			formatted[i] = v
		}

		err := rdb.SAdd(ctx, WatchedWalletKey, formatted...).Err()
		if err != nil {
			log.Printf("Error saving watched addresses to Redis: %v", err)
		} else {
			log.Printf("Loaded %d watched addresses to Redis", len(addresses))
		}
	} else {
		log.Println("No watched addresses found in DB to watch!")
	}
}

// ---------------------------------------------------------
// 3. MAIN & INIT
// ---------------------------------------------------------

func main() {
	initDatabase()
	initRedis()
	LoadWatchedAddresses()

	http.HandleFunc("/quicknode-webhook", webhookHandler)
	http.HandleFunc("/quicknode-webhook/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	log.Println("Backtrans Service Listening on :8800")
	log.Fatal(http.ListenAndServe(":8800", nil))
}

func initDatabase() {
	DbUrl := os.Getenv("DATABASE_URL")
	if DbUrl == "" {
		// Replace with your actual local string if needed
		DbUrl = "root:Password@tcp(127.0.0.1:3306)/token13_app?parseTime=True"
		log.Printf("DATABASE_URL not set, using default: %s", DbUrl)
	}

	var err error
	db, err = gorm.Open(mysql.Open(DbUrl), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to db: %v", err)
	}

	// AutoMigrate both tables to ensure they exist (wont change schema if already exists)
	if err := db.AutoMigrate(&WalletTransactionHistory{}, &EvmTransactionHistory{}); err != nil {
		log.Fatalf("AutoMigrate failed: %v", err)
	}
	log.Println("Connected to DB and Tables Checked")
}

func initRedis() {
	redisURL := os.Getenv("REDIS_URL")
	log.Printf("REDIS_URL: %s", redisURL)
	if redisURL == "" {
		log.Println("Notice: REDIS_URL not set. Running without Redis.")
		return
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Printf("Invalid REDIS_URL: %v", err)
		return
	}

	rdb = redis.NewClient(opt)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("Redis connection failed: %v", err)
		rdb = nil
	} else {
		log.Println("Connected to Redis")
	}
}

// ---------------------------------------------------------
// 4. WEBHOOK HANDLER
// ---------------------------------------------------------

func webhookHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Read error: %v", err)
		http.Error(w, "Read Error", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	var payload QuickNodePayload
	if err := json.Unmarshal(bodyBytes, &payload); err != nil {
		log.Printf("JSON decode error: %v", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	log.Printf("Received Payload: %s", string(bodyBytes))

	log.Printf("Received Batch. Network: %s | Count: %d", payload.Metadata.Network, len(payload.Transfers))

	// Basic network detection
	network := strings.ToLower(payload.Metadata.Network)
	var chainFamily string
	if strings.Contains(network, "tron") {
		chainFamily = "tron"
	} else if strings.Contains(network, "ethereum") || strings.Contains(network, "eth") {
		chainFamily = "eth"
	} else {
		// Fallback to standard check if needed, or assume Tron if your stream is Tron-only
		chainFamily = "eth"
	}

	for _, tx := range payload.Transfers {
		// Safety check: if standard is native or TRC20/ERC20 but chain is ambiguous
		if chainFamily == "tron" {
			processTronTransaction(tx)
		} else {
			processEvmTransaction(tx)
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Received"))
}

// ---------------------------------------------------------
// 5. TRON PROCESSOR
// ---------------------------------------------------------
func processTronTransaction(tx Transfer) {
	// We loop through the RAW HEX addresses from QuickNode
	wallets := []string{tx.From, tx.To}

	// 1. Format Amount
	amountStr := tx.Value
	decimals := 6
	if tx.TokenDecimals > 0 {
		decimals = tx.TokenDecimals
	}
	if tx.Value != "" && tx.Value != "0" {
		valBig, _ := new(big.Int).SetString(cleanHex(tx.Value), 0)
		if valBig != nil {
			amountStr = formatTokenAmount(valBig, decimals)
		}
	}

	// 2. Parse Cost
	var costFloat float64 = 0
	if tx.CostInTrx != "" {
		if s, err := strconv.ParseFloat(tx.CostInTrx, 64); err == nil {
			costFloat = s
		}
	}

	for _, walletHex := range wallets {
		if walletHex == "" {
			continue
		}

		// 1. Convert Hex to Base58 (T-Address) ONLY for the check
		walletBase58 := HexToTronAddress(walletHex)
		log.Printf("[TRON] Processing Tx: %s (Base58: %s)", tx.TxHash, walletBase58)

		// 2. Check Redis using the Base58 address
		if !isWalletWatched(walletBase58) {
			log.Printf("[TRON] Wallet %s not found in Redis. Skipping.", walletBase58)
			continue // Not our user
		}

		direction := "receive"
		if strings.EqualFold(walletHex, tx.From) {
			direction = "send"
		}

		dbTx := WalletTransactionHistory{
			WalletAddress:   walletHex,
			TxHash:          tx.TxHash,
			Chain:           "tron",
			BlockNumber:     int64(tx.BlockNumber),
			BlockTime:       tx.BlockTime,
			FromAddress:     tx.From,
			ToAddress:       tx.To,
			Amount:          amountStr,
			Status:          "success",
			TransactionType: "transfer",
			Standard:        tx.Standard,
			ContractAddress: tx.Contract,
			TokenName:       tx.TokenName,
			TokenSymbol:     tx.TokenSymbol,
			TokenDecimal:    uint8(decimals),
			Direction:       direction,
			CreatedAt:       time.Now(),
			BandwidthUsed:   tx.NetUsage,
			EnergyUsed:      tx.EnergyUsed,
			CostInTrx:       costFloat,
			CostInUsd:       0,
		}

		err := db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "address"}, {Name: "tx_hash"}},
			DoNothing: true,
		}).Create(&dbTx).Error

		if err != nil {
			log.Printf("[TRON] DB Error for %s: %v", walletHex, err)
		} else {
			log.Printf("[TRON] Saved Tx for %s (Match Found via %s)", walletHex, walletBase58)
			updateRedis(walletHex, "tron", dbTx)
		}
	}
}

// isWalletWatched checks Redis & DB to see if this address belongs to a user.
func isWalletWatched(address string) bool {
	//Check Redis First
	if rdb == nil {
		_, err := rdb.SIsMember(ctx, WatchedWalletKey, address).Result()
		if err != nil {
			return true
		}
	}

	//Check DB First
	var count int64
	err := db.Table("wallet_addresses").Where("address = ?", address).Count(&count).Error
	if err != nil {
		log.Printf("DB Error: %v", err)
		return false
	}

	//If Found in DB, Add to Redis
	if count > 0 && rdb != nil {
		rdb.SAdd(ctx, WatchedWalletKey, address)
	}
	return count > 0
}

// ---------------------------------------------------------
// 6. EVM PROCESSOR
// ---------------------------------------------------------
func processEvmTransaction(tx Transfer) {
	wallets := []string{tx.From, tx.To}
	log.Printf("[EVM] Processing Tx: %s", tx.TxHash)

	// Calc Fees
	gasUsed, _ := new(big.Int).SetString(cleanHex(tx.GasUsed), 0)
	gasPrice, _ := new(big.Int).SetString(cleanHex(tx.EffectiveGasPrice), 0)
	if gasUsed == nil {
		gasUsed = big.NewInt(0)
	}
	if gasPrice == nil {
		gasPrice = big.NewInt(0)
	}
	feeWei := new(big.Int).Mul(gasUsed, gasPrice)

	// Format Amount
	amountStr := tx.Value
	decimals := 18
	if tx.TokenDecimals > 0 {
		decimals = tx.TokenDecimals
	}

	if tx.Value != "" && tx.Value != "0" {
		valBig, _ := new(big.Int).SetString(cleanHex(tx.Value), 0)
		if valBig != nil {
			amountStr = formatTokenAmount(valBig, decimals)
		}
	}

	for _, walletAddr := range wallets {
		if walletAddr == "" {
			continue
		}
		direction := "receive"
		if strings.EqualFold(walletAddr, tx.From) {
			direction = "send"
		}

		dbTx := EvmTransactionHistory{
			WalletAddress:   strings.ToLower(walletAddr),
			TxHash:          tx.TxHash,
			Chain:           "ETH",
			BlockNumber:     int64(tx.BlockNumber),
			BlockTime:       tx.BlockTime,
			FromAddress:     strings.ToLower(tx.From),
			ToAddress:       strings.ToLower(tx.To),
			Amount:          amountStr,
			GasUsed:         gasUsed.Int64(),
			GasPrice:        gasPrice.String(),
			NetworkFee:      feeWei.String(),
			Status:          "success",
			TransactionType: "transfer",
			Standard:        tx.Standard,
			ContractAddress: strings.ToLower(tx.Contract),
			TokenName:       tx.TokenName,
			TokenSymbol:     tx.TokenSymbol,
			TokenDecimal:    uint8(decimals),
			Direction:       direction,
			CreatedAt:       time.Now(),
		}

		err := db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "address"}, {Name: "tx_hash"}},
			DoNothing: true,
		}).Create(&dbTx).Error

		if err != nil {
			log.Printf("[EVM] DB Error for %s: %v", walletAddr, err)
		} else {
			log.Printf("[EVM] Saved Tx for %s", walletAddr)
			updateRedis(strings.ToLower(walletAddr), "eth", dbTx)
		}
	}
}

// ---------------------------------------------------------
// 7. HELPERS
// ---------------------------------------------------------

func updateRedis(wallet, chain string, data interface{}) {
	if rdb == nil {
		return
	}
	cacheKey := fmt.Sprintf("%s:%s:txns", chain, wallet)

	jsonBytes, err := json.Marshal(data)
	if err == nil {
		rdb.LPush(ctx, cacheKey, jsonBytes)
		rdb.LTrim(ctx, cacheKey, 0, 199)
	}
}

func HexToTronAddress(hexStr string) string {
	if strings.HasPrefix(hexStr, "0x") || strings.HasPrefix(hexStr, "0X") {
		hexStr = hexStr[2:]
	}

	if len(hexStr) == 0 {
		return ""
	}
	if len(hexStr) == 40 {
		hexStr = "40" + hexStr
	}

	inputBytes, err := hex.DecodeString(hexStr)
	if err != nil {
		log.Printf("Error decoding hex string: %v", err)
		return ""
	}
	return base58.CheckEncode(inputBytes[1:], inputBytes[0])
}

func cleanHex(h string) string {
	if strings.HasPrefix(h, "0x") {
		return h
	}
	return "0x" + h
}

func formatTokenAmount(value *big.Int, decimals int) string {
	fValue := new(big.Float).SetInt(value)
	divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
	res := new(big.Float).Quo(fValue, divisor)
	s := res.Text('f', decimals)
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
	}
	return s
}
