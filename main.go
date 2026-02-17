package main

import (
	"context"
	"crypto/sha256"
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
	ID               int64     `gorm:"column:id;primaryKey;autoIncrement"`
	WalletAddress    string    `gorm:"column:address;size:191;uniqueIndex:idx_evm_tx_to;index:idx_address;index:idx_address_chain,priority:1"`
	TxHash           string    `gorm:"column:tx_hash;size:66;uniqueIndex:idx_evm_tx_to;index:idx_tx_hash"`
	Chain            string    `gorm:"column:chain;size:20;primaryKey;index:idx_chain"`
	BlockNumber      int64     `gorm:"column:block_number"`
	BlockTime        int64     `gorm:"column:block_time;index:idx_block_time"`
	FromAddress      string    `gorm:"column:from_address;size:191;index:idx_from_address"`
	ToAddress        string    `gorm:"column:to_address;size:191;index:idx_to_address"`
	Amount           string    `gorm:"column:token_amount"`
	GasUsed          int64     `gorm:"column:gas_used"`
	GasPrice         string    `gorm:"column:gas_price"`
	NetworkFee       string    `gorm:"column:network_fee"`
	Status           string    `gorm:"column:status;type:varchar(20);default:'success'"`
	TransactionType  string    `gorm:"column:transaction_type;type:varchar(50);default:'transfer'"`
	MethodId         string    `gorm:"column:method_id;size:100"`
	Standard         string    `gorm:"column:standard;index:idx_standard"`
	ContractAddress  string    `gorm:"column:contract_address;index:idx_contract_address"`
	TokenName        string    `gorm:"column:token_name"`
	TokenSymbol      string    `gorm:"column:token_symbol"`
	TokenDecimal     uint8     `gorm:"column:token_decimal"`
	Direction        string    `gorm:"column:direction;size:10"`
	CreatedAt        time.Time `gorm:"column:created_at"`
	FiatValue        float64   `gorm:"column:fiat_value;type:decimal(20,8);default:0"`
	GasLimit         int64     `gorm:"column:gas_limit"`
	NetworkFeeUsd    float64   `gorm:"column:network_fee_usd;type:decimal(20,8);default:0"`
	ChainId          int64     `gorm:"column:chain_id;index:idx_chain_id"`
	ChainName        string    `gorm:"column:chain_name;size:50;index:idx_chain_name"`
	NetworkFeeNative string    `gorm:"column:network_fee_native"`
}

// BTC Schema
type BtcTransactionHistory struct {
	ID              int64     `gorm:"column:id;primaryKey;autoIncrement"`
	WalletAddress   string    `gorm:"column:address;size:191;uniqueIndex:idx_btc_tx;index:idx_address;index:idx_address_chain,priority:1"`
	TxHash          string    `gorm:"column:tx_hash;size:66;uniqueIndex:idx_btc_tx;index:idx_tx_hash"`
	Chain           string    `gorm:"column:chain;size:20;primaryKey;index:idx_chain"`
	BlockNumber     int64     `gorm:"column:block_number"`
	BlockTime       int64     `gorm:"column:block_time;index:idx_block_time"`
	FromAddress     string    `gorm:"column:from_address;size:191;index:idx_from_address"`
	ToAddress       string    `gorm:"column:to_address;size:191;index:idx_to_address"`
	Amount          string    `gorm:"column:token_amount"`
	NetworkFee      string    `gorm:"column:network_fee"`
	Status          string    `gorm:"column:status;type:varchar(20);default:'success'"`
	TransactionType string    `gorm:"column:transaction_type;type:varchar(50);default:'transfer'"`
	Direction       string    `gorm:"column:direction;size:10"`
	CreatedAt       time.Time `gorm:"column:created_at"`
	NetworkFeeSats  int64     `gorm:"column:network_fee_sats"`
	Standard        string    `gorm:"column:standard;index:idx_standard"`
	ContractAddress string    `gorm:"column:contract_address;index:idx_contract_address"`
	TokenName       string    `gorm:"column:token_name"`
	TokenSymbol     string    `gorm:"column:token_symbol"`
	TokenDecimal    uint8     `gorm:"column:token_decimal"`
}

// SOL Schema
type SolTransactionHistory struct {
	ID                 int64     `gorm:"column:id;primaryKey;autoIncrement"`
	WalletAddress      string    `gorm:"column:address;size:191;uniqueIndex:idx_sol_tx;index:idx_address;index:idx_address_chain,priority:1"`
	Signature          string    `gorm:"column:signature;size:128;uniqueIndex:idx_sol_tx;index:idx_signature"`
	Chain              string    `gorm:"column:chain;size:20;index:idx_chain"`
	BlockNumber        int64     `gorm:"column:block_number"`
	BlockTime          int64     `gorm:"column:block_time;index:idx_block_time"`
	FromAddress        string    `gorm:"column:from_address;size:191;index:idx_from_address"`
	ToAddress          string    `gorm:"column:to_address;size:191;index:idx_to_address"`
	Amount             string    `gorm:"column:token_amount"`
	NetworkFee         string    `gorm:"column:network_fee"`
	NetworkFeeLamports int64     `gorm:"column:network_fee_lamports"`
	Direction          string    `gorm:"column:direction;size:10"`
	Status             string    `gorm:"column:status;type:varchar(20);default:'success'"`
	TransactionType    string    `gorm:"column:transaction_type;type:varchar(50);default:'transfer'"`
	Source             string    `gorm:"column:source;size:100"`
	Description        string    `gorm:"column:description;type:text"`
	Standard           string    `gorm:"column:standard;size:191;index:idx_standard"`
	ContractAddress    string    `gorm:"column:contract_address;size:191;index:idx_contract_address"`
	TokenName          string    `gorm:"column:token_name"`
	TokenSymbol        string    `gorm:"column:token_symbol"`
	TokenDecimal       uint8     `gorm:"column:token_decimal"`
	CreatedAt          time.Time `gorm:"column:created_at"`
}

// UserBalance maps to the `user_balances` table.
// It stores decimal-adjusted balances per wallet/address/chain/token.
type UserBalance struct {
	WalletID     string    `gorm:"column:wallet_id;type:char(64);not null;primaryKey" json:"wallet_id"`
	Address      string    `gorm:"column:address;size:100;not null;primaryKey" json:"address"`
	ChainID      string    `gorm:"column:chain_id;size:100;not null;primaryKey" json:"chain_id"`
	TokenAddress string    `gorm:"column:token_address;size:100;not null;primaryKey" json:"token_address"`
	Balance      string    `gorm:"column:balance;type:decimal(65,18);not null" json:"balance"`
	LastActive   time.Time `gorm:"column:last_active;autoCreateTime;autoUpdateTime" json:"last_active"`
}

// WalletAddress maps to the `wallet_addresses` table for wallet_id lookup.
type WalletAddress struct {
	ID             int64     `gorm:"column:id;primaryKey;autoIncrement"`
	WalletID       string    `gorm:"column:wallet_id;type:char(64);not null;index:idx_addr_wallet_chain;uniqueIndex:uq_wallet_chain_index"`
	ChainID        string    `gorm:"column:chain_id;type:varchar(64);not null;index:idx_addr_wallet_chain;uniqueIndex:uq_wallet_chain_index;uniqueIndex:uq_chain_address"`
	IndexN         int64     `gorm:"column:index_n;not null;uniqueIndex:uq_wallet_chain_index"`
	Address        string    `gorm:"column:address;type:varchar(128);not null;uniqueIndex:uq_chain_address"`
	DerivationPath string    `gorm:"column:derivation_path;type:varchar(255)"`
	AddressHex     string    `gorm:"column:address_hex;type:varchar(255)"`
	CreatedAt      time.Time `gorm:"column:created_at"`
	UpdatedAt      time.Time `gorm:"column:updated_at"`
	Label          string    `gorm:"column:label;type:varchar(255);not null"`
	Active         bool      `gorm:"column:active;type:tinyint(1);not null;default:1"`
}

// UserWalletDevice represents the link between a WalletID and a physical device (iOS/Android)
type UserWalletDevice struct {
	WalletID    string    `gorm:"primaryKey;type:char(64)" json:"wallet_id"`
	DeviceToken string    `gorm:"primaryKey;size:255" json:"device_token"`
	DeviceType  string    `gorm:"size:10" json:"device_type"` // ios or android
	CreatedAt   time.Time `gorm:"autoCreateTime"`
}

func (t *WalletTransactionHistory) TableName() string { return "tron_transaction_histories" }
func (t *EvmTransactionHistory) TableName() string    { return "evm_transaction_histories" }
func (t *BtcTransactionHistory) TableName() string    { return "btc_transaction_histories" }
func (t *SolTransactionHistory) TableName() string    { return "sol_transaction_histories" }
func (u *UserBalance) TableName() string              { return "user_balances" }
func (w *WalletAddress) TableName() string            { return "wallet_addresses" }
func (d *UserWalletDevice) TableName() string         { return "user_wallet_devices" }

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
	TokenSymbol   string `json:"token_symbol,omitempty"`
	TokenDecimals int    `json:"decimals,omitempty"`
}

// Moralis EVM payload (ETH, POL, BASE, BSC)
type MoralisEvmPayload struct {
	Confirmed      bool                   `json:"confirmed"`
	ChainId        string                 `json:"chainId"`
	Block          MoralisBlock           `json:"block"`
	Txs            []MoralisTx            `json:"txs"`
	Erc20Transfers []MoralisErc20Transfer `json:"erc20Transfers"`
}

type MoralisBlock struct {
	Number    string `json:"number"`
	Hash      string `json:"hash"`
	Timestamp string `json:"timestamp"`
}

type MoralisTx struct {
	Hash           string `json:"hash"`
	FromAddress    string `json:"fromAddress"`
	ToAddress      string `json:"toAddress"`
	Value          string `json:"value"`
	Gas            string `json:"gas"`
	GasPrice       string `json:"gasPrice"`
	ReceiptGasUsed string `json:"receiptGasUsed"`
	Input          string `json:"input"`
}

type MoralisErc20Transfer struct {
	TransactionHash   string `json:"transactionHash"`
	LogIndex          string `json:"logIndex"`
	Contract          string `json:"contract"`
	From              string `json:"from"`
	To                string `json:"to"`
	Value             string `json:"value"`
	TokenName         string `json:"tokenName"`
	TokenSymbol       string `json:"tokenSymbol"`
	TokenDecimals     string `json:"tokenDecimals"`
	ValueWithDecimals string `json:"valueWithDecimals"`
}

// QuickNode BTC payload
type QuickNodeBtcPayload struct {
	Data []BtcBlock `json:"data"`
}

// QuickNode Solana payload
type QuickNodeSolanaPayload struct {
	Matches   []SolMatch `json:"matches"`
	BlockTime int64      `json:"blockTime"`
	Slot      int64      `json:"slot"`
}

type SolMatch struct {
	Meta        SolMeta        `json:"meta"`
	Transaction SolTransaction `json:"transaction"`
}

type SolMeta struct {
	Err interface{} `json:"err"`
	Fee int64       `json:"fee"`
}

type SolTransaction struct {
	Message    SolMessage `json:"message"`
	Signatures []string   `json:"signatures"`
}

type SolMessage struct {
	AccountKeys  []SolAccountKey  `json:"accountKeys"`
	Instructions []SolInstruction `json:"instructions"`
}

type SolAccountKey struct {
	Pubkey string `json:"pubkey"`
	Signer bool   `json:"signer"`
}

type SolInstruction struct {
	ProgramId string     `json:"programId"`
	Program   string     `json:"program,omitempty"`
	Parsed    *SolParsed `json:"parsed,omitempty"`
}

type SolParsed struct {
	Info SolParsedInfo `json:"info"`
	Type string        `json:"type"`
}

type SolParsedInfo struct {
	Destination string          `json:"destination"`
	Source      string          `json:"source"`
	Lamports    interface{}     `json:"lamports"` // Can be string or number
	Amount      interface{}     `json:"amount"`   // For SPL tokens
	TokenAmount *SolTokenAmount `json:"tokenAmount"`
	Authority   string          `json:"authority"`
}

type SolTokenAmount struct {
	Amount         string `json:"amount"`
	Decimals       uint8  `json:"decimals"`
	UiAmountString string `json:"uiAmountString"`
}

type BtcBlock struct {
	Height int64   `json:"height"`
	Time   int64   `json:"time"`
	Txs    []BtcTx `json:"txs"`
}

type BtcTx struct {
	BlockHash     string    `json:"blockHash"`
	BlockHeight   int64     `json:"blockHeight"`
	BlockTime     int64     `json:"blockTime"`
	Confirmations int64     `json:"confirmations"`
	Fees          string    `json:"fees"`
	Txid          string    `json:"txid"`
	Value         string    `json:"value"`
	ValueIn       string    `json:"valueIn"`
	Vin           []BtcVin  `json:"vin"`
	Vout          []BtcVout `json:"vout"`
}

type BtcVin struct {
	Addresses []string `json:"addresses"`
	IsAddress bool     `json:"isAddress"`
	Value     string   `json:"value"`
}

type BtcVout struct {
	Addresses []string `json:"addresses"`
	IsAddress bool     `json:"isAddress"`
	Value     string   `json:"value"`
}

const (
	WatchedWalletKey          = "watched_wallets"
	BtcWatchedWalletKey       = "btc-addresses-list"
	userBalanceRedisKeyPrefix = "user_balances"
)

//Load Addresses From DB into Redis

// ---------------------------------------------------------
// 3. MAIN & INIT
// ---------------------------------------------------------

func main() {
	initDatabase()
	initRedis()
	InitNotificationService()

	http.HandleFunc("/quicknode-webhook", webhookHandler)
	http.HandleFunc("/quicknode-webhook/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Manual notification trigger endpoint
	http.HandleFunc("/send-notification", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Token      string            `json:"token"`
			DeviceType string            `json:"device_type"`
			Title      string            `json:"title"`
			Body       string            `json:"body"`
			Data       map[string]string `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		err := SendPushNotification(req.Token, req.DeviceType, req.Title, req.Body, req.Data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("Notification sent successfully"))
	})

	log.Println("Backtrans Service Listening on :8800")
	log.Fatal(http.ListenAndServe(":8800", nil))
}

func initDatabase() {
	DbUrl := os.Getenv("DATABASE_URL")
	if DbUrl == "" {
		// Replace with your actual local string if needed
		DbUrl = "root:@tcp(127.0.0.1:3306)/token13_app_new?parseTime=True"
		log.Printf("DATABASE_URL not set, using default: %s", DbUrl)
	}

	var err error
	db, err = gorm.Open(mysql.Open(DbUrl), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to db: %v", err)
	}

	// AutoMigrate tables to ensure they exist
	if err := db.AutoMigrate(&WalletTransactionHistory{}, &EvmTransactionHistory{}, &BtcTransactionHistory{}, &SolTransactionHistory{}, &UserBalance{}, &WalletAddress{}, &UserWalletDevice{}); err != nil {
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

	log.Printf("Received payload body: %s", string(bodyBytes))

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(bodyBytes, &envelope); err != nil {
		log.Printf("JSON decode error: %v", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	switch {
	case envelope["metadata"] != nil && envelope["transfers"] != nil:
		var payload QuickNodePayload
		if err := json.Unmarshal(bodyBytes, &payload); err != nil {
			log.Printf("QuickNode decode error: %v", err)
			http.Error(w, "Invalid QuickNode JSON", http.StatusBadRequest)
			return
		}
		handleQuickNodePayload(payload)
	case envelope["chainId"] != nil && (envelope["txs"] != nil || envelope["erc20Transfers"] != nil):
		var payload MoralisEvmPayload
		if err := json.Unmarshal(bodyBytes, &payload); err != nil {
			log.Printf("Moralis decode error: %v", err)
			http.Error(w, "Invalid Moralis JSON", http.StatusBadRequest)
			return
		}
		handleMoralisPayload(payload)
	case envelope["data"] != nil:
		var payload QuickNodeBtcPayload
		if err := json.Unmarshal(bodyBytes, &payload); err != nil {
			log.Printf("BTC decode error: %v", err)
			http.Error(w, "Invalid BTC JSON", http.StatusBadRequest)
			return
		}
		handleBtcPayload(payload)
	case envelope["matches"] != nil:
		var payload QuickNodeSolanaPayload
		if err := json.Unmarshal(bodyBytes, &payload); err != nil {
			log.Printf("Solana decode error: %v", err)
			http.Error(w, "Invalid Solana JSON", http.StatusBadRequest)
			return
		}
		handleSolanaPayload(payload)
	default:
		log.Printf("Unsupported payload shape")
		http.Error(w, "Unsupported payload", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Received"))
}

// ---------------------------------------------------------
// 5. PAYLOAD HANDLERS
// ---------------------------------------------------------
type evmChainInfo struct {
	Chain    string
	RedisKey string
}

func chainInfoFromMoralisChainId(chainId string) evmChainInfo {
	switch strings.ToLower(chainId) {
	case "0x1":
		return evmChainInfo{Chain: "ETH", RedisKey: "eth"}
	case "0x89":
		return evmChainInfo{Chain: "POL", RedisKey: "pol"}
	case "0x2105":
		return evmChainInfo{Chain: "BASE", RedisKey: "base"}
	case "0x38":
		return evmChainInfo{Chain: "BSC", RedisKey: "bsc"}
	default:
		return evmChainInfo{Chain: "ETH", RedisKey: "eth"}
	}
}

func handleQuickNodePayload(payload QuickNodePayload) {
	log.Printf("Received Batch. Network: %s | Count: %d", payload.Metadata.Network, len(payload.Transfers))

	// Basic network detection
	network := strings.ToLower(payload.Metadata.Network)
	var chainFamily string
	if strings.Contains(network, "tron") {
		chainFamily = "tron"
	} else if strings.Contains(network, "ethereum") || strings.Contains(network, "eth") {
		chainFamily = "eth"
	} else {
		chainFamily = "eth"
	}

	for _, tx := range payload.Transfers {
		if chainFamily == "tron" {
			processTronTransaction(tx)
		} else {
			processEvmTransaction(tx)
		}
	}
}

func handleMoralisPayload(payload MoralisEvmPayload) {
	chainInfo := chainInfoFromMoralisChainId(payload.ChainId)
	blockNumber := parseInt64(payload.Block.Number)
	blockTime := parseInt64(payload.Block.Timestamp)

	log.Printf("Received Moralis Batch. Chain: %s | Native Tx: %d | ERC20: %d", chainInfo.Chain, len(payload.Txs), len(payload.Erc20Transfers))

	txMap := make(map[string]MoralisTx, len(payload.Txs))
	for _, tx := range payload.Txs {
		if tx.Hash == "" {
			continue
		}
		txMap[strings.ToLower(tx.Hash)] = tx
	}

	for _, tx := range payload.Txs {
		processMoralisNativeTx(tx, chainInfo, blockNumber, blockTime)
	}

	for _, transfer := range payload.Erc20Transfers {
		processMoralisErc20Transfer(transfer, txMap, chainInfo, blockNumber, blockTime)
	}
}

func handleBtcPayload(payload QuickNodeBtcPayload) {
	log.Printf("Received BTC Batch. Blocks: %d", len(payload.Data))
	for _, block := range payload.Data {
		for _, tx := range block.Txs {
			processBtcTransaction(block, tx)
		}
	}
}

func handleSolanaPayload(payload QuickNodeSolanaPayload) {
	log.Printf("Received Solana Batch. Slot: %d | Matches: %d", payload.Slot, len(payload.Matches))
	for _, match := range payload.Matches {
		processSolanaTransaction(match, payload.Slot, payload.BlockTime)
	}
}

func processSolanaTransaction(match SolMatch, slot int64, blockTime int64) {
	signature := ""
	if len(match.Transaction.Signatures) > 0 {
		signature = match.Transaction.Signatures[0]
	}
	if signature == "" {
		return
	}

	log.Printf("[SOLANA] Processing Tx: %s", signature)

	for _, inst := range match.Transaction.Message.Instructions {
		// Only process transfers for now
		if inst.Parsed == nil {
			continue
		}

		if inst.Parsed.Type == "transfer" || inst.Parsed.Type == "transferChecked" {
			info := inst.Parsed.Info
			from := info.Source
			to := info.Destination

			if from == "" || to == "" {
				continue
			}

			amountStr := "0"
			decimals := 9 // Default for SOL

			if info.Lamports != nil {
				// Native SOL transfer
				var lamports int64
				switch v := info.Lamports.(type) {
				case float64:
					lamports = int64(v)
				case string:
					lamports, _ = strconv.ParseInt(v, 10, 64)
				case json.Number:
					lamports, _ = v.Int64()
				}
				if lamports > 0 {
					amountStr = formatTokenAmount(big.NewInt(lamports), 9)
				}
			} else if info.TokenAmount != nil {
				// SPL Token transferChecked
				amountStr = info.TokenAmount.UiAmountString
				decimals = int(info.TokenAmount.Decimals)
			} else if info.Amount != nil {
				// SPL Token transfer
				var amount int64
				switch v := info.Amount.(type) {
				case float64:
					amount = int64(v)
				case string:
					amount, _ = strconv.ParseInt(v, 10, 64)
				case json.Number:
					amount, _ = v.Int64()
				}
				amountStr = strconv.FormatInt(amount, 10)
				decimals = 0
			}

			if amountStr == "" || amountStr == "0" {
				continue
			}

			wallets := []string{from, to}
			insertedAny := false

			for _, walletAddr := range wallets {
				if walletAddr == "" {
					continue
				}

				waList, _ := resolveWalletAddresses("solana", walletAddr)
				if len(waList) == 0 {
					continue
				}

				for _, wa := range waList {
					direction := "receive"
					if strings.EqualFold(walletAddr, from) {
						direction = "send"
					}

					dbTx := SolTransactionHistory{
						WalletAddress:      wa.Address,
						Signature:          signature,
						Chain:              "solana",
						BlockNumber:        slot,
						BlockTime:          blockTime,
						FromAddress:        from,
						ToAddress:          to,
						Amount:             amountStr,
						NetworkFee:         formatTokenAmount(big.NewInt(match.Meta.Fee), 9),
						NetworkFeeLamports: match.Meta.Fee,
						Status:             "success",
						TransactionType:    "transfer",
						Direction:          direction,
						CreatedAt:          time.Now(),
						Standard:           inst.Program,
						ContractAddress:    inst.ProgramId,
						TokenDecimal:       uint8(decimals),
					}

					if match.Meta.Err != nil {
						dbTx.Status = "failed"
					}

					result := db.Clauses(clause.OnConflict{
						Columns:   []clause.Column{{Name: "address"}, {Name: "signature"}},
						DoNothing: true,
					}).Create(&dbTx)

					if result.Error != nil {
						log.Printf("[SOLANA] DB Error for %s: %v", wa.Address, result.Error)
					} else {
						log.Printf("[SOLANA] Saved Tx for %s", wa.Address)
						updateRedis(wa.Address, "solana", dbTx)
						if result.RowsAffected > 0 {
							insertedAny = true
						}
					}
				}
			}

			if insertedAny {
				tokenAddr := inst.ProgramId
				if inst.Program == "system" {
					tokenAddr = nativeTokenAddress("solana")
				}
				updateUserBalancesForTransfer("solana", from, to, tokenAddr, amountStr)
			}
		}
	}
}

// ---------------------------------------------------------
// 6. TRON PROCESSOR
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

	insertedAny := false
	for _, walletHex := range wallets {
		if walletHex == "" {
			continue
		}

		waList, _ := resolveWalletAddresses("tron", walletHex)
		if len(waList) == 0 {
			continue
		}

		for _, wa := range waList {
			direction := "receive"
			if strings.EqualFold(walletHex, tx.From) {
				direction = "send"
			}

			if tx.Contract != "" {
				direction = "contract"
			}

			dbTx := WalletTransactionHistory{
				WalletAddress:   wa.Address,
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

			result := db.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "address"}, {Name: "tx_hash"}, {Name: "to_address"}},
				DoNothing: true,
			}).Create(&dbTx)

			if result.Error != nil {
				log.Printf("[TRON] DB Error for %s: %v", wa.Address, result.Error)
			} else {
				log.Printf("[TRON] Saved Tx for %s", wa.Address)
				triggerNotification(walletHex, "tron", dbTx.Amount, dbTx.TokenSymbol, dbTx.Direction)
				updateRedis(wa.Address, "tron", dbTx)
				if result.RowsAffected > 0 {
					insertedAny = true
				}
			}
		}
	}

	if insertedAny {
		tokenAddress := strings.TrimSpace(tx.Contract)
		if tokenAddress == "" {
			tokenAddress = nativeTokenAddress("tron")
		} else {
			tokenAddress = normalizeTokenAddress("tron", tokenAddress)
		}
		updateUserBalancesForTransfer("tron", tx.From, tx.To, tokenAddress, amountStr)
	}
}

// ---------------------------------------------------------
// 7. EVM PROCESSOR (QUICKNODE)
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

	insertedAny := false
	for _, walletAddr := range wallets {
		if walletAddr == "" {
			continue
		}

		waList, _ := resolveWalletAddresses("eth", walletAddr)
		if len(waList) == 0 {
			continue
		}

		for _, wa := range waList {
			direction := "receive"
			if strings.EqualFold(walletAddr, tx.From) {
				direction = "send"
			}

			dbTx := EvmTransactionHistory{
				WalletAddress:   wa.Address,
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

			result := db.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "address"}, {Name: "tx_hash"}},
				DoNothing: true,
			}).Create(&dbTx)

			if result.Error != nil {
				log.Printf("[EVM] DB Error for %s: %v", wa.Address, result.Error)
			} else {
				log.Printf("[EVM] Saved Tx for %s", wa.Address)
				triggerNotification(strings.ToLower(walletAddr), "eth", dbTx.Amount, dbTx.TokenSymbol, dbTx.Direction)
				updateRedis(wa.Address, "eth", dbTx)
				if result.RowsAffected > 0 {
					insertedAny = true
				}
			}
		}
	}

	if insertedAny {
		tokenAddress := strings.TrimSpace(tx.Contract)
		if tokenAddress == "" {
			tokenAddress = nativeTokenAddress("eth")
		} else {
			tokenAddress = normalizeTokenAddress("eth", tokenAddress)
		}
		updateUserBalancesForTransfer("eth", tx.From, tx.To, tokenAddress, amountStr)
	}
}

// ---------------------------------------------------------
// 8. MORALIS EVM PROCESSOR
// ---------------------------------------------------------
func processMoralisNativeTx(tx MoralisTx, chainInfo evmChainInfo, blockNumber int64, blockTime int64) {
	if tx.Hash == "" {
		return
	}

	valueInt := parseBigInt(tx.Value)
	if valueInt.Sign() == 0 {
		return
	}
	amountStr := formatTokenAmount(valueInt, 18)

	gasUsed := parseBigInt(tx.ReceiptGasUsed)
	if gasUsed.Sign() == 0 && tx.Gas != "" {
		gasUsed = parseBigInt(tx.Gas)
	}
	gasPrice := parseBigInt(tx.GasPrice)
	feeWei := new(big.Int).Mul(gasUsed, gasPrice)

	wallets := []string{tx.FromAddress, tx.ToAddress}
	insertedAny := false
	for _, walletAddr := range wallets {
		if walletAddr == "" {
			continue
		}

		waList, _ := resolveWalletAddresses(chainInfo.RedisKey, walletAddr)
		if len(waList) == 0 {
			continue
		}

		for _, wa := range waList {
			direction := "receive"
			if strings.EqualFold(walletAddr, tx.FromAddress) {
				direction = "send"
			}

			dbTx := EvmTransactionHistory{
				WalletAddress:   wa.Address,
				TxHash:          tx.Hash,
				Chain:           chainInfo.Chain,
				BlockNumber:     blockNumber,
				BlockTime:       blockTime,
				FromAddress:     strings.ToLower(tx.FromAddress),
				ToAddress:       strings.ToLower(tx.ToAddress),
				Amount:          amountStr,
				GasUsed:         gasUsed.Int64(),
				GasPrice:        gasPrice.String(),
				NetworkFee:      feeWei.String(),
				Status:          "success",
				TransactionType: "transfer",
				Standard:        "native",
				Direction:       direction,
				CreatedAt:       time.Now(),
				ChainId:         parseInt64(chainInfo.Chain), // fallback
				GasLimit:        parseInt64(tx.Gas),
			}
			// If we had more info from chainInfo
			if chainInfo.Chain == "ETH" {
				dbTx.ChainId = 1
				dbTx.ChainName = "Ethereum"
			} else if chainInfo.Chain == "POL" {
				dbTx.ChainId = 137
				dbTx.ChainName = "Polygon"
			} else if chainInfo.Chain == "BASE" {
				dbTx.ChainId = 8453
				dbTx.ChainName = "Base"
			} else if chainInfo.Chain == "BSC" {
				dbTx.ChainId = 56
				dbTx.ChainName = "BSC"
			}

			result := db.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "address"}, {Name: "tx_hash"}},
				DoNothing: true,
			}).Create(&dbTx)

			if result.Error != nil {
				log.Printf("[MORALIS] DB Error for %s: %v", wa.Address, result.Error)
			} else {
				log.Printf("[MORALIS] Saved native tx for %s", wa.Address)
				triggerNotification(walletAddr, chainInfo.Chain, dbTx.Amount, "native", dbTx.Direction)
				updateRedis(wa.Address, chainInfo.RedisKey, dbTx)
				if result.RowsAffected > 0 {
					insertedAny = true
				}
			}
		}
	}

	if insertedAny {
		tokenAddress := nativeTokenAddress(chainInfo.RedisKey)
		updateUserBalancesForTransfer(chainInfo.RedisKey, tx.FromAddress, tx.ToAddress, tokenAddress, amountStr)
	}
}

func processMoralisErc20Transfer(transfer MoralisErc20Transfer, txMap map[string]MoralisTx, chainInfo evmChainInfo, blockNumber int64, blockTime int64) {
	if transfer.TransactionHash == "" {
		return
	}

	decimals := 18
	if transfer.TokenDecimals != "" {
		if parsed, err := strconv.Atoi(transfer.TokenDecimals); err == nil {
			decimals = parsed
		}
	}

	amountStr := transfer.ValueWithDecimals
	if amountStr == "" {
		valueInt := parseBigInt(transfer.Value)
		if valueInt.Sign() == 0 {
			return
		}
		amountStr = formatTokenAmount(valueInt, decimals)
	}
	if amountStr == "" || amountStr == "0" {
		return
	}

	txRef, hasTx := txMap[strings.ToLower(transfer.TransactionHash)]
	gasUsed := big.NewInt(0)
	gasPrice := big.NewInt(0)
	if hasTx {
		gasUsed = parseBigInt(txRef.ReceiptGasUsed)
		if gasUsed.Sign() == 0 && txRef.Gas != "" {
			gasUsed = parseBigInt(txRef.Gas)
		}
		gasPrice = parseBigInt(txRef.GasPrice)
	}
	feeWei := new(big.Int).Mul(gasUsed, gasPrice)

	wallets := []string{transfer.From, transfer.To}
	insertedAny := false
	for _, walletAddr := range wallets {
		if walletAddr == "" {
			continue
		}

		waList, _ := resolveWalletAddresses(chainInfo.RedisKey, walletAddr)
		if len(waList) == 0 {
			continue
		}

		for _, wa := range waList {
			direction := "receive"
			if strings.EqualFold(walletAddr, transfer.From) {
				direction = "send"
			}

			dbTx := EvmTransactionHistory{
				WalletAddress:   wa.Address,
				TxHash:          transfer.TransactionHash,
				Chain:           chainInfo.Chain,
				BlockNumber:     blockNumber,
				BlockTime:       blockTime,
				FromAddress:     strings.ToLower(transfer.From),
				ToAddress:       strings.ToLower(transfer.To),
				Amount:          amountStr,
				GasUsed:         gasUsed.Int64(),
				GasPrice:        gasPrice.String(),
				NetworkFee:      feeWei.String(),
				Status:          "success",
				TransactionType: "transfer",
				Standard:        "erc20",
				ContractAddress: strings.ToLower(transfer.Contract),
				TokenName:       transfer.TokenName,
				TokenSymbol:     transfer.TokenSymbol,
				TokenDecimal:    uint8(decimals),
				Direction:       direction,
				CreatedAt:       time.Now(),
				GasLimit:        parseInt64(txRef.Gas),
			}
			if chainInfo.Chain == "ETH" {
				dbTx.ChainId = 1
				dbTx.ChainName = "Ethereum"
			} else if chainInfo.Chain == "POL" {
				dbTx.ChainId = 137
				dbTx.ChainName = "Polygon"
			} else if chainInfo.Chain == "BASE" {
				dbTx.ChainId = 8453
				dbTx.ChainName = "Base"
			} else if chainInfo.Chain == "BSC" {
				dbTx.ChainId = 56
				dbTx.ChainName = "BSC"
			}

			result := db.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "address"}, {Name: "tx_hash"}},
				DoNothing: true,
			}).Create(&dbTx)

			if result.Error != nil {
				log.Printf("[MORALIS] DB Error for %s: %v", wa.Address, result.Error)
			} else {
				log.Printf("[MORALIS] Saved ERC20 tx for %s", wa.Address)
				triggerNotification(walletAddr, chainInfo.Chain, dbTx.Amount, dbTx.TokenSymbol, dbTx.Direction)
				updateRedis(wa.Address, chainInfo.RedisKey, dbTx)
				if result.RowsAffected > 0 {
					insertedAny = true
				}
			}
		}
	}

	if insertedAny {
		tokenAddress := normalizeTokenAddress(chainInfo.RedisKey, transfer.Contract)
		if strings.TrimSpace(tokenAddress) == "" {
			tokenAddress = nativeTokenAddress(chainInfo.RedisKey)
		}
		updateUserBalancesForTransfer(chainInfo.RedisKey, transfer.From, transfer.To, tokenAddress, amountStr)
	}
}

// ---------------------------------------------------------
// 9. BTC PROCESSOR
// ---------------------------------------------------------
func processBtcTransaction(block BtcBlock, tx BtcTx) {
	if tx.Txid == "" {
		return
	}

	blockNumber := tx.BlockHeight
	if blockNumber == 0 {
		blockNumber = block.Height
	}
	blockTime := tx.BlockTime
	if blockTime == 0 {
		blockTime = block.Time
	}

	netByAddress := map[string]*big.Int{}

	for _, vin := range tx.Vin {
		if !vin.IsAddress {
			continue
		}
		valueInt := parseBigInt(vin.Value)
		for _, addr := range vin.Addresses {
			if addr == "" {
				continue
			}
			if netByAddress[addr] == nil {
				netByAddress[addr] = big.NewInt(0)
			}
			netByAddress[addr].Sub(netByAddress[addr], valueInt)
		}
	}

	for _, vout := range tx.Vout {
		if !vout.IsAddress {
			continue
		}
		valueInt := parseBigInt(vout.Value)
		for _, addr := range vout.Addresses {
			if addr == "" {
				continue
			}
			if netByAddress[addr] == nil {
				netByAddress[addr] = big.NewInt(0)
			}
			netByAddress[addr].Add(netByAddress[addr], valueInt)
		}
	}

	if len(netByAddress) == 0 {
		return
	}

	firstInputAddr := firstVinAddress(tx.Vin)
	firstOutputAddr := firstVoutAddress(tx.Vout)
	feeBtc := "0"
	feeInt := parseBigInt(tx.Fees)
	if feeInt.Sign() > 0 {
		feeBtc = formatTokenAmount(feeInt, 8)
	}

	for walletAddr, netAmount := range netByAddress {
		if netAmount.Sign() == 0 {
			continue
		}

		waList, _ := resolveWalletAddresses("btc", walletAddr)
		if len(waList) == 0 {
			continue
		}

		for _, wa := range waList {
			direction := "receive"
			absAmount := new(big.Int).Set(netAmount)
			if netAmount.Sign() < 0 {
				direction = "send"
				absAmount.Abs(netAmount)
			}
			amountStr := formatTokenAmount(absAmount, 8)
			networkFee := "0"
			fromAddr := ""
			toAddr := ""
			if direction == "send" {
				networkFee = feeBtc
				fromAddr = walletAddr
				toAddr = firstOutputAddr
			} else {
				fromAddr = firstInputAddr
				toAddr = walletAddr
			}

			dbTx := BtcTransactionHistory{
				WalletAddress:   wa.Address,
				TxHash:          tx.Txid,
				Chain:           "BTC",
				BlockNumber:     blockNumber,
				BlockTime:       blockTime,
				FromAddress:     fromAddr,
				ToAddress:       toAddr,
				Amount:          amountStr,
				NetworkFee:      networkFee,
				Status:          "success",
				TransactionType: "transfer",
				Direction:       direction,
				CreatedAt:       time.Now(),
				NetworkFeeSats:  feeInt.Int64(),
				Standard:        "native",
			}

			result := db.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "address"}, {Name: "tx_hash"}},
				DoNothing: true,
			}).Create(&dbTx)

			if result.Error != nil {
				log.Printf("[BTC] DB Error for %s: %v", wa.Address, result.Error)
			} else {
				log.Printf("[BTC] Saved tx for %s", wa.Address)
				triggerNotification(walletAddr, "BTC", dbTx.Amount, "BTC", dbTx.Direction)
				updateRedis(wa.Address, "btc", dbTx)
				if result.RowsAffected > 0 {
					delta := amountStr
					if netAmount.Sign() < 0 {
						delta = negateAmount(amountStr)
					}
					// Use wa directly to avoid re-resolving
					effectiveToken := nativeTokenAddress(wa.ChainID)
					if err := upsertUserBalance(wa.WalletID, wa.Address, wa.ChainID, effectiveToken, delta); err != nil {
						log.Printf("[BALANCE] update failed for %s (%s): %v", wa.Address, wa.ChainID, err)
					}
				}
			}
		}
	}
}

// ---------------------------------------------------------
// 10. HELPERS
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

func userBalanceRedisKey(walletID, chainID, address string) string {
	// Key format: user_balances:{wallet_id}:{chain_id}:{address} (hash of token_address -> balance)
	return fmt.Sprintf("%s:%s:%s:%s", userBalanceRedisKeyPrefix, walletID, chainID, address)
}

func nativeTokenAddress(chainID string) string {
	if v := strings.TrimSpace(os.Getenv("NATIVE_TOKEN_ADDRESS")); v != "" {
		return v
	}
	return "native"
}

func normalizeAddress(chainID, address string) string {
	if address == "" {
		return ""
	}
	switch strings.ToLower(chainID) {
	case "eth", "pol", "base", "bsc":
		return strings.ToLower(address)
	default:
		return address
	}
}

func normalizeTokenAddress(chainID, tokenAddress string) string {
	if tokenAddress == "" {
		return ""
	}
	switch strings.ToLower(chainID) {
	case "eth", "pol", "base", "bsc":
		return strings.ToLower(tokenAddress)
	default:
		return tokenAddress
	}
}

func negateAmount(amount string) string {
	amount = strings.TrimSpace(amount)
	if amount == "" || amount == "0" {
		return amount
	}
	if strings.HasPrefix(amount, "-") {
		return amount
	}
	return "-" + amount
}

func updateUserBalancesForTransfer(chainID, fromAddr, toAddr, tokenAddress, amountStr string) {
	amountStr = strings.TrimSpace(amountStr)
	if amountStr == "" || amountStr == "0" {
		return
	}
	if fromAddr != "" && toAddr != "" && strings.EqualFold(fromAddr, toAddr) {
		return
	}
	tokenAddress = strings.TrimSpace(tokenAddress)
	if tokenAddress == "" {
		tokenAddress = nativeTokenAddress(chainID)
	} else {
		tokenAddress = normalizeTokenAddress(chainID, tokenAddress)
	}

	if fromAddr != "" {
		updateUserBalanceForAddress(chainID, fromAddr, tokenAddress, negateAmount(amountStr))
	}
	if toAddr != "" {
		updateUserBalanceForAddress(chainID, toAddr, tokenAddress, amountStr)
	}
}

func updateUserBalanceForAddress(chainID, address, tokenAddress, delta string) {
	if db == nil {
		return
	}
	delta = strings.TrimSpace(delta)
	if address == "" || delta == "" || delta == "0" {
		return
	}

	walletAddrs, err := resolveWalletAddresses(chainID, address)
	if err != nil {
		log.Printf("[BALANCE] wallet lookup failed for %s (%s): %v", address, chainID, err)
		return
	}
	if len(walletAddrs) == 0 {
		log.Printf("[BALANCE] wallet_id not found for %s (%s)", address, chainID)
		return
	}

	for _, wa := range walletAddrs {
		effectiveToken := tokenAddress
		if strings.TrimSpace(effectiveToken) == "" {
			effectiveToken = nativeTokenAddress(wa.ChainID)
		} else {
			effectiveToken = normalizeTokenAddress(wa.ChainID, effectiveToken)
		}
		if err := upsertUserBalance(wa.WalletID, wa.Address, wa.ChainID, effectiveToken, delta); err != nil {
			log.Printf("[BALANCE] update failed for %s (%s): %v", wa.Address, wa.ChainID, err)
		}
	}
}

func resolveWalletAddresses(chainID, address string) ([]WalletAddress, error) {
	normalizedAddr := normalizeAddress(chainID, address)
	addrCandidates := []string{normalizedAddr}

	if isEvmChain(chainID) {
		if strings.HasPrefix(normalizedAddr, "0x") {
			addrCandidates = append(addrCandidates, strings.TrimPrefix(normalizedAddr, "0x"))
		} else {
			addrCandidates = append(addrCandidates, "0x"+normalizedAddr)
		}
	}

	if strings.EqualFold(chainID, "tron") {
		if base58Addr, err := HexToTronAddress(normalizedAddr); err == nil {
			if base58Addr != "" && !contains(addrCandidates, base58Addr) {
				addrCandidates = append(addrCandidates, base58Addr)
			}
		}
	}

	chainCandidates := getChainSynonyms(chainID)

	for _, chain := range chainCandidates {
		for _, addr := range addrCandidates {
			var rows []WalletAddress
			if err := db.Where("chain_id = ? AND (address = ? OR address_hex = ?)", chain, addr, addr).Find(&rows).Error; err != nil {
				return nil, err
			}
			if len(rows) > 0 {
				return rows, nil
			}
		}
	}

	return nil, nil
}

func getChainSynonyms(chainID string) []string {
	c := strings.ToLower(chainID)
	switch c {
	case "eth", "ethereum", "1", "0x1", "ethereum-mainnet":
		return []string{"ETH", "eth", "1", "0x1", "ethereum-mainnet", "ethereum"}
	case "tron", "tron-mainnet", "trx":
		return []string{"tron", "TRON", "tron-mainnet", "TRX"}
	case "btc", "bitcoin", "bitcoin-mainnet":
		return []string{"BTC", "btc", "bitcoin", "bitcoin-mainnet"}
	case "sol", "solana", "solana-mainnet":
		return []string{"solana", "SOL", "sol", "solana-mainnet"}
	case "pol", "polygon", "137", "0x89", "polygon-mainnet":
		return []string{"POL", "pol", "137", "0x89", "polygon-mainnet", "polygon"}
	case "base", "8453", "0x2105", "base-mainnet":
		return []string{"BASE", "base", "8453", "0x2105", "base-mainnet"}
	case "bsc", "56", "0x38", "bsc-mainnet", "binance-smart-chain":
		return []string{"BSC", "bsc", "56", "0x38", "bsc-mainnet", "binance-smart-chain"}
	default:
		return []string{chainID, strings.ToLower(chainID), strings.ToUpper(chainID)}
	}
}

func isEvmChain(chainID string) bool {
	c := strings.ToLower(chainID)
	return c == "eth" || c == "pol" || c == "base" || c == "bsc" || c == "1" || c == "137" || c == "8453" || c == "56" || c == "ethereum" || c == "polygon"
}

func contains(slice []string, val string) bool {
	for _, item := range slice {
		if strings.EqualFold(item, val) {
			return true
		}
	}
	return false
}

func upsertUserBalance(walletID, address, chainID, tokenAddress, delta string) error {
	now := time.Now()
	record := UserBalance{
		WalletID:     walletID,
		Address:      address,
		ChainID:      chainID,
		TokenAddress: tokenAddress,
		Balance:      delta,
		LastActive:   now,
	}

	result := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "wallet_id"},
			{Name: "address"},
			{Name: "chain_id"},
			{Name: "token_address"},
		},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"balance":     gorm.Expr("balance + ?", delta),
			"last_active": now,
		}),
	}).Create(&record)

	if result.Error != nil {
		return result.Error
	}

	updateUserBalanceRedisIfExists(walletID, chainID, address, tokenAddress)
	return nil
}

func updateUserBalanceRedisIfExists(walletID, chainID, address, tokenAddress string) {
	if rdb == nil {
		return
	}

	key := userBalanceRedisKey(walletID, chainID, address)
	exists, err := rdb.Exists(ctx, key).Result()
	if err != nil {
		log.Printf("[BALANCE] redis exists check failed for %s: %v", key, err)
		return
	}
	if exists == 0 {
		return
	}

	var updated UserBalance
	if err := db.Select("balance").Where(
		"wallet_id = ? AND address = ? AND chain_id = ? AND token_address = ?",
		walletID, address, chainID, tokenAddress,
	).First(&updated).Error; err != nil {
		log.Printf("[BALANCE] db read failed for %s (%s): %v", address, chainID, err)
		return
	}

	if err := rdb.HSet(ctx, key, tokenAddress, updated.Balance).Err(); err != nil {
		log.Printf("[BALANCE] redis update failed for %s: %v", key, err)
	}
}

func parseBigInt(value string) *big.Int {
	if value == "" {
		return big.NewInt(0)
	}
	base := 10
	if strings.HasPrefix(value, "0x") || strings.HasPrefix(value, "0X") {
		base = 0
	}
	parsed, ok := new(big.Int).SetString(value, base)
	if !ok {
		return big.NewInt(0)
	}
	return parsed
}

func parseInt64(value string) int64 {
	if value == "" {
		return 0
	}
	base := 10
	if strings.HasPrefix(value, "0x") || strings.HasPrefix(value, "0X") {
		base = 0
	}
	parsed, err := strconv.ParseInt(value, base, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func firstVinAddress(vins []BtcVin) string {
	for _, vin := range vins {
		if !vin.IsAddress {
			continue
		}
		if len(vin.Addresses) > 0 && vin.Addresses[0] != "" {
			return vin.Addresses[0]
		}
	}
	return ""
}

func firstVoutAddress(vouts []BtcVout) string {
	for _, vout := range vouts {
		if !vout.IsAddress {
			continue
		}
		if len(vout.Addresses) > 0 && vout.Addresses[0] != "" {
			return vout.Addresses[0]
		}
	}
	return ""
}
func sha256d(data []byte) []byte {
	first := sha256.Sum256(data)
	second := sha256.Sum256(first[:])
	return second[:]
}

func HexToTronAddress(hexAddr string) (string, error) {
	hexAddr = strings.TrimPrefix(hexAddr, "0x")
	addrBytes, err := hex.DecodeString(hexAddr)
	if err != nil {
		return "", err
	}
	if len(addrBytes) != 20 {
		return "", fmt.Errorf("invalid address length: got %d bytes", len(addrBytes))
	}
	prefixed := append([]byte{0x41}, addrBytes...)
	checksum := sha256d(prefixed)[:4]
	final := append(prefixed, checksum...)
	return base58.Encode(final), nil
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

func triggerNotification(address string, chain string, amount string, symbol string, direction string) {
	if symbol == "" || symbol == "native" {
		switch strings.ToLower(chain) {
		case "tron":
			symbol = "TRX"
		case "btc":
			symbol = "BTC"
		case "eth":
			symbol = "ETH"
		default:
			symbol = strings.ToUpper(chain)
		}
	}

	// 1. Find the WalletID(s) associated with this address
	var walletAddrs []WalletAddress
	err := db.Where("address = ? AND chain_id = ?", normalizeAddress(chain, address), chain).Find(&walletAddrs).Error
	if err != nil {
		log.Printf("[Notification] DB error looking up wallet for %s: %v", address, err)
		return
	}
	if len(walletAddrs) == 0 {
		return
	}

	// 2. Broadcast to all devices linked to these WalletIDs
	for _, wa := range walletAddrs {
		var devices []UserWalletDevice
		db.Where("wallet_id = ?", wa.WalletID).Find(&devices)

		title := "Transaction Detected"
		body := ""

		if direction == "send" {
			title = "Transaction Sent"
			body = fmt.Sprintf("Successfully sent %s %s on %s network", amount, symbol, chain)
		} else {
			title = "Transaction Received"
			body = fmt.Sprintf("You received %s %s on %s network", amount, symbol, chain)
		}

		data := map[string]string{
			"wallet_id": wa.WalletID,
			"address":   address,
			"amount":    amount,
			"symbol":    symbol,
			"chain":     chain,
			"direction": direction,
			"type":      "transaction_alert",
		}

		for _, dev := range devices {
			err := SendPushNotification(dev.DeviceToken, dev.DeviceType, title, body, data)
			if err != nil {
				log.Printf("[Notification] Failed to send to %s: %v", dev.DeviceToken, err)
				if strings.Contains(err.Error(), ErrTokenInvalid) {
					log.Printf("[Notification] Purging invalid token: %s", dev.DeviceToken)
					//db.Where("device_token = ?", dev.DeviceToken).Delete(&UserWalletDevice{})
				}
			} else {
				log.Printf("[Notification] Successfully triggered for %s (%s)", address, direction)
			}
		}
	}
}
