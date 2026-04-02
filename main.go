package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
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

	priceHTTPClient     = &http.Client{Timeout: 5 * time.Second}
	nativePriceCacheMu  sync.Mutex
	nativePriceCache    = map[string]priceCacheEntry{}
	nativePriceCacheTTL = 60 * time.Second
	webhookForwardURL   = "https://test.first.digiedgete.click/webhook-listener"
	webhookForwardHTTP  = &http.Client{Timeout: 20 * time.Second}

	walletResolveCacheMu         sync.RWMutex
	walletResolveCache           = map[string]walletResolveCacheEntry{}
	walletResolveCacheTTL        = 30 * time.Second
	walletResolveCacheMaxEntries = 20000

	enableBalanceAuditLogs   = true
	errInvalidWebhookPayload = errors.New("invalid webhook payload")
)

type walletResolveCacheEntry struct {
	rows      []WalletAddress
	expiresAt time.Time
}

const (
	txTypeDefaultTransfer          = "SMART_CONTRACT_INTERACTION"
	txTypeSmartContractInteraction = "SMART_CONTRACT_INTERACTION"
	txTypeNativeTransfer           = "NATIVE_TRANSFER"
	txTypeTokenTransfer            = "TOKEN_TRANSFER"
	txTypeNftTransfer              = "NFT_TRANSFER"
	txTypeSwap                     = "SWAP"
	txTypeApproval                 = "APPROVAL"
)

var (
	evmApprovalEventTopicPrefix = "0x8c5be1e5"
	evmSwapEventV2Topic         = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
	evmSwapEventV3Topic         = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
	evmTransferEventTopic       = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	erc1155TransferSinglePrefix = "0xc3d58168"
	erc1155TransferBatchPrefix  = "0x4a39dc06"
	erc20TransferMethodID       = "0xa9059cbb"
	solanaMetaplexProgramID     = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"

	evmApprovalMethodSelectors = map[string]struct{}{
		"0x095ea7b3": {}, // approve(address,uint256)
		"0xa22cb465": {}, // setApprovalForAll(address,bool)
	}
	evmSwapMethodSelectors = map[string]struct{}{
		"0x14d08fca": {}, // onChainSwaps() - Rango router style aggregator swap
	}
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
	ID             int64     `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	WalletID       string    `gorm:"column:wallet_id;type:varchar(255);not null;index:idx_addr_wallet_chain;uniqueIndex:uq_wallet_chain_index,priority:1;uniqueIndex:uq_wallet_chain_address,priority:1" json:"wallet_id"`
	Wallet         Wallet    `gorm:"constraint:OnDelete:CASCADE" json:"-"`
	ChainID        string    `gorm:"column:chain_id;size:64;not null;index:idx_addr_wallet_chain;index:idx_chain_address,priority:1;uniqueIndex:uq_wallet_chain_index,priority:2;uniqueIndex:uq_wallet_chain_address,priority:2" json:"chain_id"`
	IndexN         int       `gorm:"column:index_n;not null;uniqueIndex:uq_wallet_chain_index,priority:3" json:"index_n"`
	Address        string    `gorm:"column:address;size:128;not null;index:idx_chain_address,priority:2;uniqueIndex:uq_wallet_chain_address,priority:3" json:"address"`
	DerivationPath *string   `gorm:"column:derivation_path;size:255" json:"derivation_path"`
	AddressHex     *string   `gorm:"column:address_hex;size:255" json:"address_hex"`
	CreatedAt      time.Time `gorm:"column:created_at;autoCreateTime;type:timestamp" json:"created_at"`
	UpdatedAt      time.Time `gorm:"column:updated_at;autoUpdateTime;type:timestamp" json:"updated_at"`
	Label          string    `gorm:"column:label;size:255;not null" json:"label"`
	Active         bool      `gorm:"column:active;not null;default:true" json:"active"`
}

type Wallet struct {
	WalletID           string          `gorm:"column:wallet_id;type:varchar(255);primaryKey" json:"wallet_id"`
	Label              string          `gorm:"column:label;size:255;not null" json:"label"`
	CreatedAt          time.Time       `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt          time.Time       `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
	Addresses          []WalletAddress `gorm:"constraint:OnDelete:CASCADE" json:"addresses"`
	Active             bool            `gorm:"column:active;not null;default:true" json:"active"`
	IsPrivateKeyImport bool            `gorm:"column:is_private_key_import;not null;default:false" json:"is_private_key_import"`
}

func (Wallet) TableName() string {
	return "wallets"
}

// UserWalletDevice represents the link between a WalletID and a physical device (iOS/Android)
type UserWalletDevice struct {
	WalletID    string    `gorm:"primaryKey;type:char(64)" json:"wallet_id"`
	DeviceToken string    `gorm:"primaryKey;size:255" json:"device_token"`
	DeviceType  string    `gorm:"size:10" json:"device_type"` // ios or android
	CreatedAt   time.Time `gorm:"autoCreateTime"`
}

// NotificationLog stores the history/status of every push notification sent
type NotificationLog struct {
	ID          int64     `gorm:"primaryKey;autoIncrement"`
	WalletID    string    `gorm:"size:64;index:idx_log_wallet"`
	DeviceToken string    `gorm:"size:255;index:idx_log_token"`
	DeviceType  string    `gorm:"size:10"`
	Title       string    `gorm:"size:255"`
	Body        string    `gorm:"type:text"`
	Status      string    `gorm:"size:20"` // success or failed
	ErrorMsg    string    `gorm:"type:text"`
	TxHash      string    `gorm:"size:100;index:idx_log_tx"`
	Chain       string    `gorm:"size:20"`
	Direction   string    `gorm:"size:10"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
}

func (t *WalletTransactionHistory) TableName() string { return "tron_transaction_histories" }
func (t *EvmTransactionHistory) TableName() string    { return "evm_transaction_histories" }
func (t *BtcTransactionHistory) TableName() string    { return "btc_transaction_histories" }
func (t *SolTransactionHistory) TableName() string    { return "sol_transaction_histories" }
func (u *UserBalance) TableName() string              { return "user_balances" }
func (w *WalletAddress) TableName() string            { return "wallet_addresses" }
func (d *UserWalletDevice) TableName() string         { return "user_wallet_devices" }
func (l *NotificationLog) TableName() string          { return "notification_logs" }

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
	BlockNumber     int    `json:"block_number"`
	BlockTime       int64  `json:"block_time"`
	From            string `json:"from_address"`
	To              string `json:"to_address"`
	Standard        string `json:"standard"`
	TxHash          string `json:"tx_hash"`
	Value           string `json:"token_amount"`
	Contract        string `json:"contract_address"`
	TransactionType string `json:"transaction_type,omitempty"`
	MethodId        string `json:"method_id,omitempty"`
	TxFrom          string `json:"tx_from,omitempty"`
	TxTo            string `json:"tx_to,omitempty"`
	TxInput         string `json:"tx_input,omitempty"`

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
	Confirmed         bool                   `json:"confirmed"`
	ChainId           string                 `json:"chainId"`
	Block             MoralisBlock           `json:"block"`
	Txs               []MoralisTx            `json:"txs"`
	Logs              []MoralisLog           `json:"logs"`
	TxsInternal       []MoralisInternalTx    `json:"txsInternal"`
	Erc20Transfers    []MoralisErc20Transfer `json:"erc20Transfers"`
	Erc20Approvals    []MoralisErc20Approval `json:"erc20Approvals"`
	NftApprovals      MoralisNftApprovals    `json:"nftApprovals"`
	NftTokenApprovals []MoralisApprovalEvent `json:"nftTokenApprovals"`
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

type MoralisInternalTx struct {
	From            string `json:"from"`
	To              string `json:"to"`
	Value           string `json:"value"`
	TransactionHash string `json:"transactionHash"`
}

type MoralisLog struct {
	TransactionHash string `json:"transactionHash"`
	Address         string `json:"address"`
	Topic0          string `json:"topic0"`
	Topic1          string `json:"topic1"`
	Topic2          string `json:"topic2"`
	Topic3          string `json:"topic3"`
}

type MoralisApprovalEvent struct {
	TransactionHash string `json:"transactionHash"`
}

type MoralisErc20Approval struct {
	TransactionHash string `json:"transactionHash"`
	Contract        string `json:"contract"`
	Owner           string `json:"owner"`
	Spender         string `json:"spender"`
	Value           string `json:"value"`
	TokenName       string `json:"tokenName,omitempty"`
	TokenSymbol     string `json:"tokenSymbol,omitempty"`
	TokenDecimals   string `json:"tokenDecimals,omitempty"`
}

type MoralisNftApprovals struct {
	ERC721  []MoralisApprovalEvent `json:"ERC721"`
	ERC1155 []MoralisApprovalEvent `json:"ERC1155"`
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
	Transactions []BtcTx `json:"transactions"`
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
	Err               interface{}       `json:"err"`
	Fee               int64             `json:"fee"`
	PreBalances       []int64           `json:"preBalances"`
	PostBalances      []int64           `json:"postBalances"`
	PreTokenBalances  []SolTokenBalance `json:"preTokenBalances"`
	PostTokenBalances []SolTokenBalance `json:"postTokenBalances"`
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
	ProgramId string          `json:"programId"`
	Program   string          `json:"program,omitempty"`
	Parsed    json.RawMessage `json:"parsed,omitempty"`
}

type SolParsed struct {
	Info SolParsedInfo `json:"info"`
	Type string        `json:"type"`
}

type SolParsedInfo struct {
	Destination string          `json:"destination"`
	Source      string          `json:"source"`
	Delegate    string          `json:"delegate"`
	Mint        string          `json:"mint"`
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

type SolTokenBalance struct {
	AccountIndex  int            `json:"accountIndex"`
	Mint          string         `json:"mint"`
	Owner         string         `json:"owner"`
	UiTokenAmount SolTokenAmount `json:"uiTokenAmount"`
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
	chainEventsStreamName     = "chain_events"
)

//Load Addresses From DB into Redis

// ---------------------------------------------------------
// 3. MAIN & INIT
// ---------------------------------------------------------

func main() {
	// err := godotenv.Load("cfg.env")
	// if err != nil {
	// 	log.Printf("Notice: cfg.env not found or error loading it: %v", err)
	// }

	initDatabase()
	initRedis()
	InitNotificationService()

	http.HandleFunc("/quicknode-webhook", webhookHandler)
	http.HandleFunc("/webhook-listener", webhookHandler)

	http.HandleFunc("/quicknode-webhook/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("/migration/health", func(w http.ResponseWriter, r *http.Request) {
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
		//DbUrl = "token13:token132026@tcp(43.204.164.104:3306)/token13_app?charset=utf8mb4&parseTime=True"
		DbUrl = "root:Password@tcp(127.0.0.1:3306)/token13_app?parseTime=True"
		log.Printf("DATABASE_URL not set, using default: %s", DbUrl)
	}

	var err error
	db, err = gorm.Open(mysql.Open(DbUrl), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to db: %v", err)
	}

	if err := db.AutoMigrate(
		&WalletTransactionHistory{},
		&EvmTransactionHistory{},
		&BtcTransactionHistory{},
		&SolTransactionHistory{},
		&UserBalance{},
		&WalletAddress{},
		&UserWalletDevice{},
		&NotificationLog{},
	); err != nil {
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
func AutoMigrate(gdb *gorm.DB) error {
	if gdb == nil {
		return fmt.Errorf("db is nil")
	}
	orig := gdb.Config.DisableForeignKeyConstraintWhenMigrating
	gdb.Config.DisableForeignKeyConstraintWhenMigrating = true
	err := gdb.AutoMigrate(
		&Wallet{},
		&WalletAddress{},
		&UserBalance{},
		&UserWalletDevice{},
		&EvmTransactionHistory{},
		&SolTransactionHistory{},
	)
	gdb.Config.DisableForeignKeyConstraintWhenMigrating = orig
	if err != nil {
		return err
	}

	// Verify crucial column addition for Wallet table
	if !gdb.Migrator().HasColumn(&Wallet{}, "is_private_key_import") {
		return fmt.Errorf("migration failed: column is_private_key_import missing in wallets table")
	}

	// Migration: allow the same chain/address to exist across different wallet IDs.
	// Previous schema enforced global uniqueness on (chain_id, address) via uq_chain_address.
	if gdb.Migrator().HasIndex(&WalletAddress{}, "uq_chain_address") {
		if err := gdb.Migrator().DropIndex(&WalletAddress{}, "uq_chain_address"); err != nil {
			return fmt.Errorf("failed to drop legacy index uq_chain_address: %w", err)
		}
	}
	if !gdb.Migrator().HasIndex(&WalletAddress{}, "uq_wallet_chain_address") {
		if err := gdb.Migrator().CreateIndex(&WalletAddress{}, "uq_wallet_chain_address"); err != nil {
			return fmt.Errorf("failed to create index uq_wallet_chain_address: %w", err)
		}
	}

	return nil
}

// ---------------------------------------------------------
// 4. WEBHOOK HANDLER
// ---------------------------------------------------------

func webhookHandler(w http.ResponseWriter, r *http.Request) {
	reqID := fmt.Sprintf("req-%d", time.Now().UnixNano())
	start := time.Now()
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("[REQUEST %s] Failure: panic: %v", reqID, rec)
			log.Printf("[REQUEST %s] Panic stack: %s", reqID, debug.Stack())
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
	}()

	if r.Method != http.MethodPost {
		log.Printf("[REQUEST %s] Failure: method not allowed (%s)", reqID, r.Method)
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("[REQUEST %s] Failure: read error: %v (Body length: %d)", reqID, err, len(bodyBytes))
		http.Error(w, "Read Error", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	if len(bodyBytes) == 0 {
		log.Printf("[REQUEST %s] Failure: empty body received. Content-Length: %d", reqID, r.ContentLength)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Empty body"))
		return
	}
	forwardWebhookListenerRequest(reqID, r, bodyBytes)

	log.Printf("[REQUEST %s] Received. Method: %s, URL: %s, Content-Length: %d, Actual body length: %d", reqID, r.Method, r.URL.Path, r.ContentLength, len(bodyBytes))
	log.Printf("[REQUEST %s] Payload body: [%s]", reqID, string(bodyBytes))

	if err := processWebhookPayloads(reqID, bodyBytes); err != nil {
		statusCode := http.StatusInternalServerError
		statusText := "Internal Server Error"
		if errors.Is(err, errInvalidWebhookPayload) {
			statusCode = http.StatusBadRequest
			statusText = "Bad Request"
		}
		log.Printf("[REQUEST %s] Failure: webhook producer error: %v", reqID, err)
		http.Error(w, statusText, statusCode)
		return
	}

	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("Accepted")); err != nil {
		log.Printf("[REQUEST %s] Failure: write response error: %v", reqID, err)
		return
	}
	log.Printf("[REQUEST %s] Accepted. AckDuration=%s", reqID, time.Since(start))
}

func forwardWebhookListenerRequest(reqID string, r *http.Request, bodyBytes []byte) {
	targetURL, err := url.Parse(webhookForwardURL)
	if err != nil {
		log.Printf("[REQUEST %s] Forward failure: invalid target URL %q: %v", reqID, webhookForwardURL, err)
		return
	}
	targetURL.RawQuery = r.URL.RawQuery

	upstreamReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL.String(), bytes.NewReader(bodyBytes))
	if err != nil {
		log.Printf("[REQUEST %s] Forward failure: build request error: %v", reqID, err)
		return
	}
	copyHTTPHeaders(upstreamReq.Header, r.Header)

	resp, err := webhookForwardHTTP.Do(upstreamReq)
	if err != nil {
		log.Printf("[REQUEST %s] Forward failure: upstream request error: %v", reqID, err)
		return
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		log.Printf("[REQUEST %s] Forward warning: response body drain error: %v", reqID, err)
	}
	log.Printf("[REQUEST %s] Forwarded to %s with status=%d body_len=%d", reqID, targetURL.String(), resp.StatusCode, len(bodyBytes))
}

func copyHTTPHeaders(dst, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func processWebhookPayloads(reqID string, bodyBytes []byte) error {
	events, err := extractWebhookChainEvents(reqID, bodyBytes)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		log.Printf("[REQUEST %s] No chain events extracted from payload", reqID)
		return nil
	}

	if rdb == nil {
		return fmt.Errorf("redis client is not initialized")
	}

	for _, event := range events {
		if err := enqueueChainEvent(reqID, event); err != nil {
			return err
		}
	}

	log.Printf("[REQUEST %s] Enqueued %d chain events to stream=%s", reqID, len(events), chainEventsStreamName)
	return nil
}

type chainEventMessage struct {
	WalletAddress string
	ChainID       string
	TxHash        string
}

func enqueueChainEvent(reqID string, event chainEventMessage) error {
	xAddCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	msgID, err := rdb.XAdd(xAddCtx, &redis.XAddArgs{
		Stream: chainEventsStreamName,
		Values: map[string]interface{}{
			"wallet_address": event.WalletAddress,
			"chain_id":       event.ChainID,
			"tx_hash":        event.TxHash,
		},
	}).Result()
	if err != nil {
		return fmt.Errorf("redis xadd failed stream=%s wallet=%s chain_id=%s tx_hash=%s: %w", chainEventsStreamName, event.WalletAddress, event.ChainID, event.TxHash, err)
	}

	log.Printf("[REQUEST %s] Enqueued chain event stream=%s id=%s wallet=%s chain_id=%s tx_hash=%s", reqID, chainEventsStreamName, msgID, event.WalletAddress, event.ChainID, event.TxHash)
	return nil
}

func extractWebhookChainEvents(reqID string, bodyBytes []byte) ([]chainEventMessage, error) {
	var payloads [][]byte
	bodyTrimmed := strings.TrimSpace(string(bodyBytes))
	if strings.HasPrefix(bodyTrimmed, "[") {
		var rawMessages []json.RawMessage
		if err := json.Unmarshal(bodyBytes, &rawMessages); err != nil {
			return nil, fmt.Errorf("%w: invalid json array: %v", errInvalidWebhookPayload, err)
		}
		for _, msg := range rawMessages {
			payloads = append(payloads, []byte(msg))
		}
	} else {
		payloads = append(payloads, bodyBytes)
	}

	events := make([]chainEventMessage, 0)
	seen := make(map[string]struct{})
	trackedAddressCache := make(map[string]bool)
	addEvent := func(chainID, txHash string, walletAddresses ...string) error {
		chainID = strings.TrimSpace(chainID)
		txHash = normalizeChainEventTxHash(chainID, txHash)
		if chainID == "" || txHash == "" {
			return nil
		}
		for _, walletAddress := range walletAddresses {
			normalizedWallet := normalizeChainEventWalletAddress(chainID, walletAddress)
			if normalizedWallet == "" {
				continue
			}
			trackedKey := strings.ToLower(strings.TrimSpace(chainID)) + "|" + strings.ToLower(strings.TrimSpace(normalizedWallet))
			isTracked, cached := trackedAddressCache[trackedKey]
			if !cached {
				var lookupErr error
				isTracked, lookupErr = isTrackedWalletAddress(chainID, normalizedWallet)
				if lookupErr != nil {
					return lookupErr
				}
				trackedAddressCache[trackedKey] = isTracked
			}
			if !isTracked {
				continue
			}
			uniqueKey := chainID + "|" + txHash + "|" + normalizedWallet
			if _, exists := seen[uniqueKey]; exists {
				continue
			}
			seen[uniqueKey] = struct{}{}
			events = append(events, chainEventMessage{
				WalletAddress: normalizedWallet,
				ChainID:       chainID,
				TxHash:        txHash,
			})
		}
		return nil
	}

	for i, currentPayload := range payloads {
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(currentPayload, &envelope); err != nil {
			return nil, fmt.Errorf("%w: invalid json item %d: %v", errInvalidWebhookPayload, i, err)
		}

		switch {
		case envelope["metadata"] != nil && envelope["transfers"] != nil:
			var payload QuickNodePayload
			if err := json.Unmarshal(currentPayload, &payload); err != nil {
				return nil, fmt.Errorf("%w: invalid quicknode item %d: %v", errInvalidWebhookPayload, i, err)
			}
			chainID := producerChainIDFromQuickNodeNetwork(payload.Metadata.Network)
			log.Printf("[REQUEST %s] Extracting (item %d): type=quicknode chain_id=%s network=%s transfers=%d", reqID, i, chainID, payload.Metadata.Network, len(payload.Transfers))
			for _, tx := range payload.Transfers {
				if err := addEvent(chainID, tx.TxHash, tx.From, tx.To, tx.TxFrom, tx.TxTo); err != nil {
					return nil, err
				}
			}
		case envelope["confirmed"] != nil && envelope["chainId"] != nil:
			var payload MoralisEvmPayload
			if err := json.Unmarshal(currentPayload, &payload); err != nil {
				return nil, fmt.Errorf("%w: invalid moralis item %d: %v", errInvalidWebhookPayload, i, err)
			}
			chainID := producerChainIDFromMoralisChainID(payload.ChainId)
			log.Printf("[REQUEST %s] Extracting (item %d): type=moralis chain_id=%s chainId=%s native=%d internal=%d erc20=%d approvals=%d", reqID, i, chainID, payload.ChainId, len(payload.Txs), len(payload.TxsInternal), len(payload.Erc20Transfers), len(payload.Erc20Approvals))
			for _, tx := range payload.Txs {
				if err := addEvent(chainID, tx.Hash, tx.FromAddress, tx.ToAddress); err != nil {
					return nil, err
				}
			}
			for _, internalTx := range payload.TxsInternal {
				if err := addEvent(chainID, internalTx.TransactionHash, internalTx.From, internalTx.To); err != nil {
					return nil, err
				}
			}
			for _, transfer := range payload.Erc20Transfers {
				if err := addEvent(chainID, transfer.TransactionHash, transfer.From, transfer.To); err != nil {
					return nil, err
				}
			}
			for _, approval := range payload.Erc20Approvals {
				if err := addEvent(chainID, approval.TransactionHash, approval.Owner, approval.Spender); err != nil {
					return nil, err
				}
			}
		case envelope["transactions"] != nil:
			var payload QuickNodeBtcPayload
			if err := json.Unmarshal(currentPayload, &payload); err != nil {
				return nil, fmt.Errorf("%w: invalid btc item %d: %v", errInvalidWebhookPayload, i, err)
			}
			const btcChainID = "btc"
			log.Printf("[REQUEST %s] Extracting (item %d): type=btc count=%d", reqID, i, len(payload.Transactions))
			for _, tx := range payload.Transactions {
				for _, vin := range tx.Vin {
					if err := addEvent(btcChainID, tx.Txid, vin.Addresses...); err != nil {
						return nil, err
					}
				}
				for _, vout := range tx.Vout {
					if err := addEvent(btcChainID, tx.Txid, vout.Addresses...); err != nil {
						return nil, err
					}
				}
			}
		case envelope["matches"] != nil:
			var payload QuickNodeSolanaPayload
			if err := json.Unmarshal(currentPayload, &payload); err != nil {
				return nil, fmt.Errorf("%w: invalid solana item %d: %v", errInvalidWebhookPayload, i, err)
			}
			const solanaChainID = "solana"
			log.Printf("[REQUEST %s] Extracting (item %d): type=solana slot=%d matches=%d", reqID, i, payload.Slot, len(payload.Matches))
			for _, match := range payload.Matches {
				signature := ""
				if len(match.Transaction.Signatures) > 0 {
					signature = strings.TrimSpace(match.Transaction.Signatures[0])
				}
				if signature == "" {
					continue
				}

				for _, accountKey := range match.Transaction.Message.AccountKeys {
					if err := addEvent(solanaChainID, signature, accountKey.Pubkey); err != nil {
						return nil, err
					}
				}

				for _, instruction := range match.Transaction.Message.Instructions {
					parsed, ok := parseSolParsed(instruction.Parsed)
					if !ok {
						continue
					}
					if err := addEvent(solanaChainID, signature, parsed.Info.Source, parsed.Info.Destination, parsed.Info.Authority, parsed.Info.Delegate); err != nil {
						return nil, err
					}
				}

				for _, preBalance := range match.Meta.PreTokenBalances {
					if err := addEvent(solanaChainID, signature, preBalance.Owner); err != nil {
						return nil, err
					}
				}
				for _, postBalance := range match.Meta.PostTokenBalances {
					if err := addEvent(solanaChainID, signature, postBalance.Owner); err != nil {
						return nil, err
					}
				}
			}
		default:
			if envelope["message"] != nil {
				msg, _ := envelope["message"]
				msgStr := string(msg)
				if strings.Contains(msgStr, "PING") {
					log.Printf("[REQUEST %s] Pong (item %d)", reqID, i)
					continue
				}
			}
			return nil, fmt.Errorf("%w: unsupported payload item %d", errInvalidWebhookPayload, i)
		}
	}

	return events, nil
}

func producerChainIDFromMoralisChainID(chainID string) string {
	switch strings.ToLower(strings.TrimSpace(chainID)) {
	case "0x1", "1", "eth", "ethereum", "ethereum-mainnet":
		return "1"
	case "0x89", "137", "pol", "polygon", "polygon-mainnet":
		return "137"
	case "0x2105", "8453", "base", "base-mainnet":
		return "8453"
	case "0x38", "56", "bsc", "bsc-mainnet", "binance-smart-chain":
		return "56"
	default:
		return parseNumericChainIDOrFallback(chainID)
	}
}

func producerChainIDFromQuickNodeNetwork(network string) string {
	n := strings.ToLower(strings.TrimSpace(network))
	switch {
	case n == "65" || n == "tron" || n == "tron-mainnet" || n == "trx" || strings.Contains(n, "tron"):
		return "65"
	case n == "8453" || n == "0x2105" || n == "base" || n == "base-mainnet" || strings.Contains(n, "base"):
		return "8453"
	case n == "137" || n == "0x89" || n == "pol" || n == "polygon-mainnet" || strings.Contains(n, "polygon"):
		return "137"
	case n == "56" || n == "0x38" || n == "bsc" || n == "bsc-mainnet" || strings.Contains(n, "bsc") || strings.Contains(n, "binance"):
		return "56"
	case n == "1" || n == "0x1" || n == "eth" || n == "ethereum-mainnet" || strings.Contains(n, "ethereum") || strings.Contains(n, "eth"):
		return "1"
	default:
		return parseNumericChainIDOrFallback(network)
	}
}

func parseNumericChainIDOrFallback(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "0x") || strings.HasPrefix(trimmed, "0X") {
		parsed, err := strconv.ParseUint(trimmed[2:], 16, 64)
		if err == nil {
			return strconv.FormatUint(parsed, 10)
		}
		return strings.ToLower(trimmed)
	}
	if parsed, err := strconv.ParseUint(trimmed, 10, 64); err == nil {
		return strconv.FormatUint(parsed, 10)
	}
	return strings.ToLower(trimmed)
}

func normalizeChainEventWalletAddress(chainID, walletAddress string) string {
	return strings.TrimSpace(normalizeAddress(chainID, walletAddress))
}

func normalizeChainEventTxHash(chainID, txHash string) string {
	h := strings.TrimSpace(txHash)
	if h == "" {
		return ""
	}
	normalizedChain := strings.ToLower(strings.TrimSpace(chainID))
	if normalizedChain == "65" || isEvmChain(normalizedChain) {
		return strings.ToLower(h)
	}
	return h
}

func isTrackedWalletAddress(chainID, walletAddress string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("database client is not initialized")
	}

	normalizedAddr := normalizeAddress(chainID, walletAddress)
	if normalizedAddr == "" {
		return false, nil
	}

	chainCandidates := uniqueNonEmptyStrings(getChainSynonyms(chainID))
	if len(chainCandidates) == 0 {
		chainCandidates = []string{strings.TrimSpace(chainID)}
	}

	addrCandidates := []string{normalizedAddr}
	c := strings.ToLower(strings.TrimSpace(chainID))
	if c == "tron" || c == "tron-mainnet" || c == "trx" || c == "65" {
		if base58Addr, err := HexToTronAddress(normalizedAddr); err == nil && base58Addr != "" && !contains(addrCandidates, base58Addr) {
			addrCandidates = append(addrCandidates, base58Addr)
		}
	}
	addrCandidates = uniqueNonEmptyStrings(addrCandidates)

	var row WalletAddress
	if err := db.Select("id").Where("chain_id IN ? AND address IN ?", chainCandidates, addrCandidates).Limit(1).Take(&row).Error; err == nil {
		return true, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return false, err
	}

	if err := db.Select("id").Where("chain_id IN ? AND address_hex IN ?", chainCandidates, addrCandidates).Limit(1).Take(&row).Error; err == nil {
		return true, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return false, err
	}

	lowerChains := lowerCopy(chainCandidates)
	lowerAddrs := lowerCopy(addrCandidates)

	if err := db.Select("id").Where("LOWER(chain_id) IN ? AND LOWER(address) IN ?", lowerChains, lowerAddrs).Limit(1).Take(&row).Error; err == nil {
		return true, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return false, err
	}

	if err := db.Select("id").Where("LOWER(chain_id) IN ? AND LOWER(address_hex) IN ?", lowerChains, lowerAddrs).Limit(1).Take(&row).Error; err == nil {
		return true, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return false, err
	}

	return false, nil
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

func chainInfoFromQuickNodeNetwork(network string) evmChainInfo {
	n := strings.ToLower(strings.TrimSpace(network))
	switch {
	case n == "8453" || n == "0x2105" || strings.Contains(n, "base"):
		return evmChainInfo{Chain: "BASE", RedisKey: "base"}
	case n == "137" || n == "0x89" || strings.Contains(n, "polygon") || n == "pol":
		return evmChainInfo{Chain: "POL", RedisKey: "pol"}
	case n == "56" || n == "0x38" || strings.Contains(n, "bsc") || strings.Contains(n, "binance"):
		return evmChainInfo{Chain: "BSC", RedisKey: "bsc"}
	case n == "1" || n == "0x1" || strings.Contains(n, "ethereum") || strings.Contains(n, "eth"):
		return evmChainInfo{Chain: "ETH", RedisKey: "eth"}
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
	chainInfo := chainInfoFromQuickNodeNetwork(network)
	tronTxTypeByHash := map[string]string{}
	evmTxTypeByHash := map[string]string{}
	if chainFamily == "tron" {
		tronTxTypeByHash = classifyQuickNodeTronTxTypes(payload.Transfers)
	} else {
		evmTxTypeByHash = classifyQuickNodeEvmTxTypes(payload.Transfers)
	}

	for _, tx := range payload.Transfers {
		if chainFamily == "tron" {
			processTronTransaction(tx, tronTxTypeByHash)
		} else {
			processEvmTransaction(tx, chainInfo, evmTxTypeByHash)
		}
	}
}

func handleMoralisPayload(payload MoralisEvmPayload) {
	chainInfo := chainInfoFromMoralisChainId(payload.ChainId)
	blockNumber := parseInt64(payload.Block.Number)
	blockTime := parseInt64(payload.Block.Timestamp)

	log.Printf("Received Moralis Batch. Chain: %s | Native Tx: %d | Internal: %d | ERC20: %d", chainInfo.Chain, len(payload.Txs), len(payload.TxsInternal), len(payload.Erc20Transfers))

	txMap := make(map[string]MoralisTx, len(payload.Txs))
	for _, tx := range payload.Txs {
		if tx.Hash == "" {
			continue
		}
		txMap[strings.ToLower(tx.Hash)] = tx
	}
	txTypeByHash := classifyMoralisTxTypes(payload, chainInfo)

	for _, tx := range payload.Txs {
		processMoralisNativeTx(tx, chainInfo, blockNumber, blockTime, txTypeByHash)
	}

	for _, internalTx := range payload.TxsInternal {
		processMoralisInternalTx(internalTx, txMap, chainInfo, blockNumber, blockTime, txTypeByHash)
	}

	for _, approval := range payload.Erc20Approvals {
		processMoralisErc20Approval(approval, txMap, chainInfo, blockNumber, blockTime, txTypeByHash)
	}

	for _, transfer := range payload.Erc20Transfers {
		processMoralisErc20Transfer(transfer, txMap, chainInfo, blockNumber, blockTime, txTypeByHash)
	}
}

func handleBtcPayload(payload QuickNodeBtcPayload) {
	log.Printf("Received BTC Batch. Count: %d", len(payload.Transactions))
	for _, tx := range payload.Transactions {
		processBtcTransaction(BtcBlock{Height: tx.BlockHeight, Time: tx.BlockTime}, tx)
	}
}

func handleSolanaPayload(payload QuickNodeSolanaPayload) {
	log.Printf("Received Solana Batch. Slot: %d | Matches: %d", payload.Slot, len(payload.Matches))
	for _, match := range payload.Matches {
		processSolanaTransaction(match, payload.Slot, payload.BlockTime)
	}
}

func parseSolParsed(raw json.RawMessage) (SolParsed, bool) {
	if len(raw) == 0 {
		return SolParsed{}, false
	}

	var parsed SolParsed
	if err := json.Unmarshal(raw, &parsed); err == nil {
		return parsed, true
	}

	// Some QuickNode payloads send parsed as a JSON string; try decoding it.
	var parsedStr string
	if err := json.Unmarshal(raw, &parsedStr); err != nil {
		return SolParsed{}, false
	}

	parsedStr = strings.TrimSpace(parsedStr)
	if parsedStr == "" || !strings.HasPrefix(parsedStr, "{") {
		return SolParsed{}, false
	}

	if err := json.Unmarshal([]byte(parsedStr), &parsed); err != nil {
		return SolParsed{}, false
	}
	return parsed, true
}

func normalizeMethodID(methodID string) string {
	m := strings.ToLower(strings.TrimSpace(methodID))
	if m == "" {
		return ""
	}
	if !strings.HasPrefix(m, "0x") {
		m = "0x" + m
	}
	if len(m) >= 10 {
		return m[:10]
	}
	return m
}

func methodIDFromInput(input string) string {
	in := strings.ToLower(strings.TrimSpace(input))
	if in == "" || in == "0x" {
		return ""
	}
	if !strings.HasPrefix(in, "0x") {
		in = "0x" + in
	}
	if len(in) < 10 {
		return ""
	}
	return in[:10]
}

func parseTxTypeHint(raw string) string {
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "" {
		return ""
	}
	switch {
	case strings.Contains(v, "approve"), strings.Contains(v, "approval"):
		return txTypeApproval
	case strings.Contains(v, "swap"):
		return txTypeSwap
	case strings.Contains(v, "nft") && strings.Contains(v, "transfer"):
		return txTypeNftTransfer
	case strings.Contains(v, "token") && strings.Contains(v, "transfer"):
		return txTypeTokenTransfer
	case strings.Contains(v, "native") && strings.Contains(v, "transfer"):
		return txTypeNativeTransfer
	case strings.Contains(v, "smart") && strings.Contains(v, "contract"):
		return txTypeSmartContractInteraction
	case v == "transfer":
		return txTypeDefaultTransfer
	default:
		return ""
	}
}

func isTokenStandard(standard string) bool {
	s := strings.ToLower(strings.TrimSpace(standard))
	return strings.Contains(s, "erc20") || strings.Contains(s, "trc20") || strings.Contains(s, "spl")
}

func isNftStandard(standard string) bool {
	s := strings.ToLower(strings.TrimSpace(standard))
	return strings.Contains(s, "erc721") || strings.Contains(s, "erc1155") || strings.Contains(s, "trc721") || strings.Contains(s, "trc1155") || strings.Contains(s, "nft")
}

func isEvmApprovalMethod(methodID string) bool {
	_, ok := evmApprovalMethodSelectors[normalizeMethodID(methodID)]
	return ok
}

func isEvmSwapMethod(methodID string) bool {
	_, ok := evmSwapMethodSelectors[normalizeMethodID(methodID)]
	return ok
}

func isEvmApprovalEventTopic(topic0 string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(topic0)), evmApprovalEventTopicPrefix)
}

func isEvmSwapEventTopic(topic0 string) bool {
	topic := strings.ToLower(strings.TrimSpace(topic0))
	return topic == evmSwapEventV2Topic || topic == evmSwapEventV3Topic
}

func isErc721TransferLog(logItem MoralisLog) bool {
	topic0 := strings.ToLower(strings.TrimSpace(logItem.Topic0))
	if topic0 != evmTransferEventTopic {
		return false
	}
	return strings.TrimSpace(logItem.Topic1) != "" && strings.TrimSpace(logItem.Topic2) != "" && strings.TrimSpace(logItem.Topic3) != ""
}

func isErc1155TransferLog(topic0 string) bool {
	topic := strings.ToLower(strings.TrimSpace(topic0))
	return strings.HasPrefix(topic, erc1155TransferSinglePrefix) || strings.HasPrefix(topic, erc1155TransferBatchPrefix)
}

func isInputEmpty(txInput string) bool {
	in := strings.ToLower(strings.TrimSpace(txInput))
	return in == "" || in == "0x"
}

func transferMethodID(tx Transfer) string {
	if method := normalizeMethodID(tx.MethodId); method != "" {
		return method
	}
	return methodIDFromInput(tx.TxInput)
}

func isApprovalInput(input string) bool {
	return isEvmApprovalMethod(methodIDFromInput(input))
}

func isTransferApproval(tx Transfer) bool {
	return isEvmApprovalMethod(transferMethodID(tx))
}

func isContractInteractionFromInput(input string) bool {
	methodID := methodIDFromInput(input)
	return methodID != "" && methodID != erc20TransferMethodID
}

func addBigIntDelta(deltaByAsset map[string]*big.Int, asset string, delta *big.Int) {
	if delta == nil || delta.Sign() == 0 {
		return
	}
	key := strings.ToLower(strings.TrimSpace(asset))
	if key == "" {
		return
	}
	if existing, ok := deltaByAsset[key]; ok {
		existing.Add(existing, delta)
		return
	}
	deltaByAsset[key] = new(big.Int).Set(delta)
}

func hasOnePositiveAndOneNegativeDelta(deltaByAsset map[string]*big.Int) bool {
	negatives := 0
	positives := 0
	for _, delta := range deltaByAsset {
		if delta == nil || delta.Sign() == 0 {
			continue
		}
		if delta.Sign() < 0 {
			negatives++
		} else if delta.Sign() > 0 {
			positives++
		}
		if negatives > 1 || positives > 1 {
			return false
		}
	}
	return negatives == 1 && positives == 1
}

type quickNodeTxSummary struct {
	hasApproval       bool
	hasSwapHint       bool
	hasNftTransfer    bool
	hasTokenTransfer  bool
	hasNativeTransfer bool
	hasEmptyInput     bool
	hasContractInput  bool
	sender            string
	deltaByAsset      map[string]*big.Int
}

func ensureQuickNodeTxSummary(summaryByHash map[string]*quickNodeTxSummary, hash, sender string) *quickNodeTxSummary {
	summary := summaryByHash[hash]
	if summary == nil {
		summary = &quickNodeTxSummary{
			sender:       sender,
			deltaByAsset: map[string]*big.Int{},
		}
		summaryByHash[hash] = summary
	} else if summary.sender == "" && sender != "" {
		summary.sender = sender
	}
	return summary
}

func classifyQuickNodeSummary(summary *quickNodeTxSummary) string {
	if summary == nil {
		return txTypeSmartContractInteraction
	}
	if summary.hasApproval {
		return txTypeApproval
	}
	if summary.hasSwapHint || hasOnePositiveAndOneNegativeDelta(summary.deltaByAsset) {
		return txTypeSwap
	}
	if summary.hasNftTransfer {
		return txTypeNftTransfer
	}
	if summary.hasTokenTransfer {
		return txTypeTokenTransfer
	}
	if summary.hasNativeTransfer && summary.hasEmptyInput && !summary.hasContractInput {
		return txTypeNativeTransfer
	}
	return txTypeSmartContractInteraction
}

func classifyQuickNodeTronTxTypes(transfers []Transfer) map[string]string {
	summaryByHash := make(map[string]*quickNodeTxSummary)
	const nativeAssetKey = "__native__"

	for _, tx := range transfers {
		hash := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(tx.TxHash)), "0x")
		if hash == "" {
			continue
		}
		sender := tx.TxFrom
		if strings.TrimSpace(sender) == "" {
			sender = tx.From
		}
		summary := ensureQuickNodeTxSummary(summaryByHash, hash, normalizeAddressForFlow("tron", sender))
		methodID := transferMethodID(tx)

		hint := parseTxTypeHint(tx.TransactionType)
		if hint == txTypeApproval || isEvmApprovalMethod(methodID) {
			summary.hasApproval = true
		}
		if hint == txTypeSwap {
			summary.hasSwapHint = true
		}
		if hint == txTypeNftTransfer || isNftStandard(tx.Standard) {
			summary.hasNftTransfer = true
		}

		if isInputEmpty(tx.TxInput) && methodID == "" {
			summary.hasEmptyInput = true
		} else {
			summary.hasContractInput = true
		}

		value := parseBigInt(tx.Value)
		if value.Sign() <= 0 {
			continue
		}

		isNft := isNftStandard(tx.Standard)
		isToken := !isNft && (strings.TrimSpace(tx.Contract) != "" || isTokenStandard(tx.Standard))
		isNative := !isNft && !isToken
		if isNft {
			summary.hasNftTransfer = true
		}
		if isToken {
			summary.hasTokenTransfer = true
		}
		if isNative {
			summary.hasNativeTransfer = true
		}

		from := normalizeAddressForFlow("tron", tx.From)
		to := normalizeAddressForFlow("tron", tx.To)
		if summary.sender != "" && !isNft {
			asset := nativeAssetKey
			if isToken {
				asset = strings.ToLower(strings.TrimSpace(tx.Contract))
				if asset == "" {
					asset = strings.ToLower(strings.TrimSpace(tx.Standard))
				}
			}
			if strings.EqualFold(from, summary.sender) {
				addBigIntDelta(summary.deltaByAsset, asset, new(big.Int).Neg(new(big.Int).Set(value)))
			}
			if strings.EqualFold(to, summary.sender) {
				addBigIntDelta(summary.deltaByAsset, asset, value)
			}
		}
	}

	txTypeByHash := make(map[string]string, len(summaryByHash))
	for hash, summary := range summaryByHash {
		txTypeByHash[hash] = classifyQuickNodeSummary(summary)
	}

	return txTypeByHash
}

func classifyQuickNodeEvmTxTypes(transfers []Transfer) map[string]string {
	summaryByHash := make(map[string]*quickNodeTxSummary)
	const nativeAssetKey = "__native__"

	for _, tx := range transfers {
		hash := strings.ToLower(strings.TrimSpace(tx.TxHash))
		if hash == "" {
			continue
		}
		sender := tx.TxFrom
		if strings.TrimSpace(sender) == "" {
			sender = tx.From
		}
		summary := ensureQuickNodeTxSummary(summaryByHash, hash, normalizeAddressForFlow("evm", sender))
		methodID := transferMethodID(tx)

		hint := parseTxTypeHint(tx.TransactionType)
		if hint == txTypeApproval || isEvmApprovalMethod(methodID) {
			summary.hasApproval = true
		}
		if hint == txTypeSwap {
			summary.hasSwapHint = true
		}
		if hint == txTypeNftTransfer || isNftStandard(tx.Standard) {
			summary.hasNftTransfer = true
		}

		if isInputEmpty(tx.TxInput) && methodID == "" {
			summary.hasEmptyInput = true
		} else {
			summary.hasContractInput = true
		}

		value := parseBigInt(tx.Value)
		if value.Sign() <= 0 {
			continue
		}

		isNft := isNftStandard(tx.Standard)
		isToken := !isNft && (strings.TrimSpace(tx.Contract) != "" || isTokenStandard(tx.Standard))
		isNative := !isNft && !isToken
		if isNft {
			summary.hasNftTransfer = true
		}
		if isToken {
			summary.hasTokenTransfer = true
		}
		if isNative {
			summary.hasNativeTransfer = true
		}

		from := normalizeAddressForFlow("evm", tx.From)
		to := normalizeAddressForFlow("evm", tx.To)
		if summary.sender != "" && !isNft {
			asset := nativeAssetKey
			if isToken {
				asset = strings.ToLower(strings.TrimSpace(tx.Contract))
				if asset == "" {
					asset = strings.ToLower(strings.TrimSpace(tx.Standard))
				}
			}
			if strings.EqualFold(from, summary.sender) {
				addBigIntDelta(summary.deltaByAsset, asset, new(big.Int).Neg(new(big.Int).Set(value)))
			}
			if strings.EqualFold(to, summary.sender) {
				addBigIntDelta(summary.deltaByAsset, asset, value)
			}
		}
	}

	txTypeByHash := make(map[string]string, len(summaryByHash))
	for hash, summary := range summaryByHash {
		txTypeByHash[hash] = classifyQuickNodeSummary(summary)
	}
	return txTypeByHash
}

func classifyTronTxType(tx Transfer) string {
	hinted := parseTxTypeHint(tx.TransactionType)
	if hinted == txTypeApproval {
		return txTypeApproval
	}
	if isTransferApproval(tx) {
		return txTypeApproval
	}
	if hinted == txTypeSwap {
		return txTypeSwap
	}
	if hinted == txTypeNftTransfer || isNftStandard(tx.Standard) {
		return txTypeNftTransfer
	}
	if hinted == txTypeTokenTransfer {
		return txTypeTokenTransfer
	}
	if strings.TrimSpace(tx.Contract) != "" || isTokenStandard(tx.Standard) {
		return txTypeTokenTransfer
	}
	if hinted == txTypeNativeTransfer {
		return txTypeNativeTransfer
	}
	if parseBigInt(tx.Value).Sign() > 0 && isInputEmpty(tx.TxInput) && transferMethodID(tx) == "" {
		return txTypeNativeTransfer
	}
	return txTypeSmartContractInteraction
}

func classifyQuickNodeEvmTxType(tx Transfer) string {
	hinted := parseTxTypeHint(tx.TransactionType)
	if hinted == txTypeApproval {
		return txTypeApproval
	}
	if isTransferApproval(tx) {
		return txTypeApproval
	}
	if hinted == txTypeSwap {
		return txTypeSwap
	}
	if hinted == txTypeNftTransfer || isNftStandard(tx.Standard) {
		return txTypeNftTransfer
	}
	if hinted == txTypeTokenTransfer {
		return txTypeTokenTransfer
	}
	if strings.TrimSpace(tx.Contract) != "" || isTokenStandard(tx.Standard) {
		return txTypeTokenTransfer
	}
	if hinted == txTypeNativeTransfer {
		return txTypeNativeTransfer
	}
	if parseBigInt(tx.Value).Sign() > 0 && isInputEmpty(tx.TxInput) && transferMethodID(tx) == "" {
		return txTypeNativeTransfer
	}
	return txTypeSmartContractInteraction
}

type moralisTxSummary struct {
	hasApproval       bool
	hasSwapHint       bool
	hasNftTransfer    bool
	hasTokenTransfer  bool
	hasNativeTransfer bool
	hasEmptyInput     bool
	hasContractInput  bool
	sender            string
	deltaByAsset      map[string]*big.Int
}

func ensureMoralisTxSummary(summaryByHash map[string]*moralisTxSummary, hash string) *moralisTxSummary {
	summary := summaryByHash[hash]
	if summary == nil {
		summary = &moralisTxSummary{
			deltaByAsset: map[string]*big.Int{},
		}
		summaryByHash[hash] = summary
	}
	return summary
}

func classifyMoralisSummary(summary *moralisTxSummary) string {
	if summary == nil {
		return txTypeSmartContractInteraction
	}
	if summary.hasApproval {
		return txTypeApproval
	}
	if summary.hasSwapHint || hasOnePositiveAndOneNegativeDelta(summary.deltaByAsset) {
		return txTypeSwap
	}
	if summary.hasNftTransfer {
		return txTypeNftTransfer
	}
	if summary.hasTokenTransfer {
		return txTypeTokenTransfer
	}
	if summary.hasNativeTransfer && summary.hasEmptyInput && !summary.hasContractInput {
		return txTypeNativeTransfer
	}
	return txTypeSmartContractInteraction
}

func classifyMoralisTxTypes(payload MoralisEvmPayload, _ evmChainInfo) map[string]string {
	summaryByHash := make(map[string]*moralisTxSummary)
	const nativeAssetKey = "__native__"

	for _, tx := range payload.Txs {
		hash := strings.ToLower(strings.TrimSpace(tx.Hash))
		if hash == "" {
			continue
		}
		summary := ensureMoralisTxSummary(summaryByHash, hash)
		summary.sender = strings.ToLower(strings.TrimSpace(tx.FromAddress))
		if isApprovalInput(tx.Input) {
			summary.hasApproval = true
		}
		if isEvmSwapMethod(methodIDFromInput(tx.Input)) {
			summary.hasSwapHint = true
		}
		if isInputEmpty(tx.Input) {
			summary.hasEmptyInput = true
		} else {
			summary.hasContractInput = true
		}
		value := parseBigInt(tx.Value)
		if value.Sign() > 0 {
			summary.hasNativeTransfer = true
			if summary.sender != "" {
				addBigIntDelta(summary.deltaByAsset, nativeAssetKey, new(big.Int).Neg(new(big.Int).Set(value)))
			}
		}
	}

	for _, logItem := range payload.Logs {
		hash := strings.ToLower(strings.TrimSpace(logItem.TransactionHash))
		if hash == "" {
			continue
		}
		summary := ensureMoralisTxSummary(summaryByHash, hash)
		if isEvmApprovalEventTopic(logItem.Topic0) {
			summary.hasApproval = true
		}
		if isEvmSwapEventTopic(logItem.Topic0) {
			summary.hasSwapHint = true
		}
		if isErc721TransferLog(logItem) || isErc1155TransferLog(logItem.Topic0) {
			summary.hasNftTransfer = true
		}
	}

	for _, transfer := range payload.Erc20Transfers {
		hash := strings.ToLower(strings.TrimSpace(transfer.TransactionHash))
		if hash == "" {
			continue
		}
		value := parseBigInt(transfer.Value)
		if value.Sign() <= 0 {
			continue
		}
		summary := ensureMoralisTxSummary(summaryByHash, hash)
		summary.hasTokenTransfer = true
		if summary.sender == "" {
			continue
		}
		asset := strings.ToLower(strings.TrimSpace(transfer.Contract))
		if asset == "" {
			asset = strings.ToLower(strings.TrimSpace(transfer.TokenSymbol))
		}
		if strings.EqualFold(strings.TrimSpace(transfer.From), summary.sender) {
			addBigIntDelta(summary.deltaByAsset, asset, new(big.Int).Neg(new(big.Int).Set(value)))
		}
		if strings.EqualFold(strings.TrimSpace(transfer.To), summary.sender) {
			addBigIntDelta(summary.deltaByAsset, asset, value)
		}
	}

	for _, internalTx := range payload.TxsInternal {
		hash := strings.ToLower(strings.TrimSpace(internalTx.TransactionHash))
		if hash == "" {
			continue
		}
		value := parseBigInt(internalTx.Value)
		if value.Sign() <= 0 {
			continue
		}
		summary := ensureMoralisTxSummary(summaryByHash, hash)
		summary.hasNativeTransfer = true
		if summary.sender == "" {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(internalTx.From), summary.sender) {
			addBigIntDelta(summary.deltaByAsset, nativeAssetKey, new(big.Int).Neg(new(big.Int).Set(value)))
		}
		if strings.EqualFold(strings.TrimSpace(internalTx.To), summary.sender) {
			addBigIntDelta(summary.deltaByAsset, nativeAssetKey, value)
		}
	}

	for _, approval := range payload.Erc20Approvals {
		hash := strings.ToLower(strings.TrimSpace(approval.TransactionHash))
		if hash != "" {
			summary := ensureMoralisTxSummary(summaryByHash, hash)
			summary.hasApproval = true
		}
	}
	for _, approval := range payload.NftTokenApprovals {
		hash := strings.ToLower(strings.TrimSpace(approval.TransactionHash))
		if hash != "" {
			summary := ensureMoralisTxSummary(summaryByHash, hash)
			summary.hasApproval = true
		}
	}
	for _, approval := range payload.NftApprovals.ERC721 {
		hash := strings.ToLower(strings.TrimSpace(approval.TransactionHash))
		if hash != "" {
			summary := ensureMoralisTxSummary(summaryByHash, hash)
			summary.hasApproval = true
		}
	}
	for _, approval := range payload.NftApprovals.ERC1155 {
		hash := strings.ToLower(strings.TrimSpace(approval.TransactionHash))
		if hash != "" {
			summary := ensureMoralisTxSummary(summaryByHash, hash)
			summary.hasApproval = true
		}
	}

	txTypeByHash := make(map[string]string, len(summaryByHash))
	for hash, summary := range summaryByHash {
		txTypeByHash[hash] = classifyMoralisSummary(summary)
	}

	return txTypeByHash
}

func classifyMoralisNativeTxType(tx MoralisTx, txTypeByHash map[string]string) string {
	hash := strings.ToLower(strings.TrimSpace(tx.Hash))
	if txType, ok := txTypeByHash[hash]; ok && txType != "" {
		return txType
	}
	if isApprovalInput(tx.Input) {
		return txTypeApproval
	}
	if parseBigInt(tx.Value).Sign() > 0 && isInputEmpty(tx.Input) {
		return txTypeNativeTransfer
	}
	return txTypeSmartContractInteraction
}

func classifyMoralisErc20TxType(transfer MoralisErc20Transfer, txTypeByHash map[string]string) string {
	hash := strings.ToLower(strings.TrimSpace(transfer.TransactionHash))
	if txType, ok := txTypeByHash[hash]; ok && txType != "" {
		return txType
	}
	return txTypeTokenTransfer
}

func classifyMoralisApprovalTxType(approval MoralisErc20Approval, txTypeByHash map[string]string) string {
	hash := strings.ToLower(strings.TrimSpace(approval.TransactionHash))
	if txType, ok := txTypeByHash[hash]; ok && txType != "" {
		return txType
	}
	return txTypeApproval
}

func inferTransferDirection(chainID, walletAddress, fromAddress, toAddress string) string {
	wallet := normalizeAddressForFlow(chainID, walletAddress)
	from := normalizeAddressForFlow(chainID, fromAddress)
	to := normalizeAddressForFlow(chainID, toAddress)

	if wallet != "" && from != "" && strings.EqualFold(wallet, from) {
		return "send"
	}
	if wallet != "" && to != "" && strings.EqualFold(wallet, to) {
		return "receive"
	}
	return "receive"
}

func normalizeAddressForFlow(chainID, address string) string {
	addr := strings.TrimSpace(address)
	if addr == "" {
		return ""
	}
	if strings.EqualFold(strings.TrimSpace(chainID), "tron") {
		return normalizeTronAddress(addr)
	}
	return strings.ToLower(addr)
}

func addRatValue(totalByKey map[string]*big.Rat, key string, value *big.Rat) {
	if value == nil || value.Sign() == 0 {
		return
	}
	if existing, ok := totalByKey[key]; ok {
		existing.Add(existing, value)
		return
	}
	totalByKey[key] = new(big.Rat).Set(value)
}

func solanaTokenBalanceToRat(balance SolTokenBalance) *big.Rat {
	uiAmount := strings.TrimSpace(balance.UiTokenAmount.UiAmountString)
	if uiAmount != "" {
		if v, ok := new(big.Rat).SetString(uiAmount); ok {
			return v
		}
	}

	rawAmount := parseBigInt(balance.UiTokenAmount.Amount)
	if rawAmount.Sign() == 0 {
		return big.NewRat(0, 1)
	}
	denominator := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(balance.UiTokenAmount.Decimals)), nil)
	return new(big.Rat).SetFrac(rawAmount, denominator)
}

func solanaPrimaryWalletAddress(match SolMatch) string {
	for _, accountKey := range match.Transaction.Message.AccountKeys {
		if accountKey.Signer {
			return strings.TrimSpace(accountKey.Pubkey)
		}
	}
	if len(match.Transaction.Message.AccountKeys) > 0 {
		return strings.TrimSpace(match.Transaction.Message.AccountKeys[0].Pubkey)
	}
	return ""
}

func solanaWalletDeltas(match SolMatch, walletAddress string) map[string]*big.Rat {
	deltas := map[string]*big.Rat{}
	wallet := strings.TrimSpace(walletAddress)
	if wallet == "" {
		return deltas
	}

	for _, pre := range match.Meta.PreTokenBalances {
		if !strings.EqualFold(strings.TrimSpace(pre.Owner), wallet) {
			continue
		}
		token := strings.TrimSpace(pre.Mint)
		if token == "" {
			continue
		}
		addRatValue(deltas, token, new(big.Rat).Neg(solanaTokenBalanceToRat(pre)))
	}

	for _, post := range match.Meta.PostTokenBalances {
		if !strings.EqualFold(strings.TrimSpace(post.Owner), wallet) {
			continue
		}
		token := strings.TrimSpace(post.Mint)
		if token == "" {
			continue
		}
		addRatValue(deltas, token, solanaTokenBalanceToRat(post))
	}

	for i, acc := range match.Transaction.Message.AccountKeys {
		if !strings.EqualFold(strings.TrimSpace(acc.Pubkey), wallet) {
			continue
		}
		if i >= len(match.Meta.PreBalances) || i >= len(match.Meta.PostBalances) {
			continue
		}
		lamportDelta := match.Meta.PostBalances[i] - match.Meta.PreBalances[i]
		if lamportDelta == 0 {
			continue
		}
		addRatValue(deltas, nativeTokenAddress("solana"), new(big.Rat).SetFrac(big.NewInt(lamportDelta), big.NewInt(1_000_000_000)))
	}

	return deltas
}

func solanaAccountIndexForPubkey(match SolMatch, pubkey string) int {
	target := strings.TrimSpace(pubkey)
	if target == "" {
		return -1
	}
	for idx, accountKey := range match.Transaction.Message.AccountKeys {
		if strings.EqualFold(strings.TrimSpace(accountKey.Pubkey), target) {
			return idx
		}
	}
	return -1
}

func solanaTokenBalanceOwnerByAccountIndex(balances []SolTokenBalance, accountIndex int) string {
	if accountIndex < 0 {
		return ""
	}
	for _, balance := range balances {
		if balance.AccountIndex != accountIndex {
			continue
		}
		owner := strings.TrimSpace(balance.Owner)
		if owner != "" {
			return owner
		}
	}
	return ""
}

func solanaTokenBalanceMintByAccountIndex(balances []SolTokenBalance, accountIndex int) string {
	if accountIndex < 0 {
		return ""
	}
	for _, balance := range balances {
		if balance.AccountIndex != accountIndex {
			continue
		}
		mint := strings.TrimSpace(balance.Mint)
		if mint != "" {
			return mint
		}
	}
	return ""
}

func solanaTokenAccountOwner(match SolMatch, tokenAccount string, preferPostBalance bool) string {
	accountIndex := solanaAccountIndexForPubkey(match, tokenAccount)
	if accountIndex < 0 {
		return ""
	}

	if preferPostBalance {
		if owner := solanaTokenBalanceOwnerByAccountIndex(match.Meta.PostTokenBalances, accountIndex); owner != "" {
			return owner
		}
		return solanaTokenBalanceOwnerByAccountIndex(match.Meta.PreTokenBalances, accountIndex)
	}

	if owner := solanaTokenBalanceOwnerByAccountIndex(match.Meta.PreTokenBalances, accountIndex); owner != "" {
		return owner
	}
	return solanaTokenBalanceOwnerByAccountIndex(match.Meta.PostTokenBalances, accountIndex)
}

func solanaTokenAccountMint(match SolMatch, tokenAccount string, preferPostBalance bool) string {
	accountIndex := solanaAccountIndexForPubkey(match, tokenAccount)
	if accountIndex < 0 {
		return ""
	}

	if preferPostBalance {
		if mint := solanaTokenBalanceMintByAccountIndex(match.Meta.PostTokenBalances, accountIndex); mint != "" {
			return mint
		}
		return solanaTokenBalanceMintByAccountIndex(match.Meta.PreTokenBalances, accountIndex)
	}

	if mint := solanaTokenBalanceMintByAccountIndex(match.Meta.PreTokenBalances, accountIndex); mint != "" {
		return mint
	}
	return solanaTokenBalanceMintByAccountIndex(match.Meta.PostTokenBalances, accountIndex)
}

func isSolanaSwapTransaction(match SolMatch) bool {
	walletAddress := solanaPrimaryWalletAddress(match)
	if walletAddress == "" {
		return false
	}
	deltas := solanaWalletDeltas(match, walletAddress)
	negatives := 0
	positives := 0
	for _, delta := range deltas {
		if delta.Sign() < 0 {
			negatives++
		}
		if delta.Sign() > 0 {
			positives++
		}
	}
	return negatives == 1 && positives == 1
}

func isSolanaTokenProgram(programID string) bool {
	p := strings.TrimSpace(programID)
	return p == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" ||
		strings.HasPrefix(p, "TokenzQd")
}

func isSolanaApprovalInstruction(inst SolInstruction, parsed SolParsed) bool {
	pt := strings.ToLower(strings.TrimSpace(parsed.Type))
	if pt != "approve" && pt != "approvechecked" {
		return false
	}
	return isSolanaTokenProgram(inst.ProgramId) || strings.EqualFold(strings.TrimSpace(inst.Program), "spl-token")
}

func isSolanaUnitTokenMovement(parsed SolParsed) bool {
	if parsed.Info.TokenAmount != nil {
		if strings.TrimSpace(parsed.Info.TokenAmount.Amount) == "1" && parsed.Info.TokenAmount.Decimals == 0 {
			return true
		}
		if strings.TrimSpace(parsed.Info.TokenAmount.UiAmountString) == "1" && parsed.Info.TokenAmount.Decimals == 0 {
			return true
		}
	}
	if parsed.Info.Amount != nil {
		switch v := parsed.Info.Amount.(type) {
		case string:
			return strings.TrimSpace(v) == "1"
		case float64:
			return int64(v) == 1
		case json.Number:
			i, _ := v.Int64()
			return i == 1
		}
	}
	return false
}

func isSolanaNftTransaction(match SolMatch) bool {
	hasMetaplexInstruction := false
	hasUnitTokenMovement := false

	for _, inst := range match.Transaction.Message.Instructions {
		if strings.EqualFold(strings.TrimSpace(inst.ProgramId), solanaMetaplexProgramID) ||
			strings.Contains(strings.ToLower(strings.TrimSpace(inst.Program)), "metaplex") {
			hasMetaplexInstruction = true
		}

		parsed, ok := parseSolParsed(inst.Parsed)
		if !ok {
			continue
		}
		pt := strings.ToLower(strings.TrimSpace(parsed.Type))
		if (pt == "transfer" || pt == "transferchecked") && isSolanaUnitTokenMovement(parsed) {
			hasUnitTokenMovement = true
		}
	}

	return hasMetaplexInstruction && hasUnitTokenMovement
}

func classifySolanaInstructionTxType(inst SolInstruction, parsed SolParsed, isApprovalTx, isSwapTx, isNftTx bool) string {
	if isApprovalTx {
		return txTypeApproval
	}
	if isSwapTx {
		return txTypeSwap
	}
	if isNftTx {
		return txTypeNftTransfer
	}
	pt := strings.ToLower(strings.TrimSpace(parsed.Type))
	if strings.Contains(pt, "transfer") && (isSolanaTokenProgram(inst.ProgramId) || strings.EqualFold(strings.TrimSpace(inst.Program), "spl-token")) {
		return txTypeTokenTransfer
	}
	if isSolanaApprovalInstruction(inst, parsed) {
		return txTypeApproval
	}
	if parsed.Info.Lamports != nil || strings.EqualFold(strings.TrimSpace(inst.Program), "system") {
		return txTypeNativeTransfer
	}
	if strings.Contains(pt, "transfer") {
		return txTypeTokenTransfer
	}
	return txTypeSmartContractInteraction
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
	isApprovalTx := false
	for _, inst := range match.Transaction.Message.Instructions {
		parsed, ok := parseSolParsed(inst.Parsed)
		if !ok {
			continue
		}
		if isSolanaApprovalInstruction(inst, parsed) {
			isApprovalTx = true
			break
		}
	}
	isSwapTx := isSolanaSwapTransaction(match)
	isNftTx := isSolanaNftTransaction(match)

	for _, inst := range match.Transaction.Message.Instructions {
		if len(inst.Parsed) == 0 {
			continue
		}

		parsed, ok := parseSolParsed(inst.Parsed)
		if !ok {
			continue
		}

		parsedType := strings.ToLower(strings.TrimSpace(parsed.Type))
		if parsedType == "transfer" || parsedType == "transferchecked" || parsedType == "approve" || parsedType == "approvechecked" {
			info := parsed.Info
			isTokenInstruction := isSolanaTokenProgram(inst.ProgramId) || strings.EqualFold(strings.TrimSpace(inst.Program), "spl-token")

			sourceTokenAccount := strings.TrimSpace(info.Source)
			destinationTokenAccount := strings.TrimSpace(info.Destination)
			from := sourceTokenAccount
			to := destinationTokenAccount
			if isTokenInstruction && (parsedType == "transfer" || parsedType == "transferchecked") {
				if sourceOwner := solanaTokenAccountOwner(match, from, false); sourceOwner != "" {
					from = sourceOwner
				} else if strings.TrimSpace(info.Authority) != "" {
					from = strings.TrimSpace(info.Authority)
				}
				if destinationOwner := solanaTokenAccountOwner(match, to, true); destinationOwner != "" {
					to = destinationOwner
				}
			}

			if from == "" {
				from = strings.TrimSpace(info.Authority)
			}
			if to == "" {
				to = strings.TrimSpace(info.Delegate)
			}
			if from == "" {
				continue
			}
			if to == "" {
				to = from
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

			if amountStr == "" {
				amountStr = "0"
			}
			if amountStr == "0" && !(parsedType == "approve" || parsedType == "approvechecked") {
				continue
			}
			txType := classifySolanaInstructionTxType(inst, parsed, isApprovalTx, isSwapTx, isNftTx)
			tokenAddr := inst.ProgramId
			if isTokenInstruction && (parsedType == "transfer" || parsedType == "transferchecked") {
				tokenAddr = strings.TrimSpace(info.Mint)
				if tokenAddr == "" {
					tokenAddr = solanaTokenAccountMint(match, sourceTokenAccount, false)
				}
				if tokenAddr == "" {
					tokenAddr = solanaTokenAccountMint(match, destinationTokenAccount, true)
				}
				if tokenAddr == "" {
					tokenAddr = inst.ProgramId
				}
				log.Printf("[SOLANA_TOKEN] signature=%s parsed_type=%s from_owner=%s to_owner=%s source_token_account=%s destination_token_account=%s mint=%s amount=%s", signature, parsedType, from, to, sourceTokenAccount, destinationTokenAccount, tokenAddr, amountStr)
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
						TransactionType:    txType,
						Direction:          direction,
						CreatedAt:          time.Now(),
						Standard:           inst.Program,
						ContractAddress:    tokenAddr,
						TokenDecimal:       uint8(decimals),
					}

					if match.Meta.Err != nil {
						dbTx.Status = "failed"
					}

					result := db.Clauses(clause.OnConflict{
						Columns:   []clause.Column{{Name: "address"}, {Name: "signature"}},
						DoNothing: true,
					}).Create(&dbTx)

					logTxHistoryResult("SOLANA", "sol_transaction_histories", wa.Address, "solana", dbTx.Signature, dbTx.Direction, dbTx.Amount, result.RowsAffected, result.Error)
					if result.Error == nil {
						triggerNotification(NotificationParams{
							Address:   wa.Address,
							Chain:     "solana",
							Amount:    dbTx.Amount,
							Symbol:    dbTx.TokenSymbol,
							Direction: dbTx.Direction,
							TxHash:    dbTx.Signature,
							TxType:    dbTx.TransactionType,
							Source:    "sol_transaction_histories",
							Rows:      result.RowsAffected,
						})
						updateRedis(wa.Address, "solana", dbTx)
						if result.RowsAffected > 0 {
							insertedAny = true
						}
					}
				}
			}

			if insertedAny && (parsedType == "transfer" || parsedType == "transferchecked") {
				if inst.Program == "system" {
					// Native SOL balance change is handled via pre/post balances below
					// We only process it here for transaction history records
				} else {
					tokenAddr = normalizeTokenAddress("solana", tokenAddr)
					updateUserBalancesForTransfer("solana", from, to, tokenAddr, amountStr)
				}
			}
		}
	}

	// Native SOL Balance Adjustment using Pre/Post Balances (Includes fees, rent, and transfers)
	for i, acc := range match.Transaction.Message.AccountKeys {
		if i >= len(match.Meta.PreBalances) || i >= len(match.Meta.PostBalances) {
			continue
		}
		diff := match.Meta.PostBalances[i] - match.Meta.PreBalances[i]
		if diff != 0 {
			updateUserBalanceForAddress("solana", acc.Pubkey, nativeTokenAddress("solana"), formatTokenAmount(big.NewInt(diff), 9))
		}
	}
}

// ---------------------------------------------------------
// 6. TRON PROCESSOR
// ---------------------------------------------------------
func processTronTransaction(tx Transfer, txTypeByHash map[string]string) {
	// 1. Format Address (HEX -> Base58)
	fromBase58 := normalizeTronAddress(tx.From)
	toBase58 := normalizeTronAddress(tx.To)
	txHash := strings.TrimPrefix(tx.TxHash, "0x")

	contractBase58 := normalizeTronAddress(tx.Contract)

	if fromBase58 != "" {
		tx.From = fromBase58
	}
	if toBase58 != "" {
		tx.To = toBase58
	}
	if txHash != "" {
		tx.TxHash = txHash
	}
	if contractBase58 != "" {
		tx.Contract = contractBase58
	}

	// We loop through the Base58 addresses
	wallets := []string{tx.From, tx.To}

	// 1. Format Amount
	amountStr := tx.Value
	decimals := 6
	if tx.TokenDecimals > 0 {
		decimals = tx.TokenDecimals
	}
	if tx.Value != "" && tx.Value != "0" {
		if valBig, ok := parseFlexibleBigInt(tx.Value); ok {
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
	classifiedTxType := classifyTronTxType(tx)
	if mappedType, ok := txTypeByHash[strings.ToLower(strings.TrimSpace(tx.TxHash))]; ok && mappedType != "" {
		classifiedTxType = mappedType
	}
	var waList []WalletAddress
	for _, walletHex := range wallets {
		if walletHex == "" {
			continue
		}

		waList, _ = resolveWalletAddresses("tron", walletHex)
		if len(waList) == 0 {
			continue
		}

		for _, wa := range waList {
			direction := inferTransferDirection("tron", wa.Address, tx.From, tx.To)

			if classifiedTxType == txTypeSmartContractInteraction {
				direction = "contract"
			}

			walletAddress := normalizeTronAddressForStorage(wa.Address)
			if walletAddress == "" {
				continue
			}
			fromAddress := normalizeTronAddressForStorage(tx.From)
			toAddress := normalizeTronAddressForStorage(tx.To)
			contractAddress := normalizeTronAddressForStorage(tx.Contract)

			dbTx := WalletTransactionHistory{
				WalletAddress:   walletAddress,
				TxHash:          tx.TxHash,
				Chain:           "tron",
				BlockNumber:     int64(tx.BlockNumber),
				BlockTime:       tx.BlockTime,
				FromAddress:     fromAddress,
				ToAddress:       toAddress,
				Amount:          amountStr,
				Status:          "success",
				TransactionType: classifiedTxType,
				Standard:        tx.Standard,
				ContractAddress: contractAddress,
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

			logTxHistoryResult("TRON", "tron_transaction_histories", walletAddress, "tron", dbTx.TxHash, dbTx.Direction, dbTx.Amount, result.RowsAffected, result.Error)
			if result.Error == nil {
				triggerNotification(NotificationParams{
					Address:   walletAddress,
					Chain:     "tron",
					Amount:    dbTx.Amount,
					Symbol:    dbTx.TokenSymbol,
					Direction: dbTx.Direction,
					TxHash:    dbTx.TxHash,
					TxType:    dbTx.TransactionType,
					Source:    "tron_transaction_histories",
					Rows:      result.RowsAffected,
				})
				updateRedis(walletAddress, "tron", dbTx)
				if result.RowsAffected > 0 {
					insertedAny = true
					// Only deduct gas fees if the wallet we are processing is the one who PAID the gas
					// Usually the 'tx_from' (transaction initiator)
					if tx.CostInTrx != "" && tx.CostInTrx != "0" && strings.EqualFold(tx.TxFrom, wa.Address) {
						updateUserBalanceForAddress("tron", wa.Address, nativeTokenAddress("tron"), negateAmount(tx.CostInTrx))
					}
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
func processEvmTransaction(tx Transfer, chainInfo evmChainInfo, txTypeByHash map[string]string) {
	wallets := []string{tx.From, tx.To}
	log.Printf("[EVM] Processing %s Tx: %s", chainInfo.Chain, tx.TxHash)

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
	feeNativeStr, feeUsd := computeNetworkFeeNativeAndUsd(feeWei, chainInfo)

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
	classifiedTxType := classifyQuickNodeEvmTxType(tx)
	if mappedType, ok := txTypeByHash[strings.ToLower(strings.TrimSpace(tx.TxHash))]; ok && mappedType != "" {
		classifiedTxType = mappedType
	}

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
			if strings.EqualFold(walletAddr, tx.From) {
				direction = "send"
			}

			dbTx := EvmTransactionHistory{
				WalletAddress:    wa.Address,
				TxHash:           tx.TxHash,
				Chain:            chainInfo.Chain,
				BlockNumber:      int64(tx.BlockNumber),
				BlockTime:        tx.BlockTime,
				FromAddress:      strings.ToLower(tx.From),
				ToAddress:        strings.ToLower(tx.To),
				Amount:           amountStr,
				GasUsed:          gasUsed.Int64(),
				GasPrice:         gasPrice.String(),
				NetworkFee:       feeWei.String(),
				NetworkFeeNative: feeNativeStr,
				NetworkFeeUsd:    feeUsd,
				Status:           "success",
				TransactionType:  classifiedTxType,
				MethodId:         normalizeMethodID(tx.MethodId),
				Standard:         tx.Standard,
				ContractAddress:  strings.ToLower(tx.Contract),
				TokenName:        tx.TokenName,
				TokenSymbol:      tx.TokenSymbol,
				TokenDecimal:     uint8(decimals),
				Direction:        direction,
				CreatedAt:        time.Now(),
			}

			if dbTx.TokenName == "" && tx.Contract == "" {
				if chainInfo.Chain == "BASE" {
					dbTx.TokenName = "Base"
					dbTx.TokenSymbol = "ETH"
				} else if chainInfo.Chain == "ETH" {
					dbTx.TokenName = "Ethereum"
					dbTx.TokenSymbol = "ETH"
				} else if chainInfo.Chain == "POL" {
					dbTx.TokenName = "Polygon"
					dbTx.TokenSymbol = "POL"
				} else if chainInfo.Chain == "BSC" {
					dbTx.TokenName = "Binance Smart Chain"
					dbTx.TokenSymbol = "BNB"
				}
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

			logTxHistoryResult("EVM", "evm_transaction_histories", wa.Address, chainInfo.Chain, dbTx.TxHash, dbTx.Direction, dbTx.Amount, result.RowsAffected, result.Error)
			if result.Error == nil {
				triggerNotification(NotificationParams{
					Address:   wa.Address,
					Chain:     dbTx.Chain,
					Amount:    dbTx.Amount,
					Symbol:    dbTx.TokenSymbol,
					Direction: dbTx.Direction,
					TxHash:    dbTx.TxHash,
					TxType:    dbTx.TransactionType,
					ChainID:   strconv.FormatInt(dbTx.ChainId, 10),
					Source:    "evm_transaction_histories",
					Rows:      result.RowsAffected,
				})
				updateRedis(wa.Address, chainInfo.RedisKey, dbTx)
				if result.RowsAffected > 0 {
					insertedAny = true
				}
			}
		}
	}

	if insertedAny {
		tokenAddress := strings.TrimSpace(tx.Contract)
		if tokenAddress == "" {
			tokenAddress = nativeTokenAddress(chainInfo.RedisKey)
		} else {
			tokenAddress = normalizeTokenAddress(chainInfo.RedisKey, tokenAddress)
		}
		updateUserBalancesForTransfer(chainInfo.RedisKey, tx.From, tx.To, tokenAddress, amountStr)

		// Deduct gas fees
		if feeNativeStr != "" && feeNativeStr != "0" {
			updateUserBalanceForAddress(chainInfo.RedisKey, tx.From, nativeTokenAddress(chainInfo.RedisKey), negateAmount(feeNativeStr))
		}
	}
}

// ---------------------------------------------------------
// 8. MORALIS EVM PROCESSOR
// ---------------------------------------------------------
func processMoralisNativeTx(tx MoralisTx, chainInfo evmChainInfo, blockNumber int64, blockTime int64, txTypeByHash map[string]string) {
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
	feeNativeStr, feeUsd := computeNetworkFeeNativeAndUsd(feeWei, chainInfo)

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
				WalletAddress:    wa.Address,
				TxHash:           tx.Hash,
				Chain:            chainInfo.Chain,
				BlockNumber:      blockNumber,
				BlockTime:        blockTime,
				FromAddress:      strings.ToLower(tx.FromAddress),
				ToAddress:        strings.ToLower(tx.ToAddress),
				Amount:           amountStr,
				GasUsed:          gasUsed.Int64(),
				GasPrice:         gasPrice.String(),
				NetworkFee:       feeWei.String(),
				NetworkFeeNative: feeNativeStr,
				NetworkFeeUsd:    feeUsd,
				Status:           "success",
				TransactionType:  classifyMoralisNativeTxType(tx, txTypeByHash),
				MethodId:         methodIDFromInput(tx.Input),
				Standard:         "native",
				Direction:        direction,
				CreatedAt:        time.Now(),
				ChainId:          parseInt64(chainInfo.Chain), // fallback
				GasLimit:         parseInt64(tx.Gas),
			}

			if dbTx.TokenName == "" {
				if chainInfo.Chain == "BASE" {
					dbTx.TokenName = "Base"
					dbTx.TokenSymbol = "ETH"
				} else if chainInfo.Chain == "ETH" {
					dbTx.TokenName = "Ethereum"
					dbTx.TokenSymbol = "ETH"
				} else if chainInfo.Chain == "POL" {
					dbTx.TokenName = "Polygon"
					dbTx.TokenSymbol = "POL"
				} else if chainInfo.Chain == "BSC" {
					dbTx.TokenName = "Binance Smart Chain"
					dbTx.TokenSymbol = "BNB"
				}
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

			logTxHistoryResult("MORALIS", "evm_transaction_histories", wa.Address, chainInfo.Chain, dbTx.TxHash, dbTx.Direction, dbTx.Amount, result.RowsAffected, result.Error)
			if result.Error == nil {
				triggerNotification(NotificationParams{
					Address:   wa.Address,
					Chain:     chainInfo.Chain,
					Amount:    dbTx.Amount,
					Symbol:    "native",
					Direction: dbTx.Direction,
					TxHash:    dbTx.TxHash,
					TxType:    dbTx.TransactionType,
					ChainID:   strconv.FormatInt(dbTx.ChainId, 10),
					Source:    "evm_transaction_histories",
					Rows:      result.RowsAffected,
				})
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

		// Deduct gas fees
		if feeNativeStr != "" && feeNativeStr != "0" {
			updateUserBalanceForAddress(chainInfo.RedisKey, tx.FromAddress, tokenAddress, negateAmount(feeNativeStr))
		}
	}
}

func processMoralisInternalTx(internalTx MoralisInternalTx, txMap map[string]MoralisTx, chainInfo evmChainInfo, blockNumber int64, blockTime int64, txTypeByHash map[string]string) {
	hash := strings.TrimSpace(internalTx.TransactionHash)
	if hash == "" {
		return
	}

	valueInt := parseBigInt(internalTx.Value)
	if valueInt.Sign() <= 0 {
		return
	}
	amountStr := formatTokenAmount(valueInt, 18)

	txRef, hasTx := txMap[strings.ToLower(hash)]
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
	feeNativeStr, feeUsd := computeNetworkFeeNativeAndUsd(feeWei, chainInfo)

	methodID := ""
	if hasTx {
		methodID = methodIDFromInput(txRef.Input)
	}

	txType := txTypeSmartContractInteraction
	if mapped, ok := txTypeByHash[strings.ToLower(hash)]; ok && mapped != "" {
		txType = mapped
	} else if hasTx {
		txType = classifyMoralisNativeTxType(txRef, txTypeByHash)
	}

	wallets := []string{internalTx.From, internalTx.To}
	insertedDeltaByAddress := map[string]string{}
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
			delta := amountStr
			if strings.EqualFold(walletAddr, internalTx.From) {
				direction = "send"
				delta = negateAmount(amountStr)
			}

			dbTx := EvmTransactionHistory{
				WalletAddress:    wa.Address,
				TxHash:           hash,
				Chain:            chainInfo.Chain,
				BlockNumber:      blockNumber,
				BlockTime:        blockTime,
				FromAddress:      strings.ToLower(strings.TrimSpace(internalTx.From)),
				ToAddress:        strings.ToLower(strings.TrimSpace(internalTx.To)),
				Amount:           amountStr,
				GasUsed:          gasUsed.Int64(),
				GasPrice:         gasPrice.String(),
				NetworkFee:       feeWei.String(),
				NetworkFeeNative: feeNativeStr,
				NetworkFeeUsd:    feeUsd,
				Status:           "success",
				TransactionType:  txType,
				MethodId:         methodID,
				Standard:         "native_internal",
				Direction:        direction,
				CreatedAt:        time.Now(),
				GasLimit:         parseInt64(txRef.Gas),
			}

			if dbTx.TokenName == "" {
				if chainInfo.Chain == "BASE" {
					dbTx.TokenName = "Base"
					dbTx.TokenSymbol = "ETH"
				} else if chainInfo.Chain == "ETH" {
					dbTx.TokenName = "Ethereum"
					dbTx.TokenSymbol = "ETH"
				} else if chainInfo.Chain == "POL" {
					dbTx.TokenName = "Polygon"
					dbTx.TokenSymbol = "POL"
				} else if chainInfo.Chain == "BSC" {
					dbTx.TokenName = "Binance Smart Chain"
					dbTx.TokenSymbol = "BNB"
				}
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

			logTxHistoryResult("MORALIS", "evm_transaction_histories", wa.Address, chainInfo.Chain, dbTx.TxHash, dbTx.Direction, dbTx.Amount, result.RowsAffected, result.Error)
			if result.Error == nil {
				triggerNotification(NotificationParams{
					Address:   wa.Address,
					Chain:     chainInfo.Chain,
					Amount:    dbTx.Amount,
					Symbol:    "native",
					Direction: dbTx.Direction,
					TxHash:    dbTx.TxHash,
					TxType:    dbTx.TransactionType,
					ChainID:   strconv.FormatInt(dbTx.ChainId, 10),
					Source:    "evm_transaction_histories",
					Rows:      result.RowsAffected,
				})
				updateRedis(wa.Address, chainInfo.RedisKey, dbTx)
				if result.RowsAffected > 0 {
					insertedDeltaByAddress[wa.Address] = delta
				}
			}
		}
	}

	if len(insertedDeltaByAddress) == 0 {
		return
	}

	tokenAddress := nativeTokenAddress(chainInfo.RedisKey)
	for address, delta := range insertedDeltaByAddress {
		updateUserBalanceForAddress(chainInfo.RedisKey, address, tokenAddress, delta)
	}
}

func processMoralisErc20Approval(approval MoralisErc20Approval, txMap map[string]MoralisTx, chainInfo evmChainInfo, blockNumber int64, blockTime int64, txTypeByHash map[string]string) {
	if approval.TransactionHash == "" {
		return
	}

	txRef, hasTx := txMap[strings.ToLower(approval.TransactionHash)]
	from := approval.Owner
	to := approval.Spender
	if from == "" {
		from = txRef.FromAddress
	}
	if to == "" {
		to = txRef.ToAddress
	}
	if from == "" && to == "" {
		return
	}

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
	feeNativeStr, feeUsd := computeNetworkFeeNativeAndUsd(feeWei, chainInfo)

	amountStr := "0"
	if strings.TrimSpace(approval.Value) != "" {
		amountStr = strings.TrimSpace(approval.Value)
	}
	decimals := 0
	if approval.TokenDecimals != "" {
		if parsed, err := strconv.Atoi(approval.TokenDecimals); err == nil {
			decimals = parsed
		}
	}

	wallets := []string{from, to}
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
			if strings.EqualFold(walletAddr, from) {
				direction = "send"
			}

			dbTx := EvmTransactionHistory{
				WalletAddress:    wa.Address,
				TxHash:           approval.TransactionHash,
				Chain:            chainInfo.Chain,
				BlockNumber:      blockNumber,
				BlockTime:        blockTime,
				FromAddress:      strings.ToLower(from),
				ToAddress:        strings.ToLower(to),
				Amount:           amountStr,
				GasUsed:          gasUsed.Int64(),
				GasPrice:         gasPrice.String(),
				NetworkFee:       feeWei.String(),
				NetworkFeeNative: feeNativeStr,
				NetworkFeeUsd:    feeUsd,
				Status:           "success",
				TransactionType:  classifyMoralisApprovalTxType(approval, txTypeByHash),
				MethodId:         methodIDFromInput(txRef.Input),
				Standard:         "erc20_approval",
				ContractAddress:  strings.ToLower(strings.TrimSpace(approval.Contract)),
				TokenName:        approval.TokenName,
				TokenSymbol:      approval.TokenSymbol,
				TokenDecimal:     uint8(decimals),
				Direction:        direction,
				CreatedAt:        time.Now(),
				GasLimit:         parseInt64(txRef.Gas),
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

			logTxHistoryResult("MORALIS", "evm_transaction_histories", wa.Address, chainInfo.Chain, dbTx.TxHash, dbTx.Direction, dbTx.Amount, result.RowsAffected, result.Error)
			if result.Error == nil {
				triggerNotification(NotificationParams{
					Address:   wa.Address,
					Chain:     chainInfo.Chain,
					Amount:    dbTx.Amount,
					Symbol:    dbTx.TokenSymbol,
					Direction: dbTx.Direction,
					TxHash:    dbTx.TxHash,
					TxType:    dbTx.TransactionType,
					ChainID:   strconv.FormatInt(dbTx.ChainId, 10),
					Source:    "evm_transaction_histories",
					Rows:      result.RowsAffected,
				})
				updateRedis(wa.Address, chainInfo.RedisKey, dbTx)
			}
		}
	}
}

func processMoralisErc20Transfer(transfer MoralisErc20Transfer, txMap map[string]MoralisTx, chainInfo evmChainInfo, blockNumber int64, blockTime int64, txTypeByHash map[string]string) {
	if transfer.TransactionHash == "" {
		return
	}

	amountStr, decimals, ok := moralisErc20TransferAmount(transfer)
	if !ok {
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
	feeNativeStr, feeUsd := computeNetworkFeeNativeAndUsd(feeWei, chainInfo)

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
				WalletAddress:    wa.Address,
				TxHash:           transfer.TransactionHash,
				Chain:            chainInfo.Chain,
				BlockNumber:      blockNumber,
				BlockTime:        blockTime,
				FromAddress:      strings.ToLower(transfer.From),
				ToAddress:        strings.ToLower(transfer.To),
				Amount:           amountStr,
				GasUsed:          gasUsed.Int64(),
				GasPrice:         gasPrice.String(),
				NetworkFee:       feeWei.String(),
				NetworkFeeNative: feeNativeStr,
				NetworkFeeUsd:    feeUsd,
				Status:           "success",
				TransactionType:  classifyMoralisErc20TxType(transfer, txTypeByHash),
				MethodId:         methodIDFromInput(txRef.Input),
				Standard:         "erc20",
				ContractAddress:  strings.ToLower(transfer.Contract),
				TokenName:        transfer.TokenName,
				TokenSymbol:      transfer.TokenSymbol,
				TokenDecimal:     uint8(decimals),
				Direction:        direction,
				CreatedAt:        time.Now(),
				GasLimit:         parseInt64(txRef.Gas),
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

			logTxHistoryResult("MORALIS", "evm_transaction_histories", wa.Address, chainInfo.Chain, dbTx.TxHash, dbTx.Direction, dbTx.Amount, result.RowsAffected, result.Error)
			if result.Error == nil {
				triggerNotification(NotificationParams{
					Address:   wa.Address,
					Chain:     chainInfo.Chain,
					Amount:    dbTx.Amount,
					Symbol:    dbTx.TokenSymbol,
					Direction: dbTx.Direction,
					TxHash:    dbTx.TxHash,
					TxType:    dbTx.TransactionType,
					ChainID:   strconv.FormatInt(dbTx.ChainId, 10),
					Source:    "evm_transaction_histories",
					Rows:      result.RowsAffected,
				})
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

		// Deduct gas fees (native token) from sender
		if feeNativeStr != "" && feeNativeStr != "0" {
			updateUserBalanceForAddress(chainInfo.RedisKey, transfer.From, nativeTokenAddress(chainInfo.RedisKey), negateAmount(feeNativeStr))
		}
	}
}

func moralisErc20TransferAmount(transfer MoralisErc20Transfer) (string, int, bool) {
	valueInt := parseBigInt(transfer.Value)
	if valueInt.Sign() <= 0 {
		return "", 0, false
	}

	decimals := 18
	if transfer.TokenDecimals != "" {
		if parsed, err := strconv.Atoi(transfer.TokenDecimals); err == nil {
			decimals = parsed
		}
	}

	amountStr := formatTokenAmount(valueInt, decimals)
	if amountStr == "" || amountStr == "0" {
		return "", decimals, false
	}

	return amountStr, decimals, true
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
	txStatus := btcTransactionStatus(tx, blockNumber, blockTime)

	netByAddress := map[string]*big.Int{}

	for _, vin := range tx.Vin {
		for _, addr := range vin.Addresses {
			if addr == "" {
				continue
			}
			valueInt := parseBigInt(vin.Value)
			if netByAddress[addr] == nil {
				netByAddress[addr] = big.NewInt(0)
			}
			netByAddress[addr].Sub(netByAddress[addr], valueInt)
		}
	}

	for _, vout := range tx.Vout {
		for _, addr := range vout.Addresses {
			if addr == "" {
				continue
			}
			valueInt := parseBigInt(vout.Value)
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
				Status:          txStatus,
				TransactionType: txTypeNativeTransfer,
				Direction:       direction,
				CreatedAt:       time.Now(),
				NetworkFeeSats:  feeInt.Int64(),
				Standard:        "native",
			}

			result := db.Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "address"}, {Name: "tx_hash"}},
				DoUpdates: clause.Assignments(map[string]interface{}{
					"chain":            gorm.Expr("IF(status = 'success', chain, VALUES(chain))"),
					"block_number":     gorm.Expr("IF(status = 'success', block_number, VALUES(block_number))"),
					"block_time":       gorm.Expr("IF(status = 'success', block_time, VALUES(block_time))"),
					"from_address":     gorm.Expr("IF(status = 'success', from_address, VALUES(from_address))"),
					"to_address":       gorm.Expr("IF(status = 'success', to_address, VALUES(to_address))"),
					"token_amount":     gorm.Expr("IF(status = 'success', token_amount, VALUES(token_amount))"),
					"network_fee":      gorm.Expr("IF(status = 'success', network_fee, VALUES(network_fee))"),
					"status":           gorm.Expr("IF(status = 'success', status, VALUES(status))"),
					"transaction_type": gorm.Expr("IF(status = 'success', transaction_type, VALUES(transaction_type))"),
					"direction":        gorm.Expr("IF(status = 'success', direction, VALUES(direction))"),
					"network_fee_sats": gorm.Expr("IF(status = 'success', network_fee_sats, VALUES(network_fee_sats))"),
					"standard":         gorm.Expr("IF(status = 'success', standard, VALUES(standard))"),
					"contract_address": gorm.Expr("IF(status = 'success', contract_address, VALUES(contract_address))"),
					"token_name":       gorm.Expr("IF(status = 'success', token_name, VALUES(token_name))"),
					"token_symbol":     gorm.Expr("IF(status = 'success', token_symbol, VALUES(token_symbol))"),
					"token_decimal":    gorm.Expr("IF(status = 'success', token_decimal, VALUES(token_decimal))"),
				}),
			}).Create(&dbTx)

			logTxHistoryResult("BTC", "btc_transaction_histories", wa.Address, "BTC", dbTx.TxHash, dbTx.Direction, dbTx.Amount, result.RowsAffected, result.Error)
			if result.Error == nil {
				if dbTx.Status == "success" {
					deleteReplacedBtcPendingTransactions(dbTx)
					triggerNotification(NotificationParams{
						Address:   wa.Address,
						Chain:     "BTC",
						Amount:    dbTx.Amount,
						Symbol:    "BTC",
						Direction: dbTx.Direction,
						TxHash:    dbTx.TxHash,
						TxType:    dbTx.TransactionType,
						Source:    "btc_transaction_histories",
						Rows:      result.RowsAffected,
					})
				}
				updateRedis(wa.Address, "btc", dbTx)
				if dbTx.Status == "success" && result.RowsAffected > 0 {
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
	switch strings.ToLower(strings.TrimSpace(chainID)) {
	case "eth", "ethereum", "1", "0x1", "ethereum-mainnet":
		return "ETH"
	case "tron", "tron-mainnet", "trx":
		return "TRX"
	case "btc", "bitcoin", "bitcoin-mainnet", "000000000019d6689c085ae165831e93":
		return "BTC"
	case "sol", "solana", "solana-mainnet", "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp":
		return "SOL"
	case "pol", "polygon", "137", "0x89", "polygon-mainnet":
		return "POL"
	case "base", "8453", "0x2105", "base-mainnet":
		return "BASE ETH"
	case "bsc", "56", "0x38", "bsc-mainnet", "binance-smart-chain":
		return "BNB"
	default:
		trimmed := strings.TrimSpace(chainID)
		if trimmed == "" {
			return "native"
		}
		return strings.ToUpper(trimmed)
	}
}

func normalizeAddress(chainID, address string) string {
	if address == "" {
		return ""
	}
	c := strings.ToLower(chainID)
	switch {
	case c == "tron" || c == "tron-mainnet" || c == "trx" || c == "65":
		return normalizeTronAddress(address)
	case c == "eth" || c == "pol" || c == "base" || c == "bsc" || c == "1" || c == "137" || c == "8453" || c == "56":
		return strings.ToLower(address)
	default:
		return address
	}
}

func normalizeTokenAddress(chainID, tokenAddress string) string {
	if tokenAddress == "" {
		return ""
	}
	c := strings.ToLower(strings.TrimSpace(chainID))
	t := strings.TrimSpace(tokenAddress)

	// Special case for BASE native token. Provider might send "ETH" for BASE.
	if (c == "base" || c == "8453" || c == "0x2105") && strings.EqualFold(t, "ETH") {
		return "BASE ETH"
	}

	switch {
	case c == "tron" || c == "tron-mainnet" || c == "trx" || c == "65":
		if strings.EqualFold(t, nativeTokenAddress("tron")) {
			return nativeTokenAddress("tron")
		}
		return normalizeTronAddress(t)
	case c == "eth" || c == "pol" || c == "base" || c == "bsc" || c == "1" || c == "137" || c == "8453" || c == "56":
		if looksLikeEvmAddress(t) {
			return strings.ToLower(t)
		}
		if strings.Contains(t, " ") {
			return t
		}
		// Check if it's a native token symbol for other chains to be consistent with nativeTokenAddress
		officialNative := nativeTokenAddress(c)
		if strings.EqualFold(t, officialNative) {
			return officialNative
		}
		return strings.ToLower(t)
	default:
		return t
	}
}

func looksLikeEvmAddress(value string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false
	}
	if strings.HasPrefix(trimmed, "0x") || strings.HasPrefix(trimmed, "0X") {
		trimmed = trimmed[2:]
	}
	if len(trimmed) != 40 {
		return false
	}
	for _, r := range trimmed {
		if !isHexChar(r) {
			return false
		}
	}
	return true
}

func isHexChar(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
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

func isNegativeDecimalString(value string) bool {
	v := strings.TrimSpace(value)
	return strings.HasPrefix(v, "-")
}

func normalizeTronAddress(address string) string {
	trimmed := strings.TrimSpace(address)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "T") {
		return trimmed
	}
	hexPart := trimmed
	base58, err := HexToTronAddress(hexPart)
	if err != nil || base58 == "" {
		return trimmed
	}
	return base58
}

func looksLikeTronHex41Address(value string) bool {
	trimmed := strings.TrimSpace(value)
	if strings.HasPrefix(trimmed, "0x") || strings.HasPrefix(trimmed, "0X") {
		trimmed = trimmed[2:]
	}
	if len(trimmed) != 42 {
		return false
	}
	if !(trimmed[0] == '4' && trimmed[1] == '1') {
		return false
	}
	for _, r := range trimmed {
		if !isHexChar(r) {
			return false
		}
	}
	return true
}

func normalizeTronAddressForStorage(address string) string {
	normalized := strings.TrimSpace(normalizeTronAddress(address))
	if normalized == "" {
		return ""
	}
	// Prevent hex-like values from being persisted into Tron history fields.
	if strings.HasPrefix(strings.ToLower(normalized), "0x") || looksLikeEvmAddress(normalized) || looksLikeTronHex41Address(normalized) {
		return ""
	}
	return normalized
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
	chainID = strings.TrimSpace(chainID)
	address = strings.TrimSpace(address)
	tokenAddress = strings.TrimSpace(tokenAddress)
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
		// Use the already normalized tokenAddress if possible
		effectiveToken := tokenAddress
		if strings.TrimSpace(effectiveToken) == "" {
			effectiveToken = nativeTokenAddress(wa.ChainID)
		} else {
			// Ensure it's correctly normalized for THIS specific wallet record's chain
			effectiveToken = normalizeTokenAddress(wa.ChainID, effectiveToken)
		}

		if err := upsertUserBalance(strings.TrimSpace(wa.WalletID), normalizeAddress(wa.ChainID, wa.Address), strings.TrimSpace(wa.ChainID), effectiveToken, delta); err != nil {
			log.Printf("[BALANCE] update failed for %s (%s): %v", wa.Address, wa.ChainID, err)
		}
	}
}

func resolveWalletAddresses(chainID, address string) ([]WalletAddress, error) {
	normalizedAddr := normalizeAddress(chainID, address)
	if normalizedAddr == "" {
		return nil, nil
	}

	chainCandidates := uniqueNonEmptyStrings(getChainSynonyms(chainID))
	if len(chainCandidates) == 0 {
		chainCandidates = []string{strings.TrimSpace(chainID)}
	}

	cacheKey := strings.ToLower(strings.Join(chainCandidates, ",")) + "|" + strings.ToLower(strings.TrimSpace(normalizedAddr))
	if cachedRows, ok := walletResolveCacheLookup(cacheKey); ok {
		return cachedRows, nil
	}

	addrCandidates := []string{normalizedAddr}
	log.Printf("[ADDR_RESOLVE] start chain_id=%s input_address=%s normalized_address=%s", chainID, address, normalizedAddr)

	c := strings.ToLower(strings.TrimSpace(chainID))
	if c == "tron" || c == "tron-mainnet" || c == "trx" || c == "65" {
		if base58Addr, err := HexToTronAddress(normalizedAddr); err == nil {
			if base58Addr != "" && !contains(addrCandidates, base58Addr) {
				addrCandidates = append(addrCandidates, base58Addr)
			}
		} else {
			log.Printf("[ADDR_RESOLVE] tron hex->base58 conversion skipped chain_id=%s normalized_address=%s err=%v", chainID, normalizedAddr, err)
		}
	}

	addrCandidates = uniqueNonEmptyStrings(addrCandidates)
	log.Printf("[ADDR_RESOLVE] candidates chain_id=%s chains=%v addresses=%v", chainID, chainCandidates, addrCandidates)

	// Fast path: exact matches keep index usage on wallet_addresses(chain_id, address).
	var rows []WalletAddress
	if err := db.Where("chain_id IN ? AND address IN ?", chainCandidates, addrCandidates).Find(&rows).Error; err != nil {
		log.Printf("[ADDR_RESOLVE] query_error exact_address chain_candidates=%v address_candidates=%v err=%v", chainCandidates, addrCandidates, err)
		return nil, err
	}
	if len(rows) > 0 {
		log.Printf("[ADDR_RESOLVE] matched source=address rows=%d", len(rows))
		walletResolveCacheStore(cacheKey, rows)
		return rows, nil
	}

	if err := db.Where("chain_id IN ? AND address_hex IN ?", chainCandidates, addrCandidates).Find(&rows).Error; err != nil {
		log.Printf("[ADDR_RESOLVE] query_error exact_address_hex chain_candidates=%v address_candidates=%v err=%v", chainCandidates, addrCandidates, err)
		return nil, err
	}
	if len(rows) > 0 {
		log.Printf("[ADDR_RESOLVE] matched source=address_hex rows=%d", len(rows))
		walletResolveCacheStore(cacheKey, rows)
		return rows, nil
	}

	// Compatibility fallback for legacy mixed-case data.
	lowerChains := lowerCopy(chainCandidates)
	lowerAddrs := lowerCopy(addrCandidates)

	if err := db.Where("LOWER(chain_id) IN ? AND LOWER(address) IN ?", lowerChains, lowerAddrs).Find(&rows).Error; err != nil {
		log.Printf("[ADDR_RESOLVE] query_error fallback_address chain_candidates=%v address_candidates=%v err=%v", chainCandidates, addrCandidates, err)
		return nil, err
	}
	if len(rows) > 0 {
		log.Printf("[ADDR_RESOLVE] matched source=fallback_address rows=%d", len(rows))
		walletResolveCacheStore(cacheKey, rows)
		return rows, nil
	}

	if err := db.Where("LOWER(chain_id) IN ? AND LOWER(address_hex) IN ?", lowerChains, lowerAddrs).Find(&rows).Error; err != nil {
		log.Printf("[ADDR_RESOLVE] query_error fallback_address_hex chain_candidates=%v address_candidates=%v err=%v", chainCandidates, addrCandidates, err)
		return nil, err
	}
	if len(rows) > 0 {
		log.Printf("[ADDR_RESOLVE] matched source=fallback_address_hex rows=%d", len(rows))
		walletResolveCacheStore(cacheKey, rows)
		return rows, nil
	}

	log.Printf("[ADDR_RESOLVE] no_match chain_id=%s input_address=%s", chainID, address)
	return nil, nil
}

func getChainSynonyms(chainID string) []string {
	c := strings.ToLower(chainID)
	switch c {
	case "eth", "ethereum", "1", "0x1", "ethereum-mainnet":
		return []string{"1"}
	case "tron", "tron-mainnet", "trx", "65":
		return []string{"65"}
	case "btc", "bitcoin", "bitcoin-mainnet", "000000000019d6689c085ae165831e93":
		return []string{"000000000019d6689c085ae165831e93"}
	case "sol", "solana", "solana-mainnet", "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp":
		return []string{"5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"}
	case "pol", "polygon", "137", "0x89", "polygon-mainnet":
		return []string{"137"}
	case "base", "8453", "0x2105", "base-mainnet":
		return []string{"8453"}
	case "bsc", "56", "0x38", "bsc-mainnet", "binance-smart-chain":
		return []string{"56"}
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

func parseEnvBool(name string) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	switch value {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func uniqueNonEmptyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		key := strings.ToLower(trimmed)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func lowerCopy(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		out = append(out, strings.ToLower(trimmed))
	}
	return out
}

func cloneWalletAddressRows(rows []WalletAddress) []WalletAddress {
	if len(rows) == 0 {
		return []WalletAddress{}
	}
	cloned := make([]WalletAddress, len(rows))
	copy(cloned, rows)
	return cloned
}

func walletResolveCacheLookup(key string) ([]WalletAddress, bool) {
	now := time.Now()

	walletResolveCacheMu.RLock()
	entry, ok := walletResolveCache[key]
	walletResolveCacheMu.RUnlock()
	if !ok {
		return nil, false
	}
	if now.After(entry.expiresAt) {
		walletResolveCacheMu.Lock()
		if current, exists := walletResolveCache[key]; exists && now.After(current.expiresAt) {
			delete(walletResolveCache, key)
		}
		walletResolveCacheMu.Unlock()
		return nil, false
	}
	return cloneWalletAddressRows(entry.rows), true
}

func walletResolveCacheStore(key string, rows []WalletAddress) {
	walletResolveCacheMu.Lock()
	if len(walletResolveCache) >= walletResolveCacheMaxEntries {
		walletResolveCache = map[string]walletResolveCacheEntry{}
	}
	walletResolveCache[key] = walletResolveCacheEntry{
		rows:      cloneWalletAddressRows(rows),
		expiresAt: time.Now().Add(walletResolveCacheTTL),
	}
	walletResolveCacheMu.Unlock()
}

func upsertUserBalance(walletID, address, chainID, tokenAddress, delta string) error {
	// Normalize all keys for case-insensitive DB operations
	walletID = strings.TrimSpace(walletID)
	chainID = strings.TrimSpace(chainID)
	address = normalizeAddress(chainID, strings.TrimSpace(address))
	tokenAddress = normalizeTokenAddress(chainID, strings.TrimSpace(tokenAddress))
	delta = strings.TrimSpace(delta)
	insertBalance := delta
	if isNegativeDecimalString(delta) {
		insertBalance = "0"
	}

	beforeBalance := ""
	if enableBalanceAuditLogs {
		beforeBalance = "0"
		var before UserBalance
		if err := db.Select("balance").Where(
			"LOWER(wallet_id) = LOWER(?) AND LOWER(address) = LOWER(?) AND LOWER(chain_id) = LOWER(?) AND LOWER(token_address) = LOWER(?)",
			walletID, address, chainID, tokenAddress,
		).First(&before).Error; err == nil {
			beforeBalance = before.Balance
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			log.Printf("[BALANCE] read-before failed for wallet_id=%s address=%s chain_id=%s token=%s: %v", walletID, address, chainID, tokenAddress, err)
		}
	}

	now := time.Now()
	record := UserBalance{
		WalletID:     walletID,
		Address:      address,
		ChainID:      chainID,
		TokenAddress: tokenAddress,
		Balance:      insertBalance,
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
			"balance":     gorm.Expr("GREATEST(balance + ?, 0)", delta),
			"last_active": now,
		}),
	}).Create(&record)

	if result.Error != nil {
		log.Printf("[BALANCE] update failed for wallet_id=%s address=%s chain_id=%s token=%s delta=%s error: %v", walletID, address, chainID, tokenAddress, delta, result.Error)
		return result.Error
	}

	afterBalance := ""
	if enableBalanceAuditLogs {
		afterBalanceForLog := "unknown"
		var after UserBalance
		if err := db.Select("balance").Where(
			"LOWER(wallet_id) = LOWER(?) AND LOWER(address) = LOWER(?) AND LOWER(chain_id) = LOWER(?) AND LOWER(token_address) = LOWER(?)",
			walletID, address, chainID, tokenAddress,
		).First(&after).Error; err == nil {
			afterBalance = after.Balance
			afterBalanceForLog = after.Balance
		} else {
			log.Printf("[BALANCE] read-after failed for wallet_id=%s address=%s chain_id=%s token=%s: %v", walletID, address, chainID, tokenAddress, err)
		}

		log.Printf("[USER_BALANCE] wallet_id=%s chain_id=%s address=%s token=%s before=%s delta=%s after=%s", walletID, chainID, address, tokenAddress, beforeBalance, delta, afterBalanceForLog)
	}

	updateUserBalanceRedisIfExists(walletID, chainID, address, tokenAddress, afterBalance)
	return nil
}

func logTxHistoryResult(tag, table, wallet, chain, txHash, direction, amount string, rowsAffected int64, err error) {
	if err != nil {
		log.Printf("[TX][%s] status=error table=%s wallet=%s chain=%s tx=%s err=%v", tag, table, wallet, chain, txHash, err)
		return
	}
	if rowsAffected > 0 {
		log.Printf("[TX][%s] status=added table=%s wallet=%s chain=%s tx=%s direction=%s amount=%s", tag, table, wallet, chain, txHash, direction, amount)
		return
	}
	log.Printf("[TX][%s] status=not_added_exists table=%s wallet=%s chain=%s tx=%s direction=%s amount=%s", tag, table, wallet, chain, txHash, direction, amount)
}

func updateUserBalanceRedisIfExists(walletID, chainID, address, tokenAddress, balanceHint string) {
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

	updatedBalance := strings.TrimSpace(balanceHint)
	if updatedBalance == "" {
		var updated UserBalance
		if err := db.Select("balance").Where(
			"LOWER(wallet_id) = LOWER(?) AND LOWER(address) = LOWER(?) AND LOWER(chain_id) = LOWER(?) AND LOWER(token_address) = LOWER(?)",
			walletID, address, chainID, tokenAddress,
		).First(&updated).Error; err != nil {
			log.Printf("[BALANCE] db read failed for %s (%s): %v", address, chainID, err)
			return
		}
		updatedBalance = updated.Balance
	}

	if err := rdb.HSet(ctx, key, tokenAddress, updatedBalance).Err(); err != nil {
		log.Printf("[BALANCE] redis update failed for %s: %v", key, err)
	}
}

func parseFlexibleBigInt(value string) (*big.Int, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, false
	}
	base := 10
	if strings.HasPrefix(value, "0x") || strings.HasPrefix(value, "0X") {
		base = 0
	}
	return new(big.Int).SetString(value, base)
}

func parseBigInt(value string) *big.Int {
	parsed, ok := parseFlexibleBigInt(value)
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
		if len(vin.Addresses) > 0 && vin.Addresses[0] != "" {
			return vin.Addresses[0]
		}
	}
	return ""
}

func firstVoutAddress(vouts []BtcVout) string {
	for _, vout := range vouts {
		if len(vout.Addresses) > 0 && vout.Addresses[0] != "" {
			return vout.Addresses[0]
		}
	}
	return ""
}

func btcTransactionStatus(tx BtcTx, blockNumber, blockTime int64) string {
	if tx.Confirmations > 0 {
		return "success"
	}
	if blockNumber > 0 || blockTime > 0 || strings.TrimSpace(tx.BlockHash) != "" {
		return "success"
	}
	return "pending"
}

func deleteReplacedBtcPendingTransactions(confirmedTx BtcTransactionHistory) {
	if db == nil {
		return
	}

	address := normalizeAddress("btc", strings.TrimSpace(confirmedTx.WalletAddress))
	chain := strings.ToUpper(strings.TrimSpace(confirmedTx.Chain))
	txHash := strings.TrimSpace(confirmedTx.TxHash)
	direction := strings.TrimSpace(confirmedTx.Direction)
	amount := strings.TrimSpace(confirmedTx.Amount)
	fromAddr := normalizeAddress("btc", strings.TrimSpace(confirmedTx.FromAddress))
	toAddr := normalizeAddress("btc", strings.TrimSpace(confirmedTx.ToAddress))

	if address == "" || chain == "" || txHash == "" || direction == "" || amount == "" {
		return
	}

	recentPendingCutoff := time.Now().Add(-72 * time.Hour)
	result := db.Where(
		"LOWER(address) = LOWER(?) AND LOWER(chain) = LOWER(?) AND LOWER(status) = 'pending' AND LOWER(tx_hash) <> LOWER(?) AND direction = ? AND token_amount = ? AND LOWER(COALESCE(from_address, '')) = LOWER(?) AND LOWER(COALESCE(to_address, '')) = LOWER(?) AND created_at >= ?",
		address, chain, txHash, direction, amount, fromAddr, toAddr, recentPendingCutoff,
	).Delete(&BtcTransactionHistory{})

	if result.Error != nil {
		log.Printf("[BTC] pending replace-delete failed address=%s tx=%s err=%v", address, txHash, result.Error)
		return
	}
	if result.RowsAffected > 0 {
		log.Printf("[BTC] deleted_replaced_pending address=%s tx=%s rows=%d", address, txHash, result.RowsAffected)
	}
}

func sha256d(data []byte) []byte {
	first := sha256.Sum256(data)
	second := sha256.Sum256(first[:])
	return second[:]
}

func HexToTronAddress(hexAddr string) (string, error) {
	hexAddr = strings.TrimSpace(hexAddr)
	hexAddr = strings.TrimPrefix(hexAddr, "0x")
	hexAddr = strings.TrimPrefix(hexAddr, "0X")

	addrBytes, err := hex.DecodeString(hexAddr)
	if err != nil {
		return "", err
	}

	// Some providers send TRON addresses as 21-byte hex with 0x41 prefix.
	// Convert both 20-byte (EVM-style) and 21-byte TRON-prefixed formats.
	if len(addrBytes) == 21 {
		if addrBytes[0] != 0x41 {
			return "", fmt.Errorf("invalid tron address prefix byte: 0x%x", addrBytes[0])
		}
		addrBytes = addrBytes[1:]
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
	if value == nil {
		return "0"
	}
	if decimals <= 0 {
		return value.String()
	}

	sign := ""
	absValue := new(big.Int).Set(value)
	if absValue.Sign() < 0 {
		sign = "-"
		absValue.Abs(absValue)
	}

	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	whole := new(big.Int)
	fraction := new(big.Int)
	whole.QuoRem(absValue, divisor, fraction)

	if fraction.Sign() == 0 {
		return sign + whole.String()
	}

	fracStr := fraction.String()
	if len(fracStr) < decimals {
		fracStr = strings.Repeat("0", decimals-len(fracStr)) + fracStr
	}
	fracStr = strings.TrimRight(fracStr, "0")
	if fracStr == "" {
		return sign + whole.String()
	}

	return sign + whole.String() + "." + fracStr
}

type priceCacheEntry struct {
	price     float64
	fetchedAt time.Time
}

func computeNetworkFeeNativeAndUsd(feeWei *big.Int, chainInfo evmChainInfo) (string, float64) {
	if feeWei == nil {
		return "0", 0
	}

	decimals := nativeTokenDecimals(chainInfo)
	feeNativeStr := formatTokenAmount(feeWei, decimals)
	if feeWei.Sign() == 0 {
		return feeNativeStr, 0
	}

	priceUsd, ok := getNativeTokenPriceUSD(chainInfo)
	if !ok || priceUsd <= 0 {
		return feeNativeStr, 0
	}

	feeNativeFloat := new(big.Float).SetInt(feeWei)
	divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
	feeNativeFloat.Quo(feeNativeFloat, divisor)
	feeUsdFloat := new(big.Float).Mul(feeNativeFloat, big.NewFloat(priceUsd))
	feeUsd, _ := feeUsdFloat.Float64()

	return feeNativeStr, roundToDecimals(feeUsd, 8)
}

func nativeTokenDecimals(chainInfo evmChainInfo) int {
	switch strings.ToUpper(chainInfo.Chain) {
	case "ETH", "POL", "BASE", "BSC":
		return 18
	default:
		return 18
	}
}

func getNativeTokenPriceUSD(chainInfo evmChainInfo) (float64, bool) {
	chainKey := strings.ToUpper(chainInfo.Chain)
	if chainKey == "" {
		return 0, false
	}

	nativePriceCacheMu.Lock()
	if entry, ok := nativePriceCache[chainKey]; ok && time.Since(entry.fetchedAt) < nativePriceCacheTTL {
		nativePriceCacheMu.Unlock()
		return entry.price, true
	}
	nativePriceCacheMu.Unlock()

	price, err := fetchNativeTokenPriceUSD(chainInfo)
	if err != nil || price <= 0 {
		if err != nil {
			log.Printf("[PRICE] fetch failed for %s: %v", chainKey, err)
		}
		return 0, false
	}

	nativePriceCacheMu.Lock()
	nativePriceCache[chainKey] = priceCacheEntry{price: price, fetchedAt: time.Now()}
	nativePriceCacheMu.Unlock()
	return price, true
}

func fetchNativeTokenPriceUSD(chainInfo evmChainInfo) (float64, error) {
	chainKey := strings.ToUpper(chainInfo.Chain)
	switch chainKey {
	case "ETH", "BASE":
		if apiKey := strings.TrimSpace("QVDVP85WK5D2UT77DWYEZPHGWZ2U5JFU3K"); apiKey != "" {
			if price, err := fetchEtherscanEthPriceUSD(apiKey); err == nil && price > 0 {
				return price, nil
			}
		}
		return fetchCoinGeckoPriceUSD("ethereum")
	case "POL":
		return fetchCoinGeckoPriceUSD("polygon-ecosystem-token")
	case "BSC":
		return fetchCoinGeckoPriceUSD("binancecoin")
	default:
		return 0, fmt.Errorf("unsupported chain: %s", chainKey)
	}
}

type etherscanPriceResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Result  struct {
		Ethusd string `json:"ethusd"`
	} `json:"result"`
}

func fetchEtherscanEthPriceUSD(apiKey string) (float64, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("https://api.etherscan.io/api?module=stats&action=ethprice&apikey=%s", apiKey), nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "back-trans/price")

	resp, err := priceHTTPClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("etherscan status %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}

	var payload etherscanPriceResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0, err
	}

	if payload.Status != "" && payload.Status != "1" {
		return 0, fmt.Errorf("etherscan error: %s", payload.Message)
	}

	price, err := strconv.ParseFloat(payload.Result.Ethusd, 64)
	if err != nil {
		return 0, err
	}
	return price, nil
}

func fetchCoinGeckoPriceUSD(coinID string) (float64, error) {
	if coinID == "" {
		return 0, fmt.Errorf("missing coingecko id")
	}

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("https://api.coingecko.com/api/v3/simple/price?ids=%s&vs_currencies=usd", coinID), nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "back-trans/price")

	resp, err := priceHTTPClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("coingecko status %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}

	var payload map[string]map[string]float64
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0, err
	}

	coinData, ok := payload[coinID]
	if !ok {
		return 0, fmt.Errorf("coingecko response missing %s", coinID)
	}
	price, ok := coinData["usd"]
	if !ok {
		return 0, fmt.Errorf("coingecko response missing usd price for %s", coinID)
	}
	return price, nil
}

func roundToDecimals(value float64, decimals int) float64 {
	if decimals < 0 {
		return value
	}
	pow := math.Pow10(decimals)
	return math.Round(value*pow) / pow
}

type NotificationParams struct {
	Address   string
	Chain     string
	Amount    string
	Symbol    string
	Direction string
	TxHash    string
	TxType    string
	ChainID   string
	Extras    map[string]string
	Source    string
	Rows      int64
}

func triggerNotification(params NotificationParams) {
	address := params.Address
	chain := params.Chain
	amount := params.Amount
	symbol := params.Symbol
	direction := params.Direction
	txHash := params.TxHash
	txType := params.TxType
	extras := params.Extras
	source := params.Source
	rows := params.Rows

	txStatus := "not_added_exists"
	if rows > 0 {
		txStatus = "added"
	}

	if symbol == "" || symbol == "native" {
		symbol = nativeTokenAddress(chain)
		if symbol == "native" && chain != "" {
			symbol = strings.ToUpper(chain)
		}
	}

	// 1. Find the WalletID(s) associated with this address
	chainID := getChainSynonyms(chain)[0]
	var walletAddrs []WalletAddress
	err := db.Where("address = ? AND chain_id = ?", normalizeAddress(chain, address), chainID).Find(&walletAddrs).Error
	if err != nil {
		log.Printf("[NOTIFY] tx_status=%s tx=%s chain=%s address=%s source=%s status=wallet_lookup_error err=%v", txStatus, txHash, chain, address, source, err)
		return
	}
	if len(walletAddrs) == 0 {
		log.Printf("[NOTIFY] tx_status=%s tx=%s chain=%s address=%s source=%s status=skipped_no_wallet", txStatus, txHash, chain, address, source)
		return
	}

	totalDevices := 0
	sendSuccess := 0
	sendFailed := 0
	invalidToken := 0

	// 2. Broadcast to all devices linked to these WalletIDs
	for _, wa := range walletAddrs {
		var devices []UserWalletDevice
		if err := db.Where("wallet_id = ?", wa.WalletID).Find(&devices).Error; err != nil {
			log.Printf("[NOTIFY] tx_status=%s tx=%s chain=%s address=%s source=%s wallet_id=%s status=device_lookup_error err=%v", txStatus, txHash, chain, address, source, wa.WalletID, err)
			continue
		}
		totalDevices += len(devices)

		title := "Transaction Detected"
		body := ""

		if direction == "send" {
			title = "Transaction Sent"
			body = fmt.Sprintf("Successfully sent %s %s", amount, symbol)
		} else {
			title = "Transaction Received"
			body = fmt.Sprintf("You received %s %s ", amount, symbol)
		}

		finalChainID := wa.ChainID

		data := map[string]string{
			"wallet_id": wa.WalletID,
			"address":   address,
			"amount":    amount,
			"symbol":    symbol,
			"chain":     chain,
			"chain_id":  finalChainID,
			"direction": direction,
			"tx_hash":   txHash,
			"tx_type":   txType,
			"type":      "transaction_alert",
		}

		// Add any extra generic fields
		for k, v := range extras {
			data[k] = v
		}

		for _, dev := range devices {
			err := SendPushNotification(dev.DeviceToken, dev.DeviceType, title, body, data)

			// Record in DB
			logEntry := NotificationLog{
				WalletID:    wa.WalletID,
				DeviceToken: dev.DeviceToken,
				DeviceType:  dev.DeviceType,
				Title:       title,
				Body:        body,
				Status:      "success",
				TxHash:      txHash,
				Chain:       chain,
				Direction:   direction,
			}

			if err != nil {
				sendFailed++
				logEntry.Status = "failed"
				logEntry.ErrorMsg = err.Error()
				if strings.Contains(err.Error(), ErrTokenInvalid) {
					invalidToken++
					//db.Where("device_token = ?", dev.DeviceToken).Delete(&UserWalletDevice{})
				}
			} else {
				sendSuccess++
			}
			if err := db.Create(&logEntry).Error; err != nil {
				log.Printf("[NOTIFY] tx_status=%s tx=%s chain=%s address=%s source=%s status=notification_log_write_error wallet_id=%s err=%v", txStatus, txHash, chain, address, source, wa.WalletID, err)
			}
		}
	}
	status := "sent"
	switch {
	case totalDevices == 0:
		status = "skipped_no_device"
	case sendSuccess == 0 && sendFailed > 0:
		status = "failed"
	case sendFailed > 0:
		status = "partial"
	}
	log.Printf("[NOTIFY] tx_status=%s tx=%s chain=%s address=%s source=%s status=%s wallets=%d devices=%d sent=%d failed=%d invalid_tokens=%d", txStatus, txHash, chain, address, source, status, len(walletAddrs), totalDevices, sendSuccess, sendFailed, invalidToken)
}
