package main

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/joho/godotenv"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// Run this test with:
// go test -v -run TestManualPush

func TestManualPush(t *testing.T) {
	// 1. Load config
	err := godotenv.Load("cfg.env")
	if err != nil {
		log.Printf("Warning: cfg.env not loaded: %v", err)
	}

	// 2. Initialize Database
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "root:@tcp(127.0.0.1:3306)/token13_app_new?parseTime=True"
	}
	testDb, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("Failed to connect to DB: %v", err)
	}
	// Note: We use the local testDb here to fetch the record,
	// but the global 'db' variable used by triggerNotification
	// needs to be set if we were calling that.
	// Since we are calling SendPushNotification directly, we don't need the global db.
	db = testDb

	// 3. Initialize Notification Service
	InitNotificationService()

	// 4. Fetch the transaction (ID=1)
	var tx EvmTransactionHistory
	if err := db.First(&tx, 1).Error; err != nil {
		t.Logf("Could not find EVM transaction with ID 1: %v. Please ensure a record exists.", err)
		return
	}

	fmt.Printf("Found Transaction: %s | Amount: %s %s\n", tx.TxHash, tx.Amount, tx.TokenSymbol)

	// 5. HARDCODED DEVICE INFO - REPLACE THESE
	testDeviceToken := "dIljpRdiTHS_jTIPN1SOHq:APA91bFdc_VGPPN7iR9grO5cuT0n6VPo0geXWzQkZ7a_tgt-FUwa3Y06c7MoJjuPsHnM3xlL5kNmYMgRQy-bQI9jBqswD4dQfKQh605xtmJ2UwqEaX377Gc"
	testDeviceType := "android" // or "ios"

	if testDeviceToken == "PASTE_YOUR_DEVICE_TOKEN_HERE" {
		t.Skip("Please provide a real device token to run this test")
	}

	// 6. Construct Notification Data
	title := "Test Transaction Alert"
	body := fmt.Sprintf("Manual Test: You received %s %s on %s", tx.Amount, tx.TokenSymbol, tx.Chain)

	data := map[string]string{
		"tx_hash":   tx.TxHash,
		"amount":    tx.Amount,
		"symbol":    tx.TokenSymbol,
		"chain":     tx.Chain,
		"direction": tx.Direction,
		"type":      "manual_test",
	}

	// 7. Send Push
	fmt.Printf("Sending %s push to: %s...\n", testDeviceType, testDeviceToken)
	err = SendPushNotification(testDeviceToken, testDeviceType, title, body, data)
	if err != nil {
		t.Fatalf("Failed to send notification: %v", err)
	}

	fmt.Println("Notification sent successfully!")
}
