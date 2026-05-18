package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	redEnvelopeStreamName                    = "red_envelope_events"
	redEnvelopeConsumerGroup                 = "red_envelope_forwarders"
	redEnvelopeForwardBatchSize        int64 = 20
	RED_ENVELOPE_MIGRATION_WEBHOOK_URL       = "https://test.first.digiedgete.click/migration/internal/webhooks/red-envelope"
)

var redEnvelopeForwardHTTPClient = &http.Client{Timeout: 15 * time.Second}

type quickNodeRedEnvelopeWebhookPayload struct {
	MatchingReceipts []json.RawMessage `json:"matchingReceipts"`
}

func redEnvelopeWebhookHandler(w http.ResponseWriter, r *http.Request) {
	reqID := fmt.Sprintf("re-req-%d", time.Now().UnixNano())
	start := time.Now()
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("[RED_ENVELOPE REQUEST %s] Failure: panic: %v", reqID, rec)
			log.Printf("[RED_ENVELOPE REQUEST %s] Panic stack: %s", reqID, debug.Stack())
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
	}()

	if r.Method != http.MethodPost {
		log.Printf("[RED_ENVELOPE REQUEST %s] Failure: method not allowed (%s)", reqID, r.Method)
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("[RED_ENVELOPE REQUEST %s] Failure: read error: %v", reqID, err)
		http.Error(w, "Read Error", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	if len(bodyBytes) == 0 {
		log.Printf("[RED_ENVELOPE REQUEST %s] Failure: empty body", reqID)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	payloads, pingOnly, err := extractRedEnvelopePayloads(bodyBytes)
	if err != nil {
		log.Printf("[RED_ENVELOPE REQUEST %s] Failure: invalid payload: %v", reqID, err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	if pingOnly {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Pong"))
		log.Printf("[RED_ENVELOPE REQUEST %s] Pong. AckDuration=%s", reqID, time.Since(start))
		return
	}

	if rdb == nil {
		log.Printf("[RED_ENVELOPE REQUEST %s] Failure: redis client is not initialized", reqID)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	for idx, payload := range payloads {
		if err := enqueueRedEnvelopeEvent(reqID, idx, payload); err != nil {
			log.Printf("[RED_ENVELOPE REQUEST %s] Failure: enqueue error: %v", reqID, err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Accepted"))
	log.Printf("[RED_ENVELOPE REQUEST %s] Accepted. Enqueued=%d AckDuration=%s", reqID, len(payloads), time.Since(start))
}

func extractRedEnvelopePayloads(bodyBytes []byte) ([][]byte, bool, error) {
	bodyTrimmed := strings.TrimSpace(string(bodyBytes))
	if bodyTrimmed == "" {
		return nil, false, fmt.Errorf("%w: empty body", errInvalidWebhookPayload)
	}

	payloads := make([][]byte, 0)
	pingCount := 0

	appendIfEvent := func(raw []byte, itemIdx int) error {
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return fmt.Errorf("%w: invalid json item %d: %v", errInvalidWebhookPayload, itemIdx, err)
		}

		if envelope["matchingReceipts"] != nil {
			var payload quickNodeRedEnvelopeWebhookPayload
			if err := json.Unmarshal(raw, &payload); err != nil {
				return fmt.Errorf("%w: invalid matchingReceipts item %d: %v", errInvalidWebhookPayload, itemIdx, err)
			}
			payloads = append(payloads, raw)
			return nil
		}

		if envelope["message"] != nil {
			msg := strings.ToLower(string(envelope["message"]))
			if strings.Contains(msg, "ping") {
				pingCount++
				return nil
			}
		}

		return fmt.Errorf("%w: unsupported payload item %d", errInvalidWebhookPayload, itemIdx)
	}

	if strings.HasPrefix(bodyTrimmed, "[") {
		var rawMessages []json.RawMessage
		if err := json.Unmarshal(bodyBytes, &rawMessages); err != nil {
			return nil, false, fmt.Errorf("%w: invalid json array: %v", errInvalidWebhookPayload, err)
		}
		if len(rawMessages) == 0 {
			return nil, false, fmt.Errorf("%w: empty json array", errInvalidWebhookPayload)
		}
		for i, msg := range rawMessages {
			if err := appendIfEvent([]byte(msg), i); err != nil {
				return nil, false, err
			}
		}
	} else {
		if err := appendIfEvent(bodyBytes, 0); err != nil {
			return nil, false, err
		}
	}

	if len(payloads) == 0 && pingCount > 0 {
		return nil, true, nil
	}
	if len(payloads) == 0 {
		return nil, false, fmt.Errorf("%w: no matchingReceipts events", errInvalidWebhookPayload)
	}

	return payloads, false, nil
}

func enqueueRedEnvelopeEvent(reqID string, itemIndex int, payload []byte) error {
	sum := sha256.Sum256(payload)
	payloadHash := hex.EncodeToString(sum[:])

	xAddCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	msgID, err := rdb.XAdd(xAddCtx, &redis.XAddArgs{
		Stream: redEnvelopeStreamName,
		Values: map[string]interface{}{
			"payload":      string(payload),
			"payload_hash": payloadHash,
			"received_at":  strconv.FormatInt(time.Now().Unix(), 10),
		},
	}).Result()
	if err != nil {
		return fmt.Errorf("redis xadd failed stream=%s item=%d hash=%s: %w", redEnvelopeStreamName, itemIndex, payloadHash, err)
	}

	log.Printf("[RED_ENVELOPE REQUEST %s] Enqueued stream=%s id=%s item=%d hash=%s", reqID, redEnvelopeStreamName, msgID, itemIndex, payloadHash)
	return nil
}

func startRedEnvelopeForwarder() {
	if rdb == nil {
		log.Printf("[RED_ENVELOPE FORWARDER] Redis unavailable; forwarder disabled")
		return
	}

	targetURL := strings.TrimSpace(RED_ENVELOPE_MIGRATION_WEBHOOK_URL)
	if targetURL == "" {
		log.Printf("[RED_ENVELOPE FORWARDER] RED_ENVELOPE_MIGRATION_WEBHOOK_URL is empty; forwarder disabled")
		return
	}

	groupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := rdb.XGroupCreateMkStream(groupCtx, redEnvelopeStreamName, redEnvelopeConsumerGroup, "0").Err(); err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		log.Printf("[RED_ENVELOPE FORWARDER] Failed to create stream group stream=%s group=%s err=%v", redEnvelopeStreamName, redEnvelopeConsumerGroup, err)
		return
	}

	consumerName := redEnvelopeConsumerName()
	log.Printf("[RED_ENVELOPE FORWARDER] Started stream=%s group=%s consumer=%s target=%s", redEnvelopeStreamName, redEnvelopeConsumerGroup, consumerName, targetURL)
	go runRedEnvelopeForwarder(consumerName, targetURL)
}

func runRedEnvelopeForwarder(consumerName, targetURL string) {
	for {
		claimed, err := claimStaleRedEnvelopeMessages(consumerName)
		if err != nil {
			log.Printf("[RED_ENVELOPE FORWARDER] claim stale failed: %v", err)
		}
		for _, msg := range claimed {
			if err := forwardAndAckRedEnvelopeMessage(targetURL, msg); err != nil {
				log.Printf("[RED_ENVELOPE FORWARDER] claim message forward failed id=%s err=%v", msg.ID, err)
			}
		}

		readCtx, cancel := context.WithTimeout(ctx, 35*time.Second)
		streams, err := rdb.XReadGroup(readCtx, &redis.XReadGroupArgs{
			Group:    redEnvelopeConsumerGroup,
			Consumer: consumerName,
			Streams:  []string{redEnvelopeStreamName, ">"},
			Count:    redEnvelopeForwardBatchSize,
			Block:    30 * time.Second,
		}).Result()
		cancel()

		if err != nil {
			if err == redis.Nil {
				continue
			}
			log.Printf("[RED_ENVELOPE FORWARDER] XREADGROUP failed: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				if err := forwardAndAckRedEnvelopeMessage(targetURL, msg); err != nil {
					log.Printf("[RED_ENVELOPE FORWARDER] forward failed id=%s err=%v", msg.ID, err)
				}
			}
		}
	}
}

func claimStaleRedEnvelopeMessages(consumerName string) ([]redis.XMessage, error) {
	pendingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	pending, err := rdb.XPendingExt(pendingCtx, &redis.XPendingExtArgs{
		Stream: redEnvelopeStreamName,
		Group:  redEnvelopeConsumerGroup,
		Idle:   60 * time.Second,
		Start:  "-",
		End:    "+",
		Count:  redEnvelopeForwardBatchSize,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	if len(pending) == 0 {
		return nil, nil
	}

	ids := make([]string, 0, len(pending))
	for _, p := range pending {
		ids = append(ids, p.ID)
	}

	claimCtx, claimCancel := context.WithTimeout(ctx, 3*time.Second)
	defer claimCancel()

	messages, err := rdb.XClaim(claimCtx, &redis.XClaimArgs{
		Stream:   redEnvelopeStreamName,
		Group:    redEnvelopeConsumerGroup,
		Consumer: consumerName,
		MinIdle:  60 * time.Second,
		Messages: ids,
	}).Result()
	if err != nil {
		return nil, err
	}
	return messages, nil
}

func forwardAndAckRedEnvelopeMessage(targetURL string, msg redis.XMessage) error {
	rawPayload, ok := msg.Values["payload"]
	if !ok {
		return fmt.Errorf("missing payload field")
	}

	payload, ok := rawPayload.(string)
	if !ok {
		return fmt.Errorf("invalid payload field type=%T", rawPayload)
	}
	payload = strings.TrimSpace(payload)
	if payload == "" {
		return fmt.Errorf("empty payload")
	}

	forwardCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(forwardCtx, http.MethodPost, targetURL, bytes.NewBufferString(payload))
	if err != nil {
		return fmt.Errorf("build request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Backtrans-Queue-Message-Id", msg.ID)
	req.Header.Set("X-Backtrans-Source-Stream", redEnvelopeStreamName)

	resp, err := redEnvelopeForwardHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("forward request failed: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("migration service returned status=%d", resp.StatusCode)
	}

	ackCtx, ackCancel := context.WithTimeout(ctx, 2*time.Second)
	defer ackCancel()

	if _, err := rdb.XAck(ackCtx, redEnvelopeStreamName, redEnvelopeConsumerGroup, msg.ID).Result(); err != nil {
		return fmt.Errorf("xack failed id=%s: %w", msg.ID, err)
	}

	log.Printf("[RED_ENVELOPE FORWARDER] Forwarded and ACKed stream=%s id=%s", redEnvelopeStreamName, msg.ID)
	return nil
}

func redEnvelopeConsumerName() string {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s-%d", hostname, time.Now().UnixNano())
}
