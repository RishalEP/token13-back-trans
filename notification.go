package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/certificate"
	"github.com/sideshow/apns2/token"
	"google.golang.org/api/option"
)

// Constants for identifying terminal errors
const (
	ErrTokenInvalid = "token_invalid"
)

type NotificationService struct {
	apnsClient *apns2.Client
	fcmClient  *messaging.Client
}

var notifyService *NotificationService

func InitNotificationService() {
	service := &NotificationService{}

	// 1. APNs Setup
	p8File := os.Getenv("APNS_P8_FILE")
	if p8File != "" {
		// Modern .p8 Token-based Auth
		authKey, err := token.AuthKeyFromFile(p8File)
		if err != nil {
			log.Printf("[Notification] Fatal: APNs .p8 key error: %v", err)
		} else {
			tokenStruct := &token.Token{
				AuthKey: authKey,
				KeyID:   os.Getenv("APNS_KEY_ID"),
				TeamID:  os.Getenv("APNS_TEAM_ID"),
			}
			client := apns2.NewTokenClient(tokenStruct)
			setupAPNsEnvironment(service, client)
		}
	} else {
		// Fallback to legacy .p12 Certificate-based Auth
		certFile := os.Getenv("APNS_CERT_FILE")
		certPassword := os.Getenv("APNS_CERT_PASSWORD")
		if certFile != "" {
			cert, err := certificate.FromP12File(certFile, certPassword)
			if err != nil {
				log.Printf("[Notification] Fatal: APNs .p12 cert error: %v", err)
			} else {
				client := apns2.NewClient(cert)
				setupAPNsEnvironment(service, client)
			}
		} else {
			log.Println("[Notification] APNs Disabled")
		}
	}

	// 2. FCM Setup
	fcmCreds := os.Getenv("FCM_CREDENTIALS_FILE")
	if fcmCreds != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		opt := option.WithAuthCredentialsFile(option.ServiceAccount, fcmCreds)
		app, err := firebase.NewApp(ctx, nil, opt)
		if err != nil {
			log.Printf("[Notification] Fatal: Firebase init error: %v", err)
		} else {
			client, err := app.Messaging(ctx)
			if err != nil {
				log.Printf("[Notification] Fatal: FCM messaging error: %v", err)
			} else {
				service.fcmClient = client
				log.Println("[Notification] FCM Initialized")
			}
		}
	} else {
		log.Println("[Notification] FCM Disabled")
	}

	notifyService = service
}

func setupAPNsEnvironment(service *NotificationService, client *apns2.Client) {
	if strings.ToLower(os.Getenv("APNS_ENV")) == "sandbox" {
		service.apnsClient = client.Development()
		log.Println("[Notification] APNs Initialized (Development/Sandbox)")
	} else {
		service.apnsClient = client.Production()
		log.Println("[Notification] APNs Initialized (Production)")
	}
}

// SendPushNotification returns a specific error code "token_invalid" if the token should be deleted from your DB.
func SendPushNotification(token string, deviceType string, title, body string, data map[string]string) error {
	if notifyService == nil {
		return fmt.Errorf("notification service not initialized")
	}

	// Use a 10s timeout for all network push operations
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	deviceType = strings.ToLower(deviceType)
	switch deviceType {
	case "ios":
		return sendAPNsNotification(ctx, token, title, body, data)
	case "android":
		return sendFCMNotification(ctx, token, title, body, data)
	}
	return fmt.Errorf("unknown device type: %s", deviceType)
}

func sendAPNsNotification(ctx context.Context, token string, title, body string, customData map[string]string) error {
	if notifyService.apnsClient == nil {
		return fmt.Errorf("APNs client disabled")
	}

	topic := os.Getenv("APNS_TOPIC")
	if topic == "" {
		return fmt.Errorf("APNS_TOPIC missing")
	}

	payload := map[string]interface{}{
		"aps": map[string]interface{}{
			"alert": map[string]interface{}{
				"title": title,
				"body":  body,
			},
			"sound": "default",
		},
	}
	for k, v := range customData {
		payload[k] = v
	}

	notification := &apns2.Notification{
		DeviceToken: token,
		Topic:       topic,
		Payload:     payload,
		Priority:    apns2.PriorityHigh,
		PushType:    apns2.PushTypeAlert,
		Expiration:  time.Now().Add(24 * time.Hour), // Expire if not delivered in 24h
	}

	res, err := notifyService.apnsClient.PushWithContext(ctx, notification)
	if err != nil {
		return fmt.Errorf("APNs network err: %v", err)
	}

	if res.StatusCode != 200 {
		if res.Reason == apns2.ReasonBadDeviceToken || res.Reason == apns2.ReasonUnregistered {
			log.Printf("[Notification] Removing invalid APNS token: %s", token)
			return fmt.Errorf("%s: %s", ErrTokenInvalid, res.Reason)
		}
		return fmt.Errorf("APNs error %d: %s", res.StatusCode, res.Reason)
	}

	log.Printf("[Notification] APNs Sent: %s", token)
	return nil
}

func sendFCMNotification(ctx context.Context, token string, title, body string, data map[string]string) error {
	if notifyService.fcmClient == nil {
		return fmt.Errorf("FCM client disabled")
	}

	message := &messaging.Message{
		Token: token,
		Notification: &messaging.Notification{
			Title: title,
			Body:  body,
		},
		Data: data,
		Android: &messaging.AndroidConfig{
			Priority: "high", // Ensures delivery even in Doze mode
		},
	}

	_, err := notifyService.fcmClient.Send(ctx, message)
	if err != nil {
		if messaging.IsUnregistered(err) {
			log.Printf("[Notification] Removing invalid FCM token: %s", token)
			return fmt.Errorf("%s: unregistered", ErrTokenInvalid)
		}

		return fmt.Errorf("FCM error: %v", err)
	}

	log.Printf("[Notification] FCM Sent: %s", token)
	return nil
}
