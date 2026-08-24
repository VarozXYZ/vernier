package livecompare_test

import (
	"testing"

	"github.com/VarozXYZ/vernier/runtime/configuration"
	"github.com/VarozXYZ/vernier/runtime/livecompare"
)

func TestSuppressedConfiguredNotificationsDoNotResolveTelegramEnvironment(t *testing.T) {
	lookups := 0
	config := configuration.ParsedConfig{TelegramEnabled: true,
		TelegramBotTokenEnv: "BOT_TOKEN", TelegramChatIDEnv: "CHAT_ID"}
	_, err := livecompare.New(config, nil, livecompare.Options{
		SuppressConfiguredNotifications: true,
		LookupEnv: func(string) (string, bool) {
			lookups++
			return "unexpected", true
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if lookups != 0 {
		t.Fatalf("suppressed notification mode performed %d environment lookups", lookups)
	}
}
