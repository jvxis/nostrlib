package khatru

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"fiatjaf.com/nostr"
	"fiatjaf.com/nostr/nip86"
	"github.com/stretchr/testify/require"
)

func callSupportedMethods(t *testing.T, rl *Relay, sk nostr.SecretKey) []string {
	t.Helper()

	payload, err := json.Marshal(map[string]any{"method": "supportedmethods", "params": []any{}})
	require.NoError(t, err)

	hash := sha256.Sum256(payload)

	auth := nostr.Event{
		Kind:      27235,
		CreatedAt: nostr.Now(),
		Tags: nostr.Tags{
			nostr.Tag{"u", "http://localhost/"},
			nostr.Tag{"method", "POST"},
			nostr.Tag{"payload", nostr.HexEncodeToString(hash[:])},
		},
	}
	require.NoError(t, auth.Sign(sk))

	req := httptest.NewRequest(http.MethodPost, "http://localhost/", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/nostr+json+rpc")
	req.Header.Set("Authorization", "Nostr "+base64.StdEncoding.EncodeToString([]byte(auth.String())))

	rr := httptest.NewRecorder()
	rl.HandleNIP86(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp struct {
		Result []string `json:"result"`
		Error  string   `json:"error"`
	}
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Empty(t, resp.Error)

	return resp.Result
}

func TestSupportedMethods(t *testing.T) {
	sk := nostr.Generate()

	newRelay := func() *Relay {
		rl := NewRelay()
		rl.ServiceURL = "http://localhost/"
		rl.ManagementAPI.BanPubKey = func(ctx context.Context, pubkey nostr.PubKey, reason string) error { return nil }
		rl.ManagementAPI.ListBannedPubKeys = func(ctx context.Context) ([]nip86.PubKeyReason, error) { return nil, nil }
		rl.ManagementAPI.OnAPICall = func(ctx context.Context, mp nip86.MethodParams) (bool, string) { return false, "" }
		return rl
	}

	t.Run("answers for the relay when no hook is set", func(t *testing.T) {
		methods := callSupportedMethods(t, newRelay(), sk)

		require.ElementsMatch(t, []string{"banpubkey", "listbannedpubkeys"}, methods)
	})

	t.Run("a hook narrows the list to the authed pubkey", func(t *testing.T) {
		rl := newRelay()

		var seen []string
		rl.OverwriteSupportedMethods = func(ctx context.Context, methods []string) []string {
			seen = slices.Clone(methods)

			authed, ok := GetAuthed(ctx)
			require.True(t, ok)
			require.Equal(t, sk.Public(), authed)

			return []string{"listbannedpubkeys"}
		}

		methods := callSupportedMethods(t, rl, sk)

		require.ElementsMatch(t, []string{"banpubkey", "listbannedpubkeys"}, seen)
		require.Equal(t, []string{"listbannedpubkeys"}, methods)
	})
}
