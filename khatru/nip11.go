package khatru

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
)

func (rl *Relay) HandleNIP11(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/nostr+json")

	info := *rl.Info

	if nil != rl.DeleteEvent {
		info.AddSupportedNIP("9")
	}
	if nil != rl.Count {
		info.AddSupportedNIP("45")
	}
	if rl.Negentropy {
		info.AddSupportedNIP("77")
	}

	// resolve relative icon and banner URLs against the base URL, leaving
	// absolute URIs (http(s), data:, etc.) untouched
	baseURL := rl.getBaseURL(r)
	info.Icon = resolveRelativeURL(info.Icon, baseURL)
	info.Banner = resolveRelativeURL(info.Banner, baseURL)

	if nil != rl.OverwriteRelayInformation {
		info = rl.OverwriteRelayInformation(r.Context(), r, info)
	}

	json.NewEncoder(w).Encode(info)
}

// resolveRelativeURL joins a scheme-less (relative) reference onto baseURL. A
// value that already carries a scheme — http(s), but also data: and any other
// absolute URI — is returned unchanged, so an inline data: icon isn't corrupted
// into "<baseURL>/data:...".
func resolveRelativeURL(ref, baseURL string) string {
	if ref == "" {
		return ref
	}
	if u, err := url.Parse(ref); err == nil && u.Scheme != "" {
		return ref
	}
	return strings.TrimSuffix(baseURL, "/") + "/" + strings.TrimPrefix(ref, "/")
}
