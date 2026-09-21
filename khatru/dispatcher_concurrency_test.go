package khatru

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"fiatjaf.com/nostr"
)

// A subscriber that stays must never be skipped while other subscriptions leave.
//
// Writers to the dispatcher are serialized by the relay's clientsMutex, but candidates
// runs under no lock. The indexes used to be SliceSets changed in place: Slice handed
// readers the live backing array and Remove shifted it left, so a reader past the
// removed position skipped the next subscriber, which never got that event.
//
// Subscription ids only grow, so a leaving subscription only shifts the ones created
// after it. The subscriptions that leave are interleaved with the ones that stay, and
// removed from the oldest, which is the worst case: every removal moves every
// subscriber that stays behind it.
func TestCandidatesNeverSkipSubscribersThatStay(t *testing.T) {
	for _, tc := range []struct {
		name   string
		filter nostr.Filter
		event  nostr.Event
	}{
		{"indexed by kind", nostr.Filter{Kinds: []nostr.Kind{9}}, nostr.Event{Kind: 9}},
		{"indexed by author", nostr.Filter{Authors: []nostr.PubKey{{1}}}, nostr.Event{Kind: 9, PubKey: nostr.PubKey{1}}},
		{"filtered by tag only", nostr.Filter{Tags: nostr.TagMap{"h": {"geral"}}}, nostr.Event{Kind: 9, Tags: nostr.Tags{{"h", "geral"}}}},
		{"unfiltered", nostr.Filter{}, nostr.Event{Kind: 9}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for round := 0; round < 200; round++ {
				if skipped := skippedWhileOthersLeave(tc.filter, tc.event); skipped != "" {
					t.Fatalf("round %d: %s", round, skipped)
				}
			}
		})
	}
}

func skippedWhileOthersLeave(filter nostr.Filter, event nostr.Event) string {
	const pairs = 64

	d := newDispatcher()
	var clients sync.Mutex // stands in for the relay's clientsMutex

	var leaving []int
	for i := range pairs {
		clients.Lock()
		leaving = append(leaving, d.addSubscription(subscription{id: fmt.Sprintf("leaves-%d", i), filter: filter}))
		d.addSubscription(subscription{id: fmt.Sprintf("stays-%d", i), filter: filter})
		clients.Unlock()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, ssid := range leaving {
			clients.Lock()
			d.removeSubscription(ssid)
			clients.Unlock()
		}
	}()

	var skipped string
	for {
		seen := make(map[string]bool, 2*pairs)
		for sub := range d.candidates(event) {
			seen[sub.id] = true
		}

		var missing []string
		for i := range pairs {
			if id := fmt.Sprintf("stays-%d", i); !seen[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 && skipped == "" {
			skipped = fmt.Sprintf("%d subscribers that stay were skipped: %s", len(missing), strings.Join(missing, ", "))
		}

		select {
		case <-done:
			return skipped
		default:
		}
	}
}
