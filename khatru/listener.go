package khatru

import (
	"context"
	"errors"
	"iter"
	"slices"
	"sync/atomic"

	"fiatjaf.com/lib/set"
	"fiatjaf.com/nostr"
	"github.com/puzpuzpuz/xsync/v3"
)

var ErrSubscriptionClosedByClient = errors.New("subscription closed by client")

type listenerSpec struct {
	ssid   int    // internal numeric id for a listener
	sid    string // client-provided subscription id
	cancel context.CancelCauseFunc
}

type listener struct {
	id     string // duplicated here so we can easily send it on notifyListeners
	filter nostr.Filter
	ws     *WebSocket
}

type subscription struct {
	id     string
	filter nostr.Filter
	ws     *WebSocket
}

// Every set in the indexes below is frozen once published: writers build a new one and
// swap it in, and never change one in place.
//
// Writers are serialized by the relay's clientsMutex, but candidates reads the indexes
// under no lock at all. The sets used to be SliceSets changed in place, and Slice hands
// out the live backing array: a Remove shifted it under a reader, which then skipped
// the subscriber that slid into a position it had already passed, and that subscriber
// never got the event. Emptied sets also went back to a pool while a reader could still
// be walking them. Measured: with subscriptions leaving while events were dispatched, up
// to 18 of 64 subscribers that stayed were skipped in a single dispatch, on every index.
type dispatcher struct {
	serial          int
	subscriptions   *xsync.MapOf[int, subscription]
	byAuthor        *xsync.MapOf[nostr.PubKey, set.Set[int]]
	byKind          *xsync.MapOf[nostr.Kind, set.Set[int]]
	fallbackTags    *frozenSet
	fallbackNothing *frozenSet
}

// frozenSet holds a set that is replaced whole, for the indexes that are a single set
// rather than a map of them.
type frozenSet struct{ current atomic.Value }

func newFrozenSet() *frozenSet {
	f := &frozenSet{}
	f.current.Store(set.NewSliceSet[int]())
	return f
}

func (f *frozenSet) load() set.Set[int] { return f.current.Load().(set.Set[int]) }

// add and remove must be called with the relay's clientsMutex held, like every writer.
func (f *frozenSet) add(ssid int)    { f.current.Store(setWith(f.load(), ssid)) }
func (f *frozenSet) remove(ssid int) { f.current.Store(setWithout(f.load(), ssid)) }

// setWith returns a new set holding s's subscriptions and ssid. s is not touched.
func setWith(s set.Set[int], ssid int) set.Set[int] {
	var items []int
	if s != nil {
		items = slices.Clone(s.Slice())
	}
	return set.NewSliceSet(append(items, ssid)...)
}

// setWithout returns a new set holding s's subscriptions but ssid. s is not touched.
func setWithout(s set.Set[int], ssid int) set.Set[int] {
	items := slices.DeleteFunc(slices.Clone(s.Slice()), func(item int) bool { return item == ssid })
	return set.NewSliceSet(items...)
}

func newDispatcher() dispatcher {
	return dispatcher{
		subscriptions:   xsync.NewMapOf[int, subscription](),
		byAuthor:        xsync.NewMapOf[nostr.PubKey, set.Set[int]](),
		byKind:          xsync.NewMapOf[nostr.Kind, set.Set[int]](),
		fallbackTags:    newFrozenSet(),
		fallbackNothing: newFrozenSet(),
	}
}

func (d *dispatcher) addSubscription(sub subscription) int {
	d.serial++
	ssid := d.serial

	d.subscriptions.Store(ssid, sub)

	indexed := false
	if sub.filter.Authors != nil {
		indexed = true
		for _, author := range sub.filter.Authors {
			d.byAuthor.Compute(author, func(s set.Set[int], loaded bool) (set.Set[int], bool) {
				return setWith(s, ssid), false
			})
		}
	}

	if sub.filter.Kinds != nil {
		indexed = true
		for _, kind := range sub.filter.Kinds {
			d.byKind.Compute(kind, func(s set.Set[int], loaded bool) (set.Set[int], bool) {
				return setWith(s, ssid), false
			})
		}
	}

	if !indexed {
		if sub.filter.Tags != nil {
			d.fallbackTags.add(ssid)
		} else {
			d.fallbackNothing.add(ssid)
		}
	}

	return ssid
}

func (d *dispatcher) removeSubscription(ssid int) nostr.Filter {
	var filter nostr.Filter

	d.subscriptions.Compute(ssid, func(sub subscription, loaded bool) (subscription, bool) {
		indexed := false

		filter = sub.filter

		if sub.filter.Authors != nil {
			indexed = true
			for _, author := range sub.filter.Authors {
				d.byAuthor.Compute(author, func(s set.Set[int], loaded bool) (set.Set[int], bool) {
					if !loaded {
						return s, true
					}
					next := setWithout(s, ssid)
					return next, next.Len() == 0
				})
			}
		}

		if sub.filter.Kinds != nil {
			indexed = true
			for _, kind := range sub.filter.Kinds {
				d.byKind.Compute(kind, func(s set.Set[int], loaded bool) (set.Set[int], bool) {
					if !loaded {
						return s, true
					}
					next := setWithout(s, ssid)
					return next, next.Len() == 0
				})
			}
		}

		if !indexed {
			if sub.filter.Tags != nil {
				d.fallbackTags.remove(ssid)
			} else {
				d.fallbackNothing.remove(ssid)
			}
		}

		return sub, true
	})

	return filter
}

func (d *dispatcher) candidates(event nostr.Event) iter.Seq[subscription] {
	return func(yield func(subscription) bool) {
		authorSubs, hasAuthorSubs := d.byAuthor.Load(event.PubKey)
		kindSubs, hasKindSubs := d.byKind.Load(event.Kind)

		if hasAuthorSubs && hasKindSubs {
			for _, ssid := range authorSubs.Slice() {
				sub, ok := d.subscriptions.Load(ssid)
				if !ok {
					continue
				}

				if kindSubs.Has(ssid) || sub.filter.Kinds == nil {
					if filterMatchesTimestampConstraintsAndTags(sub.filter, event) {
						if !yield(sub) {
							return
						}
					}
				}
			}

			for _, ssid := range kindSubs.Slice() {
				sub, ok := d.subscriptions.Load(ssid)
				if !ok {
					continue
				}

				if sub.filter.Authors != nil {
					continue
				}

				if filterMatchesTimestampConstraintsAndTags(sub.filter, event) {
					if !yield(sub) {
						return
					}
				}
			}
		} else if hasAuthorSubs {
			for _, ssid := range authorSubs.Slice() {
				sub, ok := d.subscriptions.Load(ssid)
				if !ok {
					continue
				}

				if sub.filter.Kinds != nil {
					// if there are any kinds in the filter we already know this doesn't qualify
					continue
				}

				if filterMatchesTimestampConstraintsAndTags(sub.filter, event) {
					if !yield(sub) {
						return
					}
				}
			}
		} else if hasKindSubs {
			for _, ssid := range kindSubs.Slice() {
				sub, ok := d.subscriptions.Load(ssid)
				if !ok {
					continue
				}

				if sub.filter.Authors != nil {
					// if there are any authors in the filter we already know this doesn't qualify
					continue
				}

				if filterMatchesTimestampConstraintsAndTags(sub.filter, event) {
					if !yield(sub) {
						return
					}
				}
			}
		}

		if len(event.Tags) > 0 {
			for _, ssid := range d.fallbackTags.load().Slice() {
				sub, ok := d.subscriptions.Load(ssid)
				if !ok {
					continue
				}

				if filterMatchesTimestampConstraintsAndTags(sub.filter, event) {
					if !yield(sub) {
						return
					}
				}
			}
		}

		for _, ssid := range d.fallbackNothing.load().Slice() {
			sub, ok := d.subscriptions.Load(ssid)
			if !ok {
				continue
			}

			if filterMatchesTimestampConstraints(sub.filter, event) {
				if !yield(sub) {
					return
				}
			}
		}
	}
}

//go:inline
func filterMatchesTimestampConstraints(filter nostr.Filter, event nostr.Event) bool {
	if filter.Since != 0 && event.CreatedAt < filter.Since {
		return false
	}

	if filter.Until != 0 && event.CreatedAt > filter.Until {
		return false
	}

	return true
}

//go:inline
func filterMatchesTimestampConstraintsAndTags(filter nostr.Filter, event nostr.Event) bool {
	if !filterMatchesTimestampConstraints(filter, event) {
		return false
	}

	for f, v := range filter.Tags {
		if !event.Tags.ContainsAny(f, v) {
			return false
		}
	}

	return true
}

//go:inline
func tagKeyValueKey(tagKey, tagValue string) string {
	return tagKey + "\x00" + tagValue
}

func (rl *Relay) GetListeningFilters() []nostr.Filter {
	respfilters := make([]nostr.Filter, 0, rl.dispatcher.subscriptions.Size())
	for _, sub := range rl.dispatcher.subscriptions.Range {
		respfilters = append(respfilters, sub.filter)
	}
	return respfilters
}

// addListener may be called multiple times for each id and ws -- in which case each filter will
// be added as an independent listener
func (rl *Relay) addListener(
	ws *WebSocket,
	id string,
	filter nostr.Filter,
	cancel context.CancelCauseFunc,
) {
	select {
	case <-rl.clientsMutex.C():
		defer rl.clientsMutex.Unlock()
	case <-ws.Context.Done():
		return
	}

	if specs, ok := rl.clients[ws]; ok /* this will always be true unless client has disconnected very rapidly */ {
		ssid := rl.dispatcher.addSubscription(subscription{
			ws:     ws,
			id:     id,
			filter: filter,
		})
		rl.clients[ws] = append(specs, listenerSpec{
			ssid:   ssid,
			cancel: cancel,
			sid:    id,
		})

		if rl.OnListenerAdded != nil {
			rl.OnListenerAdded(ws, ssid, id, filter)
		}
	}
}

// remove a specific subscription id from listeners for a given ws client
// and cancel its specific context
func (rl *Relay) removeListenerId(ws *WebSocket, id string) {
	rl.clientsMutex.Lock()
	defer rl.clientsMutex.Unlock()

	if specs, ok := rl.clients[ws]; ok {
		kept := specs[:0]
		for _, spec := range specs {
			if spec.sid == id {
				spec.cancel(ErrSubscriptionClosedByClient)
				filter := rl.dispatcher.removeSubscription(spec.ssid)

				if rl.OnListenerRemoved != nil {
					rl.OnListenerRemoved(ws, spec.ssid, id, filter)
				}

				continue
			}
			kept = append(kept, spec)
		}
		rl.clients[ws] = kept
	}
}

func (rl *Relay) removeClientAndListeners(ws *WebSocket) {
	rl.clientsMutex.Lock()
	defer rl.clientsMutex.Unlock()
	if specs, ok := rl.clients[ws]; ok {
		for _, spec := range specs {
			// no need to cancel contexts since they inherit from the main connection context
			filter := rl.dispatcher.removeSubscription(spec.ssid)

			if rl.OnListenerRemoved != nil {
				rl.OnListenerRemoved(ws, spec.ssid, spec.sid, filter)
			}
		}
	}
	delete(rl.clients, ws)
}

// returns how many listeners were notified
func (rl *Relay) notifyListeners(event nostr.Event, skipPrevent bool) int {
	count := 0
listenersloop:
	for sub := range rl.dispatcher.candidates(event) {
		if !skipPrevent && nil != rl.PreventBroadcast {
			if rl.PreventBroadcast(sub.ws, sub.filter, event) {
				continue listenersloop
			}
		}
		sub.ws.WriteJSON(nostr.EventEnvelope{SubscriptionID: &sub.id, Event: event})
		count++
	}
	return count
}
