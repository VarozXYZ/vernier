// Package research retains compatibility aliases for the neutral market
// stream components used by both Research and Live.
package research

import "github.com/VarozXYZ/vernier/runtime/marketstream"

type EventSnapshotStore = marketstream.EventSnapshotStore
type EventSnapshotData = marketstream.EventSnapshotData

var NewEventSnapshotStore = marketstream.NewEventSnapshotStore
