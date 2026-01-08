package message

// MsgType represents the type of internal message
type MsgType int

const (
	// MsgTypeRaftMessage is for raft messages from network
	MsgTypeRaftMessage MsgType = iota
	// MsgTypeRaftCmd is for client commands
	MsgTypeRaftCmd
	// MsgTypeTick is for raft ticker
	MsgTypeTick
)

// Msg is an internal message for routing to peers
type Msg struct {
	Type     MsgType
	RegionID uint64
	Data     interface{}
}
