package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Message represents a stream message.
type Message struct {
	ID     string
	Fields map[string]string
}

// PendingEntry represents an entry in the Pending Entries List (PEL).
type PendingEntry struct {
	ID            string
	ConsumerName  string
	DeliveryTime  time.Time
	DeliveryCount int
}

// ConsumerGroup represents a stream consumer group.
type ConsumerGroup struct {
	Name    string
	LastID  string
	Pending map[string]*PendingEntry // MessageID -> PendingEntry
}

// Stream represents a Redis-like stream.
type Stream struct {
	Messages []Message
	Groups   map[string]*ConsumerGroup
}

// Command represents a replicated command.
type Command struct {
	Op   string
	Args []string
}

// Node represents a Redis/Dragonfly node.
type Node struct {
	ID       string
	Streams  map[string]*Stream
	Journal  []Command
	IsMaster bool
}

func NewNode(id string, isMaster bool) *Node {
	return &Node{
		ID:       id,
		Streams:  make(map[string]*Stream),
		Journal:  make([]Command, 0),
		IsMaster: isMaster,
	}
}

// Helper to parse message ID (e.g., "1-0") into int64 parts for comparison.
func parseID(id string) (int64, int64) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0
	}
	ms, _ := strconv.ParseInt(parts[0], 10, 64)
	seq, _ := strconv.ParseInt(parts[1], 10, 64)
	return ms, seq
}

// Compare IDs: returns -1 if id1 < id2, 1 if id1 > id2, 0 if equal.
func compareIDs(id1, id2 string) int {
	ms1, seq1 := parseID(id1)
	ms2, seq2 := parseID(id2)
	if ms1 < ms2 {
		return -1
	}
	if ms1 > ms2 {
		return 1
	}
	if seq1 < seq2 {
		return -1
	}
	if seq1 > seq2 {
		return 1
	}
	return 0
}

func (n *Node) XAdd(streamKey string, id string, fields map[string]string) string {
	stream, exists := n.Streams[streamKey]
	if !exists {
		stream = &Stream{
			Messages: make([]Message, 0),
			Groups:   make(map[string]*ConsumerGroup),
			}
		n.Streams[streamKey] = stream
	}
	msg := Message{ID: id, Fields: fields}
	stream.Messages = append(stream.Messages, msg)

	if n.IsMaster {
		args := []string{streamKey, id}
		for k, v := range fields {
			args = append(args, k, v)
		}
		n.Journal = append(n.Journal, Command{Op: "XADD", Args: args})
	}
	return id
}

func (n *Node) XGroupCreate(streamKey string, groupName string, startID string) error {
	stream, exists := n.Streams[streamKey]
	if !exists {
		return errors.New("ERR no such key")
	}
	if _, exists := stream.Groups[groupName]; exists {
		return errors.New("BUSYGROUP Consumer Group name already exists")
	}
	resolvedID := startID
	if startID == "$" {
		if len(stream.Messages) > 0 {
			resolvedID = stream.Messages[len(stream.Messages)-1].ID
		} else {
			resolvedID = "0-0"
		}
	}
	stream.Groups[groupName] = &ConsumerGroup{
		Name:    groupName,
		LastID:  resolvedID,
		Pending: make(map[string]*PendingEntry),
	}

	if n.IsMaster {
		n.Journal = append(n.Journal, Command{Op: "XGROUPCREATE", Args: []string{streamKey, groupName, resolvedID}})
	}
	return nil
}

func (n *Node) XReadGroup(streamKey string, groupName string, consumerName string, count int, id string) ([]Message, error) {
	stream, exists := n.Streams[streamKey]
	if !exists {
		return nil, errors.New("ERR no such key")
	}
	group, exists := stream.Groups[groupName]
	if !exists {
		return nil, errors.New("ERR no such group")
	}

	var result []Message
	if id == ">" {
		for _, msg := range stream.Messages {
			if compareIDs(msg.ID, group.LastID) > 0 {
				result = append(result, msg)
				group.Pending[msg.ID] = &PendingEntry{
					ID:            msg.ID,
					ConsumerName:  consumerName,
					DeliveryTime:  time.Now(),
					DeliveryCount: 1,
				}
				group.LastID = msg.ID
				if len(result) == count {
					break
				}
			}
		}
	} else {
		for _, msg := range stream.Messages {
			if pe, exists := group.Pending[msg.ID]; exists {
				if pe.ConsumerName == consumerName && compareIDs(msg.ID, id) >= 0 {
					result = append(result, msg)
					pe.DeliveryTime = time.Now()
					pe.DeliveryCount++
					if len(result) == count {
						break
					}
				}
			}
		}
	}

	if n.IsMaster && len(result) > 0 {
		args := []string{streamKey, groupName, consumerName, group.LastID}
		for _, msg := range result {
			args = append(args, msg.ID)
		}
		n.Journal = append(n.Journal, Command{Op: "XREADGROUP_REPLICATE", Args: args})
	}

	return result, nil
}

func (n *Node) XAck(streamKey string, groupName string, ids []string) (int, error) {
	stream, exists := n.Streams[streamKey]
	if !exists {
		return 0, errors.New("ERR no such key")
	}
	group, exists := stream.Groups[groupName]
	if !exists {
		return 0, errors.New("ERR no such group")
	}

	count := 0
	for _, id := range ids {
		if _, exists := group.Pending[id]; exists {
			delete(group.Pending, id)
			count++
		}
	}

	if n.IsMaster && count > 0 {
		args := []string{streamKey, groupName}
		args = append(args, ids...)
		n.Journal = append(n.Journal, Command{Op: "XACK", Args: args})
	}

	return count, nil
}

func (n *Node) XPending(streamKey string, groupName string) ([]PendingEntry, error) {
	stream, exists := n.Streams[streamKey]
	if !exists {
		return nil, errors.New("ERR no such key")
	}
	group, exists := stream.Groups[groupName]
	if !exists {
		return nil, errors.New("ERR no such group")
	}

	var list []PendingEntry
	for _, pe := range group.Pending {
		list = append(list, *pe)
	}
	return list, nil
}

func (replica *Node) ReplicateJournal(journal []Command) {
	for _, cmd := range journal {
		switch cmd.Op {
		case "XADD":
			streamKey := cmd.Args[0]
			id := cmd.Args[1]
			fields := make(map[string]string)
			for i := 2; i < len(cmd.Args); i += 2 {
				fields[cmd.Args[i]] = cmd.Args[i+1]
			}
			replica.XAdd(streamKey, id, fields)
		case "XGROUPCREATE":
			streamKey := cmd.Args[0]
			groupName := cmd.Args[1]
			startID := cmd.Args[2]
			replica.XGroupCreate(streamKey, groupName, startID)
		case "XREADGROUP_REPLICATE":
			streamKey := cmd.Args[0]
			groupName := cmd.Args[1]
			consumerName := cmd.Args[2]
			lastID := cmd.Args[3]
			msgIDs := cmd.Args[4:]

			stream := replica.Streams[streamKey]
			group := stream.Groups[groupName]
			group.LastID = lastID
			for _, id := range msgIDs {
				if pe, exists := group.Pending[id]; exists {
					pe.DeliveryTime = time.Now()
					pe.DeliveryCount++
				} else {
					group.Pending[id] = &PendingEntry{
						ID:            id,
						ConsumerName:  consumerName,
						DeliveryTime:  time.Now(),
						DeliveryCount: 1,
					}
				}
			}
		case "XACK":
			streamKey := cmd.Args[0]
			groupName := cmd.Args[1]
			ids := cmd.Args[2:]
			replica.XAck(streamKey, groupName, ids)
		}
	}
}

func main() {
	fmt.Println("Initializing Master-Replica Stream Replication Test...")

	master := NewNode("master", true)
	replica := NewNode("replica", false)

	master.XAdd("mystream", "1-0", map[string]string{"field1": "value1"})
	master.XGroupCreate("mystream", "mygroup", "$")

	replica.ReplicateJournal(master.Journal)
	master.Journal = nil

	master.XAdd("mystream", "2-0", map[string]string{"field2": "value2"})
	readMsgs, err := master.XReadGroup("mystream", "mygroup", "consumer1", 1, ">")
	if err != nil {
		fmt.Printf("Error reading group: %v\n", err)
		return
	}
	fmt.Printf("Master read message: %v\n", readMsgs)

	replica.ReplicateJournal(master.Journal)
	master.Journal = nil

	masterPEL, _ := master.XPending("mystream", "mygroup")
	replicaPEL, _ := replica.XPending("mystream", "mygroup")

	fmt.Printf("Master PEL count: %d\n", len(masterPEL))
	fmt.Printf("Replica PEL count: %d\n", len(replicaPEL))

	if len(masterPEL) != 1 || len(replicaPEL) != 1 {
		fmt.Println("FAIL: PEL state mismatch before failover")
		return
	}

	if masterPEL[0].ConsumerName != "consumer1" || replicaPEL[0].ConsumerName != "consumer1" {
		fmt.Println("FAIL: Consumer name mismatch in PEL")
		return
	}

	fmt.Println("Triggering failover: promoting replica to master...")
	newMaster := replica
	newMaster.IsMaster = true
	newMaster.ID = "new-master"

	newMasterPEL, _ := newMaster.XPending("mystream", "mygroup")
	fmt.Printf("New Master PEL count: %d\n", len(newMasterPEL))
	if len(newMasterPEL) != 1 {
		fmt.Println("FAIL: PEL state lost after failover")
		return
	}
	fmt.Printf("New Master PEL Consumer: %s, Delivery Count: %d\n", newMasterPEL[0].ConsumerName, newMasterPEL[0].DeliveryCount)

	moreMsgs, err := newMaster.XReadGroup("mystream", "mygroup", "consumer1", 1, ">")
	if err != nil {
		fmt.Printf("Error reading group on new master: %v\n", err)
		return
	}
	fmt.Printf("New Master read more messages: %v (expected empty)\n", moreMsgs)

	if len(moreMsgs) > 0 {
		fmt.Println("FAIL: Pending message redelivered after failover")
		return
	}

	fmt.Println("SUCCESS: PEL state is consistent and no duplicate deliveries occurred after failover!")
}