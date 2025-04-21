package main

import (
	"encoding/json"
	"fmt"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Data struct {
	sync.Mutex
	id          string
	data        any
	nodesLookup map[string]struct{}
	nodes       []string
}

// return false if node already registered
func (d *Data) RegisterNode(n string) (bool, []string) {
	d.Lock()
	defer d.Unlock()
	if _, ok := d.nodesLookup[n]; ok {
		return false, d.nodes
	}
	d.nodesLookup[n] = struct{}{}
	d.nodes = append(d.nodes, n)
	return true, d.nodes
}

func (d *Data) Nodes() []string {
	return d.nodes
}

func (d *Data) SyncNodes(nodes []string) []string {
	for _, n := range nodes {
		d.RegisterNode(n)
	}
	return d.nodes
}

type DataStore struct {
	sync.Mutex
	dataLookup map[string]*Data
}

func (s *DataStore) SaveData(currNodeID, dataID string, msg any) (bool, *Data) {
	s.Lock()
	defer s.Unlock()
	if data, ok := s.dataLookup[dataID]; ok {
		return false, data
	}
	data := &Data{
		id:          dataID,
		data:        msg,
		nodes:       []string{currNodeID},
		nodesLookup: map[string]struct{}{currNodeID: struct{}{}},
	}
	s.dataLookup[dataID] = data
	return true, data
}

func (s *DataStore) Read() []any {
	s.Lock()
	defer s.Unlock()
	res := make([]any, len(s.dataLookup))
	i := 0
	for _, data := range s.dataLookup {
		res[i] = data.data
		i++
	}
	return res
}

var dataStore = &DataStore{
	dataLookup: make(map[string]*Data),
}

type GossipRequest struct {
	maelstrom.MessageBody
	Data      any      `json:"message"`
	DataID    string   `json:"data_id"`
	SeenNodes []string `json:"seen_nodes"`
}

type GossipResponse struct {
	maelstrom.MessageBody
	AlreadySeen bool     `json:"already_seen"`
	SeenNodes   []string `json:"seen_nodes"`
}

func Gossip(currNode *maelstrom.Node, data *Data) error {
	for _, node := range topology[currNode.ID()] {
		registered, nodes := data.RegisterNode(node)
		// if already registered continue
		if !registered {
			continue
		}

		err := currNode.RPC(node, GossipRequest{
			MessageBody: maelstrom.MessageBody{
				Type:  "gossip",
				MsgID: NextMsgID(),
			},
			DataID:    data.id,
			Data:      data.data,
			SeenNodes: nodes,
		},
			func(msg maelstrom.Message) error {
				var body GossipResponse
				if err := json.Unmarshal(msg.Body, &body); err != nil {
					return err
				}
				if body.AlreadySeen {
					data.SyncNodes(body.SeenNodes)
				}
				return nil
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func getDataID(srcNode string, msgID int) string {
	return fmt.Sprintf("%s_%d", srcNode, msgID)
}

func maelstromBroadcast(node *maelstrom.Node) func(msg maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		var body GossipRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		dataID := getDataID(msg.Src, body.MsgID)
		_, data := dataStore.SaveData(node.ID(), dataID, body.Data)
		if err := Gossip(node, data); err != nil {
			return err
		}

		return node.Reply(msg, map[string]any{"msg_id": NextMsgID(), "type": "broadcast_ok"})
	}
}

func maelstromGossip(node *maelstrom.Node) func(maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		var body GossipRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		saved, data := dataStore.SaveData(node.ID(), body.DataID, body.Data)
		// if data already exists
		if !saved {
			defer data.SyncNodes(body.SeenNodes)
			return node.Reply(msg, GossipResponse{
				MessageBody: maelstrom.MessageBody{
					Type:  "gossip_ok",
					MsgID: NextMsgID(),
				},
				AlreadySeen: true,
				SeenNodes:   data.Nodes(),
			})
		}
		data.SyncNodes(body.SeenNodes)
		if err := Gossip(node, data); err != nil {
			return err
		}
		return node.Reply(msg, GossipResponse{
			MessageBody: maelstrom.MessageBody{
				Type:  "gossip_ok",
				MsgID: NextMsgID(),
			},
			AlreadySeen: false,
			SeenNodes:   data.Nodes(),
		})
	}
}

func maelstromRead(n *maelstrom.Node) func(msg maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = dataStore.Read()
		body["msg_id"] = NextMsgID()

		return n.Reply(msg, body)
	}
}

func maelstromTopology(n *maelstrom.Node) func(msg maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		var body struct {
			Type     string              `json:"type"`
			Topology map[string][]string `json:"topology"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology = body.Topology
		return n.Reply(msg, map[string]any{"msg_id": NextMsgID(), "type": "topology_ok"})
	}
}

var topology map[string][]string

var nodeMsgID int = 0
var msgIDLock sync.Mutex

func NextMsgID() int {
	msgIDLock.Lock()
	defer msgIDLock.Unlock()
	nodeMsgID++
	return nodeMsgID
}
