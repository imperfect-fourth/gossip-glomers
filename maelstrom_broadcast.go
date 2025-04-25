package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Data struct {
	ID   string `json:"id"`
	Data any    `json:"data"`
}

type DataBatch struct {
	HeardBack
	ID   int    `json:"id"`
	Data []Data `json:"data"`
}

func NewDataBatch(id int) *DataBatch {
	return &DataBatch{
		HeardBack: HeardBack{
			heardback: make(map[string]struct{}),
		},
		ID:   id,
		Data: []Data{},
	}
}

type WaitingDataQueue struct {
	sync.Mutex
	waitingData *DataBatch
}

func (w *WaitingDataQueue) WaitingData() *DataBatch {
	w.Lock()
	defer w.Unlock()

	dataBatch := w.waitingData
	w.waitingData = NewDataBatch(dataBatch.ID + 1)
	return dataBatch
}

func (w *WaitingDataQueue) Enqueue(d Data) {
	w.Lock()
	defer w.Unlock()
	w.waitingData.Data = append(w.waitingData.Data, d)
}

type HeardBack struct {
	sync.Mutex
	heardback map[string]struct{}
}

func (h *HeardBack) HaveHeardBack(id string) bool {
	h.Lock()
	defer h.Unlock()
	_, ok := h.heardback[id]
	return ok
}

func (h *HeardBack) SaveHeardBack(id string) {
	h.Lock()
	defer h.Unlock()
	h.heardback[id] = struct{}{}
}

type DataStore struct {
	sync.Mutex
	WaitingDataQueue
	dataLookup map[Data]struct{}
}

func (s *DataStore) SaveData(d Data) {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.dataLookup[d]; !ok {
		s.Enqueue(d)
	}
	s.dataLookup[d] = struct{}{}
}

func (s *DataStore) Read() []any {
	s.Lock()
	defer s.Unlock()
	res := make([]any, len(s.dataLookup))
	i := 0
	for data := range s.dataLookup {
		res[i] = data.Data
		i++
	}
	return res
}

var dataStore = &DataStore{
	dataLookup:       make(map[Data]struct{}),
	WaitingDataQueue: WaitingDataQueue{waitingData: NewDataBatch(0)},
}

type GossipMessage struct {
	maelstrom.MessageBody
	DataBatch
}

func gossip(currNode *maelstrom.Node, data *DataBatch) (int, error) {
	sent := 0
	for _, node := range topology[currNode.ID()] {
		if data.HaveHeardBack(currNode.ID()) {
			continue
		}
		sent++

		err := currNode.RPC(node, GossipMessage{
			MessageBody: maelstrom.MessageBody{
				Type:  "gossip",
				MsgID: NextMsgID(),
			},
			DataBatch: *data,
		},
			func(msg maelstrom.Message) error {
				data.SaveHeardBack(currNode.ID())
				return nil
			},
		)
		if err != nil {
			return sent, err
		}
	}
	return sent, nil
}

func Gossip(currNode *maelstrom.Node) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		waitingData := dataStore.WaitingData()
		if len(waitingData.Data) == 0 {
			continue
		}

		sent, err := gossip(currNode, waitingData)
		if err != nil {
			return err
		}
		go func() {
			for sent != 0 {
				var err error
				time.Sleep(2000 * time.Millisecond)
				sent, err = gossip(currNode, waitingData)
				if err != nil {
					return
				}
			}
		}()
	}
	return nil
}

func getDataID(srcNode string, msgID int) string {
	return fmt.Sprintf("%s_%d", srcNode, msgID)
}

func maelstromBroadcast(node *maelstrom.Node) func(msg maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		dataID := getDataID(msg.Src, int(body["msg_id"].(float64)))
		dataStore.SaveData(Data{ID: dataID, Data: body["message"]})

		return node.Reply(msg, map[string]any{"msg_id": NextMsgID(), "type": "broadcast_ok"})
	}
}

func maelstromGossip(node *maelstrom.Node) func(maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		var body GossipMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for _, data := range body.DataBatch.Data {
			dataStore.SaveData(data)
		}
		return node.Reply(msg, GossipMessage{
			MessageBody: maelstrom.MessageBody{
				Type:  "gossip_ok",
				MsgID: NextMsgID(),
			},
			DataBatch: DataBatch{
				ID: body.DataBatch.ID,
			},
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
