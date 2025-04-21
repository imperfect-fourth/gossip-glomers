package main

import (
	"encoding/json"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func generateUniqueID(n *maelstrom.Node) func(maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "generate_ok"
		body["id"] = uuid.New()

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	}
}
