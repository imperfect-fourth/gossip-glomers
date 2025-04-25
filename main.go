package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("echo", maelstromEcho(n))
	n.Handle("generate", generateUniqueID(n))
	n.Handle("broadcast", maelstromBroadcast(n))
	n.Handle("read", maelstromRead(n))
	n.Handle("topology", maelstromTopology(n))
	n.Handle("gossip", maelstromGossip(n))

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
