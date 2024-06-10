package main

import (
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type nodeHandler struct {
	*maelstrom.Node
	nextId int64
}

func (nh *nodeHandler) next() int64 {
	val := nh.nextId
	nh.nextId++
	return val
}

func (nh *nodeHandler) generateHandler(msg maelstrom.Message) error {

	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// update the message type for response
	body["type"] = "generate_ok"
	body["id"] = fmt.Sprintf("%s%d", nh.ID(), nh.next())

	return nh.Reply(msg, body)
}

func main() {
	nh := nodeHandler{
		maelstrom.NewNode(),
		0,
	}

	nh.Handle("generate", nh.generateHandler)

	if err := nh.Run(); err != nil {
		log.Fatal(err)
	}

}
