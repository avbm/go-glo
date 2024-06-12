package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"golang.org/x/sync/errgroup"
)

type nodeHandler struct {
	*maelstrom.Node
	messages []int
	mux      sync.RWMutex
	topology map[string][]string
}

func (nh *nodeHandler) addMessage(msg int) {
	// check if msg is in local messages already and skip if true
	nh.mux.RLock()
	for i := range nh.messages {
		if msg == i {
			nh.mux.RUnlock()
			return
		}
	}
	nh.mux.RUnlock()

	nh.mux.Lock()
	nh.messages = append(nh.messages, msg)
	log.Default().SetOutput(os.Stderr)
	log.Printf("DEBUG: messages: %v, new message: %d", nh.messages, msg)
	nh.mux.Unlock()
}

func (nh *nodeHandler) broadcastToPeers(msgBody json.RawMessage, src string) error {

	if len(nh.topology) == 0 || len(nh.topology[nh.ID()]) == 0 {
		return nil
	}

	eg := errgroup.Group{}
	var err error
	// WARN: this read of topology is not protected so can cause race conditions if it changes at run time
	for _, peer := range nh.topology[nh.ID()] {
		peer := peer
		if peer != src {
			// if the source is another node, ensuring one more node gets the message is "good enough"
			// otherwise keep sending to all connected peers
			if src[0:1] != "n" {
				eg.Go(func() error {
					return nh.Send(peer, msgBody)
				})
			} else {
				err = nh.Send(peer, msgBody)
				if err == nil {
					return nil
				}
			}
		}
	}

	if err != nil {
		return err
	}

	return eg.Wait()
}

func (nh *nodeHandler) broadcastHandler(msg maelstrom.Message) error {

	type request struct {
		ReqType string `json:"type"`
		Message int    `json:"message"`
	}
	var reqBody request
	if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
		return err
	}

	log.Printf("DEBUG: received request: %v, raw: %s", reqBody, msg)
	nh.addMessage(reqBody.Message)

	go nh.broadcastToPeers(msg.Body, msg.Src)

	respBody := make(map[string]any)
	// update the message type for response
	respBody["type"] = "broadcast_ok"

	return nh.Reply(msg, respBody)
}

func (nh *nodeHandler) readHandler(msg maelstrom.Message) error {
	var reqBody map[string]any
	if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
		return err
	}

	respBody := make(map[string]any)
	// update the message type for response
	respBody["type"] = "read_ok"

	// get read lock on mutex before reading messages
	nh.mux.RLock()
	defer nh.mux.RUnlock()

	respBody["messages"] = nh.messages

	return nh.Reply(msg, respBody)
}

func (nh *nodeHandler) topologyHandler(msg maelstrom.Message) error {
	type request struct {
		Type     string              `json:"type"`
		Topology map[string][]string `json:"topology"`
	}
	var body request
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	nh.topology = body.Topology
	log.Printf("DEBUG: topology: %s", nh.topology)

	respBody := make(map[string]any)
	// update the message type for response
	respBody["type"] = "topology_ok"

	// TODO - likely we need to read topology and do something with it later
	return nh.Reply(msg, respBody)
}

func main() {

	log.Default().SetOutput(os.Stderr)

	nh := nodeHandler{
		maelstrom.NewNode(),
		make([]int, 0, 10),
		sync.RWMutex{},
		nil,
	}

	nh.Handle("broadcast", nh.broadcastHandler)
	nh.Handle("read", nh.readHandler)
	nh.Handle("topology", nh.topologyHandler)

	if err := nh.Run(); err != nil {
		log.Fatal(err)
	}

}
