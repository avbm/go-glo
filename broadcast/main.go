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
	mux            sync.RWMutex
	messages       []int
	messageSources map[int]struct{}
	topology       map[string][]string
}

func (nh *nodeHandler) addMessage(msg int) {
	// check if msg is in local messages already and skip if true
	nh.mux.Lock()
	defer nh.mux.Unlock()
	if _, ok := nh.messageSources[msg]; ok {
		return
	}
	nh.messages = append(nh.messages, msg)
	nh.messageSources[msg] = struct{}{}
	log.Printf("DEBUG: messages: %v, new message: %d", nh.messages, msg)
}

func (nh *nodeHandler) broadcastToPeers() (err error) {

	nh.mux.Lock()
	defer nh.mux.Unlock()

	if len(nh.topology) == 0 || len(nh.topology[nh.ID()]) == 0 {
		return nil
	}

	eg := errgroup.Group{}
	for peer := range nh.topology {
		if peer == nh.ID() {
			continue
		}
		req := syncRequest{
			ReqType:  "sync",
			Messages: nh.messages,
		}
		peer := peer
		eg.Go(func() error {
			err := nh.Node.Send(peer, req)

			if err != nil {
				log.Printf("ERROR: failed to sync to peer: %s. error: %s", peer, err)
			}
			return err
		})
	}

	return eg.Wait()
}

type syncRequest struct {
	ReqType  string `json:"type"`
	Messages []int  `json:"messages"`
}

func (nh *nodeHandler) syncHandler(msg maelstrom.Message) error {

	var reqBody syncRequest
	if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
		return err
	}

	log.Printf("DEBUG: received request: %v, raw: %s", reqBody, msg)
	for _, m := range reqBody.Messages {
		nh.addMessage(m)
	}

	return nil
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

	nh.broadcastToPeers()

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

	return nh.Reply(msg, respBody)
}

func main() {

	log.Default().SetOutput(os.Stderr)

	nh := nodeHandler{
		maelstrom.NewNode(),
		sync.RWMutex{},
		make([]int, 0, 10),
		make(map[int]struct{}),
		nil,
	}

	nh.Handle("broadcast", nh.broadcastHandler)
	nh.Handle("read", nh.readHandler)
	nh.Handle("topology", nh.topologyHandler)
	nh.Handle("sync", nh.syncHandler)
	nh.Handle("sync_ok", func(maelstrom.Message) error { return nil })

	if err := nh.Run(); err != nil {
		log.Fatal(err)
	}

}
