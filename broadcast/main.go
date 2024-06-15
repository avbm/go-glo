package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"golang.org/x/sync/errgroup"
)

type nodeHandler struct {
	*maelstrom.Node
	mux            sync.RWMutex
	ticker         time.Ticker
	syncAck        chan struct{}
	messages       []int
	messageSources map[int][]string
	topology       map[string][]string
}

func (nh *nodeHandler) addMessageSource(msg int, src string) {
	// we dont use locks here since it should be locked by calling function
	if sources, ok := nh.messageSources[msg]; ok {
		// check if source already in sources
		for _, s := range sources {
			if s == src {
				return
			}
		}

		// if not, append
		nh.messageSources[msg] = append(sources, src)
		return
	}
	// create a new entry since one doesnt exist
	nh.messageSources[msg] = []string{src}
	return
}

func (nh *nodeHandler) addMessage(msg int, src string) {
	// check if msg is in local messages already and skip if true
	nh.mux.Lock()
	defer nh.mux.Unlock()
	if _, ok := nh.messageSources[msg]; ok {
		nh.addMessageSource(msg, src)
		return
	}
	nh.messages = append(nh.messages, msg)
	nh.addMessageSource(msg, src)
	log.Printf("DEBUG: messages: %v, new message: %d", nh.messages, msg)
}

func (nh *nodeHandler) broadcastToPeers() (err error) {
	nh.mux.Lock()
	defer nh.mux.Unlock()

	syncAck := nh.syncAck
	select {
	case <-nh.ticker.C:
		nh.syncAck = make(chan struct{})
		close(syncAck)
	case <-nh.syncAck:
		return nil
	}

	if len(nh.topology) == 0 || len(nh.topology[nh.ID()]) == 0 {
		return nil
	}

	eg := errgroup.Group{}
	// for each peer check if it is not a known source of a message.
	// if not, add it to the request and sync all messages
	//for _, peer := range nh.topology[nh.Node.ID()] {
	for peer := range nh.topology {
		if peer == nh.ID() {
			continue
		}
		req := syncRequest{
			ReqType:  "sync",
			Messages: nh.messages,
		}
		/*
			for msg, sources := range nh.messageSources {
				knownSource := false
				for _, s := range sources {
					if s == peer {
						knownSource = true
						break
					}
				}
				if !knownSource {
					req.Messages = append(req.Messages, msg)
				}
			}
		*/
		if len(req.Messages) != 0 {
			err := nh.Node.RPC(peer, req, nh.syncOkHandler)

			if err != nil {
				log.Printf("ERROR: failed to sync to peer: %s. error: %s", peer, err)
				continue
			}
		}
	}

	return eg.Wait()
}

type syncRequest struct {
	ReqType  string `json:"type"`
	Messages []int  `json:"messages"`
}

func (nh *nodeHandler) syncOkHandler(msg maelstrom.Message) error {

	var body syncRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("ERROR: failed to sync to peer: %s. error: %s", msg.Src, err)
		return err
	}
	if body.ReqType != "sync_ok" {
		return fmt.Errorf("unexpected response to sync: %v", body)
	}

	// if delivery was successful, add to known peers
	nh.mux.Lock()
	log.Printf("DEBUG: received sync_ok from: %s, topology: %v", msg.Src, nh.topology)
	for _, m := range body.Messages {
		nh.messageSources[m] = append(nh.messageSources[m], msg.Src)
	}
	nh.mux.Unlock()
	return nil
}

func (nh *nodeHandler) syncHandler(msg maelstrom.Message) error {

	var reqBody syncRequest
	if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
		return err
	}

	log.Printf("DEBUG: received request: %v, raw: %s", reqBody, msg)
	for _, m := range reqBody.Messages {
		nh.addMessage(m, msg.Src)
	}

	respBody := syncRequest{
		ReqType:  "sync_ok",
		Messages: reqBody.Messages,
	}

	return nh.Reply(msg, respBody)
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
	nh.addMessage(reqBody.Message, msg.Src)

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
		*time.NewTicker(500 * time.Millisecond),
		make(chan struct{}),
		make([]int, 0, 10),
		make(map[int][]string),
		nil,
	}

	defer nh.ticker.Stop()

	nh.Handle("broadcast", nh.broadcastHandler)
	nh.Handle("read", nh.readHandler)
	nh.Handle("topology", nh.topologyHandler)
	nh.Handle("sync", nh.syncHandler)
	nh.Handle("sync_ok", func(maelstrom.Message) error { return nil })

	if err := nh.Run(); err != nil {
		log.Fatal(err)
	}

}
