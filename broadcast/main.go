package main

import (
	"encoding/json"
	"fmt"
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

func (nh *nodeHandler) broadcastToPeers() error {

	if len(nh.topology) == 0 || len(nh.topology[nh.ID()]) == 0 {
		return nil
	}

	nh.mux.Lock()
	defer nh.mux.Unlock()

	eg := errgroup.Group{}
	// for each peer check if it is not a known source of a message.
	// if not, add it to the request and sync all messages
	mux := sync.Mutex{}
	for _, peer := range nh.topology[nh.ID()] {
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
			err := nh.Node.RPC(peer, req, func(msg maelstrom.Message) error {
				type request struct {
					ReqType string `json:"type"`
				}
				var body request
				if err := json.Unmarshal(msg.Body, &body); err != nil {
					log.Printf("ERROR: failed to sync to peer: %s. error: %s", peer, err)
					return err
				}
				if body.ReqType != "sync_ok" {
					return fmt.Errorf("unexpected response to sync: %v", body)
				}

				// if delivery was successful, add to known peers
				mux.Lock()
				for _, msg := range req.Messages {
					//TODO find source of concurrent map write
					nh.messageSources[msg] = append(nh.messageSources[msg], peer)
				}
				mux.Unlock()
				return nil

			})
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

func (nh *nodeHandler) syncHandler(msg maelstrom.Message) error {

	var reqBody syncRequest
	if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
		return err
	}

	log.Printf("DEBUG: received request: %v, raw: %s", reqBody, msg)
	for _, m := range reqBody.Messages {
		nh.addMessage(m, msg.Src)
	}

	respBody := make(map[string]any)
	// update the message type for response
	respBody["type"] = "sync_ok"

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

	go nh.broadcastToPeers()

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
		make(map[int][]string),
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
