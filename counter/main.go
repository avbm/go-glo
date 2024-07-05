package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/rs/zerolog/log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	latestKey = "latest"
)

type nodeHandler struct {
	*maelstrom.Node
	kv *maelstrom.KV
}

type addRequest struct {
	Type  string `json:"type"`
	Delta int    `json:"delta"`
}

func (nh *nodeHandler) tryAdd(ctx context.Context, val int) error {
	curr, err := nh.kv.ReadInt(ctx, latestKey)
	if err != nil {
		if rpcErr, ok := err.(*maelstrom.RPCError); ok {
			if rpcErr.Code != maelstrom.KeyDoesNotExist {
				log.Debug().Msg("received RPC error which is not KeyDoesNotExist")
				return err
			}
			log.Debug().Msg("received KeyDoesNotExist RPCError")
			createIfNoExist := true
			err2 := nh.kv.CompareAndSwap(ctx, latestKey, 0, val, createIfNoExist)
			if err2 != nil {
				log.Debug().Msg("CAS with create failed")
				return err2
			}
			log.Debug().Msg("CAS with create succeeded ")
			return nil
		} else {
			log.Debug().Msg("received an error which is not RPCError")
			return err
		}
	}
	log.Info().
		Int("latest_key", curr).
		Msg("read kv got latest key successfully")

	createIfNoExist := false
	return nh.kv.CompareAndSwap(ctx, latestKey, curr, curr+val, createIfNoExist)
}

func (nh *nodeHandler) addHandler(msg maelstrom.Message) error {

	var body addRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	log.Info().
		Any("add_body", body)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := nh.tryAdd(ctx, body.Delta)
	for err != nil {
		log.Error().
			Err(err).
			Msg("CAS failed, trying again in 1 second")

		time.Sleep(time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		err = nh.tryAdd(ctx, body.Delta)
	}
	log.Info().Msg("CAS succeeded, responding with ok")

	// update the message type for response
	resp := make(map[string]any)
	resp["type"] = "add_ok"

	return nh.Reply(msg, resp)
}

type readResponse struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

func (nh *nodeHandler) readHandler(msg maelstrom.Message) error {

	var body addRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	val, err := nh.kv.ReadInt(ctx, latestKey)
	for err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		val, err = nh.kv.ReadInt(ctx, latestKey)
		time.Sleep(time.Second)
	}

	// update the message type for response
	resp := readResponse{
		Type:  "read_ok",
		Value: val,
	}

	return nh.Reply(msg, resp)
}
func main() {

	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)
	nh := nodeHandler{
		node,
		kv,
	}

	nh.Handle("add", nh.addHandler)
	nh.Handle("read", nh.readHandler)

	if err := nh.Run(); err != nil {
		log.Fatal().Err(err)
	}

}
