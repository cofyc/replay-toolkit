package main

//
// A middleware for goreplay.
//
// https://github.com/buger/goreplay
//

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"

	lru "github.com/hashicorp/golang-lru"
)

var (
	cache *lru.Cache
)

func init() {
	var err error
	cache, err = lru.New(1024)
	if err != nil {
		panic(err)
	}
}

func process(buf []byte) {
	// First byte indicate payload type, possible values:
	//  1 - Request
	//  2 - Response
	//  3 - ReplayedResponse
	payloadType := buf[0]
	headerSize := bytes.IndexByte(buf, '\n') + 1
	header := buf[:headerSize-1]

	// Header contains space separated values of: request type, request id, and request start time (or round-trip time for responses)
	meta := bytes.Split(header, []byte(" "))

	// For each request you should receive 3 payloads (request, response, replayed response) with same request id
	reqID := string(meta[1])
	payload := buf[headerSize:]

	switch payloadType {
	case '1': // Request
		req, err := http.ReadRequest(bufio.NewReader((bytes.NewReader(payload))))
		if err != nil {
			panic(err)
		}
		cache.Add(reqID, req)
		if req.Method != "GET" {
			return
		}
		fmt.Fprintf(os.Stderr, "method: %s, host: %s", req.Method, req.Header["Host"])
		// Emitting data back
		os.Stdout.Write(encode(buf))
	case '2': // Orginal reponse
	case '3': // Replayed response
	default:
		panic("unreachable")
	}
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		encoded := scanner.Bytes()
		buf := make([]byte, len(encoded)/2)
		hex.Decode(buf, encoded)
		process(buf)
	}
}

func encode(buf []byte) []byte {
	dst := make([]byte, len(buf)*2+1)
	hex.Encode(dst, buf)
	dst[len(dst)-1] = '\n'

	return dst
}
