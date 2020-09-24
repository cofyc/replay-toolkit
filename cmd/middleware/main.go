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
	goflag "flag"
	"net/http"
	"os"
	"strings"

	lru "github.com/hashicorp/golang-lru"
	flag "github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/util/sets"
	cliflag "k8s.io/component-base/cli/flag"
	klog "k8s.io/klog/v2"
)

var (
	cache *lru.Cache

	maxInputSize = 100 * 1024 * 1024

	// options
	optMethods      []string
	optHosts        []string
	optHeaders      cliflag.MapStringString
	optPath         string
	optAmplifyRatio int = 1

	// parsed options
	allowedMethods = sets.String{}
	allowedHosts   = sets.String{}
)

type replayRequest struct {
	ID           string
	req          *http.Request
	respOrginal  *http.Response
	respReplayed *http.Response
}

func init() {
	// register klog flags to pflag.CommandLine
	klog.InitFlags(nil)
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	flag.StringSliceVar(&optMethods, "methods", optMethods, "allowed methods")
	flag.StringSliceVar(&optHosts, "hosts", optHosts, "allowed hosts")
	flag.StringVar(&optPath, "path", optPath, "allowed path")
	flag.Var(&optHeaders, "headers", "allowed headers in map")
	flag.IntVar(&optAmplifyRatio, "amplify-ratio", optAmplifyRatio, "amplify ratio, e.g. 2, 5")
	flag.Parse()

	for _, m := range optMethods {
		allowedMethods.Insert(strings.ToLower(m))
	}
	for _, m := range optHosts {
		allowedHosts.Insert(strings.ToLower(m))
	}

	// initialize cache
	var err error
	cache, err = lru.New(102400)
	if err != nil {
		klog.Fatal(err)
	}
}

func process(buf []byte) (skipped bool) {
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
	var (
		err     error
		reqID   = string(meta[1])
		req     *http.Request
		resp    *http.Response
		payload = buf[headerSize:]
	)

	defer func() {
		if skipped {
			if payloadType == '1' {
				klog.V(2).Infof("skipped request %s: %s %s %s", reqID, req.Method, req.Host, req.URL.String())
			}
		}
	}()

	switch payloadType {
	case '1': // Request
		req, err = http.ReadRequest(bufio.NewReader((bytes.NewReader(payload))))
		if err != nil {
			klog.Warningf("cannot parse request payload: %s", payload)
			return
		}
		if !allowedMethods.Has(strings.ToLower(req.Method)) {
			skipped = true
			return
		}
		if !allowedHosts.Has(strings.ToLower(req.Host)) {
			skipped = true
			return
		}
		if !strings.HasPrefix(req.URL.Path, optPath) {
			skipped = true
			return
		}
		klog.V(2).Infof("got request %s: %s %s %s", reqID, req.Method, req.Host, req.URL.String())
		cache.Add(reqID, &replayRequest{
			ID:  reqID,
			req: req,
		})
		// Emitting requests
		for i := 0; i < optAmplifyRatio; i++ {
			os.Stdout.Write(encode(buf))
		}
	case '2': // Orginal reponse
		replayRequestFromCache, ok := cache.Get(reqID)
		if !ok {
			klog.V(2).Infof("request of ID %s does not exist, skipped", reqID)
			return
		}
		replayReq := replayRequestFromCache.(*replayRequest)
		resp, err = http.ReadResponse(bufio.NewReader((bytes.NewReader(payload))), replayReq.req)
		if err != nil {
			panic(err)
			klog.Fatal(err)
		}
		replayReq.respOrginal = resp
	case '3': // Replayed response
		replayRequestFromCache, ok := cache.Get(reqID)
		if !ok {
			klog.Infof("request of ID %s is cleared, skipped")
			return
		}
		replayReq := replayRequestFromCache.(*replayRequest)
		resp, err = http.ReadResponse(bufio.NewReader((bytes.NewReader(payload))), replayReq.req)
		if err != nil {
			panic(err)
			klog.Fatal(err)
		}
		replayReq.respReplayed = resp
		if replayReq.respOrginal != nil {
			klog.Infof("[%s] %s %s, original status: %s, replay status: %s", replayReq.ID, replayReq.req.Method, replayReq.req.URL.String(), replayReq.respOrginal.Status, replayReq.respReplayed.Status)
		} else {
			klog.Infof("[%s] %s %s replayed response %s", replayReq.ID, replayReq.req.Method, replayReq.req.URL.String(), resp.Status)
		}
	default:
		panic("unreachable")
	}
	return
}

func main() {
	flag.CommandLine.VisitAll(func(flag *flag.Flag) {
		klog.Infof("FLAG: --%s=%q", flag.Name, flag.Value)
	})

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer([]byte{}, maxInputSize)

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
