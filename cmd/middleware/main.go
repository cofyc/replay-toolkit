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
	"net/http"
	"os"
	"regexp"
	"strings"

	goflag "flag"

	lru "github.com/hashicorp/golang-lru"
	flag "github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/util/sets"
	cliflag "k8s.io/component-base/cli/flag"
	klog "k8s.io/klog/v2"
)

var (
	cache *lru.Cache

	// options
	optMethods []string
	optHosts   []string
	optHeaders cliflag.MapStringString
	optPath    string

	// parsed options
	allowedMethods sets.String
	allowedHosts   sets.String
	pathRegexp     *regexp.Regexp
)

func init() {
	// register klog flags to pflag.CommandLine
	klog.InitFlags(nil)
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	flag.StringSliceVar(&optMethods, "methods", optMethods, "allowed methods")
	flag.StringSliceVar(&optHosts, "hosts", optHosts, "allowed hosts")
	flag.StringVar(&optPath, "path", optPath, "allowed path in regex")
	flag.Var(&optHeaders, "headers", "allowed headers in map")
	flag.Parse()

	for _, m := range optMethods {
		allowedMethods.Insert(strings.ToLower(m))
	}
	for _, m := range optHosts {
		allowedHosts.Insert(strings.ToLower(m))
	}
	pathRegexp = regexp.MustCompile(optPath)

	// initialize cache
	var err error
	cache, err = lru.New(1024)
	if err != nil {
		panic(err)
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
		payload = buf[headerSize:]
	)

	defer func() {
		if skipped {
			if payloadType == '1' {
				klog.V(1).Infof("skipped request: %s %s", req.Method, req.URL.String())
			}
		}
	}()

	switch payloadType {
	case '1': // Request
		req, err = http.ReadRequest(bufio.NewReader((bytes.NewReader(payload))))
		if err != nil {
			panic(err)
		}
		if !allowedMethods.Has(strings.ToLower(req.Method)) {
			skipped = true
			return
		}
		if !allowedHosts.Has(strings.ToLower(req.Host)) {
			skipped = true
			return
		}
		if !pathRegexp.MatchString(req.URL.Path) {
			skipped = true
			return
		}
		cache.Add(reqID, req)
		// Emitting data back
		os.Stdout.Write(encode(buf))
	case '2': // Orginal reponse
	case '3': // Replayed response
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
