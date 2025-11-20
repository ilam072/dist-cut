package main

import (
	"dist-cut/internal/coordinator"
	worker "dist-cut/internal/http"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
)

func main() {
	var (
		port        = flag.Int("port", 8001, "port to listen on for worker requests")
		peersStr    = flag.String("peers", "", "comma-separated peer addresses host:port")
		coord       = flag.Bool("coord", false, "run as coordinator (reads stdin and distributes tasks)")
		replication = flag.Int("rep", 2, "replication factor: how many nodes each shard is sent to")
		quorum      = flag.Int("quorum", 2, "quorum needed for each shard (e.g., 2 for majority among 3)")
		fields      = flag.String("f", "1", "field list like cut -f (required)")
		delim       = flag.String("d", "\t", "delimiter (default tab)")
	)
	flag.Parse()
	peers := []string{}
	if *peersStr != "" {
		for _, p := range strings.Split(*peersStr, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				peers = append(peers, p)
			}
		}
	}
	listenAddr := fmt.Sprintf("127.0.0.1:%d", *port)

	http.HandleFunc("/process", worker.WorkerHandler)
	go func() {
		log.Printf("worker listening on %s\n", listenAddr)
		if err := http.ListenAndServe(fmt.Sprintf(":"+strconv.Itoa(*port)), nil); err != nil {
			log.Fatalf("listen: %v", err)
		}
	}()

	if *coord {
		if *replication < 1 {
			*replication = 1
		}
		if *quorum < 1 {
			*quorum = 1
		}
		log.Printf("Coordinator: peers=%v, replication=%d, quorum=%d, fields=%s, delim=%q\n", peers, *replication, *quorum, *fields, *delim)
		err := coordinator.Main(peers, listenAddr, *fields, *delim, *replication, *quorum)
		if err != nil {
			log.Fatalf("coordinator failed: %v", err)
		}
		return
	}

	select {}
}
