package main

import (
	"flag"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:19999", "address to accept connections on")
	target := flag.String("target", "127.0.0.1:9000", "address to forward connections to")
	chaosFile := flag.String("chaos-file", "", "drop connections mid-body while this file exists")
	flag.Parse()
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("forwarding %s -> %s (chaos file %q)", *listen, *target, *chaosFile)
	for {
		client, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go proxy(client, *target, *chaosFile)
	}
}

const (
	passthrough = iota
	killRequestMidBody
	killResponseMidBody
)

func pickMode(chaosFile string) int {
	if chaosFile == "" {
		return passthrough
	}
	if _, err := os.Stat(chaosFile); err != nil {
		return passthrough
	}
	switch roll := rand.Intn(100); {
	case roll < 30:
		return killRequestMidBody
	case roll < 55:
		return killResponseMidBody
	default:
		return passthrough
	}
}

func proxy(client net.Conn, target, chaosFile string) {
	server, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		client.Close()
		return
	}
	mode := pickMode(chaosFile)
	done := make(chan struct{}, 2)
	go pipe(server, client, mode == killRequestMidBody, done)
	go pipe(client, server, mode == killResponseMidBody, done)
	<-done
	abort(client)
	abort(server)
	<-done
	if mode != passthrough {
		log.Printf("dropped connection mid-body (mode %d)", mode)
	}
}

func pipe(dst, src net.Conn, sabotage bool, done chan<- struct{}) {
	if sabotage {
		limit := int64(512 + rand.Intn(128<<10))
		_, _ = io.CopyN(dst, src, limit)
	} else {
		_, _ = io.Copy(dst, src)
	}
	done <- struct{}{}
}

func abort(conn net.Conn) {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	_ = conn.Close()
}
