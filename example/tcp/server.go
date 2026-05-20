package main

import (
	"bufio"
	"fmt"
	"net"
)

func main() {
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}

	fmt.Println("TCP server listening on :8080")

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("accept error:", err)
			continue
		}

		go handle(conn)
	}
}

func handle(conn net.Conn) {
	defer conn.Close()

	fmt.Println("client connected:", conn.RemoteAddr())

	reader := bufio.NewReader(conn)

	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("read error:", err)
			return
		}

		fmt.Printf("recv: %s", msg)

		// 回显
		conn.Write([]byte("echo: " + msg))
	}
}
