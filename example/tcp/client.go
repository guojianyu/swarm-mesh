package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	conn, err := net.Dial("tcp", "公网IP:9000") // 改成你的 tunnel 端口
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("connected to server")

	go func() {
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			fmt.Println("recv:", scanner.Text())
		}
	}()

	input := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("send> ")
		if !input.Scan() {
			break
		}

		text := input.Text() + "\n"
		conn.Write([]byte(text))
	}
}
