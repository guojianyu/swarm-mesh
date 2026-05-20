// websocket_server.go
package main

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func echo(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade failed: ", err)
		return
	}
	defer conn.Close()

	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("read error:", err)
			break
		}
		log.Printf("recv: %s", message)
		err = conn.WriteMessage(messageType, message)
		if err != nil {
			log.Println("write error:", err)
			break
		}
	}
}

func main() {
	http.HandleFunc("/ws", echo)
	log.Println("WebSocket server on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
