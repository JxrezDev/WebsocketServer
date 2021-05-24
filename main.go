package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"strings"
	"time"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}
var connections []*websocket.Conn

var myClient = &http.Client{Timeout: 10 * time.Second}

const (
	BaseURL = "https://api.conoce360.tech/bancas"
)

type Banca struct {
	IdBanca string `json:"idbanca"`
	Estado  string `json:"estado"`
}

type JSONResponse struct {
	CantLibre   int     `json:"cantLibre"`
	CantOcupado int     `json:"cantOcupado"`
	Banca       []Banca `json:"bancas"`
}

func getJson(url string, decoded interface{}) error {
	r, err := myClient.Get(url)
	if err != nil {
		return err
	}

	defer r.Body.Close()

	buf := new(bytes.Buffer)
	buf.ReadFrom(r.Body)

	return json.Unmarshal(buf.Bytes(), decoded)
}

func broadcastMessage(msg []byte, connections []*websocket.Conn) {
	var bancas []Banca
	contLibres := 0
	contOcupados := 0

	getJson(BaseURL, &bancas)

	for i := 0; i < len(bancas); i++ {
		if strings.EqualFold(bancas[i].Estado, "libre") {
			contLibres++
		}
	}
	contOcupados = len(bancas) - contLibres

	jsonRes := JSONResponse{
		CantLibre:   contLibres,
		CantOcupado: contOcupados,
		Banca:       bancas,
	}

	fmt.Println(jsonRes)

	for _, conn := range connections {
		_ = conn.WriteJSON(jsonRes)
	}
}

func main() {
	incomingMessages := make(chan []byte)
	incomingConnections := make(chan *websocket.Conn)

	r := mux.NewRouter()

	go func() {
		for {
			select {
			case msg := <-incomingMessages:
				broadcastMessage(msg, connections)
			case conn := <-incomingConnections:
				connections = append(connections, conn)
			}

		}
	}()

	r.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		conn, _ := upgrader.Upgrade(writer, request, nil)
		log.Println(request)
		defer conn.Close()

		incomingConnections <- conn

		for {
			// Receive message
			if conn != nil {
				_, message, err := conn.ReadMessage()
				if err != nil {
					log.Println(err)
				}
				if message != nil {
					log.Printf("Message received: %s", message)
					incomingMessages <- message
				} else {
					fmt.Println("Vacio...")
					return
				}
			} else {
				log.Println("Conn vacio")
				return
			}
		}

	})

	log.Fatal(http.ListenAndServe(":5555", r))
}
