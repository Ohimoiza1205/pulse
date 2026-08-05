package httpapi

import (
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// subscriberBuffer is how many readings may queue for one client before
	// the hub starts dropping frames for it.
	subscriberBuffer = 256

	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = 25 * time.Second
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// Same origin only by default. Loosen deliberately behind a real gateway
	// rather than accidentally here.
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		return origin == "" || origin == "http://"+r.Host || origin == "https://"+r.Host
	},
}

// handleStream upgrades to WebSocket and pushes live readings. Pass
// ?device=<id> to follow a single device instead of the whole fleet.
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.log.Warn("websocket upgrade failed", "err", err)
		return
	}

	device := r.URL.Query().Get("device")
	sub := s.hub.Subscribe(device, subscriberBuffer)

	closed := make(chan struct{})

	// Read pump. We never expect client payloads, but the read loop is what
	// surfaces close frames and pong replies, so it has to run.
	go func() {
		defer close(closed)
		defer conn.Close()
		conn.SetReadLimit(512)
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(pongWait))
		})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		s.hub.Unsubscribe(sub)
		conn.Close()
	}()

	for {
		select {
		case reading, ok := <-sub.C:
			if !ok {
				_ = conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseGoingAway, "server shutting down"),
					time.Now().Add(writeWait))
				return
			}
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteJSON(reading); err != nil {
				return
			}
		case <-ticker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-closed:
			return
		}
	}
}
