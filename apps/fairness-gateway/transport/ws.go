// Package transport bridges non-REST wire protocols (WebSocket, FIX) to the
// fairness-gateway core. Each transport is a thin adapter that decodes a
// frame, calls into `gateway.Server.Handle`, and writes a reply frame.
package transport

import (
	"encoding/json"
	"net/http"

	"github.com/coder/websocket"

	"github.com/KartikMehra22/IronBook/apps/fairness-gateway/gateway"
)

// WSHandler is the WebSocket adapter. One TCP connection multiplexes many
// orders; we read JSON frames in the RESTOrder shape, dispatch them through
// `srv.Handle`, and write the reply as a JSON frame.
func WSHandler(srv *gateway.Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer c.CloseNow()

		ctx := r.Context()
		for {
			typ, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			if typ != websocket.MessageText {
				continue
			}
			var ro gateway.RESTOrder
			if err := json.Unmarshal(data, &ro); err != nil {
				_ = c.Write(ctx, websocket.MessageText, []byte(`{"error":"bad json"}`))
				continue
			}
			rep, _, err := srv.Handle(ctx, &ro)
			if err != nil {
				body, _ := json.Marshal(map[string]string{"error": err.Error()})
				_ = c.Write(ctx, websocket.MessageText, body)
				continue
			}
			body, _ := json.Marshal(rep)
			_ = c.Write(ctx, websocket.MessageText, body)
		}
	}
}
