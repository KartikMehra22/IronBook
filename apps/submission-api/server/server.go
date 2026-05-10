// Package server hosts the HTTP/gRPC entry points for submission-api.
package server

import (
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/KartikMehra22/IronBook/apps/submission-api/service"
)

// Server wraps a Service for HTTP delivery.
type Server struct {
	Svc *service.Service
}

// HandleUpload accepts a POST stream of zstd-compressed source and dispatches
// to the service layer. Query param language=rust|go|cpp is required.
func (s *Server) HandleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	lang := r.URL.Query().Get("language")
	switch lang {
	case "rust", "go", "cpp":
	default:
		http.Error(w, "missing or invalid ?language= (must be rust|go|cpp)", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	res, err := s.Svc.Upload(r.Context(), lang, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":     res.ID.String(),
		"sha256": hex.EncodeToString(res.Sha256[:]),
		"size":   res.Size,
	})
}
