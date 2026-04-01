package admin

import (
	"encoding/json"
	"net/http"

	"github.com/open-ships/beacon/internal/config"
)

// ConfigHandler returns an http.Handler that serves the active configuration as JSON.
func ConfigHandler(cfg *config.Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(cfg); err != nil {
			http.Error(w, "failed to encode config", http.StatusInternalServerError)
		}
	})
}
