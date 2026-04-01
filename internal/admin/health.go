package admin

import (
	"context"
	"encoding/json"
	"net/http"
)

// HealthChecker is implemented by subsystems that can report their health.
type HealthChecker interface {
	HealthCheck(ctx context.Context) error
}

type healthResponse struct {
	Healthy bool `json:"healthy"`
}

// HealthHandler returns an http.Handler that runs all registered health checks.
// It responds with 200 and {"healthy":true} if all checks pass, or 503 and
// {"healthy":false} if any check fails.
func HealthHandler(checks map[string]HealthChecker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		healthy := true

		for _, checker := range checks {
			if err := checker.HealthCheck(ctx); err != nil {
				healthy = false
				break
			}
		}

		code := http.StatusOK
		if !healthy {
			code = http.StatusServiceUnavailable
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(healthResponse{Healthy: healthy})
	})
}
