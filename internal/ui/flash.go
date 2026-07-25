package ui

import (
	"encoding/base64"
	"net/http"
)

// flashCookie carries a one-shot success message across the full-page
// navigation a create handler triggers via HX-Redirect: the create POST sets
// it, the destination page (the dashboard) reads it once and clears it. A
// cookie — not server-side session state — suits beacon's single-operator,
// offline model: nothing to store or expire on the server. The value is
// base64url-encoded so arbitrary message text (quotes, spaces) is always a
// valid, un-sanitized cookie value.
const flashCookie = "beacon_flash"

// flashRedirect records msg as the next page's one-shot flash and tells htmx
// to navigate the whole browser to dest. htmx honours the HX-Redirect
// response header with a full page load, so dest receives the flash cookie
// and consumes it. The create handlers use this instead of swapping a
// fragment in place.
func flashRedirect(w http.ResponseWriter, msg, dest string) {
	setFlash(w, msg)
	w.Header().Set("HX-Redirect", dest)
}

func setFlash(w http.ResponseWriter, msg string) {
	http.SetCookie(w, &http.Cookie{ // #nosec G124 -- Beacon supports local HTTP; HttpOnly and SameSite protect this non-sensitive flash value.
		Name:     flashCookie,
		Value:    base64.URLEncoding.EncodeToString([]byte(msg)),
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   60, // self-expire if a redirect never lands on a reader
	})
}

// takeFlash returns and clears the one-shot flash message (empty when none).
// Clearing emits a Set-Cookie header, so it must run before the response body
// is written — page handlers call it up front.
func takeFlash(w http.ResponseWriter, r *http.Request) string {
	c, err := r.Cookie(flashCookie)
	if err != nil || c.Value == "" {
		return ""
	}
	http.SetCookie(w, &http.Cookie{ // #nosec G124 -- must match the local-HTTP-compatible cookie being cleared.
		Name:     flashCookie,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1, // delete now that it has been read
	})
	b, err := base64.URLEncoding.DecodeString(c.Value)
	if err != nil {
		return ""
	}
	return string(b)
}
