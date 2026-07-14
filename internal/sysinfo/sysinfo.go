// Package sysinfo provides best-effort discovery of CAN and USB-serial
// hardware present on the host, so beacon's UI "add source/sink" forms and
// its GET /api/v1/system endpoint can offer detected interfaces/ports
// instead of a blank text field. It is a leaf package (no beacon imports of
// its own) so both internal/api and internal/ui can depend on it directly
// without either package importing the other just for this.
package sysinfo

import (
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

// canRoot, serialRoot, and hostGOOS are package vars (rather than hard-coded
// constants) so tests can point them at a fixture directory — and, for
// canRoot, force the "linux" branch — to exercise the discovery logic
// deterministically on any host, not just Linux CI.
var (
	canRoot    = "/sys/class/net"
	serialRoot = "/dev"
	hostGOOS   = runtime.GOOS
)

// DiscoverCAN best-effort lists SocketCAN network interface names present on
// this host: entries of canRoot (normally /sys/class/net) whose "type" file
// reads "280" (ARPHRD_CAN, the CAN bus link-layer type in Linux's if_arp.h).
// SocketCAN is Linux-only, so any other OS short-circuits to an empty list
// without touching the filesystem at all; both a missing canRoot and a
// per-interface read failure are treated as "no such interface" rather than
// an error, matching this function's best-effort contract.
func DiscoverCAN() []string {
	if hostGOOS != "linux" {
		return []string{}
	}
	entries, err := os.ReadDir(canRoot)
	if err != nil {
		return []string{}
	}
	out := []string{}
	for _, e := range entries {
		typ, err := os.ReadFile(filepath.Join(canRoot, e.Name(), "type"))
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(typ)) == "280" {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

// DiscoverSerial best-effort lists likely USB-serial device paths under
// serialRoot (normally /dev), matching the naming conventions Linux
// (ttyUSB*, ttyACM*) and macOS (tty.usbserial*, tty.usbmodem*) use for USB
// serial adapters — the typical connection for USB-CAN and NMEA 0183
// hardware. Unlike DiscoverCAN this is not OS-gated: on a host with none of
// these devices the glob patterns simply match nothing.
func DiscoverSerial() []string {
	patterns := []string{"ttyUSB*", "ttyACM*", "tty.usbserial*", "tty.usbmodem*"}
	out := []string{}
	for _, p := range patterns {
		matches, _ := filepath.Glob(filepath.Join(serialRoot, p))
		out = append(out, matches...)
	}
	sort.Strings(out)
	return out
}
