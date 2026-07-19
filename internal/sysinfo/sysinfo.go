// Package sysinfo provides best-effort discovery of CAN and USB-serial
// hardware present on the host, so beacon's UI "add source/sink" forms and
// its GET /api/v1/system endpoint can offer detected interfaces/ports
// instead of a blank text field. It is a leaf package (no beacon imports of
// its own) so both internal/api and internal/ui can depend on it directly
// without either package importing the other just for this.
package sysinfo

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CANInterface struct {
	Name             string   `json:"name"`
	OperationalState string   `json:"operational_state,omitempty"`
	CANState         string   `json:"can_state,omitempty"`
	Bitrate          uint64   `json:"bitrate,omitempty"`
	RestartMS        uint64   `json:"restart_ms,omitempty"`
	RXPackets        uint64   `json:"rx_packets,omitempty"`
	TXPackets        uint64   `json:"tx_packets,omitempty"`
	RXErrors         uint64   `json:"rx_errors,omitempty"`
	TXErrors         uint64   `json:"tx_errors,omitempty"`
	RXDropped        uint64   `json:"rx_dropped,omitempty"`
	TXDropped        uint64   `json:"tx_dropped,omitempty"`
	BusLoadPercent   *float64 `json:"bus_load_percent,omitempty"`
}

// canRoot, serialRoot, and hostGOOS are package vars (rather than hard-coded
// constants) so tests can point them at a fixture directory — and, for
// canRoot, force the "linux" branch — to exercise the discovery logic
// deterministically on any host, not just Linux CI.
var (
	canRoot    = "/sys/class/net"
	serialRoot = "/dev"
	hostGOOS   = runtime.GOOS
	runIP      = func(name string) ([]byte, error) {
		return exec.Command("ip", "-details", "-statistics", "-json", "link", "show", "dev", name).Output()
	}
)

type loadSample struct {
	at      time.Time
	packets uint64
}

var loadSamples = struct {
	sync.Mutex
	values map[string]loadSample
}{values: map[string]loadSample{}}

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

func readUint(path string) uint64 {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	v, _ := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	return v
}

// DiscoverCANDetails adds link state, error counters, bitrate, controller
// state, and an approximate live bus load to DiscoverCAN's interface list.
func DiscoverCANDetails() []CANInterface {
	interfaces := DiscoverCAN()
	out := make([]CANInterface, 0, len(interfaces))
	for _, name := range interfaces {
		root := filepath.Join(canRoot, name)
		oper, _ := os.ReadFile(filepath.Join(root, "operstate"))
		info := CANInterface{Name: name, OperationalState: strings.TrimSpace(string(oper))}
		stats := filepath.Join(root, "statistics")
		info.RXPackets = readUint(filepath.Join(stats, "rx_packets"))
		info.TXPackets = readUint(filepath.Join(stats, "tx_packets"))
		info.RXErrors = readUint(filepath.Join(stats, "rx_errors"))
		info.TXErrors = readUint(filepath.Join(stats, "tx_errors"))
		info.RXDropped = readUint(filepath.Join(stats, "rx_dropped"))
		info.TXDropped = readUint(filepath.Join(stats, "tx_dropped"))
		if body, err := runIP(name); err == nil {
			applyIPDetails(body, &info)
		}
		applyBusLoad(&info)
		out = append(out, info)
	}
	return out
}

func applyIPDetails(body []byte, out *CANInterface) {
	var links []struct {
		LinkInfo struct {
			InfoData map[string]any `json:"info_data"`
		} `json:"linkinfo"`
	}
	if json.Unmarshal(body, &links) != nil || len(links) == 0 {
		return
	}
	data := links[0].LinkInfo.InfoData
	if state, ok := data["state"].(string); ok {
		out.CANState = state
	}
	out.RestartMS = number(data["restart_ms"])
	if timing, ok := data["bittiming"].(map[string]any); ok {
		out.Bitrate = number(timing["bitrate"])
	}
}

func number(value any) uint64 {
	switch v := value.(type) {
	case float64:
		return uint64(v)
	case json.Number:
		n, _ := strconv.ParseUint(v.String(), 10, 64)
		return n
	default:
		return 0
	}
}

func applyBusLoad(info *CANInterface) {
	if info.Bitrate == 0 {
		return
	}
	now := time.Now()
	packets := info.RXPackets + info.TXPackets
	loadSamples.Lock()
	previous, ok := loadSamples.values[info.Name]
	loadSamples.values[info.Name] = loadSample{at: now, packets: packets}
	loadSamples.Unlock()
	if !ok || packets < previous.packets {
		return
	}
	seconds := now.Sub(previous.at).Seconds()
	if seconds <= 0 {
		return
	}
	// An 8-byte extended N2K frame occupies roughly 128 on-wire bits after
	// arbitration, CRC, stuffing, and inter-frame spacing.
	value := float64(packets-previous.packets) * 128 / seconds / float64(info.Bitrate) * 100
	if value > 100 {
		value = 100
	}
	info.BusLoadPercent = &value
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
