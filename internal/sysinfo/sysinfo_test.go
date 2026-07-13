package sysinfo

import (
	"os"
	"path/filepath"
	"testing"
)

// withRoots temporarily overrides the discovery package vars for the
// duration of a test, restoring the originals in Cleanup.
func withRoots(t *testing.T, can, serial, goos string) {
	t.Helper()
	origCAN, origSerial, origGOOS := canRoot, serialRoot, hostGOOS
	if can != "" {
		canRoot = can
	}
	if serial != "" {
		serialRoot = serial
	}
	if goos != "" {
		hostGOOS = goos
	}
	t.Cleanup(func() {
		canRoot, serialRoot, hostGOOS = origCAN, origSerial, origGOOS
	})
}

func writeIface(t *testing.T, root, name, ifaceType string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "type"), []byte(ifaceType+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoverCANFiltersByType280(t *testing.T) {
	root := t.TempDir()
	writeIface(t, root, "can0", "280")
	writeIface(t, root, "vcan0", "280")
	writeIface(t, root, "eth0", "1")
	withRoots(t, root, "", "linux")

	got := DiscoverCAN()
	want := []string{"can0", "vcan0"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("DiscoverCAN() = %v, want %v", got, want)
	}
}

func TestDiscoverCANNonLinuxIsEmpty(t *testing.T) {
	root := t.TempDir()
	writeIface(t, root, "can0", "280")
	withRoots(t, root, "", "darwin")

	got := DiscoverCAN()
	if len(got) != 0 {
		t.Fatalf("DiscoverCAN() on non-linux = %v, want empty", got)
	}
}

func TestDiscoverCANMissingRoot(t *testing.T) {
	withRoots(t, filepath.Join(t.TempDir(), "does-not-exist"), "", "linux")

	got := DiscoverCAN()
	if len(got) != 0 {
		t.Fatalf("DiscoverCAN() with missing root = %v, want empty", got)
	}
}

func TestDiscoverSerialGlobsKnownPatterns(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"ttyUSB0", "ttyACM0", "tty.usbserial-A1", "tty.usbmodem14201", "ttyS0", "randomfile"} {
		if err := os.WriteFile(filepath.Join(root, name), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	withRoots(t, "", root, "")

	got := DiscoverSerial()
	want := []string{
		filepath.Join(root, "tty.usbmodem14201"),
		filepath.Join(root, "tty.usbserial-A1"),
		filepath.Join(root, "ttyACM0"),
		filepath.Join(root, "ttyUSB0"),
	}
	if len(got) != len(want) {
		t.Fatalf("DiscoverSerial() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("DiscoverSerial() = %v, want %v", got, want)
		}
	}
}

func TestDiscoverSerialMissingRoot(t *testing.T) {
	withRoots(t, "", filepath.Join(t.TempDir(), "does-not-exist"), "")

	got := DiscoverSerial()
	if len(got) != 0 {
		t.Fatalf("DiscoverSerial() with missing root = %v, want empty", got)
	}
}
