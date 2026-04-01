//go:build linux

package can

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/brutella/can"
	"golang.org/x/sys/unix"

	"github.com/open-ships/beacon/internal/admin"
)

// canFrame is the Linux on-wire CAN frame layout.
type canFrame struct {
	ID   uint32
	DLC  uint8
	Pad  uint8
	Res0 uint8
	Res1 uint8
	Data [8]uint8
}

// Reader reads CAN frames from a Linux SocketCAN interface.
type Reader struct {
	iface     string
	bitrate   int
	autoUp    bool
	restartMS int
	log       *slog.Logger
	metrics   *admin.Metrics

	mu sync.Mutex // guards fd
	fd int

	lastFrameNano atomic.Int64

	dropLogLimiter atomic.Int64 // unix nano of last drop log
}

// NewReader creates a new CAN Reader.
func NewReader(iface string, bitrate int, autoUp bool, restartMS int, log *slog.Logger, metrics *admin.Metrics) *Reader {
	return &Reader{
		iface:     iface,
		bitrate:   bitrate,
		autoUp:    autoUp,
		restartMS: restartMS,
		log:       log,
		metrics:   metrics,
		fd:        -1,
	}
}

// HealthCheck returns an error if no frame has been received within 10 seconds.
func (r *Reader) HealthCheck(_ context.Context) error {
	nano := r.lastFrameNano.Load()
	if nano == 0 {
		return fmt.Errorf("no frames received yet")
	}
	last := time.Unix(0, nano)
	if time.Since(last) > 10*time.Second {
		return fmt.Errorf("no frame received in %s", time.Since(last).Round(time.Second))
	}
	return nil
}

// Start begins reading CAN frames and sends them to out.
// It blocks until ctx is cancelled.
func (r *Reader) Start(ctx context.Context, out chan<- *can.Frame) error {
	if r.autoUp {
		if err := r.bringUp(); err != nil {
			r.log.Warn("auto_up failed", "iface", r.iface, "err", err)
		}
	}

	if err := r.openSocket(); err != nil {
		return fmt.Errorf("open CAN socket: %w", err)
	}

	go r.readLoop(ctx, out)
	return nil
}

func (r *Reader) bringUp() error {
	cmd := exec.Command("ip", "link", "set", r.iface, "up", "type", "can", "bitrate", fmt.Sprintf("%d", r.bitrate))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ip link: %w, output: %s", err, out)
	}
	return nil
}

func (r *Reader) openSocket() error {
	fd, err := unix.Socket(unix.AF_CAN, unix.SOCK_RAW, unix.CAN_RAW)
	if err != nil {
		return fmt.Errorf("socket: %w", err)
	}

	ifreq, err := unix.NewIfreq(r.iface)
	if err != nil {
		unix.Close(fd)
		return fmt.Errorf("ifreq: %w", err)
	}

	if err := unix.IoctlIfreq(fd, unix.SIOCGIFINDEX, ifreq); err != nil {
		unix.Close(fd)
		return fmt.Errorf("ioctl SIOCGIFINDEX: %w", err)
	}

	sa := &unix.SockaddrCAN{Ifindex: int(ifreq.Uint32())}
	if err := unix.Bind(fd, sa); err != nil {
		unix.Close(fd)
		return fmt.Errorf("bind: %w", err)
	}

	r.mu.Lock()
	r.fd = fd
	r.mu.Unlock()
	return nil
}

// closeFD closes the current fd under the lock. Safe to call if fd == -1.
func (r *Reader) closeFD() {
	r.mu.Lock()
	if r.fd >= 0 {
		unix.Close(r.fd)
		r.fd = -1
	}
	r.mu.Unlock()
}

// Close closes the CAN socket. Safe to call concurrently.
func (r *Reader) Close() error {
	r.closeFD()
	return nil
}

func (r *Reader) readLoop(ctx context.Context, out chan<- *can.Frame) {
	buf := make([]byte, unsafe.Sizeof(canFrame{}))

	for {
		select {
		case <-ctx.Done():
			r.closeFD()
			return
		default:
		}

		// Set read deadline via poll
		r.mu.Lock()
		fd := r.fd
		r.mu.Unlock()

		if fd < 0 {
			time.Sleep(time.Duration(r.restartMS) * time.Millisecond)
			continue
		}

		tv := unix.Timeval{Sec: 0, Usec: 100000} // 100ms
		_ = unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv)

		n, err := unix.Read(fd, buf)
		if err != nil {
			if err == unix.EAGAIN || err == unix.EINTR {
				continue
			}
			r.log.Error("CAN read error", "err", err)
			if r.metrics != nil {
				r.metrics.CANErrorsTotal.Add(ctx, 1,
					admin.AttrIface(r.iface),
					admin.AttrType("socket_error"),
				)
			}
			// Close old fd before restarting
			r.closeFD()
			time.Sleep(time.Duration(r.restartMS) * time.Millisecond)
			_ = r.openSocket()
			continue
		}
		if n < 16 {
			continue
		}

		frame, isErr := ParseRawFrame(buf[:n])

		// Error frame — skip
		if isErr {
			r.log.Warn("CAN error frame received")
			if r.metrics != nil {
				r.metrics.CANErrorsTotal.Add(ctx, 1,
					admin.AttrIface(r.iface),
					admin.AttrType("error_frame"),
				)
			}
			time.Sleep(time.Duration(r.restartMS) * time.Millisecond)
			continue
		}

		if r.metrics != nil {
			r.metrics.CANFramesTotal.Add(ctx, 1, admin.AttrIface(r.iface))
		}

		r.lastFrameNano.Store(time.Now().UnixNano())

		select {
		case out <- frame:
		case <-ctx.Done():
			return
		default:
			// Drop frame if channel is full — log rate-limited
			now := time.Now().UnixNano()
			lastLog := r.dropLogLimiter.Load()
			if now-lastLog > int64(5*time.Second) {
				if r.dropLogLimiter.CompareAndSwap(lastLog, now) {
					r.log.Warn("CAN frame dropped: channel full", "iface", r.iface)
				}
			}
			if r.metrics != nil {
				r.metrics.CANErrorsTotal.Add(ctx, 1,
					admin.AttrIface(r.iface),
					admin.AttrType("dropped"),
				)
			}
		}
	}
}
