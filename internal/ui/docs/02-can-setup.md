# CAN setup

beacon talks to CAN hardware through the OS network stack (SocketCAN) or a
serial USB-CAN adapter. Either way, the interface has to exist and be
correctly configured *before* beacon starts a source or sink against it —
beacon never brings an interface up or sets its bitrate itself.

## SocketCAN (`socketcan`)

NMEA 2000 runs at 250 kbit/s. Bring the interface up at that bitrate before
starting beacon:

```
sudo ip link set can0 type can bitrate 250000
sudo ip link set can0 up
```

Check it took:

```
ip -details link show can0
```

The Beacon dashboard reports the same controller state plus bitrate, link
state, RX/TX/error/drop counters, and a sampled bus-load estimate. Its device
inventory decodes ISO NAME, manufacturer/class/function/instances,
product/configuration information, and advertised transmit/receive PGNs.
Once the vessel is correct, use **Commit baseline**; later new, changed, or
missing devices are flagged by stable NAME even if their source address moves.

Use the interface name (`can0`, `can1`, ...) as a source or sink's
`interface` field. A `socketcan` sink pushes each message onto the bus with
confirmation and retry (see the concepts page); a `socketcan` source
decodes every frame it receives.

If the interface flaps or refuses to come up, check physical bus
termination (120 ohm resistors at both ends of the bus — a mid-bus or
missing termination causes exactly this symptom) and that nothing else on
the host already has the interface open.

## USB-CAN adapters (`usbcan`)

A `usbcan` source or sink takes a `port` field: the adapter's serial device
path, e.g. `/dev/ttyUSB0` (Linux) — not an interface name. Unlike
`socketcan`, there's no separate bring-up step: beacon opens the serial
port itself.

Adapter behavior (baud rate, framing, autodetection) varies by hardware —
consult your adapter's documentation and the vendored `n2k` library's
supported devices if frames aren't decoding as expected. If the device path
changes across reboots (common with generic USB-serial chips), pin it with
a udev rule rather than depending on `/dev/ttyUSB0` staying the same
adapter every time.

## Virtual CAN (`vcan`) — testing without hardware

A virtual interface behaves like a real one to beacon, so you can build and
test a whole pipeline (see the getting-started page) with no adapter
attached:

```
sudo modprobe vcan
sudo ip link add dev vcan0 type vcan
sudo ip link set vcan0 up
```

Configure a `socketcan` source/sink with `interface: vcan0`, then inject a
test frame from another terminal:

```
cansend vcan0 18FF0001#0102030405060708
```

(`cansend` ships with `can-utils` — install it if you don't have it.)
Bitrate is meaningless for `vcan` (it's not a real bus), so there's no
`bitrate` step to run against it.

## Docker

The repo's `docker-compose.yml` runs with `network_mode: host`. This is
required for the container to see a CAN interface at all — `socketcan`
interfaces are host kernel network devices, not something a container's own
isolated network namespace has access to under the default bridge network
mode.

Bring the interface up on the **host** exactly as above, before or after
starting the container — beacon inside the container will see it either
way, since it's a host-level device. There is nothing CAN-specific to
configure in the container itself.

USB-CAN adapters need their device node passed through instead
(`--device=/dev/ttyUSB0` on `docker run`, or a `devices:` entry in compose)
since `usbcan` sources/sinks talk to a serial device path, not a network
interface — host networking alone does not expose `/dev` nodes.
