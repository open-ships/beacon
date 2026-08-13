# Set up CAN

Beacon uses SocketCAN interfaces and serial USB-CAN adapters. Prepare the
hardware before you enable a source or a sink.

Beacon does not bring up a SocketCAN interface. It also does not set the
bitrate of the interface.

## Set up a SocketCAN interface

NMEA 2000 uses 250 kbit/s. Set the bitrate and bring up the interface:

```bash
sudo ip link set can0 type can bitrate 250000
sudo ip link set can0 up
```

Verify the interface:

```bash
ip -details link show can0
```

Set the source or sink `interface` field to the interface name. For example,
use `can0` or `can1`.

A `socketcan` source decodes all received frames. A `socketcan` sink confirms
each successful write and retries a failed write.

## Check the CAN status

The dashboard shows this information for each SocketCAN interface:

- Controller state and link state
- Bitrate
- Receive and transmit counters
- Error and drop counters
- Estimated bus load from sampled traffic

The device inventory also decodes this information:

- ISO Device NAME
- Manufacturer, class, function, and instance values
- Product and configuration information
- Advertised transmit and receive PGNs

When the vessel configuration is correct, select **Commit baseline**. Beacon
then identifies new, changed, and missing devices. It uses the stable Device
NAME, so a source-address change does not make a known device new.

## Check the physical network

If the interface changes state or does not start, check the physical bus.

1. Install one 120 ohm termination resistor at each end of the bus.
2. Remove a termination resistor that is in the middle of the bus.
3. Make sure that no other process has exclusive use of the interface.
4. Examine the interface error counters.

Incorrect termination frequently causes an interface to change state.

## Set up a USB-CAN adapter

Set the `port` field of a `usbcan` source or sink to the serial device path.
For example, use `/dev/ttyUSB0` on Linux.

Do not use a SocketCAN interface name for `port`. Beacon opens the serial port
directly, so a separate `ip link` operation is not necessary.

Adapter baud rates, frame formats, and detection behavior differ. If Beacon
cannot decode frames, examine the adapter instructions and the adapters that
the vendored `n2k` library supports.

A generic USB serial path can change after a restart. Use a udev rule to give
the adapter a stable path. Do not assume that `/dev/ttyUSB0` always identifies
the same adapter.

## Create a virtual CAN interface

Use a virtual CAN interface to test Beacon without CAN hardware.

Create `vcan0`:

```bash
sudo modprobe vcan
sudo ip link add dev vcan0 type vcan
sudo ip link set vcan0 up
```

Configure a `socketcan` source or sink with `interface: vcan0`.

Send a test frame:

```bash
cansend vcan0 18FF0001#0102030405060708
```

The `cansend` command is part of `can-utils`. Install that package if the
command is not available.

Do not set a bitrate for `vcan`. It is not a physical bus.

## Give a container access to CAN

The supplied `docker-compose.yml` uses `network_mode: host`. This mode gives
the container access to host SocketCAN interfaces.

Bring up the interface on the host. You can do this before or after you start
the container because SocketCAN is a host network device.

For a USB-CAN adapter, also give the container access to the serial device.
Use `--device=/dev/ttyUSB0` with `docker run`, or add the device to the Compose
`devices` list. Host networking does not give a container access to host
`/dev` nodes.
