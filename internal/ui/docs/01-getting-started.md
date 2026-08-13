# Getting started

This example reads NMEA 2000 messages from one CAN bus and publishes them at
an SSE endpoint.

```text
SocketCAN can0 --> connector --> SSE /events
```

## Before you start

Make sure that these items are ready:

- Beacon is installed.
- The SocketCAN interface is named `can0`.
- The interface is up at 250 kbit/s.

Use the CAN setup page if you must prepare the interface.

## Start Beacon

Run Beacon with the default admin and data ports:

```bash
beacon --db beacon.db
```

Open `http://localhost:2112` in a browser.

## Add the CAN source

1. Open **Sources**.
2. Select **Add source**.
3. Set **Name** to `CAN bus`.
4. Set **Type** to `socketcan`.
5. Set **Interface** to `can0`.
6. Enable the source.
7. Save the source.

## Add the SSE sink

1. Open **Sinks**.
2. Select **Add sink**.
3. Set **Name** to `SSE feed`.
4. Set **Type** to `http_sse`.
5. Set **Path** to `/events`.
6. Enable the sink.
7. Save the sink.

## Connect the source to the sink

1. Open **Connectors**.
2. Select **Add connector**.
3. Set **Name** to `CAN to SSE`.
4. Select the CAN source.
5. Select the SSE sink.
6. Keep the filter list empty.
7. Enable the connector.
8. Save the connector.

## Read the SSE stream

Connect to the data port:

```bash
curl -N http://localhost:8080/events
```

Beacon writes one SSE event for each NMEA 2000 message that arrives on
`can0`. The dashboard shows the live message rate and route state.

Use the Sources, Sinks, and Connectors pages for component settings. Use the
Examples page to create the same route with the API or a virtual CAN bus.
