# Separate semantic and transparent bridges

Semantic bridge mode decodes and re-originates messages through Beacon's claimed N2K identity, while transparent bridge mode writes reconstructed CAN frames with the original priority, PGN, source, destination, and payload. Transparent forwarding is initially limited to SocketCAN sinks, rejects same-interface loops, and excludes network-management PGNs unless explicitly enabled because claiming and transport-control traffic cannot safely be copied between arbitrary segments by default.
