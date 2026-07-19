# Keep wire values canonical

The envelope retains raw bytes and raw-tick JSON payloads as its canonical lossless representation, then adds PGN catalog identity, decode status, ingress provenance, stable Device NAME, and unit-scaled physical values. This keeps replay and filters backward compatible while giving operators and integrations ergonomic engineering values without destructive conversion.
