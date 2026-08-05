# Make connector delivery boundaries explicit

Beacon reports pending delivery relative to each connector checkpoint and reports retained replay history separately. Confirmed sinks advance after a successful write or intentional acceptance by a null sink, resumable sinks advance after the message enters their replayable stream, and best-effort sinks advance after dispatch while recording that downstream receipt is not known; this preserves useful fan-out behavior without labeling dispatch as confirmed delivery.
