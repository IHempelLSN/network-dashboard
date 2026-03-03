# Receivers (Scaffold)

This folder contains protocol-specific receiver services that should remain "dumb": receive, stamp envelope, and forward.

## Planned services
- `syslog-receiver`
- `snmp-receiver`
- `netflow-receiver`
- `grpc-receiver`

Current scaffold uses `receivers/base.py` and `shared/envelope.py` to standardize message construction.
