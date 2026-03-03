# Poller Services (Scaffold)

- `snmp-poller`: scheduled outbound SNMP GET/GETBULK/WALK.
- `node-discovery`: subnet scans and device onboarding events.

Future iterations should publish via shared Kafka producer wrapper and enforce concurrency controls.
