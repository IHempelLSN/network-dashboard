from fastapi import FastAPI

app = FastAPI(title='snmp-poller')


@app.get('/health')
def health() -> dict[str, str]:
    return {'status': 'ok', 'service': 'snmp-poller'}
