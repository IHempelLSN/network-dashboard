from fastapi import FastAPI

app = FastAPI(title='snmp-receiver')


@app.get('/health')
def health() -> dict[str, str]:
    return {'status': 'ok', 'service': 'snmp-receiver'}
