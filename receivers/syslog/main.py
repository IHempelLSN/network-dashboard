from fastapi import FastAPI

app = FastAPI(title='syslog-receiver')


@app.get('/health')
def health() -> dict[str, str]:
    return {'status': 'ok', 'service': 'syslog-receiver'}
