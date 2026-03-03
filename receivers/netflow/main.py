from fastapi import FastAPI

app = FastAPI(title='netflow-receiver')


@app.get('/health')
def health() -> dict[str, str]:
    return {'status': 'ok', 'service': 'netflow-receiver'}
