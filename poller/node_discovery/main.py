from fastapi import FastAPI

app = FastAPI(title='node-discovery')


@app.get('/health')
def health() -> dict[str, str]:
    return {'status': 'ok', 'service': 'node-discovery'}
