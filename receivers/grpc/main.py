from fastapi import FastAPI

app = FastAPI(title='grpc-receiver')


@app.get('/health')
def health() -> dict[str, str]:
    return {'status': 'ok', 'service': 'grpc-receiver'}
