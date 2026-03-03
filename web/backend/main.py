from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from web.backend.auth import OIDC_CLIENT_ID, OIDC_ENABLED, OIDC_ISSUER_URL, current_user, issue_dev_token
from web.backend.models import (
    AlertEvent,
    DashboardStats,
    HealthResponse,
    InterfaceStat,
    LinkHistoryPoint,
    LoginRequest,
    LoginResponse,
    Node,
    OIDCConfigResponse,
    ProgramUtilization,
    TopologyResponse,
)
from web.backend.services import (
    CLASS_LABELS,
    current_links,
    dashboard_stats,
    filter_nodes,
    init_db,
    interfaces_for_node,
    link_history,
    list_alerts,
    load_nodes,
    program_utilization,
    update_alert,
    ingest_trap,
)

@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(title="NMP Dashboard API", version="0.4.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/assets", StaticFiles(directory="web/frontend"), name="assets")



@app.get("/api/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok", service="web-backend")


@app.post("/api/auth/login", response_model=LoginResponse)
def login(payload: LoginRequest) -> LoginResponse:
    token = issue_dev_token(payload.username, payload.password)
    return LoginResponse(access_token=token, role="admin")


@app.get("/api/auth/oidc/config", response_model=OIDCConfigResponse)
def oidc_config() -> OIDCConfigResponse:
    return OIDCConfigResponse(oidc_enabled=OIDC_ENABLED, issuer_url=OIDC_ISSUER_URL, client_id=OIDC_CLIENT_ID)


@app.get("/api/stats", response_model=DashboardStats)
def stats() -> DashboardStats:
    nodes = load_nodes()
    links = current_links(nodes)
    return dashboard_stats(nodes, links)


@app.get("/api/nodes", response_model=list[Node])
def nodes(
    node_class: str | None = Query(None),
    node_type: str | None = Query(None),
    hostname: str | None = Query(None),
) -> list[Node]:
    return filter_nodes(load_nodes(), node_class, node_type, hostname)


@app.get("/api/topology", response_model=TopologyResponse)
def topology(
    node_class: str | None = Query(None),
    node_type: str | None = Query(None),
    hostname: str | None = Query(None),
) -> TopologyResponse:
    nodes = filter_nodes(load_nodes(), node_class, node_type, hostname)
    return TopologyResponse(nodes=nodes, links=current_links(nodes), class_labels=CLASS_LABELS)


@app.get("/api/nodes/{node_id}/interfaces", response_model=list[InterfaceStat])
def node_interfaces(node_id: str) -> list[InterfaceStat]:
    nodes = load_nodes()
    match = next((n for n in nodes if n.node_id == node_id), None)
    if not match:
        raise HTTPException(status_code=404, detail="Node not found")
    return interfaces_for_node(match, current_links(nodes))


@app.get("/api/links/{link_id}/history", response_model=list[LinkHistoryPoint])
def link_history_api(link_id: str, window: str = Query("1h")) -> list[LinkHistoryPoint]:
    return link_history(link_id, window)


@app.get("/api/utilization/program", response_model=ProgramUtilization)
def util_program() -> ProgramUtilization:
    return program_utilization(current_links(load_nodes()))


@app.get("/api/alerts", response_model=list[AlertEvent])
def alerts(
    severity: str | None = Query(None),
    status: str | None = Query(None),
    include_lab: bool = Query(False),
) -> list[AlertEvent]:
    return list_alerts(severity, status, include_lab=include_lab)


@app.post("/api/alerts/{alert_id}/ack", response_model=AlertEvent)
def ack_alert(alert_id: int, user=Depends(current_user)) -> AlertEvent:
    alert = update_alert(alert_id, "ack", user.username)
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    return alert


@app.post("/api/alerts/{alert_id}/clear", response_model=AlertEvent)
def clear_alert(alert_id: int, user=Depends(current_user)) -> AlertEvent:
    alert = update_alert(alert_id, "clear", user.username)
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    return alert




@app.post("/api/traps/ingest", response_model=AlertEvent)
def traps_ingest(payload: dict[str, str]) -> AlertEvent:
    return ingest_trap(
        node_id=payload.get('node_id', ''),
        hostname=payload.get('hostname', ''),
        interface_name=payload.get('interface_name', ''),
        trap_oid=payload.get('trap_oid', ''),
        trap_description=payload.get('trap_description', ''),
        severity=payload.get('severity', 'warning'),
        raw_payload=payload.get('raw_payload', '{}'),
    )

@app.get("/")
def index() -> FileResponse:
    return FileResponse("web/frontend/index.html")
