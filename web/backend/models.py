from __future__ import annotations

from pydantic import BaseModel


class Node(BaseModel):
    node_id: str
    hostname: str
    ip_address: str
    latitude: float
    longitude: float
    environment: str
    node_class: str
    node_type: str


class InterfaceStat(BaseModel):
    node_id: str
    hostname: str
    interface_name: str
    description: str
    lag_label: str | None = None
    speed_gbps: float
    admin_up: bool
    oper_up: bool
    in_util_pct: float
    out_util_pct: float
    error_count: int


class LinkHistoryPoint(BaseModel):
    ts: str
    tx_util_pct: float
    rx_util_pct: float


class Link(BaseModel):
    link_id: str
    a_node_id: str
    z_node_id: str
    lag_label: str
    capacity_gbps: float
    tx_util_pct: float
    rx_util_pct: float


class TopologyResponse(BaseModel):
    nodes: list[Node]
    links: list[Link]
    class_labels: dict[str, str]


class DashboardStats(BaseModel):
    network_health_pct: float
    active_nodes: int
    total_traffic_gb_last_hour: float
    monitored_nodes: int
    interfaces_total: int
    alerts_open: int


class ProgramUtilization(BaseModel):
    total_capacity_gbps: float
    total_tx_gbps: float
    total_rx_gbps: float
    avg_tx_util_pct: float
    avg_rx_util_pct: float
    accounting_method: str


class AlertEvent(BaseModel):
    alert_id: int
    severity: str
    node_id: str
    hostname: str
    interface_name: str
    trap_oid: str
    trap_description: str
    status: str
    acknowledged_by: str | None = None
    cleared_by: str | None = None
    last_triggered: str
    raw_payload: str


class HealthResponse(BaseModel):
    status: str
    service: str


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str


class OIDCConfigResponse(BaseModel):
    oidc_enabled: bool
    issuer_url: str
    client_id: str
