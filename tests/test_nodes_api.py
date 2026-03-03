from fastapi.testclient import TestClient

from web.backend.main import app


client = TestClient(app)


def test_health_endpoint() -> None:
    response = client.get('/api/health')
    assert response.status_code == 200


def test_dashboard_stats_endpoint() -> None:
    response = client.get('/api/stats')
    assert response.status_code == 200
    assert 'network_health_pct' in response.json()


def test_topology_endpoint_contains_nodes_links_and_labels() -> None:
    response = client.get('/api/topology')
    assert response.status_code == 200
    payload = response.json()
    assert 'nodes' in payload and 'links' in payload and 'class_labels' in payload


def test_link_history_windows() -> None:
    response = client.get('/api/links/lnk-1/history', params={'window': '6h'})
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_lab_alerts_suppressed_by_default() -> None:
    default_rows = client.get('/api/alerts').json()
    assert all('lab' not in row['hostname'] for row in default_rows)

    all_rows = client.get('/api/alerts', params={'include_lab': 'true'}).json()
    assert any('lab' in row['hostname'] for row in all_rows)


def test_alert_clear_and_retrigger_flow() -> None:
    login = client.post('/api/auth/login', json={'username': 'admin', 'password': 'admin'})
    token = login.json()['access_token']

    alerts = client.get('/api/alerts').json()
    target = alerts[0]

    cleared = client.post(
        f"/api/alerts/{target['alert_id']}/clear",
        headers={'Authorization': f'Bearer {token}'},
    )
    assert cleared.status_code == 200
    assert cleared.json()['status'] == 'cleared'

    retrigger = client.post(
        '/api/traps/ingest',
        json={
            'node_id': target['node_id'],
            'hostname': target['hostname'],
            'interface_name': target['interface_name'],
            'trap_oid': target['trap_oid'],
            'trap_description': target['trap_description'],
            'severity': target['severity'],
            'raw_payload': target['raw_payload'],
        },
    )
    assert retrigger.status_code == 200
    assert retrigger.json()['status'] == 'open'
