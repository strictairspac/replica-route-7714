import pytest
from fastapi.testclient import TestClient
from src.main import app
from src.config.database import Base, engine
from scripts.seed_demo import seed

TICKETS = [
    {'id': 1, 'station': '北站', 'seat': 'G101 07车12A', 'label': '林舟'},
    {'id': 2, 'station': '南站', 'seat': 'D202 03车05F', 'label': '陈岚'},
    {'id': 3, 'station': '北站', 'seat': 'G101 07车18C', 'label': '备票'},
]


@pytest.fixture(scope='module')
def client():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    seed()
    with TestClient(app) as c:
        yield c


def token(client, email='operator@qiyuan.test'):
    r = client.post('/auth/login', json={'email': email, 'password': 'Demo2026!'})
    assert r.status_code == 200, r.text
    return {'Authorization': 'Bearer ' + r.json()['token']}


def payload(events=None, **kw):
    body = {
        'name': '换座演练',
        'swap_key': 'swap-test-1',
        'tickets': TICKETS,
        'events': events or [{'at_ms': 0, 'kind': 'swap', 'swap_key': 'swap-test-1', 'tickets': [1, 2]}],
    }
    body.update(kw)
    return body


def test_swap_trial_create_list_detail_export(client):
    headers = token(client)
    r = client.post('/swap_trials', json=payload(), headers=headers)
    assert r.status_code == 201, r.text
    created = r.json()
    assert created['state'] == 'confirmed'
    assert created['result']['summary']['confirmed'] == 1
    assert created['result']['checksum']

    listing = client.get('/swap_trials', headers=headers).json()
    assert any(x['id'] == created['id'] for x in listing)

    detail = client.get(f"/swap_trials/{created['id']}", headers=headers).json()
    assert detail['result']['tickets'][0]['seat_after'] == 'D202 03车05F'

    export = client.get(f"/swap_trials/{created['id']}/export", headers=headers)
    assert export.status_code == 200
    assert 'swap-' in export.headers['content-disposition']


def test_seeded_swap_cases_present(client):
    headers = token(client)
    rows = client.get('/swap_trials', headers=headers).json()
    names = {x['name'] for x in rows}
    assert {'换座-并发改签', '换座-决定后断联再恢复', '换座-中断恢复重试', '换座-断联未恢复'} <= names


def test_viewer_cannot_create(client):
    headers = token(client, 'viewer@qiyuan.test')
    assert client.post('/swap_trials', json=payload(), headers=headers).status_code == 403


def test_invalid_swap_rejected_but_other_work_continues(client):
    headers = token(client)
    bad = payload(events=[{'at_ms': 0, 'kind': 'swap', 'swap_key': 'x', 'tickets': [1, 3]}])
    assert client.post('/swap_trials', json=bad, headers=headers).status_code == 422
    bad_time = payload(events=[{'at_ms': 10, 'kind': 'link_up'}, {'at_ms': 1, 'kind': 'link_down'}])
    assert client.post('/swap_trials', json=bad_time, headers=headers).status_code == 422
    # existing routing trial flow still works untouched
    sets = client.get('/request_sets', headers=headers).json()
    assert sets


def test_unauthorized_swap_listing(client):
    assert client.get('/swap_trials').status_code in (401, 403)
