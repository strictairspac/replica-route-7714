from sqlalchemy import select, func
from src.config.database import Session, Base, engine
from src.models.entities import User, Node, RequestSet, Trial, SeatSwapTrial
from src.utils.security import password_hash
from src.services.swap import simulate_swap

TICKETS = [
    {'id': 1, 'station': '北站', 'seat': 'G101 07车12A', 'label': '值班员林舟 · 北站取票'},
    {'id': 2, 'station': '南站', 'seat': 'D202 03车05F', 'label': '值班员陈岚 · 南站取票'},
    {'id': 3, 'station': '北站', 'seat': 'G101 07车18C', 'label': '交接备票 · 北站'},
    {'id': 4, 'station': '南站', 'seat': 'D202 03车09D', 'label': '交接备票 · 南站'},
]

SWAP_CASES = [
    ('换座-并发改签', 'swap-20261121-A', [
        {'at_ms': 0, 'kind': 'swap', 'swap_key': 'swap-20261121-A', 'tickets': [1, 2]},
        {'at_ms': 5, 'kind': 'query', 'station': '南站', 'tickets': [1, 2]},
        {'at_ms': 10, 'kind': 'reissue', 'ticket': 3, 'to_seat': 'G101 07车19C', 'station': '北站'},
        {'at_ms': 30, 'kind': 'query', 'station': '北站', 'tickets': [1, 2, 3]},
        {'at_ms': 200, 'kind': 'retry', 'swap_key': 'swap-20261121-A', 'tickets': [1, 2]},
    ]),
    ('换座-决定后断联再恢复', 'swap-20261121-B', [
        {'at_ms': 0, 'kind': 'swap', 'swap_key': 'swap-20261121-B', 'tickets': [1, 2]},
        {'at_ms': 61, 'kind': 'link_down'},
        {'at_ms': 120, 'kind': 'query', 'station': '南站', 'tickets': [1, 2]},
        {'at_ms': 180, 'kind': 'reissue', 'ticket': 3, 'to_seat': 'G101 07车19C', 'station': '北站'},
        {'at_ms': 300, 'kind': 'reissue', 'ticket': 4, 'to_seat': 'D202 03车10D', 'station': '南站'},
        {'at_ms': 900, 'kind': 'link_up'},
        {'at_ms': 950, 'kind': 'retry', 'swap_key': 'swap-20261121-B', 'tickets': [1, 2]},
    ]),
    ('换座-中断恢复重试', 'swap-20261121-C', [
        {'at_ms': 0, 'kind': 'swap', 'swap_key': 'swap-20261121-C', 'tickets': [1, 2]},
        {'at_ms': 61, 'kind': 'crash', 'station': '北站'},
        {'at_ms': 300, 'kind': 'query', 'station': '南站', 'tickets': [1, 2]},
        {'at_ms': 500, 'kind': 'retry', 'swap_key': 'swap-20261121-C', 'tickets': [1, 2]},
        {'at_ms': 700, 'kind': 'retry', 'swap_key': 'swap-20261121-C', 'tickets': [1, 2]},
        {'at_ms': 800, 'kind': 'crash', 'station': '南站'},
        {'at_ms': 900, 'kind': 'retry', 'swap_key': 'swap-20261121-C', 'tickets': [1, 2]},
        {'at_ms': 950, 'kind': 'query', 'station': '北站', 'tickets': [1, 2]},
    ]),
    ('换座-断联未恢复', 'swap-20261121-D', [
        {'at_ms': 0, 'kind': 'swap', 'swap_key': 'swap-20261121-D', 'tickets': [1, 2]},
        {'at_ms': 61, 'kind': 'link_down'},
        {'at_ms': 90, 'kind': 'query', 'station': '南站', 'tickets': [1, 2]},
        {'at_ms': 200, 'kind': 'reissue', 'ticket': 3, 'to_seat': 'G101 07车19C', 'station': '北站'},
        {'at_ms': 500, 'kind': 'retry', 'swap_key': 'swap-20261121-D', 'tickets': [1, 2]},
        {'at_ms': 800, 'kind': 'query', 'station': '北站', 'tickets': [1, 2]},
    ]),
]


def seed():
    Base.metadata.create_all(engine)
    with Session() as db:
        for role in ('admin', 'operator', 'viewer'):
            email = role + '@qiyuan.test'
            if not db.scalar(select(User).where(User.email == email)):
                db.add(User(email=email, password=password_hash('Demo2026!'), role=role))
        for name, role, pos, delay, note in [('harbor-primary', 'primary', 105, 0, '栖原主站 · 写入航道'), ('north-replica', 'replica', 104, 150, '北站副本 · 延迟偏高'), ('south-replica', 'replica', 105, 20, '南站副本 · 常用读航道')]:
            if not db.scalar(select(Node).where(Node.name == name)):
                db.add(Node(name=name, role=role, status='healthy', lineage='qiyuan-log-01', position=pos, delay_ms=delay, note=note))
        mixed = [{'at_ms': 0, 'kind': 'write', 'session': 'lab-A'}, {'at_ms': 100, 'kind': 'read', 'session': 'lab-A'}, {'at_ms': 600, 'kind': 'replicate', 'node': 'south-replica', 'position': 106, 'delay_ms': 20}, {'at_ms': 700, 'kind': 'read', 'session': 'lab-A'}, {'at_ms': 800, 'kind': 'node_down', 'node': 'south-replica'}, {'at_ms': 900, 'kind': 'read', 'session': 'lab-A'}, {'at_ms': 1000, 'kind': 'node_down', 'node': 'harbor-primary'}, {'at_ms': 1100, 'kind': 'read', 'session': 'lab-A'}]
        cases = [('QY-1117-A', '交接班写后读与双节点故障', mixed), ('QY-1117-B', '延迟超标回退主库', [{'at_ms': 0, 'kind': 'node_down', 'node': 'south-replica'}, {'at_ms': 10, 'kind': 'read', 'session': 'visitor'}]), ('QY-1117-C', '双副本不可读时拒绝', [{'at_ms': 0, 'kind': 'write', 'session': 'lab-B'}, {'at_ms': 1000, 'kind': 'node_down', 'node': 'harbor-primary'}, {'at_ms': 1100, 'kind': 'read', 'session': 'lab-B'}])]
        for code, title, events in cases:
            if not db.scalar(select(RequestSet).where(RequestSet.code == code)):
                db.add(RequestSet(code=code, title=title, events=events, note='栖原站点值班交接 · 09:15 请求轨迹'))
        for name, key, events in SWAP_CASES:
            if db.scalar(select(SeatSwapTrial).where(SeatSwapTrial.swap_key == key,
                                                     SeatSwapTrial.name == name)):
                continue
            result = simulate_swap(TICKETS, events)
            states = {m['state'] for m in result['moves']}
            state = 'confirmed' if 'confirmed' in states else ('aborted' if 'aborted' in states else 'pending')
            db.add(SeatSwapTrial(name=name, swap_key=key, state=state, status='completed',
                                 input_snapshot={'tickets': TICKETS, 'events': events,
                                                 'policy': result['policy'], 'swap_key': key},
                                 result=result))
        db.commit()
        print({cls.__tablename__: db.scalar(select(func.count()).select_from(cls))
               for cls in (User, Node, RequestSet, Trial, SeatSwapTrial)})


if __name__ == '__main__':
    seed()
