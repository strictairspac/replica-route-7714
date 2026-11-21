from sqlalchemy import select
from fastapi import HTTPException
from src.models.entities import SeatSwapTrial
from src.utils.serialization import record
from .swap import simulate_swap, SwapError
from .common import save


def _payload(data):
    tickets = [t.model_dump() for t in data.tickets]
    events = [e.model_dump(exclude_none=True) for e in data.events]
    policy = data.policy.model_dump()
    return tickets, events, policy


def listing(db):
    return [record(x) for x in db.scalars(select(SeatSwapTrial).order_by(SeatSwapTrial.id.desc()))]


def detail(db, ident):
    obj = db.get(SeatSwapTrial, ident)
    if not obj:
        raise HTTPException(404, '换座演练不存在')
    return record(obj)


def _state_of(result):
    moves = result.get('moves') or []
    if any(m['state'] == 'confirmed' for m in moves):
        return 'confirmed'
    if any(m['state'] == 'aborted' for m in moves):
        return 'aborted'
    if any(m['state'] == 'interrupted' for m in moves):
        return 'pending'
    return 'pending'


def create(db, data, user):
    tickets, events, policy = _payload(data)
    try:
        result = simulate_swap(tickets, events, policy)
    except SwapError as e:
        raise HTTPException(422, str(e))
    snapshot = {'tickets': tickets, 'events': events, 'policy': policy, 'swap_key': data.swap_key}
    obj = SeatSwapTrial(name=data.name, swap_key=data.swap_key, state=_state_of(result),
                        status='completed', input_snapshot=snapshot, result=result)
    return save(db, obj, user, '换座演练 ' + data.name)
