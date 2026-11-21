from fastapi import APIRouter, Depends
from fastapi.responses import Response
import json
from src.config.database import db_session
from src.utils.security import current, writer
from src.schemas.ticket_swap import SeatSwapTrialInput
from src.services import ticket_swap as service

router = APIRouter()


@router.get('/swap_trials')
def listing(db=Depends(db_session), user=Depends(current)):
    return service.listing(db)


@router.post('/swap_trials', status_code=201)
def create(data: SeatSwapTrialInput, db=Depends(db_session), user=Depends(writer)):
    return service.create(db, data, user)


@router.get('/swap_trials/{ident}')
def detail(ident: int, db=Depends(db_session), user=Depends(current)):
    return service.detail(db, ident)


@router.get('/swap_trials/{ident}/export')
def export(ident: int, db=Depends(db_session), user=Depends(current)):
    return Response(json.dumps(service.detail(db, ident), ensure_ascii=False, indent=2),
                    media_type='application/json',
                    headers={'Content-Disposition': f'attachment; filename="swap-{ident}.json"'})
