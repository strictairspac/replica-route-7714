from typing import Literal
from pydantic import BaseModel, Field, model_validator


class TicketInput(BaseModel):
    id: int = Field(gt=0)
    station: str = Field(min_length=1, max_length=40)
    seat: str = Field(min_length=1, max_length=40)
    label: str = Field(default='', max_length=120)


class SwapEvent(BaseModel):
    at_ms: int = Field(ge=0, le=86400000)
    kind: Literal['swap', 'retry', 'reissue', 'query', 'link_down', 'link_up', 'crash', 'torn_write']
    swap_key: str | None = Field(default=None, max_length=120)
    tickets: list[int] | None = Field(default=None, max_length=12)
    ticket: int | None = Field(default=None, gt=0)
    to_seat: str | None = Field(default=None, max_length=40)
    station: str | None = Field(default=None, max_length=40)

    @model_validator(mode='after')
    def valid_event(self):
        if self.kind in ('swap', 'retry') and (not self.swap_key or not self.tickets or len(self.tickets) != 2):
            raise ValueError('换座事件需要swap_key和两张车票')
        if self.kind == 'reissue' and (self.ticket is None or not self.to_seat):
            raise ValueError('改签事件需要ticket和to_seat')
        if self.kind == 'query' and not self.station:
            raise ValueError('查票事件需要station')
        if self.kind == 'crash' and not self.station:
            raise ValueError('中断事件需要station')
        if self.kind == 'torn_write' and (self.ticket is None or not self.to_seat):
            raise ValueError('torn_write需要ticket和to_seat')
        return self


class SwapPolicy(BaseModel):
    lock_ms: int = Field(default=40, ge=0, le=3600000)
    prepare_ms: int = Field(default=20, ge=0, le=3600000)
    confirm_ms: int = Field(default=60, ge=0, le=3600000)
    abort_after_ms: int = Field(default=400, ge=0, le=3600000)

    @model_validator(mode='after')
    def enough_time(self):
        if self.abort_after_ms < self.confirm_ms:
            raise ValueError('确认超时不得短于凭据传递时间')
        return self


class SeatSwapTrialInput(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    swap_key: str = Field(min_length=1, max_length=120)
    tickets: list[TicketInput] = Field(min_length=2, max_length=12)
    events: list[SwapEvent] = Field(min_length=1, max_length=100)
    policy: SwapPolicy = Field(default_factory=SwapPolicy)

    @model_validator(mode='after')
    def chronological(self):
        if any(a.at_ms > b.at_ms for a, b in zip(self.events, self.events[1:])):
            raise ValueError('事件必须按at_ms非递减排列')
        if len({t.id for t in self.tickets}) != len(self.tickets):
            raise ValueError('车票编号必须唯一')
        stations = {t.station for t in self.tickets}
        if len(stations) < 2:
            raise ValueError('换座需要至少两个站，且两票分属不同站')
        return self
