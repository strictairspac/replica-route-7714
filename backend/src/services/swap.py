"""Deterministic two-station seat-swap rehearsal.

Model, not a live distributed cluster. Two stations each hold one ticket. The coordinator is the station
holding the first of the two tickets. It locks both tickets, records a decision, then waits for the
second station to hold that decision receipt; only then do the two seats change, together, in one step.

Receipt rule (ours, fixed here and surfaced in every result):
  * the deciding station writes a ``decision`` receipt at the instant the decision is recorded;
  * the other station writes its own ``decision`` receipt when the link carries it across;
  * a swap is ``confirmed`` only once both stations hold that receipt, and both seats then change atomically;
  * if the link is lost after the decision is recorded, confirmation waits ``abort_after_ms`` and then
    aborts with both seats untouched. A half swap is never written, so a query can never observe one.
  * all waits are simulated milliseconds, so the same input always yields the same evidence.

Locks are per ticket: reissuing other tickets runs concurrently and keeps working while the two stations
are disconnected. A confirmed swap is idempotent: retrying with the same ``swap_key`` returns the already
confirmed ownership and never swaps the seats a second time.
"""
import hashlib
import heapq
import json


class SwapError(ValueError):
    pass


DEFAULT_POLICY = {
    'lock_ms': 40,          # time to acquire both ticket locks
    'prepare_ms': 20,       # time from both locks to the recorded decision
    'confirm_ms': 60,       # time for the decision receipt to reach the other station
    'abort_after_ms': 400,  # how long a recorded decision may wait for the other station
}

IN_FLIGHT = ('pending', 'locked', 'decision')

CHECK_LABELS = {
    'two_seats_swap_together': '两票只在同一笔成交中一起换座',
    'no_half_swap_visible': '查票从不出现半笔交换',
    'reissue_concurrent_with_swap': '换座进行中，其余车票可并发改签',
    'reissue_during_outage': '两站断联时，其余车票照常改签',
    'retry_after_confirmation_deduplicated': '成交后重试同一换座不重复生效',
    'no_double_apply': '同一 swap_key 至多成交一次',
    'confirmed_ownership_preserved': '中断恢复后已确认的归属不丢',
}


def _checksum(result):
    clean = {k: v for k, v in result.items() if k != 'checksum'}
    return hashlib.sha256(json.dumps(clean, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def _validate(tickets, events, policy):
    if not 2 <= len(tickets) <= 12:
        raise SwapError('需要2–12张车票')
    ids = [t['id'] for t in tickets]
    if len(set(ids)) != len(ids):
        raise SwapError('车票编号必须唯一')
    if any(not t.get('seat') or not t.get('station') for t in tickets):
        raise SwapError('每张车票需要seat与station')
    if len({t['station'] for t in tickets}) < 2:
        raise SwapError('换座需要至少两个站，且两票分属不同站')
    if not 1 <= len(events) <= 100:
        raise SwapError('需要1–100个时序事件')
    for key in ('lock_ms', 'prepare_ms', 'confirm_ms', 'abort_after_ms'):
        if key not in policy or int(policy[key]) < 0:
            raise SwapError('时序参数不得为负')
    if int(policy['abort_after_ms']) < int(policy['confirm_ms']):
        raise SwapError('确认超时必须不短于凭据传递时间')
    last = -1
    by_id = {t['id']: t for t in tickets}
    for e in events:
        if e['at_ms'] < last:
            raise SwapError('事件必须按时间非递减排列')
        last = e['at_ms']
        kind = e['kind']
        if kind in ('swap', 'retry'):
            if not e.get('swap_key'):
                raise SwapError('换座事件需要swap_key')
            pair = e.get('tickets') or []
            if len(pair) != 2 or pair[0] == pair[1]:
                raise SwapError('换座必须指定两张不同的车票')
            if any(x not in by_id for x in pair):
                raise SwapError('换座引用未知车票')
            if by_id[pair[0]]['station'] == by_id[pair[1]]['station']:
                raise SwapError('两张换座车票必须分属两个站')
        elif kind == 'reissue':
            if not e.get('ticket') or e['ticket'] not in by_id:
                raise SwapError('改签引用未知车票')
            if not e.get('to_seat'):
                raise SwapError('改签需要to_seat')
        elif kind == 'query':
            if not e.get('station'):
                raise SwapError('查票需要station')
            for x in e.get('tickets') or []:
                if x not in by_id:
                    raise SwapError('查票引用未知车票')
        elif kind == 'crash':
            if not e.get('station'):
                raise SwapError('中断事件需要station')
        elif kind == 'torn_write':
            if not e.get('ticket') or e['ticket'] not in by_id or not e.get('to_seat'):
                raise SwapError('torn_write需要ticket与to_seat')
        elif kind in ('link_down', 'link_up'):
            pass
        else:
            raise SwapError('未知事件类型')


def simulate_swap(tickets, events, policy=None):
    policy = {**DEFAULT_POLICY, **(policy or {})}
    policy = {k: int(v) for k, v in policy.items()}
    _validate(tickets, events, policy)

    seats = {t['id']: t['seat'] for t in tickets}
    original = dict(seats)
    holder = {t['id']: t['station'] for t in tickets}
    locked = set()
    link_up = True
    moves = {}
    trace = []
    queries = []
    reissues = []
    commit_log = []
    torn = []
    seq = 0
    external = [(e['at_ms'], 0, i, e) for i, e in enumerate(events)]
    internal = []

    def log(now, kind, detail):
        trace.append({'at_ms': now, 'kind': kind, 'detail': detail})

    def schedule(now, kind, payload, delay):
        nonlocal seq
        heapq.heappush(internal, (now + delay, 1, seq, kind, payload))
        seq += 1

    def move_view(move):
        return {
            'key': move['key'], 'tickets': list(move['tickets']), 'state': move['state'],
            'requested_at_ms': move['requested_at'], 'locked_at_ms': move['locked_at'],
            'decided_at_ms': move['decided_at'], 'confirmed_at_ms': move['confirmed_at'],
            'aborted_at_ms': move['aborted_at'], 'reason': move['reason'],
            'lock_wait_ms': move['lock_wait_ms'], 'confirm_wait_ms': move['confirm_wait_ms'],
            'before': dict(move['before']) if move['before'] else {},
            'receipts': [dict(r) for r in move['receipts']],
            'attempt': move['attempt'], 'attempts': [dict(a, receipts=[dict(r) for r in a['receipts']])
                                                     for a in move['attempts']],
        }

    def close_attempt(move, state, now, reason):
        attempt = move['attempt_rec']
        attempt['state'] = state
        attempt['ended_at_ms'] = now
        attempt['reason'] = reason
        attempt['lock_wait_ms'] = move['lock_wait_ms']
        attempt['confirm_wait_ms'] = move['confirm_wait_ms']
        attempt['receipts'] = [dict(r) for r in move['receipts']]
        move['attempts'].append(attempt)
        move['attempt_rec'] = None

    def abort(move, now, reason):
        move['state'] = 'aborted'
        move['aborted_at'] = now
        move['reason'] = reason
        if move['decided_at'] is not None:
            move['confirm_wait_ms'] = now - move['decided_at']
        locked.difference_update(move['tickets'])
        if move['attempt_rec']:
            close_attempt(move, 'aborted', now, reason)
        log(now, 'swap_aborted', {'swap_key': move['key'], 'attempt': move['attempt'],
                                  'reason': reason,
                                  'seats': {x: seats[x] for x in move['tickets']},
                                  'lock_wait_ms': move['lock_wait_ms'],
                                  'confirm_wait_ms': move['confirm_wait_ms']})

    def new_attempt(move, now):
        move.update({'state': 'pending', 'requested_at': now, 'locked_at': None, 'decided_at': None,
                     'confirmed_at': None, 'aborted_at': None, 'reason': '', 'receipts': [],
                     'lock_wait_ms': None, 'confirm_wait_ms': None, 'before': None,
                     'attempt': move['attempt'] + 1})
        move['attempt_rec'] = {'attempt': move['attempt'], 'requested_at_ms': now,
                               'state': 'pending', 'decide_station': holder[move['tickets'][0]],
                               'receipts': []}
        log(now, 'swap_pending', {'swap_key': move['key'], 'tickets': list(move['tickets']),
                                  'attempt': move['attempt']})
        schedule(now, 'lock', {'key': move['key'], 'attempt': move['attempt']}, policy['lock_ms'])

    def handle_request(event, now):
        key = event['swap_key']
        move = moves.get(key)
        if move and set(event['tickets']) != set(move['tickets']):
            log(now, 'retry_rejected', {'swap_key': key,
                                        'reason': '同一 swap_key 不能更换车票，已拒绝'})
            return
        if move and move['state'] == 'confirmed':
            log(now, 'retry_deduplicated', {
                'swap_key': key, 'attempt': move['attempt'],
                'seats': {x: seats[x] for x in move['tickets']},
                'reason': '同一 swap_key 已确认成交，重试不重复生效'})
            return
        if move and move['state'] in IN_FLIGHT:
            log(now, 'retry_ignored', {'swap_key': key, 'state': move['state'],
                                       'reason': '同一 swap_key 正在处理中，忽略重复请求'})
            return
        if move and move['state'] == 'interrupted':
            log(now, 'recovered', {'swap_key': key, 'attempt': move['attempt'],
                                   'durable_receipts': [dict(r) for r in move['receipts']],
                                   'seats': {x: seats[x] for x in move['tickets']},
                                   'action': '恢复：上一笔只有决定凭据、未获两站确认，按撤销处理，两票保持原座，再重试同一笔'})
            abort(move, now, '中断恢复：上一笔决定未获两站确认，按撤销处理，两票保持原座')
        if not move:
            move = {'key': key, 'tickets': list(event['tickets']), 'state': 'idle', 'attempt': 0,
                    'requested_at': None, 'locked_at': None, 'decided_at': None, 'confirmed_at': None,
                    'aborted_at': None, 'reason': '', 'receipts': [], 'lock_wait_ms': None,
                    'confirm_wait_ms': None, 'before': None, 'attempts': [], 'attempt_rec': None}
            moves[key] = move
        new_attempt(move, now)

    def on_lock(now, key, attempt):
        move = moves.get(key)
        if not move or move['state'] != 'pending' or move['attempt'] != attempt:
            return
        if not link_up:
            move['lock_wait_ms'] = now - move['requested_at']
            abort(move, now, '两站断联，无法同时锁住两张票，换座未开始')
            return
        held = locked.intersection(move['tickets'])
        if held:
            move['lock_wait_ms'] = now - move['requested_at']
            abort(move, now, '车票已被另一笔换座锁住，本次换座撤销：' +
                  '、'.join('#' + str(x) for x in sorted(held)))
            return
        locked.update(move['tickets'])
        move['state'] = 'locked'
        move['locked_at'] = now
        move['lock_wait_ms'] = now - move['requested_at']
        move['before'] = {x: seats[x] for x in move['tickets']}
        log(now, 'both_seats_locked', {'swap_key': key, 'tickets': list(move['tickets']),
                                       'lock_wait_ms': move['lock_wait_ms']})
        schedule(now, 'decide', {'key': key, 'attempt': attempt}, policy['prepare_ms'])

    def on_decide(now, key, attempt):
        move = moves.get(key)
        if not move or move['state'] != 'locked' or move['attempt'] != attempt:
            return
        decide_station = holder[move['tickets'][0]]
        move['state'] = 'decision'
        move['decided_at'] = now
        move['receipts'].append({'station': decide_station, 'kind': 'decision', 'at_ms': now})
        log(now, 'decision_recorded', {'swap_key': key, 'station': decide_station,
                                       'receipt': 'decision@' + decide_station})
        if link_up:
            schedule(now, 'confirm', {'key': key, 'attempt': attempt}, policy['confirm_ms'])
        else:
            log(now, 'confirmation_waiting', {'swap_key': key, 'station': decide_station,
                                              'until_ms': now + policy['abort_after_ms'],
                                              'reason': '另一站尚未收到决定凭据'})
        schedule(now, 'confirm_deadline', {'key': key, 'attempt': attempt}, policy['abort_after_ms'])

    def confirm(move, now):
        decide_station = holder[move['tickets'][0]]
        other = holder[move['tickets'][1]]
        if not any(r['station'] == other and r['kind'] == 'decision' for r in move['receipts']):
            move['receipts'].append({'station': other, 'kind': 'decision', 'at_ms': now})
        before = {x: seats[x] for x in move['tickets']}
        a, b = move['tickets']
        seats[a], seats[b] = seats[b], seats[a]
        move['receipts'].append({'station': decide_station, 'kind': 'confirmed', 'at_ms': now})
        move['receipts'].append({'station': other, 'kind': 'confirmed', 'at_ms': now})
        move['state'] = 'confirmed'
        move['confirmed_at'] = now
        move['confirm_wait_ms'] = now - move['decided_at'] if move['decided_at'] is not None else None
        locked.difference_update(move['tickets'])
        if move['attempt_rec']:
            close_attempt(move, 'confirmed', now, '两站凭据齐全，两票同时换座')
        commit_log.append({'at_ms': now, 'swap_key': move['key'], 'tickets': [a, b],
                           'before': before, 'after': {a: seats[a], b: seats[b]}})
        log(now, 'swap_confirmed', {'swap_key': move['key'], 'seats': {a: seats[a], b: seats[b]},
                                    'confirm_wait_ms': move['confirm_wait_ms'],
                                    'receipts': 'decision + confirmed @ ' + decide_station + ' & ' + other})

    def on_confirm(now, key, attempt):
        move = moves.get(key)
        if not move or move['state'] != 'decision' or move['attempt'] != attempt or not link_up:
            return
        confirm(move, now)

    def on_deadline(now, key, attempt):
        move = moves.get(key)
        if not move or move['state'] != 'decision' or move['attempt'] != attempt:
            return
        abort(move, now, '决定已记下但另一站未确认成交凭据，等待超时后撤销，两票保持原座')

    def image_problems(view):
        problems = []
        for move in moves.values():
            if move['state'] not in IN_FLIGHT:
                continue
            pair = move['tickets']
            if not set(pair) <= set(view):
                continue
            pre = dict(move['before'] or {})
            if not pre:
                continue
            post = {pair[0]: pre[pair[1]], pair[1]: pre[pair[0]]}
            seen = {x: view[x] for x in pair}
            if seen != pre and seen != post:
                problems.append({'swap_key': move['key'], 'state': move['state'], 'seen': seen,
                                 'pre': pre, 'post': post})
        return problems

    while external or internal:
        if internal and (not external or internal[0][0] < external[0][0]):
            now, _, _, kind, payload = heapq.heappop(internal)
            if kind == 'lock':
                on_lock(now, payload['key'], payload['attempt'])
            elif kind == 'decide':
                on_decide(now, payload['key'], payload['attempt'])
            elif kind == 'confirm':
                on_confirm(now, payload['key'], payload['attempt'])
            elif kind == 'confirm_deadline':
                on_deadline(now, payload['key'], payload['attempt'])
            continue
        now, _, _, event = heapq.heappop(external)
        kind = event['kind']
        if kind in ('swap', 'retry'):
            handle_request(event, now)
        elif kind == 'link_down':
            link_up = False
            log(now, 'link_down', {'reason': '两站断联；本站改签继续，换座无法跨站确认'})
            for move in list(moves.values()):
                if move['state'] == 'pending':
                    move['lock_wait_ms'] = now - (move['requested_at'] if move['requested_at'] is not None else now)
                    abort(move, now, '断联发生在锁票完成前，换座撤销')
                elif move['state'] == 'locked':
                    abort(move, now, '锁票后断联，决定记不下，换座撤销')
                elif move['state'] == 'decision':
                    log(now, 'confirmation_waiting', {
                        'swap_key': move['key'],
                        'until_ms': (move['decided_at'] or now) + policy['abort_after_ms'],
                        'reason': '决定刚记下即断联，等待另一站确认凭据'})
        elif kind == 'link_up':
            link_up = True
            log(now, 'link_up', {'reason': '两站恢复联络'})
            for move in list(moves.values()):
                if move['state'] == 'decision':
                    deadline = (move['decided_at'] or now) + policy['abort_after_ms']
                    if now + policy['confirm_ms'] <= deadline:
                        schedule(now, 'confirm', {'key': move['key'], 'attempt': move['attempt']},
                                 policy['confirm_ms'])
                    else:
                        log(now, 'confirmation_too_late', {
                            'swap_key': move['key'], 'deadline_ms': deadline,
                            'reason': '恢复太晚，确认超时即将撤销'})
        elif kind == 'crash':
            log(now, 'station_crashed', {'station': event['station']})
            for move in list(moves.values()):
                if move['state'] in IN_FLIGHT:
                    move['state'] = 'interrupted'
                    locked.difference_update(move['tickets'])
                    log(now, 'swap_interrupted', {
                        'swap_key': move['key'], 'station': event['station'],
                        'durable_receipts': [dict(r) for r in move['receipts']],
                        'seats': {x: seats[x] for x in move['tickets']},
                        'reason': '站点中断：已确认的归属不变，未确认的一笔留待恢复重试'})
        elif kind == 'torn_write':
            ticket = event['ticket']
            old = seats[ticket]
            seats[ticket] = event['to_seat']
            torn.append({'at_ms': now, 'ticket': ticket, 'from_seat': old,
                         'to_seat': event['to_seat'], 'note': '人为撕裂写入，仅用于验证查票检查'})
            log(now, 'torn_write_injected', {'ticket': ticket, 'from_seat': old,
                                             'to_seat': event['to_seat']})
        elif kind == 'reissue':
            ticket = event['ticket']
            if ticket in locked:
                reissues.append({'at_ms': now, 'ticket': ticket, 'applied': False,
                                 'reason': '该票正在换座锁票中，改签被拒绝'})
                log(now, 'reissue_rejected', {'ticket': ticket, 'reason': '车票已被换座锁住'})
            else:
                old = seats[ticket]
                seats[ticket] = event['to_seat']
                reissues.append({'at_ms': now, 'ticket': ticket,
                                 'station': event.get('station') or holder[ticket],
                                 'from_seat': old, 'to_seat': event['to_seat'], 'applied': True,
                                 'scope': '本站改签，不依赖跨站链路'})
                log(now, 'reissue_applied', {'ticket': ticket, 'from_seat': old,
                                             'to_seat': event['to_seat'], 'link_up': link_up})
        elif kind == 'query':
            wanted = event.get('tickets') or list(seats)
            view = {x: seats[x] for x in wanted}
            active = next((m for m in moves.values() if m['state'] in IN_FLIGHT
                           and set(wanted) & set(m['tickets'])), None)
            problems = image_problems(view)
            queries.append({'at_ms': now, 'station': event['station'], 'tickets': list(wanted),
                            'view': view, 'move_state': active['state'] if active else 'none',
                            'consistent': not problems, 'problems': problems})
            log(now, 'query_view', {'station': event['station'], 'view': view,
                                    'move_state': active['state'] if active else 'none',
                                    'consistent': not problems})

    final_moves = [move_view(m) for m in moves.values()]
    bad_queries = [q for q in queries if not q['consistent']]
    deduped = [t for t in trace if t['kind'] == 'retry_deduplicated']
    confirmed = [m for m in final_moves if m['state'] == 'confirmed']
    aborted = [m for m in final_moves if m['state'] == 'aborted']
    interrupted = [m for m in final_moves if m['state'] == 'interrupted']
    applied_reissues = [r for r in reissues if r['applied']]
    retries = [t for t in trace if t['kind'] in ('retry_deduplicated', 'retry_ignored', 'retry_rejected')]
    confirm_times = {}
    for t in trace:
        if t['kind'] == 'swap_confirmed':
            confirm_times.setdefault(t['detail']['swap_key'], t['at_ms'])
    retry_after_confirm = [t for t in retries
                           if t['detail']['swap_key'] in confirm_times
                           and t['at_ms'] >= confirm_times[t['detail']['swap_key']]]

    def during_outage(t):
        down = [x['at_ms'] for x in trace if x['kind'] == 'link_down' and x['at_ms'] <= t]
        up = [x['at_ms'] for x in trace if x['kind'] == 'link_up' and x['at_ms'] <= t]
        return bool(down) and (not up or max(up) < max(down))

    def during_move(t):
        windows = []
        for m in final_moves:
            windows.extend(m['attempts'])
            if m.get('requested_at_ms') is not None and (
                    not m['attempts'] or m['attempts'][-1].get('ended_at_ms') is None):
                windows.append({'requested_at_ms': m['requested_at_ms'], 'ended_at_ms': None})
        return any(a['requested_at_ms'] <= t and (a.get('ended_at_ms') is None or t <= a['ended_at_ms'])
                   for a in windows)

    per_key_commits = {}
    for entry in commit_log:
        per_key_commits[entry['swap_key']] = per_key_commits.get(entry['swap_key'], 0) + 1
    receipts_complete = all(
        {r['station'] for r in m['receipts'] if r['kind'] == 'confirmed'} ==
        {holder[x] for x in m['tickets']} for m in confirmed)
    crash_events = [t for t in trace if t['kind'] == 'station_crashed']
    outage_events = [t for t in trace if t['kind'] == 'link_down']

    def status(name, exercised, ok, detail):
        return {'key': name, 'label': CHECK_LABELS[name],
                'status': ('not_exercised' if not exercised else ('pass' if ok else 'fail')),
                'detail': detail}

    checks = [
        status('two_seats_swap_together', bool(commit_log),
               all(len(c['tickets']) == 2 for c in commit_log),
               '每次成交都在一步内互换两票位号' if commit_log else '本次时序没有成交'),
        status('no_half_swap_visible', bool(queries), not bad_queries,
               f'{len(queries)} 次查票，{len(bad_queries)} 次看到半笔交换' if queries else '本次时序没有查票'),
        status('reissue_concurrent_with_swap', bool(applied_reissues),
               any(during_move(r['at_ms']) for r in applied_reissues),
               '换座在途时其余车票成功改签' if applied_reissues else '本次时序没有改签'),
        status('reissue_during_outage', bool(outage_events),
               any(during_outage(r['at_ms']) for r in applied_reissues),
               '断联期间本站改签仍生效' if outage_events else '本次时序没有断联'),
        status('retry_after_confirmation_deduplicated', bool(retry_after_confirm),
               bool(retry_after_confirm) and all(t['kind'] == 'retry_deduplicated' for t in retry_after_confirm),
               f'{len(deduped)} 次成交后重试均按去重处理' if retry_after_confirm else '本次时序没有成交后重试'),
        status('no_double_apply', bool(commit_log),
               all(n == 1 for n in per_key_commits.values()),
               '每个 swap_key 只产生一笔成交' if commit_log else '本次时序没有成交'),
        status('confirmed_ownership_preserved', bool(confirmed) and bool(crash_events),
               receipts_complete and all(
                   c['after'] == {c['tickets'][0]: c['before'][c['tickets'][1]],
                                  c['tickets'][1]: c['before'][c['tickets'][0]]} for c in commit_log),
               '中断前后已确认的归属与位号不变' if crash_events else '本次时序没有站点中断'),
    ]
    timing = {}
    for m in final_moves:
        attempts = m['attempts'] or [{'attempt': m['attempt'], 'state': m['state'],
                                       'lock_wait_ms': m['lock_wait_ms'],
                                       'confirm_wait_ms': m['confirm_wait_ms']}]
        last = attempts[-1]
        timing[m['key']] = {'lock_wait_ms': last['lock_wait_ms'],
                            'confirm_wait_ms': last['confirm_wait_ms'],
                            'state': last['state'],
                            'attempts': [{'attempt': a['attempt'], 'state': a['state'],
                                          'lock_wait_ms': a['lock_wait_ms'],
                                          'confirm_wait_ms': a['confirm_wait_ms']} for a in attempts]}
    result = {
        'mode': '受控两站换座模型 / 非真实分布式事务',
        'tickets': [{'id': t['id'], 'station': t['station'], 'seat_before': original[t['id']],
                     'seat_after': seats[t['id']], 'holder': holder[t['id']]} for t in tickets],
        'moves': final_moves,
        'timing': timing,
        'trace': trace,
        'queries': queries,
        'reissues': reissues,
        'commit_log': commit_log,
        'torn_writes': torn,
        'checks': checks,
        'summary': {
            'swaps': len(moves), 'confirmed': len(confirmed), 'aborted': len(aborted),
            'interrupted': len(interrupted), 'deduplicated_retries': len(deduped),
            'queries': len(queries), 'half_swap_observations': len(bad_queries),
            'reissues_applied': len(applied_reissues),
            'reissues_rejected': len(reissues) - len(applied_reissues),
        },
        'policy': {
            'lock_ms': policy['lock_ms'], 'prepare_ms': policy['prepare_ms'],
            'confirm_ms': policy['confirm_ms'], 'abort_after_ms': policy['abort_after_ms'],
            'receipt_rule': '决定站于决定时写decision凭据；另一站在链路送达时写decision凭据；两站凭据齐全后才同时换座并写confirmed凭据',
            'lock_scope': '只锁本次换座的两张票，其他车票改签可并发',
            'simulated_clock': '所有等待为模拟毫秒，不依赖真实挂钟或外部网络',
        },
    }
    result['checksum'] = _checksum(result)
    return result
