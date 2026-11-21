import pytest
from src.services.swap import simulate_swap, SwapError

TICKETS = [
    {'id': 1, 'station': 'A', 'seat': '12A'},
    {'id': 2, 'station': 'B', 'seat': '05F'},
    {'id': 3, 'station': 'A', 'seat': '18C'},
]


def run(events, policy=None):
    return simulate_swap(TICKETS, events, policy)


def checks(result):
    return {c['key']: c['status'] for c in result['checks']}


def test_normal_swap_confirms_and_swaps_both_seats():
    r = run([{'at_ms': 0, 'kind': 'swap', 'swap_key': 'K1', 'tickets': [1, 2]}])
    assert r['summary']['confirmed'] == 1
    seats = {t['id']: t['seat_after'] for t in r['tickets']}
    assert seats[1] == '05F' and seats[2] == '12A'
    move = r['moves'][0]
    assert move['lock_wait_ms'] == r['policy']['lock_ms']
    assert move['confirm_wait_ms'] == r['policy']['confirm_ms']


def test_same_input_same_checksum():
    events = [{'at_ms': 0, 'kind': 'swap', 'swap_key': 'K1', 'tickets': [1, 2]},
              {'at_ms': 5, 'kind': 'query', 'station': 'B', 'tickets': [1, 2]}]
    assert run(events)['checksum'] == run(events)['checksum']


def test_reissue_other_ticket_concurrently_with_swap():
    r = run([{'at_ms': 0, 'kind': 'swap', 'swap_key': 'K1', 'tickets': [1, 2]},
             {'at_ms': 10, 'kind': 'reissue', 'ticket': 3, 'to_seat': '19C'}])
    assert r['summary']['confirmed'] == 1
    assert r['reissues'][0]['applied'] is True
    assert checks(r)['reissue_concurrent_with_swap'] == 'pass'


def test_lock_blocks_reissue_of_a_swapping_ticket():
    r = run([{'at_ms': 0, 'kind': 'swap', 'swap_key': 'K1', 'tickets': [1, 2]},
             {'at_ms': 45, 'kind': 'reissue', 'ticket': 1, 'to_seat': '99Z'}])
    assert r['reissues'][0]['applied'] is False


def test_decision_then_disconnect_aborts_after_timeout_with_original_seats():
    r = run([{'at_ms': 0, 'kind': 'swap', 'swap_key': 'K1', 'tickets': [1, 2]},
             {'at_ms': 61, 'kind': 'link_down'}])
    move = r['moves'][0]
    assert move['state'] == 'aborted'
    assert move['decided_at_ms'] == 60
    assert move['confirm_wait_ms'] == r['policy']['abort_after_ms']
    assert {t['id']: t['seat_after'] for t in r['tickets']} == {1: '12A', 2: '05F', 3: '18C'}


def test_reissue_still_works_while_disconnected():
    r = run([{'at_ms': 0, 'kind': 'swap', 'swap_key': 'K1', 'tickets': [1, 2]},
             {'at_ms': 61, 'kind': 'link_down'},
             {'at_ms': 200, 'kind': 'reissue', 'ticket': 3, 'to_seat': '19C', 'station': 'A'}])
    assert r['reissues'][0]['applied'] is True
    assert checks(r)['reissue_during_outage'] == 'pass'


def test_query_never_sees_half_swap():
    r = run([{'at_ms': 0, 'kind': 'swap', 'swap_key': 'K1', 'tickets': [1, 2]}]
            + [{'at_ms': t, 'kind': 'query', 'station': 'B', 'tickets': [1, 2]} for t in (5, 45, 70, 200)])
    assert all(q['consistent'] for q in r['queries'])
    assert checks(r)['no_half_swap_visible'] == 'pass'


def test_torn_write_is_detected_by_query_check():
    r = run([{'at_ms': 0, 'kind': 'swap', 'swap_key': 'K1', 'tickets': [1, 2]},
             {'at_ms': 45, 'kind': 'torn_write', 'ticket': 1, 'to_seat': '05F'},
             {'at_ms': 50, 'kind': 'query', 'station': 'B', 'tickets': [1, 2]}])
    assert checks(r)['no_half_swap_visible'] == 'fail'
    assert r['queries'][0]['consistent'] is False


def test_retry_after_confirmation_does_not_swap_twice():
    r = run([{'at_ms': 0, 'kind': 'swap', 'swap_key': 'K1', 'tickets': [1, 2]},
             {'at_ms': 500, 'kind': 'retry', 'swap_key': 'K1', 'tickets': [1, 2]}])
    assert r['summary']['deduplicated_retries'] == 1
    assert len(r['commit_log']) == 1
    assert checks(r)['retry_after_confirmation_deduplicated'] == 'pass'
    assert checks(r)['no_double_apply'] == 'pass'


def test_crash_then_retry_recovers_and_confirms_once():
    r = run([{'at_ms': 0, 'kind': 'swap', 'swap_key': 'K1', 'tickets': [1, 2]},
             {'at_ms': 45, 'kind': 'crash', 'station': 'A'},
             {'at_ms': 300, 'kind': 'retry', 'swap_key': 'K1', 'tickets': [1, 2]},
             {'at_ms': 600, 'kind': 'retry', 'swap_key': 'K1', 'tickets': [1, 2]}])
    move = r['moves'][0]
    assert move['state'] == 'confirmed'
    assert [a['state'] for a in move['attempts']] == ['aborted', 'confirmed']
    assert len(r['commit_log']) == 1
    assert checks(r)['confirmed_ownership_preserved'] == 'pass'


def test_confirmed_ownership_survives_later_crash():
    r = run([{'at_ms': 0, 'kind': 'swap', 'swap_key': 'K1', 'tickets': [1, 2]},
             {'at_ms': 200, 'kind': 'crash', 'station': 'B'},
             {'at_ms': 300, 'kind': 'retry', 'swap_key': 'K1', 'tickets': [1, 2]}])
    assert r['summary']['confirmed'] == 1
    assert {t['id']: t['seat_after'] for t in r['tickets']}[1] == '05F'
    assert r['summary']['deduplicated_retries'] == 1


def test_same_key_with_different_tickets_is_rejected():
    r = run([{'at_ms': 0, 'kind': 'swap', 'swap_key': 'K1', 'tickets': [1, 2]},
             {'at_ms': 200, 'kind': 'retry', 'swap_key': 'K1', 'tickets': [2, 3]}])
    assert any(t['kind'] == 'retry_rejected' for t in r['trace'])
    assert len(r['commit_log']) == 1


def test_overlapping_swaps_do_not_double_lock():
    r = run([{'at_ms': 0, 'kind': 'swap', 'swap_key': 'K1', 'tickets': [1, 2]},
             {'at_ms': 10, 'kind': 'swap', 'swap_key': 'K2', 'tickets': [2, 3]}])
    by_key = {m['key']: m['state'] for m in r['moves']}
    assert by_key['K1'] == 'confirmed'
    assert by_key['K2'] == 'aborted'
    assert len(r['commit_log']) == 1


def test_validation_rejects_same_station_pair_and_bad_times():
    with pytest.raises(SwapError):
        simulate_swap(TICKETS, [{'at_ms': 0, 'kind': 'swap', 'swap_key': 'K', 'tickets': [1, 3]}])
    with pytest.raises(SwapError):
        simulate_swap(TICKETS, [{'at_ms': 10, 'kind': 'link_up'}, {'at_ms': 5, 'kind': 'link_down'}])
    with pytest.raises(SwapError):
        simulate_swap(TICKETS, [{'at_ms': 0, 'kind': 'swap', 'swap_key': 'K', 'tickets': [1, 2]}],
                      {'abort_after_ms': 10, 'confirm_ms': 60})
