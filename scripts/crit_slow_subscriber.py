"""Criterion: a /v1/tail subscriber that reads very slowly is held back by TCP or cut with
`lagged`, and what it received has no gap (section 4.9, D11)."""

import http.client
import json
import socket
import sys
import time

import lib

SEED = 5
TENANT = "t1"
KIND = "logs"
# Low enough for a subscriber that reads a few hundred events per second to
# fall behind a load of 20 000 per second.
MAX_LAG_EVENTS = 10_000
# The subscriber takes one event every SLOW_PAUSE_S while the load runs, with
# a small receive buffer so that the kernel does not read for it.
SLOW_PAUSE_S = 0.002
RECEIVE_BUFFER = 4096
# Once the load ended it reads at full speed until the stream ends or it has
# everything, within DRAIN_TIMEOUT_S.
DRAIN_TIMEOUT_S = 60
READ_TIMEOUT_S = 10


class SmallBufferConnection(http.client.HTTPConnection):
    """An HTTP connection whose socket gets a small receive buffer before connecting."""

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RECEIVE_BUFFER)
        self.sock.settimeout(self.timeout)
        self.sock.connect((self.host, self.port))


def outcome(offsets, expected, lagged, complete):
    """Return (outcome, differences) of a slow subscriber.

    With lagged, the error event, what it received must be the expected
    offsets up to its last one: the cut may come anywhere, a jump may not.
    Without it the stream must have kept up and delivered everything.
    """
    if lagged is not None:
        last = offsets[-1] if offsets else -1
        return "lagged", lib.offset_differences(offsets, [o for o in expected if o <= last])
    if complete:
        return "contrapresión", lib.offset_differences(offsets, expected)
    return None, lib.offset_differences(offsets, expected)


def main():
    work = lib.work_dir("crit_slow_subscriber")
    report = work / "report.json"
    offsets = []
    lagged = None
    with lib.Serve(
        work, lib.tenants(1, lib.HIGH_QUOTA), tail_max_lag_events=MAX_LAG_EVENTS
    ) as engine:
        engine.start()
        conn, resp = lib.open_tail(
            engine.url,
            TENANT,
            KIND,
            connection=SmallBufferConnection("127.0.0.1", engine.port, timeout=READ_TIMEOUT_S),
        )
        if resp.status != 200:
            raise RuntimeError(f"tail answered {resp.status}: {resp.read()!r}")
        events = lib.sse_events(lib.response_lines(resp))
        load = lib.start_otgen("tail", SEED, engine.url, report)
        expected = None
        ended = False
        deadline = None

        def load_ended():
            lib.accepted_events(lib.wait_otgen(load, report))
            return lib.run_oracle_offsets(engine.data_dir, work / "oracle", TENANT, KIND)

        try:
            while True:
                if expected is None and load.poll() is not None:
                    expected = load_ended()
                    deadline = time.monotonic() + DRAIN_TIMEOUT_S
                if expected is not None and (
                    (offsets and expected and offsets[-1] >= expected[-1])
                    or time.monotonic() > deadline
                ):
                    break
                try:
                    event = next(events)
                except StopIteration:
                    ended = True
                    break
                except (OSError, http.client.HTTPException) as err:
                    print(f"lectura interrumpida: {err!r}")
                    break
                if event["event"] == "error":
                    lagged = json.loads(event["data"])
                    continue
                if lagged is not None:
                    raise RuntimeError(f"event after the error: {event}")
                offsets.append(json.loads(event["data"])["o"])
                if expected is None:
                    time.sleep(SLOW_PAUSE_S)
            if expected is None:
                expected = load_ended()
        finally:
            if load.poll() is None:
                load.kill()
                load.wait()
            conn.close()
        engine.stop()

    complete = bool(expected) and bool(offsets) and offsets[-1] >= expected[-1]
    result, diffs = outcome(offsets, expected or [], lagged, complete)
    if lagged is not None:
        print(f"error: {lagged}")
        if not ended:
            diffs.append("the stream did not end after the error")
        if lagged.get("code") != "lagged":
            diffs.append(f"error code {lagged.get('code')!r}, not lagged")
    print(f"offsets esperados: {len(expected or [])}")
    print(f"offsets recibidos: {len(offsets)}")
    print(f"desenlace: {result or 'ninguno'}")
    ok = lib.print_offsets(diffs)
    return 0 if ok and result is not None and offsets else 1


if __name__ == "__main__":
    sys.exit(main())
