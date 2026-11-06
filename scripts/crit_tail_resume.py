"""Criterion: a /v1/tail subscriber that disconnects at seeded instants and resumes with its
last token receives every offset that matches its filter, once and in order (section 4.9)."""

import http.client
import json
import random
import sys
import time

import lib

SEED = 4
TENANT = "t1"
KIND = "logs"
# One of the 200 log streams of the tail profile: about 3 000 of its 600 000 events.
MATCH = "stream=l0007"
# The disconnections fall after a seeded number of events, all within the
# first EARLY events, which the filter is sure to pass during the load.
DISCONNECTIONS = 24
EARLY = 2000
MIN_RESUMES = 20
# How long the subscriber may take to reach the last offset once the load ended.
DRAIN_TIMEOUT_S = 60
READ_TIMEOUT_S = 10


class Subscriber:
    """A tail subscriber that remembers its last token and the offsets it received."""

    def __init__(self, url, rng, cuts):
        self.url = url
        self.rng = rng
        self.cuts = cuts
        self.token = None
        self.offsets = []
        self.resumes = 0
        self.conn = None
        self.lines = None

    def connect(self):
        if self.conn is not None:
            self.conn.close()
            self.resumes += 1
        header = self.rng.random() < 0.5
        self.conn, resp = lib.open_tail(
            self.url, TENANT, KIND, MATCH, self.token, header, READ_TIMEOUT_S
        )
        if resp.status != 200:
            raise RuntimeError(f"tail answered {resp.status}: {resp.read()!r}")
        self.lines = lib.sse_events(lib.response_lines(resp))

    def read(self, done):
        """Read until done(offsets) holds, disconnecting at the seeded counts.

        A read that times out or a stream that ends reconnects with the token.
        Without a token a new connection would start at the head and could skip
        events, so that is an error.
        """
        while not done(self.offsets):
            try:
                event = next(self.lines)
            except (StopIteration, OSError, http.client.HTTPException) as err:
                if self.token is None:
                    raise RuntimeError(f"no event before the stream failed: {err!r}") from err
                self.connect()
                continue
            if event["event"] != "message":
                raise RuntimeError(f"tail sent {event}")
            self.offsets.append(json.loads(event["data"])["o"])
            self.token = event["id"]
            if self.cuts and len(self.offsets) >= self.cuts[0]:
                self.cuts.pop(0)
                self.connect()

    def close(self):
        if self.conn is not None:
            self.conn.close()


def main():
    work = lib.work_dir("crit_tail_resume")
    rng = random.Random(SEED)
    cuts = sorted(rng.sample(range(1, EARLY), DISCONNECTIONS))
    report = work / "report.json"
    with lib.Serve(work, lib.tenants(1, lib.HIGH_QUOTA)) as engine:
        engine.start()
        subscriber = Subscriber(engine.url, rng, cuts)
        # Before the load: without a token the tail starts at the head, offset 0.
        subscriber.connect()
        load = lib.start_otgen("tail", SEED, engine.url, report)
        try:
            subscriber.read(lambda _: load.poll() is not None)
            lib.accepted_events(lib.wait_otgen(load, report))
        finally:
            if load.poll() is None:
                load.kill()
                load.wait()
        expected = lib.run_oracle_offsets(engine.data_dir, work / "oracle", TENANT, KIND, MATCH)
        deadline = time.monotonic() + DRAIN_TIMEOUT_S
        last = expected[-1] if expected else -1
        subscriber.read(
            lambda got: (got and got[-1] >= last) or time.monotonic() > deadline
        )
        subscriber.close()
        engine.stop()

    print(f"offsets esperados: {len(expected)}")
    print(f"offsets recibidos: {len(subscriber.offsets)}")
    print(f"reanudaciones: {subscriber.resumes}")
    if subscriber.cuts:
        print(f"desconexiones sembradas sin hacer: {len(subscriber.cuts)}")
    ok = lib.print_offsets(lib.offset_differences(subscriber.offsets, expected))
    resumed = not subscriber.cuts and subscriber.resumes >= MIN_RESUMES
    return 0 if ok and expected and resumed else 1


if __name__ == "__main__":
    sys.exit(main())
