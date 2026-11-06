import unittest

import crit_slow_subscriber
import lib


class OffsetDifferencesTest(unittest.TestCase):
    def test_contiguous_passes(self):
        self.assertEqual(lib.offset_differences([1, 4, 7], [1, 4, 7]), [])

    def test_gap_is_detected(self):
        self.assertEqual(lib.offset_differences([1, 7], [1, 4, 7]), ["gap: missing 4"])

    def test_repeat_is_detected(self):
        self.assertEqual(lib.offset_differences([1, 4, 4, 7], [1, 4, 7]), ["repeated 4"])

    def test_going_back_and_extra_are_detected(self):
        self.assertEqual(
            lib.offset_differences([1, 7, 4, 9], [1, 4, 7]),
            ["goes back: 4 after 7", "extra 9"],
        )

    def test_lagged_compares_up_to_the_last_received(self):
        result, diffs = crit_slow_subscriber.outcome([1, 4], [1, 4, 7], {"code": "lagged"}, False)
        self.assertEqual((result, diffs), ("lagged", []))
        result, diffs = crit_slow_subscriber.outcome([1, 7], [1, 4, 7, 9], {"code": "lagged"}, False)
        self.assertEqual((result, diffs), ("lagged", ["gap: missing 4"]))

    def test_backpressure_needs_everything(self):
        result, diffs = crit_slow_subscriber.outcome([1, 4, 7], [1, 4, 7], None, True)
        self.assertEqual((result, diffs), ("contrapresión", []))
        result, _ = crit_slow_subscriber.outcome([1, 4], [1, 4, 7], None, False)
        self.assertIsNone(result)


class SSEParserTest(unittest.TestCase):
    def test_reads_id_event_and_data(self):
        stream = [
            "id: 00000000000000ab-3\n",
            'data: {"o":3}\n',
            "\n",
            ": a comment\n",
            "event: error\n",
            'data: {"code":"lagged","last":"00000000000000ab-3"}\n',
            "\n",
        ]
        self.assertEqual(
            list(lib.sse_events(stream)),
            [
                {"id": "00000000000000ab-3", "event": "message", "data": '{"o":3}'},
                {
                    "id": None,
                    "event": "error",
                    "data": '{"code":"lagged","last":"00000000000000ab-3"}',
                },
            ],
        )

    def test_data_lines_join_and_crlf_and_no_space(self):
        stream = ["id:7\r\n", "data:a\r\n", "data: b\r\n", "\r\n"]
        self.assertEqual(list(lib.sse_events(stream)), [{"id": "7", "event": "message", "data": "a\nb"}])

    def test_event_without_data_or_end_is_not_dispatched(self):
        self.assertEqual(list(lib.sse_events(["id: 1\n", "\n", "data: x\n"])), [])


class ParseMetricsTest(unittest.TestCase):
    def test_labels_sorted_and_comments_skipped(self):
        text = (
            "# TYPE oteadero_events_dropped_total counter\n"
            'oteadero_events_dropped_total{cause="quota",tenant="t1"} 5\n'
            "oteadero_sketch_observed_rel_error_max 0.004\n"
        )
        self.assertEqual(
            lib.parse_metrics(text),
            {
                ("oteadero_events_dropped_total", (("cause", "quota"), ("tenant", "t1"))): 5.0,
                ("oteadero_sketch_observed_rel_error_max", ()): 0.004,
            },
        )


if __name__ == "__main__":
    unittest.main()
