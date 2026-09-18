import unittest

from io_stats import io_stats_interval, worker_stats_interval


def snapshot(enobufs=0, leases=0, lease_peak=0):
    return {'loops': [{'loop_id': 0, 'window_id': 1,
                       'peaks': {'buffer_outstanding_leases': lease_peak},
                       'counters': {'provided_buffer_enobufs': enobufs},
                       'gauges': {'buffer_capacity': 8, 'buffer_size': 4096,
                                  'buffer_outstanding_leases': leases}}],
            'worker_pool': {'window_id': 1, 'pending_logical_jobs': 0,
                            'queues': [{'worker_id': 0, 'gauges': {'queued_batches': 0},
                                        'peaks': {'queued_batches': 2}}]}}


class IoStatsIntervalTest(unittest.TestCase):
    def test_interval_reports_pressure_counter_and_lease_samples(self):
        result = io_stats_interval(snapshot(100, 2, 2), snapshot(105, 0, 3))
        self.assertEqual(result['counters']['provided_buffer_enobufs'], 5)
        self.assertEqual(result['loops'][0]['gauges_before']['buffer_outstanding_leases'], 2)
        self.assertEqual(result['loops'][0]['gauges_after']['buffer_outstanding_leases'], 0)
        self.assertEqual(result['loops'][0]['peaks']['buffer_outstanding_leases'], 3)

    def test_multiple_loops_are_summed_after_taking_differences(self):
        before = snapshot(10)
        after = snapshot(12)
        before['loops'].append(dict(snapshot(30)['loops'][0], loop_id=1))
        after['loops'].append(dict(snapshot(34)['loops'][0], loop_id=1))
        result = io_stats_interval(before, after)
        self.assertEqual(len(result['loops']), 2)
        self.assertEqual(result['counters']['provided_buffer_enobufs'], 6)

    def test_reset_counter_or_loop_change_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'decreased'):
            io_stats_interval(snapshot(2), snapshot(1))
        with self.assertRaisesRegex(RuntimeError, 'loop set changed'):
            io_stats_interval(snapshot(), {'loops': []})
        changed_window = snapshot()
        changed_window['loops'][0]['window_id'] = 2
        with self.assertRaisesRegex(RuntimeError, 'peak window changed'):
            io_stats_interval(snapshot(), changed_window)

    def test_worker_peaks_and_gauges_are_preserved(self):
        before = snapshot()['worker_pool']
        after = snapshot()['worker_pool']
        after['queues'][0]['peaks']['queued_batches'] = 1
        result = worker_stats_interval(before, after)
        self.assertEqual(result['queues'][0]['peaks']['queued_batches'], 1)
        self.assertEqual(result['pending_logical_jobs_after'], 0)
        after['window_id'] = 2
        with self.assertRaisesRegex(RuntimeError, 'worker peak window changed'):
            worker_stats_interval(before, after)


if __name__ == '__main__':
    unittest.main()
