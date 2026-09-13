import unittest

from io_stats import io_stats_interval, worker_stats_interval


def snapshot(recv, cqes, submits, submitted, active):
    return {'loops': [{'loop_id': 0, 'window_id': 1,
                       'peaks': {'staged_operations': 3, 'cq_ready_sampled': 4},
                       'counters': {'prepared_recv_sqes': recv, 'recv_cqes': cqes,
                                    'submit_calls': submits, 'submitted_sqes': submitted},
                       'gauges': {'active_recv_requests': active}}],
            'worker_pool': {'window_id': 1, 'pending_logical_jobs': 0,
                            'queues': [{'worker_id': 0, 'gauges': {'queued_batches': 0},
                                        'peaks': {'queued_batches': 2}}]}}


class IoStatsIntervalTest(unittest.TestCase):
    def test_interval_excludes_warmup_and_keeps_gauges_as_samples(self):
        result = io_stats_interval(snapshot(10, 100, 20, 40, 2), snapshot(12, 120, 24, 52, 0), 10)
        self.assertEqual(result['counters']['prepared_recv_sqes'], 2)
        self.assertEqual(result['ratios']['cqes_per_prepared_recv'], 10)
        self.assertEqual(result['ratios']['sqes_per_submit_call'], 3)
        self.assertEqual(result['ratios']['prepared_recv_sqes_per_1000_success'], 200)
        self.assertEqual(result['loops'][0]['gauges_before']['active_recv_requests'], 2)
        self.assertEqual(result['loops'][0]['gauges_after']['active_recv_requests'], 0)

    def test_buffer_exhaustion_counters_and_lease_samples(self):
        before = snapshot(10, 100, 20, 40, 0)
        after = snapshot(12, 120, 24, 52, 0)
        for point, count in [(before, 100), (after, 105)]:
            point['loops'][0]['counters']['provided_buffer_enobufs'] = count
        after['loops'][0]['gauges']['buffer_outstanding_leases'] = 0
        after['loops'][0]['peaks']['buffer_outstanding_leases'] = 3
        result = io_stats_interval(before, after, 10)
        self.assertEqual(result['counters']['provided_buffer_enobufs'], 5)
        self.assertEqual(result['loops'][0]['gauges_after']['buffer_outstanding_leases'], 0)
        self.assertEqual(result['loops'][0]['peaks']['buffer_outstanding_leases'], 3)

    def test_idle_interval_has_null_ratios(self):
        point = snapshot(0, 0, 0, 0, 0)
        result = io_stats_interval(point, point, 0)
        self.assertTrue(all(value is None for value in result['ratios'].values()))

    def test_multiple_loops_are_summed_after_taking_differences(self):
        before = snapshot(10, 100, 20, 40, 1)
        after = snapshot(12, 120, 24, 52, 0)
        before['loops'].append(dict(snapshot(30, 300, 60, 120, 1)['loops'][0], loop_id=1))
        after['loops'].append(dict(snapshot(32, 320, 64, 132, 0)['loops'][0], loop_id=1))
        result = io_stats_interval(before, after, 20)
        self.assertEqual(len(result['loops']), 2)
        self.assertEqual(result['counters']['prepared_recv_sqes'], 4)
        self.assertEqual(result['ratios']['cqes_per_prepared_recv'], 10)

    def test_reset_or_loop_change_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'decreased'):
            io_stats_interval(snapshot(2, 2, 2, 2, 1), snapshot(1, 1, 1, 1, 0), 1)
        with self.assertRaisesRegex(RuntimeError, 'loop set changed'):
            io_stats_interval(snapshot(0, 0, 0, 0, 0), {'loops': []}, 0)

    def test_peaks_are_kept_per_loop_without_subtraction_or_sum(self):
        before = snapshot(0, 0, 0, 0, 0)
        after = snapshot(1, 10, 1, 2, 0)
        after['loops'][0]['peaks']['staged_operations'] = 2
        result = io_stats_interval(before, after, 10)
        self.assertEqual(result['loops'][0]['peaks']['staged_operations'], 2)
        self.assertNotIn('peaks', result)
        after['loops'][0]['window_id'] = 2
        with self.assertRaisesRegex(RuntimeError, 'peak window changed'):
            io_stats_interval(before, after, 10)

    def test_worker_peaks_and_gauges_are_preserved(self):
        before = snapshot(0, 0, 0, 0, 0)['worker_pool']
        after = snapshot(1, 10, 1, 2, 0)['worker_pool']
        after['queues'][0]['peaks']['queued_batches'] = 1
        result = worker_stats_interval(before, after)
        self.assertEqual(result['queues'][0]['peaks']['queued_batches'], 1)
        self.assertEqual(result['pending_logical_jobs_after'], 0)
        after['window_id'] = 2
        with self.assertRaisesRegex(RuntimeError, 'worker peak window changed'):
            worker_stats_interval(before, after)


if __name__ == '__main__':
    unittest.main()
