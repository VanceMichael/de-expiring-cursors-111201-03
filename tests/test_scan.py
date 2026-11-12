import copy
import json
from pathlib import Path
import tempfile
import unittest

from collector.domain import Catalog
from collector.scan import ScanError, Scanner
from collector.storage import Store
from collector.supplier import ScriptedSupplier

SCRIPT = json.loads(Path('fixtures/windows.json').read_text())


class FlakySupplier:
    """在指定请求序号抛出传输异常，用于复现部分窗口失败。"""

    def __init__(self, inner, fail_calls):
        self.inner = inner
        self.fail_calls = set(fail_calls)
        self.calls = 0

    def page(self, start, end, cursor=None):
        self.calls += 1
        if self.calls in self.fail_calls:
            raise ConnectionError('模拟传输中断')
        return self.inner.page(start, end, cursor)


class QueueSupplier:
    """按预定脚本返回页面，用于构造供应商边界行为。"""

    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def page(self, start, end, cursor=None):
        self.calls.append(dict(start=start, end=end, cursor=cursor))
        return self.pages.pop(0)


class JumpClockSupplier:
    """每次请求时钟都大步前进，游标永远失效。"""

    def __init__(self, rows, step=10):
        self.rows = rows
        self.step = step
        self.clock = 0

    def page(self, start, end, cursor=None):
        self.clock += self.step
        return Catalog(self.rows, page_size=1, lifetime=5).page(
            start, end, clock=self.clock, cursor=cursor)


class ScanTestCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temporary.name) / 'scan.sqlite3')

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def fresh_supplier(self, **overrides):
        script = copy.deepcopy(SCRIPT)
        script.update(overrides)
        return ScriptedSupplier(script)

    def run_fixture(self):
        supplier = self.fresh_supplier()
        scanner = Scanner(self.store, supplier, precision=supplier.precision,
                          lookback=supplier.lookback)
        scan_id, resumed = scanner.plan_or_resume(0, 10)
        self.assertFalse(resumed)
        return supplier, scanner.run(scan_id)


class FixtureScanTests(ScanTestCase):
    def test_same_second_burst_suspends_min_granularity_shard(self):
        # 同秒大批记录：5 条 updated=3 超过单窗上限 3
        supplier, report = self.run_fixture()
        self.assertEqual(report['status'], 'done')
        self.assertFalse(report['complete'])
        # 覆盖证明：叶子分片恰好拼满整个扫描窗口
        self.assertEqual([(s['start'], s['end']) for s in report['coverage']],
                         [(0, 3), (3, 4), (4, 5), (5, 10)])
        statuses = {(s['start'], s['end']): s['status'] for s in report['coverage']}
        self.assertEqual(statuses[(3, 4)], 'suspended')
        self.assertEqual(statuses[(0, 3)], 'complete')
        self.assertEqual(statuses[(5, 10)], 'complete')
        # 可靠水位停在挂起分片之前
        self.assertEqual(report['watermark'], 3)
        # 未完成区间对业务可见
        self.assertEqual(len(report['incomplete']), 1)
        self.assertEqual((report['incomplete'][0]['start'], report['incomplete'][0]['end']), (3, 4))
        self.assertEqual(report['incomplete'][0]['status'], 'suspended')
        # r4 落在挂起分片内真实缺失，其余到齐
        records = {row['key']: row for row in self.store.records()}
        self.assertEqual(sorted(records), ['r0', 'r1', 'r2', 'r3'])

    def test_mid_scan_update_collected_and_wins(self):
        # 扫描途中 r1 被修订（rev2, updated=7），由 [5,10) 分片补采
        supplier, report = self.run_fixture()
        records = {row['key']: row for row in self.store.records()}
        self.assertEqual(records['r1']['revision'], 2)
        self.assertEqual(records['r1']['updated'], 7)

    def test_cursor_expiry_rescans_only_local_window(self):
        supplier, report = self.run_fixture()
        calls = supplier.requests
        # 第二次请求带着 clock 0 签发的游标，clock 6 时已失效
        self.assertEqual(calls[1]['cursor'], '0:2')
        self.assertEqual(calls[1]['clock'], 6)
        # 失效后从头重扫的是同一个分片窗口，而非整库
        self.assertEqual((calls[2]['start'], calls[2]['end']), (0, 10))
        self.assertIsNone(calls[2]['cursor'])
        expired = self.store.connection.execute(
            "SELECT attempts FROM shards WHERE start=0 AND end=10").fetchone()
        self.assertEqual(expired[0], 1)

    def test_duplicate_returns_digested_by_key_and_revision(self):
        supplier, report = self.run_fixture()
        # r0 在失效前、重扫及多个子分片中重复返回，只保留一份
        records = self.store.records()
        self.assertEqual(len(records), 4)
        self.assertEqual(len([row for row in records if row['key'] == 'r0']), 1)
        self.assertEqual(report['records'], 4)

    def test_lookback_rescan_recovers_gap_and_stale_revision_loses(self):
        # 首轮：挂起 [3,4)，水位 3
        supplier, report = self.run_fixture()
        self.assertEqual(self.store.watermark(), 3)
        # 次轮：供应商上限放宽；起点按约定回看范围自动回退到 1
        relaxed = self.fresh_supplier(cap=10, events=[])
        scanner = Scanner(self.store, relaxed, precision=1, lookback=2)
        scan_id, resumed = scanner.plan_or_resume(end=10)
        self.assertFalse(resumed)
        self.assertEqual(self.store.scan(scan_id)['start'], 1)
        report = scanner.run(scan_id)
        self.assertTrue(report['complete'])
        # 前方区间全部完整，水位推进到窗口终点
        self.assertEqual(report['watermark'], 10)
        self.assertEqual(report['incomplete'], [])
        records = {row['key']: row for row in self.store.records()}
        # r4 补采到位；放宽后的供应商只有 r1 旧修订，不得盖掉新值
        self.assertEqual(sorted(records), ['r0', 'r1', 'r2', 'r3', 'r4'])
        self.assertEqual(records['r1']['revision'], 2)


class ResumeTests(ScanTestCase):
    def test_partial_window_failure_resumes_with_saved_cursor(self):
        supplier = self.fresh_supplier()
        flaky = FlakySupplier(supplier, fail_calls={9})
        scanner = Scanner(self.store, flaky, precision=1, lookback=2)
        scan_id, _ = scanner.plan_or_resume(0, 10)
        with self.assertRaises(ConnectionError):
            scanner.run(scan_id)
        # 中断现场保留：扫描仍在进行，失败分片带着已翻页的游标
        self.assertEqual(self.store.scan(scan_id)['status'], 'running')
        shard = self.store.connection.execute(
            "SELECT * FROM shards WHERE start=3 AND end=5").fetchone()
        self.assertEqual(shard['status'], 'scanning')
        self.assertEqual(shard['cursor'], '7:2')
        self.assertEqual(shard['pages'], 1)
        self.assertEqual(len(supplier.requests), 8)
        # 异常续跑：同一入口继续未完成的扫描
        resumed_scanner = Scanner(self.store, flaky, precision=1, lookback=2)
        resumed_id, resumed = resumed_scanner.plan_or_resume(0, 10)
        self.assertTrue(resumed)
        self.assertEqual(resumed_id, scan_id)
        report = resumed_scanner.run(resumed_id)
        # 续跑从保存的游标继续，已翻过的页不重抓
        self.assertEqual(supplier.requests[8]['cursor'], '7:2')
        self.assertEqual((supplier.requests[8]['start'], supplier.requests[8]['end']), (3, 5))
        self.assertEqual(len(supplier.requests), 13)
        # 重复页被消化，没有重复记录
        self.assertEqual(len(self.store.records()), 4)
        self.assertEqual(report['watermark'], 3)

    def test_repeated_cursor_expiry_suspends_without_infinite_retry(self):
        rows = [dict(id='a', revision=1, updated=1, value=1),
                dict(id='b', revision=1, updated=2, value=2)]
        supplier = JumpClockSupplier(rows)
        scanner = Scanner(self.store, supplier, precision=1, lookback=0, max_restarts=3)
        scan_id, _ = scanner.plan_or_resume(0, 5)
        report = scanner.run(scan_id)
        self.assertEqual(report['status'], 'done')
        self.assertFalse(report['complete'])
        leaf = report['coverage'][0]
        self.assertEqual(leaf['status'], 'suspended')
        self.assertEqual(leaf['attempts'], 4)
        self.assertIn('游标', leaf['note'])
        # 每次重试的第一页反复返回 a，被修订号消化；b 始终翻不到，由挂起交代
        self.assertEqual([row['key'] for row in self.store.records()], ['a'])
        self.assertEqual(report['watermark'], 0)


class EmptyPageTests(ScanTestCase):
    def test_transient_empty_page_is_not_terminal(self):
        supplier = QueueSupplier([
            dict(items=[], next='c:1', terminal=False, truncated=False),
            dict(items=[dict(id='a', revision=1, updated=1, value='甲')],
                 next=None, terminal=True, truncated=False),
        ])
        scanner = Scanner(self.store, supplier, precision=1, lookback=0)
        scan_id, _ = scanner.plan_or_resume(0, 5)
        report = scanner.run(scan_id)
        self.assertTrue(report['complete'])
        self.assertEqual(len(supplier.calls), 2)
        self.assertEqual(self.store.records()[0]['key'], 'a')
        self.assertEqual(report['watermark'], 5)

    def test_stuck_cursor_stops_instead_of_looping(self):
        supplier = QueueSupplier([
            dict(items=[], next='c:1', terminal=False, truncated=False),
            dict(items=[], next='c:1', terminal=False, truncated=False),
        ])
        scanner = Scanner(self.store, supplier, precision=1, lookback=0)
        scan_id, _ = scanner.plan_or_resume(0, 5)
        with self.assertRaises(ScanError):
            scanner.run(scan_id)
        self.assertEqual(len(supplier.calls), 2)
        # 分片保持可续跑状态，扫描未结束
        self.assertEqual(self.store.scan(scan_id)['status'], 'running')

    def test_empty_window_seals_on_terminal_evidence(self):
        supplier = QueueSupplier([
            dict(items=[], next=None, terminal=True, truncated=False),
        ])
        scanner = Scanner(self.store, supplier, precision=1, lookback=0)
        scan_id, _ = scanner.plan_or_resume(0, 5)
        report = scanner.run(scan_id)
        self.assertTrue(report['complete'])
        self.assertEqual(report['watermark'], 5)


class WatermarkTests(ScanTestCase):
    def test_watermark_blocked_by_gap_between_scans(self):
        # 首次扫描 [0,5) 完整，水位到 5
        first = QueueSupplier([dict(items=[], next=None, terminal=True, truncated=False)])
        scanner = Scanner(self.store, first, precision=1, lookback=0)
        scan_id, _ = scanner.plan_or_resume(0, 5)
        scanner.run(scan_id)
        self.assertEqual(self.store.watermark(), 5)
        # 人为指定一个与水位脱节的窗口，水位不得越空洞前移
        second = QueueSupplier([dict(items=[], next=None, terminal=True, truncated=False)])
        scanner = Scanner(self.store, second, precision=1, lookback=0)
        scan_id, _ = scanner.plan_or_resume(8, 10, fresh=True)
        scanner.run(scan_id)
        self.assertEqual(self.store.watermark(), 5)
