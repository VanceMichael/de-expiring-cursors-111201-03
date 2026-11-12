"""可重启的目录扫描计划：把半开时间窗切成分片，逐片翻页、细分、封窗。

完整性约定：
- 只有拿到有效终页证据（next 为 None）且窗口未被截断，分片才封窗；
  短暂空页不等于结束，游标未推进则立即停止以防死循环。
- 窗口被截断（truncated）时按时间二分细分；到达最小时间粒度仍超出
  供应商上限的分片明确挂起（suspended），不自称完整，也不会无限自分裂。
- 短效游标失效后只重扫当前分片窗口，而非整库；重复返回的记录按供应商
  主键与修订号消化，旧修订不能盖掉新值（见 storage.Store.ingest）。
- 分片状态持久化在数据库中，进程异常后可用同一入口续跑。
"""
import json

from .domain import CursorExpired


class ScanError(RuntimeError):
    pass


class Scanner:
    def __init__(self, store, supplier, precision=1, lookback=0,
                 max_restarts=5, source='本地目录供应商'):
        self.store = store
        self.supplier = supplier
        self.precision = precision
        self.lookback = lookback
        self.max_restarts = max_restarts
        self.source = source

    def plan_or_resume(self, start=None, end=None, fresh=False):
        """返回 (scan_id, resumed)。存在未完成扫描时默认续跑；
        新扫描未给起点时，按约定回看范围从可靠水位向前回退补采。"""
        if not fresh:
            opened = self.store.latest_open_scan()
            if opened is not None:
                return opened['id'], True
        if start is None:
            watermark = self.store.watermark()
            if watermark is None:
                raise ScanError('首次扫描需要显式起点或脚本 open 事件')
            start = max(0, watermark - self.lookback)
        if end is None or end <= start:
            raise ScanError('扫描需要大于起点的终点')
        return self.store.start_scan(start, end, self.lookback, self.precision), False

    def run(self, scan_id):
        scan = self.store.scan(scan_id)
        if scan is None:
            raise ScanError(f'扫描 {scan_id} 不存在')
        while True:
            shard = self.store.next_shard(scan_id)
            if shard is None:
                break
            self._scan_shard(scan, shard)
        self.store.finish_scan(scan_id)
        self.store.advance_watermark(scan_id)
        return self.store.report(scan_id)

    def _scan_shard(self, scan, shard):
        shard_id = shard['id']
        start, end = shard['start'], shard['end']
        cursor = shard['cursor']
        attempts = shard['attempts']
        pages = shard['pages']
        items = shard['items']
        truncated = False
        self.store.update_shard(shard_id, status='scanning')
        while True:
            try:
                page = self.supplier.page(start, end, cursor=cursor)
            except CursorExpired:
                attempts += 1
                if attempts > self.max_restarts:
                    self.store.update_shard(
                        shard_id, status='suspended', cursor=None, attempts=attempts,
                        note='游标反复失效，重试超过上限，挂起等待人工处理')
                    return
                # 短效游标失效：只从头重扫当前分片窗口，
                # 重翻期间重复返回的记录靠主键与修订号消化
                cursor = None
                truncated = False
                self.store.update_shard(shard_id, cursor=None, attempts=attempts)
                continue
            rows = [dict(key=row['id'], revision=row['revision'],
                         updated=row['updated'], value=row['value'])
                    for row in page['items']]
            raw = json.dumps(page, ensure_ascii=False, sort_keys=True,
                             allow_nan=False).encode('utf-8')
            pages += 1
            items += len(rows)
            # 页原文、导入凭据与分片进度在同一事务提交
            with self.store.transaction():
                self.store.ingest(f'{self.source}#扫描{scan["id"]}:分片{shard_id}', raw, rows)
                self.store.update_shard(shard_id, cursor=page['next'],
                                        pages=pages, items=items)
            truncated = truncated or page['truncated']
            if page['next'] is None:
                # 有效终页证据：未截断才能封窗，截断则细分
                if truncated:
                    self._split_or_suspend(scan, shard_id, start, end)
                else:
                    self.store.update_shard(shard_id, status='complete')
                return
            if page['next'] == cursor:
                raise ScanError('供应商返回的游标没有推进，停止扫描以避免死循环')
            cursor = page['next']

    def _split_or_suspend(self, scan, shard_id, start, end):
        width = end - start
        if width <= self.precision:
            self.store.update_shard(
                shard_id, status='suspended', cursor=None,
                note=f'窗口宽度 {width} 已达最小时间粒度 {self.precision}，'
                     '仍超出供应商单窗上限，挂起且不自称完整')
            return
        mid = start + (width + 1) // 2
        with self.store.transaction():
            self.store.update_shard(shard_id, status='superseded', cursor=None)
            self.store.add_shard(scan['id'], start, mid)
            self.store.add_shard(scan['id'], mid, end)
