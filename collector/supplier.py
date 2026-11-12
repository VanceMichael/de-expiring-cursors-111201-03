"""本地模拟供应商：把分页脚本变成可分页的目录来源，不访问真实账户。

脚本（如 fixtures/windows.json）字段：
- records：初始记录；page_size / cap / lifetime：供应商分页参数；
- precision：最小时间粒度；lookback：约定的回看范围；
- events：时间线。每次分页请求前，时钟推进到下一事件时刻并应用该时刻
  全部事件。open / continue 只是时钟标记；replace 按主键改写修订号与
  更新时间；insert 追加新记录。requests 保留每次请求的窗口与游标，
  供核对调用顺序。
"""
from .domain import Catalog


class ScriptedSupplier:
    def __init__(self, script):
        self.rows = [dict(row) for row in script.get('records', [])]
        self.page_size = script.get('page_size', 2)
        self.cap = script.get('cap', 20)
        self.lifetime = script.get('lifetime', 5)
        self.precision = script.get('precision', 1)
        self.lookback = script.get('lookback', 0)
        self.events = sorted(script.get('events', []), key=lambda event: event['clock'])
        self.clock = 0
        self.requests = []

    def _apply_due_events(self):
        if not self.events:
            return
        upcoming = self.events[0]['clock']
        if upcoming > self.clock:
            self.clock = upcoming
        while self.events and self.events[0]['clock'] <= self.clock:
            event = self.events.pop(0)
            if event['type'] == 'replace':
                for row in self.rows:
                    if row['id'] == event['id']:
                        row['revision'] = event['revision']
                        row['updated'] = event['updated']
                        if 'value' in event:
                            row['value'] = event['value']
            elif event['type'] == 'insert':
                self.rows.append(dict(event['record']))

    def page(self, start, end, cursor=None):
        self._apply_due_events()
        self.requests.append(dict(start=start, end=end, cursor=cursor, clock=self.clock))
        catalog = Catalog(self.rows, page_size=self.page_size,
                          lifetime=self.lifetime, cap=self.cap)
        return catalog.page(start, end, clock=self.clock, cursor=cursor)
