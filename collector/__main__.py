import argparse
import json
from pathlib import Path
import tempfile

from .domain import collect
from .scan import Scanner
from .storage import Store
from .supplier import ScriptedSupplier
from .transport import Client, replay_server

# 扫描结束但存在挂起分片时的退出码：结果不完整，不得与完整封窗混淆
INCOMPLETE_EXIT = 3


def run_scan(store, payload, arguments):
    supplier = ScriptedSupplier(payload if isinstance(payload, dict) else {})
    scanner = Scanner(store, supplier, precision=supplier.precision,
                      lookback=supplier.lookback)
    start, end = arguments.start, arguments.end
    opened = next((event for event in supplier.events
                   if event.get('type') == 'open'), None)
    if end is None and opened is not None:
        end = opened['end']
    if start is None and opened is not None and store.watermark() is None:
        # 仅在还没有可靠水位时采用脚本给定的初始起点；
        # 水位建立后由 plan_or_resume 按约定回看范围推导起点
        start = opened['start']
    scan_id, resumed = scanner.plan_or_resume(start, end, fresh=arguments.fresh)
    report = scanner.run(scan_id)
    report['resumed'] = resumed
    return report


def coverage_report(store):
    latest = store.latest_scan()
    if latest is None:
        return dict(watermark=None, scans=[], incomplete=[], records=len(store.records()))
    report = store.report(latest['id'])
    report['scans'] = [dict(row) for row in store.scans()]
    return report


def main():
    parser = argparse.ArgumentParser(description='本地采集与凭据查看')
    parser.add_argument('command', choices=['demo', 'import', 'show', 'serve', 'scan', 'coverage'])
    parser.add_argument('--input', default='fixtures/sample.json')
    parser.add_argument('--db', default='var/records.sqlite3')
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--start', type=int, default=None)
    parser.add_argument('--end', type=int, default=None)
    parser.add_argument('--fresh', action='store_true', help='忽略未完成的扫描，另起新计划')
    arguments = parser.parse_args()
    if arguments.command == 'serve':
        from .api import serve
        serve(arguments.db, arguments.port)
        return
    if arguments.command == 'demo':
        with tempfile.TemporaryDirectory() as directory:
            payload = Path(arguments.input).read_bytes()
            with replay_server({'/source': [dict(body=payload.decode('utf-8'))]}) as (base, calls):
                status, headers, raw = Client().request(base + '/source')
                if status != 200:
                    raise RuntimeError('本地来源未返回成功响应')
                store = Store(Path(directory) / 'demo.sqlite3')
                rows = collect(json.loads(raw))
                digest = store.ingest('本地演示来源', raw, rows)
                print(json.dumps(dict(digest=digest, records=store.records(), requests=len(calls)), ensure_ascii=False, indent=2))
                store.close()
        return
    Path(arguments.db).parent.mkdir(parents=True, exist_ok=True)
    store = Store(arguments.db)
    try:
        if arguments.command == 'coverage':
            print(json.dumps(coverage_report(store), ensure_ascii=False, indent=2))
            return
        if arguments.command in ('import', 'scan'):
            raw = Path(arguments.input).read_bytes()
            payload = json.loads(raw)
            if arguments.command == 'scan' or (isinstance(payload, dict) and 'events' in payload):
                # 带事件时间线的输入视为本地供应商分页脚本，走可续跑的扫描导入
                report = run_scan(store, payload, arguments)
                print(json.dumps(report, ensure_ascii=False, indent=2))
                raise SystemExit(0 if report['complete'] else INCOMPLETE_EXIT)
            store.ingest(str(Path(arguments.input)), raw, collect(payload))
        print(json.dumps(store.records(), ensure_ascii=False, indent=2))
    finally:
        store.close()


if __name__ == '__main__':
    main()
