import hashlib
import json
import sqlite3
from contextlib import contextmanager


class Store:
    def __init__(self, path):
        self.connection = sqlite3.connect(path, timeout=10, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA journal_mode=WAL")
        self._savepoint_seq = 0
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS raw_objects(
                digest TEXT PRIMARY KEY, payload BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS imports(
                id INTEGER PRIMARY KEY, source TEXT NOT NULL,
                digest TEXT NOT NULL REFERENCES raw_objects(digest),
                UNIQUE(source,digest));
            CREATE TABLE IF NOT EXISTS records(
                record_key TEXT PRIMARY KEY, payload TEXT NOT NULL,
                import_id INTEGER NOT NULL REFERENCES imports(id));
            CREATE TABLE IF NOT EXISTS scans(
                id INTEGER PRIMARY KEY,
                start INTEGER NOT NULL, end INTEGER NOT NULL,
                lookback INTEGER NOT NULL, precision INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'running',
                complete INTEGER NOT NULL DEFAULT 0,
                created TEXT NOT NULL DEFAULT (datetime('now')));
            CREATE TABLE IF NOT EXISTS shards(
                id INTEGER PRIMARY KEY,
                scan_id INTEGER NOT NULL REFERENCES scans(id),
                start INTEGER NOT NULL, end INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                cursor TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                pages INTEGER NOT NULL DEFAULT 0,
                items INTEGER NOT NULL DEFAULT 0,
                note TEXT);
            CREATE TABLE IF NOT EXISTS watermark(
                id INTEGER PRIMARY KEY CHECK(id=1),
                value INTEGER);
            INSERT OR IGNORE INTO watermark(id,value) VALUES (1,NULL);
        """)

    @contextmanager
    def transaction(self):
        if self.connection.in_transaction:
            self._savepoint_seq += 1
            name = f'sp_{self._savepoint_seq}'
            self.connection.execute(f'SAVEPOINT {name}')
            try:
                yield
            except BaseException:
                self.connection.execute(f'ROLLBACK TO SAVEPOINT {name}')
                self.connection.execute(f'RELEASE SAVEPOINT {name}')
                raise
            else:
                self.connection.execute(f'RELEASE SAVEPOINT {name}')
            return
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def ingest(self, source, raw, records):
        digest = hashlib.sha256(raw).hexdigest()
        with self.transaction():
            self.connection.execute("INSERT OR IGNORE INTO raw_objects VALUES (?,?)", (digest, raw))
            self.connection.execute("INSERT OR IGNORE INTO imports(source,digest) VALUES (?,?)", (source, digest))
            import_id = self.connection.execute("SELECT id FROM imports WHERE source=? AND digest=?", (source, digest)).fetchone()[0]
            for record in records:
                key = record.get('key')
                if not isinstance(key, str) or not key:
                    raise ValueError('记录缺少有效标识')
                revision = record.get('revision')
                if revision is not None:
                    if isinstance(revision, bool) or not isinstance(revision, int):
                        raise ValueError('修订号必须为整数')
                    row = self.connection.execute("SELECT payload FROM records WHERE record_key=?", (key,)).fetchone()
                    if row is not None:
                        current = json.loads(row[0]).get('revision')
                        if isinstance(current, int) and not isinstance(current, bool) and current >= revision:
                            # 重复返回或旧修订：消化掉，不允许盖掉新值
                            continue
                encoded = json.dumps(record, ensure_ascii=False, sort_keys=True, allow_nan=False)
                self.connection.execute("INSERT INTO records VALUES (?,?,?) ON CONFLICT(record_key) DO UPDATE SET payload=excluded.payload, import_id=excluded.import_id", (key, encoded, import_id))
        return digest

    def records(self):
        return [json.loads(row[0]) for row in self.connection.execute("SELECT payload FROM records ORDER BY record_key")]

    def provenance(self, key):
        row = self.connection.execute("SELECT imports.source, imports.digest FROM records JOIN imports ON imports.id=records.import_id WHERE record_key=?", (key,)).fetchone()
        return dict(row) if row else None

    # ---- 扫描计划状态 ----

    def start_scan(self, start, end, lookback, precision):
        with self.transaction():
            cursor = self.connection.execute(
                "INSERT INTO scans(start,end,lookback,precision) VALUES (?,?,?,?)",
                (start, end, lookback, precision))
            scan_id = cursor.lastrowid
            self.connection.execute(
                "INSERT INTO shards(scan_id,start,end) VALUES (?,?,?)", (scan_id, start, end))
        return scan_id

    def scan(self, scan_id):
        return self.connection.execute("SELECT * FROM scans WHERE id=?", (scan_id,)).fetchone()

    def latest_open_scan(self):
        return self.connection.execute(
            "SELECT * FROM scans WHERE status='running' ORDER BY id DESC LIMIT 1").fetchone()

    def latest_scan(self):
        return self.connection.execute("SELECT * FROM scans ORDER BY id DESC LIMIT 1").fetchone()

    def scans(self):
        return list(self.connection.execute(
            "SELECT id,start,end,lookback,precision,status,complete,created FROM scans ORDER BY id"))

    def next_shard(self, scan_id):
        return self.connection.execute(
            "SELECT * FROM shards WHERE scan_id=? AND status IN ('pending','scanning')"
            " ORDER BY start,end,id LIMIT 1", (scan_id,)).fetchone()

    def shards(self, scan_id):
        return list(self.connection.execute(
            "SELECT * FROM shards WHERE scan_id=? ORDER BY start,end,id", (scan_id,)))

    def leaf_shards(self, scan_id):
        return list(self.connection.execute(
            "SELECT * FROM shards WHERE scan_id=? AND status!='superseded' ORDER BY start,end,id",
            (scan_id,)))

    _SHARD_FIELDS = frozenset({'status', 'cursor', 'attempts', 'pages', 'items', 'note'})

    def update_shard(self, shard_id, **fields):
        unknown = set(fields) - self._SHARD_FIELDS
        if unknown:
            raise ValueError(f'未知分片字段: {sorted(unknown)}')
        assignments = ', '.join(f'{name}=?' for name in fields)
        self.connection.execute(
            f'UPDATE shards SET {assignments} WHERE id=?', (*fields.values(), shard_id))

    def add_shard(self, scan_id, start, end):
        self.connection.execute(
            "INSERT INTO shards(scan_id,start,end) VALUES (?,?,?)", (scan_id, start, end))

    def finish_scan(self, scan_id):
        complete = all(shard['status'] == 'complete' for shard in self.leaf_shards(scan_id))
        with self.transaction():
            self.connection.execute(
                "UPDATE scans SET status='done', complete=? WHERE id=?",
                (1 if complete else 0, scan_id))
        return complete

    # ---- 可靠水位 ----

    def watermark(self):
        row = self.connection.execute("SELECT value FROM watermark WHERE id=1").fetchone()
        return row[0] if row else None

    def advance_watermark(self, scan_id):
        scan = self.scan(scan_id)
        current = self.watermark()
        if current is not None and scan['start'] > current:
            # 水位与本次扫描之间存在未核实空洞，不得前移
            return current
        position = scan['start'] if current is None else current
        for leaf in self.leaf_shards(scan_id):
            if leaf['end'] <= position:
                continue
            if leaf['start'] > position or leaf['status'] != 'complete':
                break
            position = leaf['end']
        if current is None or position > current:
            with self.transaction():
                self.connection.execute("UPDATE watermark SET value=? WHERE id=1", (position,))
            return position
        return current

    def incomplete_intervals(self):
        watermark = self.watermark()
        rows = self.connection.execute(
            "SELECT scan_id,start,end,status,note FROM shards"
            " WHERE status NOT IN ('superseded','complete') AND (? IS NULL OR end > ?)"
            " ORDER BY start,end,scan_id DESC", (watermark, watermark)).fetchall()
        seen = set()
        intervals = []
        for row in rows:
            interval = (row['start'], row['end'])
            if interval in seen:
                continue
            seen.add(interval)
            intervals.append(dict(start=row['start'], end=row['end'], status=row['status'],
                                  note=row['note'], scan=row['scan_id']))
        return intervals

    def report(self, scan_id):
        scan = self.scan(scan_id)
        leaves = self.leaf_shards(scan_id)
        return dict(
            scan=scan_id,
            range=[scan['start'], scan['end']],
            status=scan['status'],
            complete=bool(scan['complete']),
            watermark=self.watermark(),
            coverage=[dict(start=leaf['start'], end=leaf['end'], status=leaf['status'],
                           pages=leaf['pages'], items=leaf['items'],
                           attempts=leaf['attempts'], note=leaf['note']) for leaf in leaves],
            incomplete=self.incomplete_intervals(),
            records=len(self.records()),
        )

    def close(self):
        self.connection.close()
