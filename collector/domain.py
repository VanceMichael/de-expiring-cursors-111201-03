from dataclasses import dataclass


class CursorExpired(ValueError):
    pass


@dataclass
class Catalog:
    rows: list
    page_size: int = 2
    lifetime: int = 5
    cap: int = 20

    def page(self, start, end, clock=0, cursor=None):
        position = 0
        issued = clock
        if cursor:
            issued, position = map(int, cursor.split(':'))
            if clock - issued >= self.lifetime:
                raise CursorExpired('游标已过期')
        candidates = sorted((row for row in self.rows if start <= row['updated'] < end), key=lambda row: (row['updated'], row['id']))
        truncated = len(candidates) > self.cap
        visible = candidates[:self.cap]
        selected = visible[position:position + self.page_size]
        next_position = position + len(selected)
        token = f'{issued}:{next_position}' if next_position < len(visible) else None
        return dict(items=selected, next=token, terminal=token is None, truncated=truncated)


def collect(payload):
    source = Catalog(payload['records'], page_size=payload.get('page_size', 2))
    page = source.page(payload['start'], payload['end'])
    return [dict(key=row['id'], revision=row['revision'], updated=row['updated'], value=row['value']) for row in page['items']]
