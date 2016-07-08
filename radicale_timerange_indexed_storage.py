from datetime import date, datetime, time, timezone

from radicale.storage import Collection as FileSystemCollection
from radicale.xmlutils import _tag
import os
import sqlite3


class Db(object):
    def __init__(self, db_path):
        self._connection = None
        self.db_path = db_path

    @property
    def connection(self):
        if not self._connection:
            must_create = not os.path.exists(self.db_path)
            self._connection = sqlite3.connect(self.db_path)
            if must_create:
                self.create_table()
        return self._connection

    @property
    def cursor(self):
        return self.connection.cursor()

    def create_table(self):
        self.cursor.execute(
            'CREATE TABLE events (href, start, end, load)')
        self.commit()

    def commit(self):
        self.connection.commit()

    def add(self, href, start, end, load):
        self.cursor.execute('INSERT INTO events VALUES (?, ?, ?, ?)', (
            href, start, end, load))
        self.commit()

    def search(self, start, end):
        return self.cursor.execute(
            'SELECT href FROM events WHERE load OR ('
            '? < end and ? > start)', (start, end))


class Collection(FileSystemCollection):
    db_name = 'timerange-index.db.props'

    def __init__(self, path, principal=False):
        super().__init__(path, principal)
        db_path = os.path.join(self._filesystem_path, self.db_name)
        self.db = Db(db_path)

    def dt_to_timestamp(self, dt):
        if dt.tzinfo is None:
            # Naive dates to utc
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()

    def _get_time_range(self, filters):
        for filter_ in filters:
            if filter_.tag == _tag("C", "time-range"):
                return (filter_.get('start'), filter_.get('end'))
            if len(filter_):
                return self._get_time_range(filter_)

    def pre_filtered_list(self, filters):
        # Get time range filter
        time_range = self._get_time_range(filters)
        if not time_range:
            return super().pre_filtered_list(filters)
        start, end = time_range
        start = self.dt_to_timestamp(
            datetime.strptime(start, "%Y%m%dT%H%M%SZ"))
        end = self.dt_to_timestamp(
            datetime.strptime(end, "%Y%m%dT%H%M%SZ"))

        return [self.get(href) for href, in self.db.search(start, end)]

    def upload(self, href, vobject_item):
        item = super().upload(href, vobject_item)
        if hasattr(item.item, 'vevent'):
            vobj = item.item.vevent
        elif hasattr(item.item, 'vtodo'):
            vobj = item.item.vtodo
        elif hasattr(item.item, 'vjournal'):
            vobj = item.item.vjournal
        start = end = None

        if hasattr(vobj, 'dtstart'):
            start = vobj.dtstart.value
            if isinstance(start, date):
                start = datetime.combine(start, time.min)
        else:
            start = datetime.min
        start = start.timestamp()

        if hasattr(vobj, 'dtend'):
            end = vobj.dtend.value
            if isinstance(end, date):
                end = datetime.combine(end, time.max)
        else:
            end = datetime.max
        end = end.timestamp()

        load = bool(getattr(vobj, 'rruleset', False))
        self.db.add(href, start, end, load)

        return item
