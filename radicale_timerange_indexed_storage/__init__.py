from datetime import date, datetime, time, timezone

from radicale.storage import (
    Collection as FileSystemCollection, path_to_filesystem)
from radicale.xmlutils import _tag
import os
import sqlite3
from itertools import groupby
from random import getrandbits


class Db(object):
    def __init__(self, folder, file_name=".Radicale.index.db"):
        self._connection = None
        self.db_path = os.path.join(folder, file_name)

    @property
    def connection(self):
        if not self._connection:
            must_create_table = False
            if not os.path.exists(self.db_path):
                must_create_table = True
                os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self._connection = sqlite3.connect(self.db_path)
            if must_create_table:
                self.create_table()
        return self._connection

    @property
    def cursor(self):
        return self.connection.cursor()

    def create_table(self):
        self.cursor.execute('CREATE TABLE events (href, start, end, load)')
        self.connection.commit()

    def add(self, href, start, end, load):
        self.cursor.execute('INSERT INTO events VALUES (?, ?, ?, ?)', (
                href, start, end, load))
        self.connection.commit()

    def add_all(self, elements):
        self.cursor.executemany(
            'INSERT INTO events VALUES (?, ?, ?, ?)', elements)
        self.connection.commit()

    def list(self):
        try:
            for result in self.cursor.execute(
                    'SELECT href, start, end, load FROM events'):
                yield result
        finally:
            self.connection.rollback()

    def search(self, start, end):
        try:
            for result in self.cursor.execute(
                    'SELECT href FROM events WHERE load OR ('
                    '? <= end AND ? >= start)', (start, end)):
                yield result
        finally:
            self.connection.rollback()

    def update(self, href, start, end, load):
        self.cursor.execute(
            'UPDATE events SET start = ?, end = ?, load = ? WHERE href = ?', (
                start, end, load, href))
        self.connection.commit()

    def delete(self, href):
        if href is not None:
            self.cursor.execute('DELETE FROM events WHERE href = ?', (href,))
        else:
            self.cursor.execute('DELETE FROM events')
        self.connection.commit()


class Collection(FileSystemCollection):
    def __init__(self, path, principal=False, folder=None):
        super().__init__(path, principal, folder)
        self.db = Db(self._filesystem_path)

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
        if start:
            start = self.dt_to_timestamp(
                datetime.strptime(start, "%Y%m%dT%H%M%SZ"))
        else:
            start = datetime.min
        if end:
            end = self.dt_to_timestamp(
                datetime.strptime(end, "%Y%m%dT%H%M%SZ"))
        else:
            end = datetime.max

        return [self.get(href) for href, in self.db.search(start, end)]

    def get_db_params(self, item):
        if hasattr(item.item, 'vevent'):
            vobj = item.item.vevent
        elif hasattr(item.item, 'vtodo'):
            vobj = item.item.vtodo
        elif hasattr(item.item, 'vjournal'):
            vobj = item.item.vjournal
        start = end = None

        if hasattr(vobj, 'dtstart'):
            start = vobj.dtstart.value
            if not isinstance(start, datetime) and isinstance(start, date):
                start = datetime.combine(start, time.min)
        else:
            start = datetime.min
        start = self.dt_to_timestamp(start)

        if hasattr(vobj, 'dtend'):
            end = vobj.dtend.value
            if not isinstance(end, datetime) and isinstance(end, date):
                end = datetime.combine(end, time.max)
        else:
            end = datetime.max
        end = self.dt_to_timestamp(end)

        load = bool(getattr(vobj, 'rruleset', False))
        return item.href, start, end, load

    def upload(self, href, vobject_item):
        item = super().upload(href, vobject_item)
        if item:
            self.db.add(*self.get_db_params(item))
        return item

    def upload_all(self, collections):
        # TODO: See why super() does not work
        self.db.add_all([
            self.get_db_params(
                super(Collection, self).upload(href, vobject_item)
            ) for href, vobject_item in collections.items()
        ])

    def update(self, href, vobject_item):
        item = super().update(href, vobject_item)
        if item:
            self.db.update(*self.get_db_params(item))
        return item

    def delete(self, href=None):
        self.db.delete(href)
        super().delete(href)
