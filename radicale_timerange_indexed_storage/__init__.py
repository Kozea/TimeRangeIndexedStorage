from datetime import date, datetime, time, timezone

from radicale.storage import (
    Collection as FileSystemCollection, path_to_filesystem)
from radicale.xmlutils import _tag
import os
import sqlite3
from itertools import groupby
from random import getrandbits
import vobject


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

    def add_all(self, elements):
        self.cursor.executemany(
            'INSERT INTO events VALUES (?, ?, ?, ?)', elements)
        self.commit()

    def list(self):
        return self.cursor.execute('SELECT href, start, end, load FROM events')

    def search(self, start, end):
        return self.cursor.execute(
            'SELECT href FROM events WHERE load OR ('
            '? < end AND ? > start)', (start, end))

    def update(self, href, start, end, load):
        self.cursor.execute(
            'UPDATE events SET start = ?, end = ?, load = ? WHERE href = ?', (
                start, end, load, href))
        self.commit()

    def delete(self, href):
        if href is not None:
            self.cursor.execute('DELETE FROM events WHERE href = ?', (href,))
        else:
            self.cursor.execute('DELETE FROM events')
        self.commit()


class Collection(FileSystemCollection):
    db_name = '.index.db.props'  # TODO: Find a better way to avoid conflicts

    def __init__(self, path, principal=False):
        super().__init__(path, principal)
        db_path = self._filesystem_path + self.db_name
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

    def get_db_params(self, href, item):
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
        start = start.timestamp()

        if hasattr(vobj, 'dtend'):
            end = vobj.dtend.value
            if not isinstance(end, datetime) and isinstance(end, date):
                end = datetime.combine(end, time.max)
        else:
            end = datetime.max
        end = end.timestamp()

        load = bool(getattr(vobj, 'rruleset', False))
        return href, start, end, load

    def upload(self, href, vobject_item):
        item = super().upload(href, vobject_item)
        if item:
            self.db.add(*self.get_db_params(href, item))
        return item

    def update(self, href, vobject_item, etag=None):
        item = super().update(href, vobject_item, etag)
        if item:
            self.db.update(*self.get_db_params(href, item))
        return item

    def delete(self, href=None, etag=None):
        super().delete(href, etag)
        self.db.delete(href)

    @classmethod
    def create_collection(cls, href, collection=None, tag=None):
        # TODO: Improve Radicale api to avoid copy pasta
        folder = os.path.expanduser(
            cls.configuration.get("storage", "filesystem_folder"))
        path = path_to_filesystem(folder, href)
        if not os.path.exists(path):
            os.makedirs(path)
        if not tag and collection:
            tag = collection[0].name
        self = cls(href)
        if tag == "VCALENDAR":
            self.set_meta("tag", "VCALENDAR")
            if collection:
                collection, = collection
                items = []
                for content in ("vevent", "vtodo", "vjournal"):
                    items.extend(getattr(collection, "%s_list" % content, []))

                def get_uid(item):
                    return hasattr(item, 'uid') and item.uid.value

                items_by_uid = groupby(
                    sorted(items, key=get_uid), get_uid)

                added_items = set()
                for uid, items in items_by_uid:
                    new_collection = vobject.iCalendar()
                    for item in items:
                        new_collection.add(item)
                    file_name = hex(getrandbits(32))[2:]
                    item = super(Collection, self).upload(
                        file_name, new_collection)
                    added_items.add(self.get_db_params(file_name, item))
                self.db.add_all(added_items)

        elif tag == "VCARD":
            self.set_meta("tag", "VADDRESSBOOK")
            if collection:
                for card in collection:
                    file_name = hex(getrandbits(32))[2:]
                    self.upload(file_name, card)
        return self
