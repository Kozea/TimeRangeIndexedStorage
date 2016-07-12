from datetime import date, datetime, time, timezone

from radicale.storage import (
    Collection as FileSystemCollection, path_to_filesystem, get_etag, Item)
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
            'CREATE TABLE events (start, end, load, href, raw)')
        self.commit()

    def commit(self):
        self.connection.commit()

    def add(self, start, end, load, href, raw):
        self.cursor.execute('INSERT INTO events VALUES (?, ?, ?, ?, ?)', (
            start, end, load, href, raw))
        self.commit()

    def add_all(self, elements):
        self.cursor.executemany(
            'INSERT INTO events VALUES (?, ?, ?, ?, ?)', elements)
        self.commit()

    def search(self, start, end):
        return self.cursor.execute(
            'SELECT href, raw FROM events WHERE load OR ('
            '? < end AND ? > start)', (start, end))

    def list(self):
        return self.cursor.execute(
            'SELECT href, raw FROM events')

    def get(self, href):
        raws = self.cursor.execute(
            'SELECT raw FROM events WHERE href = ?', (href,)).fetchone()
        if raws:
            return raws[0]

    def update(self, href, raw):
        self.cursor.execute(
            'UPDATE events SET raw = ? WHERE href = ?', (raw, href))
        self.commit()

    def delete(self, href):
        if href:
            self.cursor.execute(
                'DELETE FROM events WHERE href = ?', (href,))
        self.cursor.execute(
            'DELETE FROM events')
        self.commit()


class Collection(FileSystemCollection):
    db_name = '.index.db.props'  # TODO: Find a better way to avoid conflicts

    def __init__(self, path, principal=False):
        self.path = path
        folder = os.path.expanduser(
            self.configuration.get("storage", "filesystem_folder"))
        self._filesystem_path = path_to_filesystem(folder, self.path)
        self.storage_encoding = self.configuration.get("encoding", "stock")
        self.db_path = self._filesystem_path + self.db_name
        print(self.db_path)
        self.db = Db(self.db_path)
        self.is_principal = principal

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

        return self.itemize(self.db.search(start, end))

    def without_none(self, it):
        for i in it:
            if i is not None:
                yield i

    def first(self, it):
        for i, in it:
            yield i

    def itemize(self, it):
        for href, raw in it:
            yield Item(self, vobject.readOne(raw), href)

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
        raw = item.serialize()
        return start, end, load, href, raw

    @classmethod
    def discover(cls, path, depth="1"):
        return cls(path),

    @classmethod
    def create_collection(cls, href, collection=None, tag=None):
        # TODO: Improve Radicale api to avoid copy pasta
        folder = os.path.expanduser(
            cls.configuration.get("storage", "filesystem_folder"))
        if not os.path.exists(folder):
            os.makedirs(folder)

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
                    item = Item(self, new_collection, href)
                    added_items.add(self.get_db_params(file_name, item))
                self.db.add_all(added_items)

        elif tag == "VCARD":
            self.set_meta("tag", "VADDRESSBOOK")
            if collection:
                for card in collection:
                    file_name = hex(getrandbits(32))[2:]
                    self.upload(file_name, card)
        return self

    def list(self):
        for href, raw in self.db.list():
            yield href, get_etag(raw)

    def get(self, href):
        if not href:
            return
        item, = self.db.get(href)
        if not item:
            return

        return Item(self, vobject.readOne(item), href)

    def has(self, href):
        return self.db.get(href) is not None

    def upload(self, href, vobject_item):
        item = Item(self, vobject_item, href)
        self.db.add(*self.get_db_params(href, item))
        return item

    def update(self, href, vobject_item, etag=None):
        item = Item(self, vobject_item, href)
        self.db.update(href, item.serialize())
        return item

    def delete(self, href=None, etag=None):
        self.db.delete(href)

    def serialize(self):
        items = []
        for href, raw in self.db.list():
            items.append(vobject.readOne(raw))
        if self.get_meta("tag") == "VCALENDAR":
            collection = vobject.iCalendar()
            for item in items:
                for content in ("vevent", "vtodo", "vjournal"):
                    if content in item.contents:
                        for item_part in getattr(item, "%s_list" % content):
                            collection.add(item_part)
                        break
            return collection.serialize()
        elif self.get_meta("tag") == "VADDRESSBOOK":
            return "".join([item.serialize() for item in items])
        return ""
