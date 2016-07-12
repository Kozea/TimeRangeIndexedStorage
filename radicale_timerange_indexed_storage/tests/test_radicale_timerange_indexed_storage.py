import tempfile
import shutil

from radicale import Application
from radicale.tests import BaseTest
from radicale.tests.test_base import BaseRequests, get_file_content
from datetime import datetime, timezone


def ts(dt):
    return dt.replace(tzinfo=timezone.utc).timestamp()


class TestTimeRangeIndexedStorage(BaseRequests, BaseTest):
    """Base class for custom backend tests."""
    storage_type = "radicale_timerange_indexed_storage"

    def setup(self):
        super().setup()
        self.colpath = tempfile.mkdtemp()
        self.configuration.set("storage", "filesystem_folder", self.colpath)
        self.application = Application(self.configuration, self.logger)

    def db(self, path):
        return self.application.Collection(path).db

    def teardown(self):
        shutil.rmtree(self.colpath)

    def test_index_add_event(self):
        """Add an event."""
        self.request("MKCOL", "/calendar.ics/")
        db = self.db('calendar.ics')

        assert len(list(db.list())) == 0

        self.request(
            "PUT", "/calendar.ics/", "BEGIN:VCALENDAR\r\nEND:VCALENDAR")

        assert len(list(db.list())) == 0

        event = get_file_content("event1.ics")
        path = "/calendar.ics/event1.ics"
        status, headers, answer = self.request("PUT", path, event)

        index = list(db.list())
        assert len(index) == 1
        assert index[0] == (
            'event1.ics',
            ts(datetime(2013, 9, 1, 16, 0, 0)),
            ts(datetime(2013, 9, 1, 17, 0, 0)),
            0)

    def test_index_multiple_events_with_same_uid(self):
        """Add two events with the same UID."""
        self.request("MKCOL", "/calendar.ics/")
        db = self.db('calendar.ics')

        self.request("PUT", "/calendar.ics/", get_file_content("event2.ics"))
        status, headers, answer = self.request(
            "REPORT", "/calendar.ics/",
            '<?xml version="1.0" encoding="utf-8" ?>'
            '<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav">'
            '</C:calendar-query>')

        # Hardcore parsing action
        uid = answer.split('href')[1][1:-2].replace('/calendar.ics/', '')

        index = list(db.list())
        assert len(index) == 1
        assert index[0] == (
            uid,
            ts(datetime(2013, 9, 2, 16, 0, 0)),
            ts(datetime(2013, 9, 2, 17, 0, 0)),
            1)

    def test_index_update(self):
        """Update an event."""
        self.request("MKCOL", "/calendar.ics/")
        db = self.db('calendar.ics')

        assert len(list(db.list())) == 0

        self.request(
            "PUT", "/calendar.ics/", "BEGIN:VCALENDAR\r\nEND:VCALENDAR")
        event = get_file_content("event1.ics")
        path = "/calendar.ics/event1.ics"
        status, headers, answer = self.request("PUT", path, event)
        assert status == 201

        index = list(db.list())
        assert len(index) == 1
        assert index[0] == (
            'event1.ics',
            ts(datetime(2013, 9, 1, 16, 0, 0)),
            ts(datetime(2013, 9, 1, 17, 0, 0)),
            0)

        # Then we send another PUT request
        event = get_file_content("event1-prime.ics")
        status, headers, answer = self.request("PUT", path, event)
        assert status == 201

        index = list(db.list())
        assert len(index) == 1
        assert index[0] == (
            'event1.ics',
            ts(datetime(2014, 9, 1, 16, 0, 0)),
            ts(datetime(2014, 9, 1, 19, 0, 0)),
            0)

    def test_index_delete(self):
        """Delete an event."""
        self.request("MKCOL", "/calendar.ics/")
        db = self.db('calendar.ics')

        assert len(list(db.list())) == 0

        self.request(
            "PUT", "/calendar.ics/", "BEGIN:VCALENDAR\r\nEND:VCALENDAR")
        event = get_file_content("event1.ics")
        path = "/calendar.ics/event1.ics"
        status, headers, answer = self.request("PUT", path, event)

        assert len(list(db.list())) == 1

        # Then we send a DELETE request
        status, headers, answer = self.request("DELETE", path)

        assert len(list(db.list())) == 0
