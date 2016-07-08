from radicale import Application
from radicale.tests import BaseTest
from radicale.tests.test_base import BaseRequests


class TestTimeRangeIndexedStorage(BaseRequests, BaseTest):
    """Base class for custom backend tests."""
    storage_type = "radicale_timerange_indexed_storage"

    def setup(self):
        super().setup()
        self.application = Application(self.configuration, self.logger)
