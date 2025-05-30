
class MockResponse(object):
    """Class used to mock HTTP responses"""

    def __init__(self, json_data, status_code, **kwargs):
        """Sets data, status code, and any other data passed in."""
        self.json_data = json_data
        self.status_code = status_code
        self.text = "v4.0.0"
        for k in kwargs:
            setattr(self, k, kwargs[k])

    def json(self):
        """Mocks the json method of an HTTP response"""
        return self.json_data

    def raise_for_status(self):
        pass
