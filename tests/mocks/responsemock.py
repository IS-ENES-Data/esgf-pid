
class MockRequest(object):
    '''
    This is a mocked Request object containing only an url,
    as this is the only attribute accessed during the tests.
    There is a default value for it, but it can also be passed.
    '''
    def __init__(self, url=None):
        if url is not None:
            self.url = url
        else:
            self.url = 'http://foo.foo'

class MockSolrResponse(object):
    '''
    This is a mocked Response object (can be used to replace
    a response from any call to "requests.get" or
    "request.put" or "request.delete", ...).

    It contains a request, a status code and some JSON content.
    For all of these, there is default values, but they can also
    be passed.

    Some standard cases are available, e.g. or "handle not found",
    which has a specific combination of HTTP status code, handle
    response code and content.
    '''
    def __init__(self, status_code=None, content=None, request=None, success=False, notfound=False, empty=False):

        self.content = None
        self.status_code = None
        self.request = None

        # Some predefined cases:
        if success:
            self.status_code = 200
            self.content = '{"responseHeader":{}, "response":{}, "facet_counts": {"facet_fields": {"bla": ["blub",1,"miau",4]}}}'
        elif notfound:
            self.status_code = 404
            self.content = ''
        # User-defined overrides predefined cases:
        if content is not None:
            self.content = content
        if status_code is not None:
            self.status_code = status_code
        if request is not None:
            self.request = request
        # Defaults (they do not override):
        if self.content is None:    
            self.content = '{"responseHeader":{}, "response":{}, "facet_counts": {}}'
        if self.status_code is None:
            self.status_code = 200
        if self.request is None:
            self.request = MockRequest()
        # Special case: Content should be None:
        if self.content is 'NONE':
            self.content = None
