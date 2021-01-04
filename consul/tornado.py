from __future__ import absolute_import

from tornado import httpclient
from tornado import gen

from consul import base


__all__ = ['Consul']


class HTTPClient(base.HTTPClient):
    def __init__(self, *args, **kwargs):
        self.timeout = kwargs.pop('timeout', None)
        super(HTTPClient, self).__init__(*args, **kwargs)
        self.client = httpclient.AsyncHTTPClient()

    def response(self, response):
        return base.Response(
            response.code, response.headers, response.body.decode('utf-8'))

    @gen.coroutine
    def _request(self, callback, request):
        try:
            response = yield self.client.fetch(request)
        except httpclient.HTTPError as e:
            if e.code == 599:
                raise base.Timeout
            response = e.response
        raise gen.Return(callback(self.response(response)))

    def get(self, callback, path, params=None, timeout=None):
        uri = self.uri(path, params)
        timeout = timeout if timeout is not None else self.timeout
        request = httpclient.HTTPRequest(uri, method='GET',
                                         validate_cert=self.verify, 
                                         request_timeout=timeout)
        return self._request(callback, request)

    def put(self, callback, path, params=None, data='', timeout=None):
        uri = self.uri(path, params)
        timeout = timeout if timeout is not None else self.timeout
        request = httpclient.HTTPRequest(uri, method='PUT',
                                         body='' if data is None else data,
                                         validate_cert=self.verify, 
                                         request_timeout=timeout)
        return self._request(callback, request)

    def delete(self, callback, path, params=None, timeout=None):
        uri = self.uri(path, params)
        timeout = timeout if timeout is not None else self.timeout
        request = httpclient.HTTPRequest(uri, method='DELETE',
                                         validate_cert=self.verify, 
                                         request_timeout=timeout)
        return self._request(callback, request)

    def post(self, callback, path, params=None, data='', timeout=None):
        uri = self.uri(path, params)
        timeout = timeout if timeout is not None else self.timeout
        request = httpclient.HTTPRequest(uri, method='POST', body=data,
                                         validate_cert=self.verify, 
                                         request_timeout=timeout)
        return self._request(callback, request)


class Consul(base.Consul):
    def connect(self, host, port, scheme, verify=True, cert=None, timeout=None, check_pid=None):
        return HTTPClient(host, port, scheme, verify=verify, cert=cert, timeout=None)
