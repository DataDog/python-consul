import requests
import os

from consul import base


__all__ = ["Consul"]


class HTTPClient(base.HTTPClient):
    def __init__(self, *args, **kwargs):
        self.timeout = kwargs.pop("timeout", None)
        super(HTTPClient, self).__init__(*args, **kwargs)
        self.session = requests.session()
        self._pid = os.getpid()

    def response(self, response):
        response.encoding = "utf-8"
        return base.Response(response.status_code, response.headers, response.text)

    def _request(self, callback, uri, method, data, timeout=None):
        self._renew_session_on_pid_change()
        if timeout is None:
            timeout = self.timeout
        elif timeout <= 0:
            timeout = None
        return callback(
            self.response(
                self.session.request(
                    method,
                    url=uri,
                    verify=self.verify,
                    data=data,
                    cert=self.cert,
                    timeout=timeout,
                )
            )
        )

    def get(self, callback, path, params=None, timeout=None):
        uri = self.uri(path, params)
        return self._request(
            callback, uri=uri, method="GET", data=None, timeout=timeout
        )

    def put(self, callback, path, params=None, data="", timeout=None):
        uri = self.uri(path, params)
        return self._request(
            callback, uri=uri, method="PUT", data=data, timeout=timeout
        )

    def delete(self, callback, path, params=None, timeout=None):
        uri = self.uri(path, params)
        return self._request(
            callback, uri=uri, method="DELETE", data=None, timeout=timeout
        )

    def post(self, callback, path, params=None, data="", timeout=None):
        uri = self.uri(path, params)
        return self._request(
            callback, uri=uri, method="POST", data=data, timeout=timeout
        )

    def _renew_session_on_pid_change(self):
        """ Check if the pid has changed and create new session if it has"""
        if self.check_pid:
            pid = os.getpid()
            if pid == self._pid:
                return
            self._pid = pid
            self.session = requests.session()


class Consul(base.Consul):
    def connect(
        self, host, port, scheme, verify=True, cert=None, timeout=None, check_pid=False
    ):
        return HTTPClient(
            host, port, scheme, verify, cert, timeout=timeout, check_pid=check_pid
        )
