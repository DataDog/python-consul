import requests
import os

from consul import base


__all__ = ["Consul"]


class HTTPClient(base.HTTPClient):
    def __init__(self, *args, **kwargs):
        super(HTTPClient, self).__init__(*args, **kwargs)
        self.session = requests.session()
        self._pid = os.getpid()

    def response(self, response):
        response.encoding = "utf-8"
        return base.Response(response.status_code, response.headers, response.text)

    def get(self, callback, path, params=None):
        uri = self.uri(path, params)
        self._renew_session_on_pid_change()
        return callback(self.response(self.session.get(uri, verify=self.verify, cert=self.cert)))

    def put(self, callback, path, params=None, data=""):
        uri = self.uri(path, params)
        self._renew_session_on_pid_change()
        return callback(self.response(self.session.put(uri, data=data, verify=self.verify, cert=self.cert)))

    def delete(self, callback, path, params=None):
        uri = self.uri(path, params)
        self._renew_session_on_pid_change()
        return callback(self.response(self.session.delete(uri, verify=self.verify, cert=self.cert)))

    def post(self, callback, path, params=None, data=""):
        uri = self.uri(path, params)
        self._renew_session_on_pid_change()
        return callback(self.response(self.session.post(uri, data=data, verify=self.verify, cert=self.cert)))

    def _renew_session_on_pid_change(self):
        """ Check if the pid has changed and create new session if it has"""
        if self.check_pid:
            pid = os.getpid()
            if pid == self._pid:
                return
            self._pid = pid
            self.session = requests.session()


class Consul(base.Consul):
    def connect(self, host, port, scheme, verify=True, cert=None, check_pid=False):
        return HTTPClient(host, port, scheme, verify, cert, check_pid=check_pid)
