#!/usr/bin/env python3
from pathlib import PurePath
from datetime import datetime
import logging
from requests.auth import HTTPBasicAuth
import requests
import multiprocessing as mp
import fire
# Disable ANSI colours on windows
import os
if os.name == 'nt':
    os.environ['ANSI_COLORS_DISABLED'] = "1"
try:
    import ujson as json
except ImportError:
    print("WARNING: ujson not found, install it for better performance")
    import json

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class KachokException(Exception):
    pass


class Kachok(object):

    def _put(self, *args, **kwargs):
        return self._callElasticSearch("PUT", *args, **kwargs)

    def _post(self, *args, **kwargs):
        return self._callElasticSearch("POST", *args, **kwargs)

    def _callElasticSearch(self, method, path, data=None, json=None):
        method = getattr(requests, method.lower())
        headers = {'content-type': 'application/json', 'charset': 'UTF-8'}
        url = "{}/{}".format(self.endopoint, path)
        self.logger.debug("{}: {}".format(method, url))
        response = method(
            url,
            headers=headers,
            auth=self.auth,
            data=data,
            json=json)
        if response.status_code != 200:
            self.logger.debug(response)
            self.logger.debug(response.json())
            raise KachokException(
                "Status code for {} is not 200".format(url), response)
        return response

    def __init__(self, endpoint, index=None, username=None, password=None, debug=False):
        self.endopoint = endpoint
        if username and password:
            self.auth = HTTPBasicAuth(username, password)
        else:
            self.auth = None
        self.index = index
        self.logger = logging.getLogger("kachok")
        if debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

    def _postBatch(self, path, batch):
        then = datetime.now()
        response = self._post(path, data="\n".join(batch))
        body = response.content
        errors = []
        if b'"errors":false' in body:
            self.logger.debug("Request submitted without errors")
        elif b'"errors":true' in body:
            self.logger.warning("Request submitted WITH errors")
            j = json.loads(body.decode("utf8"))
            for i, item in enumerate(j['items']):
                if not (200 <= item['index']['status'] < 300):
                    errors.append((i, item['index'], batch[i]))
        else:
            print(body[:100])
            raise Exception("WTF")

        delta = (datetime.now()-then).total_seconds()
        self.logger.debug(
            "Posted a batch of {} in {} seconds".format(len(batch), delta))
        return errors

    def pumpJSONND(self, index, filepath, batchsize=3200, doctype="securitylogs", errordir=None):
        fp = open(filepath)
        path = "{}/_bulk".format(index)
        accum = []
        head = json.dumps({"index": {"_index": index, "_type": doctype}})
        errors = []
        for i, line in enumerate(fp.readlines()):
            accum.append(head+"\n"+line)
            if len(accum) >= batchsize:
                errors.extend(self._postBatch(path, accum))
                accum = []
        if accum:  # Some left
            errors.extend(self._postBatch(path, accum))
        if errors:
            if errordir:
                errfile = PurePath(errordir,
                                   PurePath(filepath).name)
            else:
                errfile = filepath+".err"
            errfp = open(errfile, "w")
            for i, err, line in errors:
                errfp.write("---\n{} - {}\n{}\n---\n".format(i, err, line))

    def makeIndex(self, index, maxfields=2000):
        try:
            self._put(index)
        except KachokException as e:
            response = e.args[1]
            assert type(response) == requests.Response
            if response.status_code == 400:
                if e.args[1].json()['error']['type'] == "resource_already_exists_exception":
                    self.logger.debug("Index {} already exists".format(index))
                else:
                    raise e
            else:
                raise e
        indexsettings = {
            'settings': {
                'index.mapping.total_fields.limit': maxfields
            }
        }
        self._put(index+"/_settings", json=indexsettings)


if __name__ == "__main__":
    from fire import Fire
    Fire(Kachok)
