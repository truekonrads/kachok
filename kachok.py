#!/usr/bin/env python3
from pathlib import PurePath
from datetime import datetime
import logging
from requests.auth import HTTPBasicAuth
import requests
import multiprocessing as mp
import fire
import glob
# Disable ANSI colours on windows
import os
import sys
import urllib3
from smart_open import open
import boto3
import time
if os.name == 'nt':
    os.environ['ANSI_COLORS_DISABLED'] = "1"
try:
    import ujson as json
except ImportError:
    print("WARNING: ujson not found, install it for better performance")
    import json
from tqdm import tqdm
from urllib.parse import urlparse
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
        if self.verify==False:            
            urllib3.disable_warnings()
        response = method(
            url,
            headers=headers,
            auth=self.auth,
            data=data,
            json=json,
            verify=self.verify)
        if response.status_code != 200:
            self.logger.debug(response)
            self.logger.debug(response.json())
            raise KachokException(
                "Status code for {} is not 200".format(url), response)
        return response

    def __init__(self, endpoint, 
            #index=None, 
            username=None, 
            password=None, 
            debug=False,
            verify=True):
        """
        
        Args:
            username: elasticsearch username
            password: list of files to import
            batchsize: batch size per request
            doctype: document type
            errordir: directory where to output errors (if unspecified, outputs to same directory as file)
            progress: display progress bar
        """     
        self.endopoint = endpoint
        if username and password:
            self.auth = HTTPBasicAuth(username, password)
        else:
            self.auth = None
        #self.index = index
        self.logger = logging.getLogger("kachok")
        if debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)
        self.verify=verify

    def _postBatch(self, path, batch):
        then = datetime.now()
        response = self._post(path, data="\n".join(batch).encode('utf8'))
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

    def pumpJSONND(self, index, *logfiles, batchsize=3200, doctype="securitylogs", errordir=None,progress=True):
        """
        Pump new-line delimited JSON documents to elasticsearch
        Args:
            index: elasticsearch index
            logfiles: list of files to import
            batchsize: batch size per request
            doctype: document type
            errordir: directory where to output errors (if unspecified, outputs to same directory as file)
            progress: display progress bar
        """        
        if len(logfiles)==0:
            self.logger.error("No log files specified")
            return
        all_logs = [item for sublist in [
                    glob.glob(k,recursive=True) for k in logfiles] 
                    for item in sublist]
        for f in logfiles:
            if f.startswith("s3://"):
                all_logs.append(f)            
        files = sorted(set(all_logs))
        # print(files)
        if progress:
            filebar=tqdm(desc='Files',total=len(files))
            linebar=tqdm()

        for filepath in files:

            if filepath.startswith("s3://"):                
                session=boto3.Session()
                self.logger.debug(session.client('sts').get_caller_identity())
                # client= session.client('s3')
                fp=open(filepath,encoding="utf-8",errors='ignore',
                #transport_params={'client': client}
                )
                # print("Opening s3")
            else:                
                if not os.path.isfile(filepath):
                    self.logger.warning(f"Skipping `{filepath}` as it is not a file")
                    continue
                fp = open(filepath,encoding="utf-8",errors='ignore')
            msg=f"Processing `{filepath}`"
            if progress:
                filebar.set_description_str(msg)
                if filepath.startswith("s3://"):
                    p=urlparse(filepath)                    
                    total=session.resource('s3').Object(p.netloc,p.path[1:]).content_length
                else:
                    total=os.path.getsize(filepath)
                linebar.reset(total=total)
            else:
                self.logger.info(msg)

            
            path = "{}/_bulk".format(index)
            accum = []
            head = json.dumps({"index": {"_index": index
            # , "_type": doctype
            }})
            errors = []
            i=0
            try: 
                while True:
                    line=fp.readline()
                    if line=='':
                        break
                    if progress:
                        linebar.update(len(line))
                    accum.append(head+"\n"+line)
                    if len(accum) >= batchsize:
                       reqerr=self._postBatch(path, accum)
                       self.logger.debug(f"Errors: {reqerr}")
                       errors.extend(reqerr)
                       accum = []
                    i+=1
            except UnicodeError as e:
                self.logger.error(f"Unable to decode Unicode {filepath}:{i}")
                raise e
            if accum:  # Some left
                for timeout in (1,2,4,8,16,32,64): 
                    try:
                        e=self._postBatch(path, accum)
                        errors.extend(e)
                        break
                    except KachokException as e:
                        self.logger.warning(f"Error during batch post, sleeping for {timeout} seconds and retrying. Exception {e}")
                        time.sleep(timeout)
                else:
                    raise KachokException("Backpressure continues, timed out after final sleep attempt")
            if errors:
                if errordir:
                    errfile = PurePath(errordir,
                                    PurePath(filepath).name)
                else:
                    errfile = filepath+".err"
                errfp = open(errfile, "w")
                for i, err, line in errors:
                    errfp.write("---\n{} - {}\n{}\n---\n".format(i, err, line))
            if progress:
                filebar.update()

    def makeIndex(self, index, maxfields=2000,shards=16):
        """
        Create a new index. You want to use this before importing data
        Args:
            index: elasticsearch index
            maxfields: maximum number of fields in index, set this to high value
            shards: number of shards for the index            
        """     
        try:
            indexsettings = {
            'settings': {
                'index.number_of_shards': shards
            }
        }

            self._put(index,json=indexsettings)
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
                'index.mapping.total_fields.limit': maxfields,
            }
        }
        self._put(index+"/_settings", json=indexsettings)

def main():
    from fire import Fire
    Fire(Kachok)
if __name__ == "__main__":
    main()
