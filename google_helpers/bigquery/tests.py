#
# Copyright 2015-2019, Institute for Systems Biology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from django.test import TestCase
from .bq_support import BigQuerySupport
import re
import json
#import unittest
from googleapiclient.http import HttpMock, HttpMockSequence

class BQTest(TestCase):
    def setUp(self):
        # Shut this up unless we need to do debug of HTTP request contents
        #import httplib2
        #httplib2.debuglevel = 1
        pass


    def test_hello_world(self):
        print("hello world")

        #
        # get the mocking set up:
        #

        response = json.dumps([{'f': [{'v': 'foo'}, {'v': 'bar'}]}, {'f': [{'v': 'bar'}, {'v': 'foo'}]},
                    {'f': [{'v': 'foobar'}, {'v': 'barfoo'}]}])

        #url = re.compile('https://bigquery.googleapis.com/bigquery/v2/projects/idc-dev/queries/.*?location=US&alt=json')
        #self.requests.mock('https://www.googleapis.ccom/oauth2/v4/token', status=200)
        #self.requests.mock('https://www.googleapis.ccom/discovery/v1/apis/bigquery/v2/rest', status=200)
        #self.requests.mock('https://bigquery.googleapis.ccom/bigquery/v2/projects/idc-dev/jobs?alt=json', status=200)
        #self.requests.mock(url, body=response, status=200)

        print("testing with mock")
        query = """
            SELECT * FROM [idc-dev:test_bq_dataset.test_table_too]
        """

        with open('/home/vagrant/www/IDC-Common/google_helpers/bigquery/testResponses/test_hello_world_discovery.json', 'r') as jr:
            respd = json.load(jr)
        with open('/home/vagrant/www/IDC-Common/google_helpers/bigquery/testResponses/test_hello_world_1.json', 'r') as jr:
            resp1 = json.load(jr)
        with open('/home/vagrant/www/IDC-Common/google_helpers/bigquery/testResponses/test_hello_world_2.json', 'r') as jr:
            resp2 = json.load(jr)
        with open('/home/vagrant/www/IDC-Common/google_helpers/bigquery/testResponses/test_hello_world_3.json', 'r') as jr:
            resp3 = json.load(jr)
        with open('/home/vagrant/www/IDC-Common/google_helpers/bigquery/testResponses/test_hello_world_4.json', 'r') as jr:
            resp4 = json.load(jr)

        http1_for_test = HttpMock()
        http2_for_test = HttpMockSequence([
            ({'status': '200'}, json.dumps(respd)),
            ({'status': '200'}, json.dumps(resp1)),
            ({'status': '200'}, json.dumps(resp2)),
            ({'status': '200'}, json.dumps(resp3)),
            ({'status': '200'}, json.dumps(resp4))
        ])

        bqs = BigQuerySupport(None, None, None, executing_project='idc-dev',
                              http1_for_test=http1_for_test, http2_for_test=http2_for_test)
        test_result = bqs.execute_query(query)

        print(str(test_result))
        print("done testing with mock")


'''
send: b'POST /oauth2/v4/token HTTP/1.1\r\n
Host: www.googleapis.com\r\n
Content-Length: 841\r\ncontent-type: application/x-www-form-urlencoded\r\naccept-encoding: gzip, deflate\r\n
user-agent: Python-httplib2/0.9.2 (gzip)\r\n\r\n'
send: b'assertion=eyJhbGciOiJSUzI1NiIsImtpZCI6IjViMGZiZmU0MjgyNzZhZGY1MTBkYzYyMGFlNDk0ZGY1MWQyZmI0OTgiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE1ODMyMDI2MDYsImF1ZCI6Imh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92NC90b2tlbiIsInNjb3BlIjoiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vYXV0aC9iaWdxdWVyeSBodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9hdXRoL2JpZ3F1ZXJ5Lmluc2VydGRhdGEiLCJpc3MiOiJ3ZWJhcHAtZGV2QGlkYy1kZXYuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLCJleHAiOjE1ODMyMDYyMDZ9.xqeATBA6ASQjqWc0SNNEWyCu0GOArZlohlPIqa28tJPMEzf2iqqyx0qecIBNSmmz7_NfbptxFVRv0_5gho7iKhlXnjG-WahGx8DR3WXiE4_JG3m8IPclRHZDHpuCHLNT5WqrmBdfRYTLyGMG3_pKorrGi3JO6sgId7kpxiTby2USmdVX3P7WPbh6dI1MgpjRMmk0xzI_hwYZ302aVfj2XUrWC3MwDXl8OKiqEMu1EHyW_Q4RZQyYNjy1mx7AaQNXrMps7fZSz8w86YNc6nkm71PeWXiT69_yIg3OoGh6vzs-G_HiEh0BMSGbnKs7a36xoRNLDrscYzLtwigvbSIt-Q&grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer'

reply: 'HTTP/1.1 200 OK\r\n'
header: Content-Type header: Vary header: Vary header: Vary header: Content-Encoding header: Date header: Server header: Cache-Control header: X-XSS-Protection header: X-Frame-Options header: X-Content-Type-Options header: Alt-Svc header: Transfer-Encoding

send: b'GET /discovery/v1/apis/bigquery/v2/rest HTTP/1.1
Host: www.googleapis.com\r\n
authorization: Bearer ya29.c.KpEBwAfYHpQ4JLxDX44qBpL1Xh35ZyopKLICTyYBeY10KQWHu2zxPZ8EnLpsjS-N5yraXuZitGDUwY2bDdE6jSMuHblIO2s35mC1JE9OSuocu9QDNvnO3H_SYvyfJ6EW2fITeGhDiymZBAGFPZqucyQsxaaxsDnEIofs4-qX317fSLnZiBjnKgXKVUIjHDgcLLlQIA\r\n
accept-encoding: gzip, deflate\r\nuser-agent: Python-httplib2/0.9.2 (gzip)\r\n\r\n'

reply: 'HTTP/1.1 200 OK\r\n'
header: Content-Type header: Vary header: Vary header: Vary header: Content-Encoding header: Date header: Server header: Cache-Control header: X-XSS-Protection header: X-Frame-Options header: X-Content-Type-Options header: Alt-Svc header: Transfer-Encoding

send: b'POST /bigquery/v2/projects/idc-dev/jobs?alt=json HTTP/1.1\r\n
Host: bigquery.googleapis.com\r\n
accept: application/json\r\n
content-type: application/json\r\n
authorization: Bearer ya29.c.KpEBwAfYHpQ4JLxDX44qBpL1Xh35ZyopKLICTyYBeY10KQWHu2zxPZ8EnLpsjS-N5yraXuZitGDUwY2bDdE6jSMuHblIO2s35mC1JE9OSuocu9QDNvnO3H_SYvyfJ6EW2fITeGhDiymZBAGFPZqucyQsxaaxsDnEIofs4-qX317fSLnZiBjnKgXKVUIjHDgcLLlQIA\r\n
accept-encoding: gzip, deflate\r\nuser-agent: google-api-python-client/1.6.1 (gzip)\r\n
content-length: 239\r\n\r\n'
send: b'{"jobReference": {"projectId": "idc-dev", "jobId": "8d973c9d-dd51-49c0-9243-060234bbee70"}, "configuration": {"query": {"query": "\\n            SELECT * FROM [idc-dev:test_bq_dataset.test_table_too]\\n        ", "priority": "INTERACTIVE"}}}'


reply: 'HTTP/1.1 200 OK\r\n'
header: ETag header: Content-Type header: Vary header: Vary header: Vary header: Content-Encoding header: Date header: Server header: Cache-Control header: X-XSS-Protection header: X-Frame-Options header: X-Content-Type-Options header: Alt-Svc header: Transfer-Encoding

send: b'GET /bigquery/v2/projects/idc-dev/jobs/8d973c9d-dd51-49c0-9243-060234bbee70?alt=json HTTP/1.1\r\n
Host: bigquery.googleapis.com\r\n
accept: application/json\r\n
authorization: Bearer ya29.c.KpEBwAfYHpQ4JLxDX44qBpL1Xh35ZyopKLICTyYBeY10KQWHu2zxPZ8EnLpsjS-N5yraXuZitGDUwY2bDdE6jSMuHblIO2s35mC1JE9OSuocu9QDNvnO3H_SYvyfJ6EW2fITeGhDiymZBAGFPZqucyQsxaaxsDnEIofs4-qX317fSLnZiBjnKgXKVUIjHDgcLLlQIA\r\n
accept-encoding: gzip, deflate\r\n
user-agent: google-api-python-client/1.6.1 (gzip)\r\n
content-length: 0\r\n\r\n'

reply: 'HTTP/1.1 200 OK\r\n'
header: ETag header: Content-Type header: Vary header: Vary header: Vary header: Content-Encoding header: Date header: Server header: Cache-Control header: X-XSS-Protection header: X-Frame-Options header: X-Content-Type-Options header: Alt-Svc header: Transfer-Encoding

send: b'GET /bigquery/v2/projects/idc-dev/jobs/8d973c9d-dd51-49c0-9243-060234bbee70?alt=json HTTP/1.1\r\n
Host: bigquery.googleapis.com\r\n
accept: application/json\r\n
authorization: Bearer ya29.c.KpEBwAfYHpQ4JLxDX44qBpL1Xh35ZyopKLICTyYBeY10KQWHu2zxPZ8EnLpsjS-N5yraXuZitGDUwY2bDdE6jSMuHblIO2s35mC1JE9OSuocu9QDNvnO3H_SYvyfJ6EW2fITeGhDiymZBAGFPZqucyQsxaaxsDnEIofs4-qX317fSLnZiBjnKgXKVUIjHDgcLLlQIA\r\n
accept-encoding: gzip, deflate\r\n
user-agent: google-api-python-client/1.6.1 (gzip)\r\n
content-length: 0\r\n\r\n'

reply: 'HTTP/1.1 200 OK\r\n'
header: ETag header: Content-Type header: Vary header: Vary header: Vary header: Content-Encoding header: Date header: Server header: Cache-Control header: X-XSS-Protection header: X-Frame-Options header: X-Content-Type-Options header: Alt-Svc header: Transfer-Encoding

send: b'GET /bigquery/v2/projects/idc-dev/jobs/8d973c9d-dd51-49c0-9243-060234bbee70?alt=json HTTP/1.1\r\n
Host: bigquery.googleapis.com\r\n
accept: application/json\r\n
authorization: Bearer ya29.c.KpEBwAfYHpQ4JLxDX44qBpL1Xh35ZyopKLICTyYBeY10KQWHu2zxPZ8EnLpsjS-N5yraXuZitGDUwY2bDdE6jSMuHblIO2s35mC1JE9OSuocu9QDNvnO3H_SYvyfJ6EW2fITeGhDiymZBAGFPZqucyQsxaaxsDnEIofs4-qX317fSLnZiBjnKgXKVUIjHDgcLLlQIA\r\n
accept-encoding: gzip, deflate\r\n
user-agent: google-api-python-client/1.6.1 (gzip)\r\n
content-length: 0\r\n\r\n'

reply: 'HTTP/1.1 200 OK\r\n'
header: ETag header: Content-Type header: Vary header: Vary header: Vary header: Content-Encoding header: Date header: Server header: Cache-Control header: X-XSS-Protection header: X-Frame-Options header: X-Content-Type-Options header: Alt-Svc header: Transfer-Encoding

send: b'GET /bigquery/v2/projects/idc-dev/queries/8d973c9d-dd51-49c0-9243-060234bbee70?location=US&alt=json HTTP/1.1\r\n
Host: bigquery.googleapis.com\r\n
accept: application/json\r\n
authorization: Bearer ya29.c.KpEBwAfYHpQ4JLxDX44qBpL1Xh35ZyopKLICTyYBeY10KQWHu2zxPZ8EnLpsjS-N5yraXuZitGDUwY2bDdE6jSMuHblIO2s35mC1JE9OSuocu9QDNvnO3H_SYvyfJ6EW2fITeGhDiymZBAGFPZqucyQsxaaxsDnEIofs4-qX317fSLnZiBjnKgXKVUIjHDgcLLlQIA\r\n
accept-encoding: gzip, deflate\r\n
user-agent: google-api-python-client/1.6.1 (gzip)\r\n
content-length: 0\r\n\r\n'


reply: 'HTTP/1.1 200 OK\r\n'
header: ETag header: Content-Type header: Vary header: Vary header: Vary header: Content-Encoding header: Date header: Server header: Cache-Control header: X-XSS-Protection header: X-Frame-Options header: X-Content-Type-Options header: Alt-Svc header: Transfer-Encoding
[{'f': [{'v': 'foo'}, {'v': 'bar'}]}, {'f': [{'v': 'bar'}, {'v': 'foo'}]}, {'f': [{'v': 'footer'}, {'v': 'barfoo'}]}]

'''



