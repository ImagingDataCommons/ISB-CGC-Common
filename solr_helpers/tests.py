#
# Copyright 2015-2020, Institute for Systems Biology
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


#
# At first, thought I could make this a non-Django test, but __init__.py for the module
# has Django code...so no joy.
#

'''
import unittest

class TestRawSolr(unittest.TestCase):
    def test_get_solr_data(self):
        print("Hello world")
        self.assertTrue("Hello world" == "Hello world")


class TestSolrSupport(unittest.TestCase):

    def test_hw_1(self):
        print("Hello world")
        self.assertTrue("Hello world" == "Hello world")
        self.assertFalse("Hello world" != "Hello world")

    def test_hw_2(self):
        self.assertEqual("Hello world", "Hello world")

if __name__ == "__main__":
    unittest.main()
'''


from django.test import TestCase
from django.conf import settings
import requests
import responses
import json

class TestRawSolr(TestCase):

    def __init__(self, *args, **kwargs):
        # Should best be done in setUp, but keeping this around to show how to override
        # the constructor in test:
        super(TestRawSolr, self).__init__(*args, **kwargs)
        self.SOLR_URI = settings.SOLR_URI
        self.SOLR_LOGIN = settings.SOLR_LOGIN
        self.SOLR_PASSWORD = settings.SOLR_PASSWORD
        self.SOLR_CERT = settings.SOLR_CERT

    def setUp(self):
        #
        # Magic numbers that will change when solr collections are updated
        # Current as of: 2/23/2020
        #
        self.TOTAL_SIZE = 14682
        self.MISSING_COLL_ID = 0
        self.COLL_ID_BUCKET_COUNT = 76
        self.COLL_ID_TEST_VAL = 'tcga_blca'
        self.COLL_ID_TEST_COUNT = 106

        self.MISSING_BPEX = 778
        self.BPEX_BUCKET_COUNT = 37
        self.BPEX_TEST_VAL = 'BREAST'
        self.BPEX_TEST_COUNT = 6953

        self.MISSING_MOD = 0
        self.MOD_BUCKET_COUNT = 13
        self.MOD_TEST_VAL = 'CT'
        self.MOD_TEST_COUNT = 5039

        self.MISSING_SPEC = 0
        self.SPEC_BUCKET_COUNT = 1
        self.SPEC_TEST_VAL = 'H. sapiens'
        self.SPEC_TEST_COUNT = self.TOTAL_SIZE


    @responses.activate
    def test_get_solr_data_fail(self):
        query_uri = "{}{}/query".format(self.SOLR_URI, "tcia_images")
        responses.add(responses.GET, query_uri,
                      json={'error' : 'not found'}, status=404)

        resp = requests.get(query_uri)
        self.assertEquals(resp.json(), {'error' : 'not found'})
        print("Back from fake solr {}".format(query_uri))


    def test_get_solr_data(self):
        query_uri = "{}{}/query".format(self.SOLR_URI, "tcia_images")

        mod_dict = {"field": "Modality",
                    "missing": True,
                    "type": "terms",
                    "limit": -1}

        species_dict = {"field": "Species",
                        "missing": True,
                        "type": "terms",
                        "limit": -1}

        coll_dict = {"field": "collection_id",
                     "missing": True,
                     "type": "terms",
                     "limit": -1}

        bpex_dict = {"field": "BodyPartExamined",
                     "missing": True,
                     "type": "terms",
                     "limit": -1}

        payload = {
            "query": "*:*",
            "limit": 0,
            "offset": 0,
            "params": {"debugQuery": "off"},
            "facet": {"Modality": mod_dict,
                      "Species": species_dict,
                      "collection_id": coll_dict,
                      "BodyPartExamined": bpex_dict},
            "filter": ["{!collapse field=PatientID}"],
            "fields": ["collection_id", "case_barcode", "PatientID", "race", "vital_status",
                             "ethnicity", "bmi", "age_at_diagnosis", "gender", "disease_code",
                             "StudyInstanceUID", "StudyDescription", "StudyDate",
                             "SeriesInstanceUID", "SeriesDescription", "SeriesNumber",
                             "BodyPartExamined", "Modality"]
        }

        query_result = {}

        try:
            query_response = requests.post(query_uri, data=json.dumps(payload),
                                           headers={'Content-type': 'application/json'},
                                           auth=(self.SOLR_LOGIN, self.SOLR_PASSWORD),
                                           verify=self.SOLR_CERT)
            print("Back from solr {}".format(query_uri))
            self.assertEquals(query_response.status_code, 200)
            query_result = query_response.json()
        except Exception as e:
            self.fail("Test caught exception {}".format(str(e)))

        #print("query_result" + json.dumps(query_result, indent=4))
        self.assertTrue(query_result is not None)
        self.assertTrue('facets' in query_result)
        facet_dict = query_result['facets']

        self.assertTrue('collection_id' in facet_dict)
        self.assertTrue('missing' in facet_dict['collection_id'])
        missing = facet_dict['collection_id']['missing']
        self.assertTrue('count' in missing)
        self.assertEquals(missing['count'], self.MISSING_COLL_ID)
        self.assertTrue('buckets' in facet_dict['collection_id'])
        bucket_list = facet_dict['collection_id']['buckets']
        self.assertEquals(len(bucket_list), self.COLL_ID_BUCKET_COUNT)
        found_bucket = False
        for bucket in bucket_list:
            if bucket['val'] == self.COLL_ID_TEST_VAL:
                self.assertEquals(bucket['count'], self.COLL_ID_TEST_COUNT)
                found_bucket = True
                break
        self.assertTrue(found_bucket)


        self.assertTrue('BodyPartExamined' in facet_dict)
        self.assertTrue('missing' in facet_dict['BodyPartExamined'])
        missing = facet_dict['BodyPartExamined']['missing']
        self.assertTrue('count' in missing)
        self.assertEquals(missing['count'], self.MISSING_BPEX)
        self.assertTrue('buckets' in facet_dict['BodyPartExamined'])
        bucket_list = facet_dict['BodyPartExamined']['buckets']
        self.assertEquals(len(bucket_list), self.BPEX_BUCKET_COUNT)
        found_bucket = False
        for bucket in bucket_list:
            if bucket['val'] == self.BPEX_TEST_VAL:
                self.assertEquals(bucket['count'], self.BPEX_TEST_COUNT)
                found_bucket = True
                break
        self.assertTrue(found_bucket)

        self.assertTrue('Modality' in facet_dict)
        self.assertTrue('missing' in facet_dict['Modality'])
        missing = facet_dict['Modality']['missing']
        self.assertTrue('count' in missing)
        self.assertEquals(missing['count'], self.MISSING_MOD)
        self.assertTrue('buckets' in facet_dict['Modality'])
        bucket_list = facet_dict['Modality']['buckets']
        self.assertEquals(len(bucket_list), self.MOD_BUCKET_COUNT)
        found_bucket = False
        for bucket in bucket_list:
            if bucket['val'] == self.MOD_TEST_VAL:
                found_bucket = True
                self.assertEquals(bucket['count'], self.MOD_TEST_COUNT)
                break
        self.assertTrue(found_bucket)


        self.assertTrue('Species' in facet_dict)
        self.assertTrue('missing' in facet_dict['Species'])
        missing = facet_dict['Species']['missing']
        self.assertTrue('count' in missing)
        self.assertEquals(missing['count'], self.MISSING_SPEC)
        self.assertTrue('buckets' in facet_dict['Species'])
        bucket_list = facet_dict['Species']['buckets']
        self.assertEquals(len(bucket_list), self.SPEC_BUCKET_COUNT)
        found_bucket = False
        for bucket in bucket_list:
            if bucket['val'] == self.SPEC_TEST_VAL:
                found_bucket = True
                self.assertEquals(bucket['count'], self.SPEC_TEST_COUNT)
                break
        self.assertTrue(found_bucket)

        self.assertTrue('response' in query_result)
        self.assertEquals(query_result['response']['numFound'], self.TOTAL_SIZE)

