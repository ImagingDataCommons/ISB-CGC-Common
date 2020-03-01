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
import pprint
import solr_helpers
from idc_collections.models import Attribute
from idc_collections.models import Attribute_Ranges
from idc_collections.models import DataVersion
from idc_collections.models import DataSource

class TestSolr(TestCase):

    def __init__(self, *args, **kwargs):
        # Should best be done in setUp, but keeping this around to show how to override
        # the constructor in test:
        super(TestSolr, self).__init__(*args, **kwargs)
        self.SOLR_URI = settings.SOLR_URI
        self.SOLR_LOGIN = settings.SOLR_LOGIN
        self.SOLR_PASSWORD = settings.SOLR_PASSWORD
        self.SOLR_CERT = settings.SOLR_CERT
        return

    def setUp(self):
        # Show full diffs on failure:
        self.maxDiff = None

        # Gotta get attributes into the database:

        self.data_version_1 = DataVersion(version='r9', data_type=DataVersion.ANCILLARY_DATA,
                                          name='Clinical and Biospecimen Data',
                                          active=True)
        self.data_version_1.save()

        self.data_version_2 = DataVersion(version='0', data_type=DataVersion.IMAGE_DATA,
                                          name='Image Data',
                                          active=True)
        self.data_version_2.save()

        self.data_source_1 = DataSource(name='tcia_images', shared_id_col='PatientID',
                                        source_type=DataSource.SOLR, version=self.data_version_2)
        self.data_source_1.save()

        self.data_source_2 = DataSource(name='tcga_clin_bios', shared_id_col='case_barcode',
                                        source_type=DataSource.SOLR, version=self.data_version_1)
        self.data_source_2.save()

        self.test_att_1 = Attribute(name='age_at_diagnosis', display_name='Age At Diagnosis',
                                    description=None, data_type=Attribute.CONTINUOUS_NUMERIC,
                                    active=True,
                                    is_cross_collex=False, preformatted_values=False,
                                    default_ui_display=True)
        self.test_att_1.save()
        self.test_att_2 = Attribute(name='bmi', display_name='BMI',
                                    description=None, data_type=Attribute.CONTINUOUS_NUMERIC,
                                    active=True,
                                    is_cross_collex=False, preformatted_values=False,
                                    default_ui_display=True)
        self.test_att_2.save()

        # +----+------+---------------+---------------+-----------+-------+------+-----+-------------+--------------+
        # | id | type | include_lower | include_upper | unbounded | first | last | gap | label | attribute_id |
        # +----+------+---------------+---------------+-----------+-------+------+-----+-------------+--------------+
        # | 1 | I | 1 | 0 | 1 | 10 | 80 | 10 | NULL | 12 |
        # | 2 | F | 1 | 0 | 1 | * | 18.5 | 0 | underweight | 63 |
        # | 3 | F | 1 | 1 | 1 | 30 | * | 0 | obese | 63 |
        # | 4 | F | 1 | 0 | 1 | 18.5 | 25 | 0 | normal | 63 |
        # | 5 | F | 1 | 0 | 1 | 25 | 30 | 0 | overweight | 63 |
        # +----+------+---------------+---------------+-----------+-------+------+-----+-------------+--------------+


        self.test_att_range_1 = Attribute_Ranges(type=Attribute_Ranges.INT,
                                                 include_lower=True,
                                                 include_upper=False,
                                                 unbounded=True,
                                                 first=10, last=80, gap=10,
                                                 label=None,
                                                 attribute=self.test_att_1)
        self.test_att_range_1.save()
        self.test_att_range_2 = Attribute_Ranges(type=Attribute_Ranges.FLOAT,
                                                 include_lower=True,
                                                 include_upper=False,
                                                 unbounded=True,
                                                 first='*', last='18.5', gap='0',
                                                 label='underweight',
                                                 attribute=self.test_att_2)
        self.test_att_range_2.save()
        self.test_att_range_3 = Attribute_Ranges(type=Attribute_Ranges.FLOAT,
                                                 include_lower=True,
                                                 include_upper=True,
                                                 unbounded=True,
                                                 first='30', last='*', gap='0',
                                                 label='obese',
                                                 attribute=self.test_att_2)
        self.test_att_range_3.save()
        self.test_att_range_4 = Attribute_Ranges(type=Attribute_Ranges.FLOAT,
                                                 include_lower=True,
                                                 include_upper=False,
                                                 unbounded=True,
                                                 first='18.5', last='25', gap='0',
                                                 label='normal',
                                                 attribute=self.test_att_2)
        self.test_att_range_4.save()
        self.test_att_range_5 = Attribute_Ranges(type=Attribute_Ranges.FLOAT,
                                                 include_lower=True,
                                                 include_upper=False,
                                                 unbounded=True,
                                                 first='25', last='30', gap='0',
                                                 label='overweight',
                                                 attribute=self.test_att_2)
        self.test_att_range_5.save()
        return

    #
    # This mimics the first image collection call we get when hitting /explore
    #

    @responses.activate
    def test_query_solr_images(self):

        #
        # Get the mocking set up:
        #
        archived_result = TestSolr._get_archived_tcga_image_response()
        test_collection = 'tcia_images'
        query_uri = "{}{}/query".format(self.SOLR_URI, test_collection)
        responses.add(responses.POST, query_uri, json=archived_result, status=200)

        test_fields = ['project_short_name',
         'project_name',
         'disease_code',
         'gender',
         'vital_status',
         'race',
         'ethnicity',
         'age_at_diagnosis',
         'pathologic_stage',
         'tumor_tissue_site',
         'country',
         'histological_type',
         'neoplasm_histologic_grade',
         'bmi',
         'sample_type',
         'sample_type_name']

        test_query_string = ('(+collection_id:("tcga_ucec" "tcga_thca" "tcga_stad" "tcga_sarc" "tcga_read" '
         '"tcga_prad" "tcga_ov" "tcga_lusc" "tcga_luad" "tcga_lihc" "tcga_lgg" '
         '"tcga_kirp" "tcga_kirc" "tcga_kich" "tcga_hnsc" "tcga_gbm" "tcga_esca" '
         '"tcga_coad" "tcga_cesc" "tcga_brca" "tcga_blca"))')

        test_fqs = ['{!tag=f0}(+collection_id:("tcga_ucec" "tcga_thca" "tcga_stad" "tcga_sarc" '
         '"tcga_read" "tcga_prad" "tcga_ov" "tcga_lusc" "tcga_luad" "tcga_lihc" '
         '"tcga_lgg" "tcga_kirp" "tcga_kirc" "tcga_kich" "tcga_hnsc" "tcga_gbm" '
         '"tcga_esca" "tcga_coad" "tcga_cesc" "tcga_brca" "tcga_blca"))']

        test_facets = {'BodyPartExamined': {'field': 'BodyPartExamined',
                              'limit': -1,
                              'missing': True,
                              'type': 'terms'},
         'Modality': {'field': 'Modality',
                      'limit': -1,
                      'missing': True,
                      'type': 'terms'},
         'Species': {'field': 'Species', 'limit': -1, 'missing': True, 'type': 'terms'},
         'collection_id': {'domain': {'excludeTags': 'f0'},
                           'field': 'collection_id',
                           'limit': -1,
                           'missing': True,
                           'type': 'terms'}}

        test_sort = None
        test_counts_only = True
        test_collapse_on = 'PatientID'
        test_offset = 0
        test_limit = 0

        test_result = solr_helpers.query_solr(collection=test_collection, fields=test_fields,
                                               query_string=test_query_string, fqs=test_fqs,
                                               facets=test_facets, sort=test_sort,
                                               counts_only=test_counts_only,
                                               collapse_on=test_collapse_on,
                                               offset=test_offset, limit=test_limit, do_debug=False)

        #pp = pprint.PrettyPrinter()
        #pp.pprint(query_result)
        self.assertEquals(test_result, archived_result)
        return

    #
    # This mimics the second clinical call we get when hitting /explore
    #

    @responses.activate
    def test_query_solr_clinical(self):

        #
        # Get the mocking set up:
        #
        archived_result = TestSolr._get_archived_tcga_clinical_response()
        test_collection = 'tcga_clin_bios'
        query_uri = "{}{}/query".format(self.SOLR_URI, test_collection)
        responses.add(responses.POST, query_uri, json=archived_result, status=200)

        test_fields = None
        test_query_string = '*:*'
        test_fqs  = ['{!join from=PatientID fromIndex=tcia_images '
                     'to=case_barcode}{!tag=f0}(+collection_id:("tcga_ucec" "tcga_thca" '
                     '"tcga_stad" "tcga_sarc" "tcga_read" "tcga_prad" "tcga_ov" "tcga_lusc" '
                     '"tcga_luad" "tcga_lihc" "tcga_lgg" "tcga_kirp" "tcga_kirc" "tcga_kich" '
                     '"tcga_hnsc" "tcga_gbm" "tcga_esca" "tcga_coad" "tcga_cesc" "tcga_brca" '
                     '"tcga_blca"))']

        test_facets = {'age_at_diagnosis:* to 10': {'field': 'age_at_diagnosis',
                                      'limit': -1,
                                      'q': 'age_at_diagnosis:[* TO 10}',
                                      'type': 'query'},
                         'age_at_diagnosis:10 to 20': {'field': 'age_at_diagnosis',
                                                       'limit': -1,
                                                       'q': 'age_at_diagnosis:[10 TO 20}',
                                                       'type': 'query'},
                         'age_at_diagnosis:20 to 30': {'field': 'age_at_diagnosis',
                                                       'limit': -1,
                                                       'q': 'age_at_diagnosis:[20 TO 30}',
                                                       'type': 'query'},
                         'age_at_diagnosis:30 to 40': {'field': 'age_at_diagnosis',
                                                       'limit': -1,
                                                       'q': 'age_at_diagnosis:[30 TO 40}',
                                                       'type': 'query'},
                         'age_at_diagnosis:40 to 50': {'field': 'age_at_diagnosis',
                                                       'limit': -1,
                                                       'q': 'age_at_diagnosis:[40 TO 50}',
                                                       'type': 'query'},
                         'age_at_diagnosis:50 to 60': {'field': 'age_at_diagnosis',
                                                       'limit': -1,
                                                       'q': 'age_at_diagnosis:[50 TO 60}',
                                                       'type': 'query'},
                         'age_at_diagnosis:60 to 70': {'field': 'age_at_diagnosis',
                                                       'limit': -1,
                                                       'q': 'age_at_diagnosis:[60 TO 70}',
                                                       'type': 'query'},
                         'age_at_diagnosis:70 to 80': {'field': 'age_at_diagnosis',
                                                       'limit': -1,
                                                       'q': 'age_at_diagnosis:[70 TO 80}',
                                                       'type': 'query'},
                         'age_at_diagnosis:80 to *': {'field': 'age_at_diagnosis',
                                                      'limit': -1,
                                                      'q': 'age_at_diagnosis:[80 TO *]',
                                                      'type': 'query'},
                         'age_at_diagnosis:None': {'field': 'age_at_diagnosis',
                                                   'limit': -1,
                                                   'q': '-age_at_diagnosis:[* TO *]',
                                                   'type': 'query'},
                         'bmi:normal': {'field': 'bmi',
                                        'limit': -1,
                                        'q': 'bmi:[18.5 TO 25}',
                                        'type': 'query'},
                         'bmi:obese': {'field': 'bmi',
                                       'limit': -1,
                                       'q': 'bmi:[30 TO *]',
                                       'type': 'query'},
                         'bmi:overweight': {'field': 'bmi',
                                            'limit': -1,
                                            'q': 'bmi:[25 TO 30}',
                                            'type': 'query'},
                         'bmi:underweight': {'field': 'bmi',
                                             'limit': -1,
                                             'q': 'bmi:[* TO 18.5}',
                                             'type': 'query'},
                         'country': {'field': 'country', 'limit': -1, 'missing': True, 'type': 'terms'},
                         'disease_code': {'field': 'disease_code',
                                          'limit': -1,
                                          'missing': True,
                                          'type': 'terms'},
                         'ethnicity': {'field': 'ethnicity',
                                       'limit': -1,
                                       'missing': True,
                                       'type': 'terms'},
                         'gender': {'field': 'gender', 'limit': -1, 'missing': True, 'type': 'terms'},
                         'histological_type': {'field': 'histological_type',
                                               'limit': -1,
                                               'missing': True,
                                               'type': 'terms'},
                         'neoplasm_histologic_grade': {'field': 'neoplasm_histologic_grade',
                                                       'limit': -1,
                                                       'missing': True,
                                                       'type': 'terms'},
                         'pathologic_stage': {'field': 'pathologic_stage',
                                              'limit': -1,
                                              'missing': True,
                                              'type': 'terms'},
                         'project_name': {'field': 'project_name',
                                          'limit': -1,
                                          'missing': True,
                                          'type': 'terms'},
                         'project_short_name': {'field': 'project_short_name',
                                                'limit': -1,
                                                'missing': True,
                                                'type': 'terms'},
                         'race': {'field': 'race', 'limit': -1, 'missing': True, 'type': 'terms'},
                         'sample_type': {'field': 'sample_type',
                                         'limit': -1,
                                         'missing': True,
                                         'type': 'terms'},
                         'sample_type_name': {'field': 'sample_type_name',
                                              'limit': -1,
                                              'missing': True,
                                              'type': 'terms'},
                         'tumor_tissue_site': {'field': 'tumor_tissue_site',
                                               'limit': -1,
                                               'missing': True,
                                               'type': 'terms'},
                         'vital_status': {'field': 'vital_status',
                                          'limit': -1,
                                          'missing': True,
                                          'type': 'terms'}}
        test_sort = None
        test_counts_only = True
        test_collapse_on = 'case_barcode'
        test_offset = 0
        test_limit = 0

        test_result = solr_helpers.query_solr(collection=test_collection, fields=test_fields,
                                               query_string=test_query_string, fqs=test_fqs,
                                               facets=test_facets, sort=test_sort,
                                               counts_only=test_counts_only,
                                               collapse_on=test_collapse_on,
                                               offset=test_offset, limit=test_limit, do_debug=False)

        #pp = pprint.PrettyPrinter()
        #pp.pprint(query_result)
        self.assertEquals(test_result, archived_result)
        return

    #
    # Test the construction of facets:
    #

    def test_build_solr_facets(self):

        expected_result = {'age_at_diagnosis:* to 10': {'field': 'age_at_diagnosis',
                                      'limit': -1,
                                      'q': 'age_at_diagnosis:[* TO 10}',
                                      'type': 'query'},
                         'age_at_diagnosis:10 to 20': {'field': 'age_at_diagnosis',
                                                       'limit': -1,
                                                       'q': 'age_at_diagnosis:[10 TO 20}',
                                                       'type': 'query'},
                         'age_at_diagnosis:20 to 30': {'field': 'age_at_diagnosis',
                                                       'limit': -1,
                                                       'q': 'age_at_diagnosis:[20 TO 30}',
                                                       'type': 'query'},
                         'age_at_diagnosis:30 to 40': {'field': 'age_at_diagnosis',
                                                       'limit': -1,
                                                       'q': 'age_at_diagnosis:[30 TO 40}',
                                                       'type': 'query'},
                         'age_at_diagnosis:40 to 50': {'field': 'age_at_diagnosis',
                                                       'limit': -1,
                                                       'q': 'age_at_diagnosis:[40 TO 50}',
                                                       'type': 'query'},
                         'age_at_diagnosis:50 to 60': {'field': 'age_at_diagnosis',
                                                       'limit': -1,
                                                       'q': 'age_at_diagnosis:[50 TO 60}',
                                                       'type': 'query'},
                         'age_at_diagnosis:60 to 70': {'field': 'age_at_diagnosis',
                                                       'limit': -1,
                                                       'q': 'age_at_diagnosis:[60 TO 70}',
                                                       'type': 'query'},
                         'age_at_diagnosis:70 to 80': {'field': 'age_at_diagnosis',
                                                       'limit': -1,
                                                       'q': 'age_at_diagnosis:[70 TO 80}',
                                                       'type': 'query'},
                         'age_at_diagnosis:80 to *': {'field': 'age_at_diagnosis',
                                                      'limit': -1,
                                                      'q': 'age_at_diagnosis:[80 TO *]',
                                                      'type': 'query'},
                         'age_at_diagnosis:None': {'field': 'age_at_diagnosis',
                                                   'limit': -1,
                                                   'q': '-age_at_diagnosis:[* TO *]',
                                                   'type': 'query'},
                         'bmi:normal': {'field': 'bmi',
                                        'limit': -1,
                                        'q': 'bmi:[18.5 TO 25}',
                                        'type': 'query'},
                         'bmi:obese': {'field': 'bmi',
                                       'limit': -1,
                                       'q': 'bmi:[30 TO *]',
                                       'type': 'query'},
                         'bmi:overweight': {'field': 'bmi',
                                            'limit': -1,
                                            'q': 'bmi:[25 TO 30}',
                                            'type': 'query'},
                         'bmi:underweight': {'field': 'bmi',
                                             'limit': -1,
                                             'q': 'bmi:[* TO 18.5}',
                                             'type': 'query'}}

        test_attr_set = ['age_at_diagnosis',
                         'bmi']
        test_filter_tags = {'collection_id': 'f0'}
        test_include_nulls = True

        test_result = solr_helpers.build_solr_facets(attr_set=test_attr_set,
                                                      filter_tags=test_filter_tags,
                                                      include_nulls=test_include_nulls)
        self.assertEquals(test_result, expected_result)
        return

    #
    # Test the construction of a query:
    #

    def test_build_solr_query(self):

        expected_result = {'filter_tags': {'BodyPartExamined': 'f0',
                                             'Modality': 'f1',
                                             'collection_id': 'f2'},
                         'full_query_str': '',
                         'queries': {'BodyPartExamined': '{!tag=f0}(+BodyPartExamined:("BRAIN"))',
                                     'Modality': '{!tag=f1}(+Modality:("MR"))',
                                     'collection_id': '{!tag=f2}(+collection_id:("tcga_blca" '
                                                      '"tcga_brca" "tcga_cesc" "tcga_coad" "tcga_esca" '
                                                      '"tcga_gbm" "tcga_hnsc" "tcga_kich" "tcga_kirc" '
                                                      '"tcga_kirp" "tcga_lgg" "tcga_lihc" "tcga_luad" '
                                                      '"tcga_lusc" "tcga_ov" "tcga_prad" "tcga_read" '
                                                      '"tcga_sarc" "tcga_stad" "tcga_thca" '
                                                      '"tcga_ucec"))'}}

        test_filters = {'BodyPartExamined': ['BRAIN'],
                         'Modality': ['MR'],
                         'collection_id': ['tcga_blca',
                                           'tcga_brca',
                                           'tcga_cesc',
                                           'tcga_coad',
                                           'tcga_esca',
                                           'tcga_gbm',
                                           'tcga_hnsc',
                                           'tcga_kich',
                                           'tcga_kirc',
                                           'tcga_kirp',
                                           'tcga_lgg',
                                           'tcga_lihc',
                                           'tcga_luad',
                                           'tcga_lusc',
                                           'tcga_ov',
                                           'tcga_prad',
                                           'tcga_read',
                                           'tcga_sarc',
                                           'tcga_stad',
                                           'tcga_thca',
                                           'tcga_ucec']}

        built_query = solr_helpers.build_solr_query(filters=test_filters, with_tags_for_ex=True)

        self.assertEquals(built_query, expected_result)
        return

    #
    # Test making a call to solr and getting back a formatted result:
    #

    @responses.activate
    def test_query_solr_and_format_result(self):

        #
        # Set up mocking environment:
        #
        archived_result = self._get_archived_tcga_image_response()
        test_collection = 'tcia_images'
        query_uri = "{}{}/query".format(self.SOLR_URI, test_collection)
        responses.add(responses.POST, query_uri, json=archived_result, status=200)

        expected_result = {'docs': [],
                         'facets': {'BodyPartExamined': {'BLADDER': 106,
                                                         'BRAIN': 461,
                                                         'BREAST': 139,
                                                         'CERVIX': 54,
                                                         'CHEST': 38,
                                                         'CHESTABDPELVIS': 3,
                                                         'COLON': 25,
                                                         'ESOPHAGUS': 16,
                                                         'HEADNECK': 192,
                                                         'KIDNEY': 295,
                                                         'Kidney': 16,
                                                         'LEG': 1,
                                                         'LIVER': 97,
                                                         'LUNG': 68,
                                                         'None': 46,
                                                         'OVARY': 143,
                                                         'PROSTATE': 14,
                                                         'RECTUM': 3,
                                                         'STOMACH': 46,
                                                         'THYROID': 6,
                                                         'TSPINE': 1,
                                                         'UTERUS': 58},
                                    'Modality': {'CT': 984,
                                                 'MG': 2,
                                                 'MR': 813,
                                                 'NM': 2,
                                                 'None': 0,
                                                 'PT': 27},
                                    'Species': {'H. sapiens': 1828, 'None': 0},
                                    'collection_id': {'None': 0,
                                                      'tcga_blca': 106,
                                                      'tcga_brca': 139,
                                                      'tcga_cesc': 54,
                                                      'tcga_coad': 25,
                                                      'tcga_esca': 16,
                                                      'tcga_gbm': 262,
                                                      'tcga_hnsc': 227,
                                                      'tcga_kich': 15,
                                                      'tcga_kirc': 267,
                                                      'tcga_kirp': 33,
                                                      'tcga_lgg': 199,
                                                      'tcga_lihc': 97,
                                                      'tcga_luad': 69,
                                                      'tcga_lusc': 37,
                                                      'tcga_ov': 143,
                                                      'tcga_prad': 14,
                                                      'tcga_read': 3,
                                                      'tcga_sarc': 5,
                                                      'tcga_stad': 46,
                                                      'tcga_thca': 6,
                                                      'tcga_ucec': 65}},
                         'numFound': 1828}


        test_query_settings = {'collapse_on': 'PatientID',
             'collection': 'tcia_images',
             'counts_only': True,
             'facets': {'BodyPartExamined': {'field': 'BodyPartExamined',
                                             'limit': -1,
                                             'missing': True,
                                             'type': 'terms'},
                        'Modality': {'field': 'Modality',
                                     'limit': -1,
                                     'missing': True,
                                     'type': 'terms'},
                        'Species': {'field': 'Species',
                                    'limit': -1,
                                    'missing': True,
                                    'type': 'terms'},
                        'collection_id': {'domain': {'excludeTags': 'f0'},
                                          'field': 'collection_id',
                                          'limit': -1,
                                          'missing': True,
                                          'type': 'terms'}},
             'fields': ['collection_id',
                        'case_barcode',
                        'PatientID',
                        'race',
                        'vital_status',
                        'ethnicity',
                        'bmi',
                        'age_at_diagnosis',
                        'gender',
                        'disease_code',
                        'StudyInstanceUID',
                        'StudyDescription',
                        'StudyDate',
                        'SeriesInstanceUID',
                        'SeriesDescription',
                        'SeriesNumber',
                        'BodyPartExamined',
                        'Modality'],
             'fqs': ['{!tag=f0}(+collection_id:("tcga_blca" "tcga_brca" "tcga_cesc" '
                     '"tcga_coad" "tcga_esca" "tcga_gbm" "tcga_hnsc" "tcga_kich" '
                     '"tcga_kirc" "tcga_kirp" "tcga_lgg" "tcga_lihc" "tcga_luad" '
                     '"tcga_lusc" "tcga_ov" "tcga_prad" "tcga_read" "tcga_sarc" '
                     '"tcga_stad" "tcga_thca" "tcga_ucec"))'],
             'limit': 50000,
             'query_string': '(+collection_id:("tcga_ucec" "tcga_thca" "tcga_stad" '
                             '"tcga_sarc" "tcga_read" "tcga_prad" "tcga_ov" "tcga_lusc" '
                             '"tcga_luad" "tcga_lihc" "tcga_lgg" "tcga_kirp" "tcga_kirc" '
                             '"tcga_kich" "tcga_hnsc" "tcga_gbm" "tcga_esca" "tcga_coad" '
                             '"tcga_cesc" "tcga_brca" "tcga_blca"))'}

        test_result = solr_helpers.query_solr_and_format_result(query_settings=test_query_settings,
                                                                normalize_facets=True, normalize_groups=True)

        self.assertEquals(test_result, expected_result)
        return

    #
    # Just test that the underlying code works
    #

    @responses.activate
    def test_get_solr_data(self):

        #
        # get the mocking set up:
        #
        query_uri = "{}{}/query".format(self.SOLR_URI, "tcia_images")
        payload, response, test_vals = TestSolr._build_full_collection_payload_and_response()
        responses.add(responses.POST, query_uri, json=response, status=200)


        test_response = requests.post(query_uri, data=json.dumps(payload),
                                       headers={'Content-type': 'application/json'},
                                       auth=(self.SOLR_LOGIN, self.SOLR_PASSWORD),
                                       verify=self.SOLR_CERT)

        query_result = test_response.json()

        self.assertTrue(query_result is not None)
        #self.assertEquals(query_result, response)
        #pp = pprint.PrettyPrinter()
        #pp.pprint(query_result)
        self.assertTrue('facets' in query_result)
        facet_dict = query_result['facets']

        self.assertTrue('collection_id' in facet_dict)
        self.assertTrue('missing' in facet_dict['collection_id'])
        missing = facet_dict['collection_id']['missing']
        self.assertTrue('count' in missing)
        self.assertEquals(missing['count'], test_vals['MISSING_COLL_ID'])
        self.assertTrue('buckets' in facet_dict['collection_id'])
        bucket_list = facet_dict['collection_id']['buckets']
        self.assertEquals(len(bucket_list), test_vals['COLL_ID_BUCKET_COUNT'])
        found_bucket = False
        for bucket in bucket_list:
            if bucket['val'] == test_vals['COLL_ID_TEST_VAL']:
                self.assertEquals(bucket['count'], test_vals['COLL_ID_TEST_COUNT'])
                found_bucket = True
                break
        self.assertTrue(found_bucket)

        self.assertTrue('BodyPartExamined' in facet_dict)
        self.assertTrue('missing' in facet_dict['BodyPartExamined'])
        missing = facet_dict['BodyPartExamined']['missing']
        self.assertTrue('count' in missing)
        self.assertEquals(missing['count'], test_vals['MISSING_BPEX'])
        self.assertTrue('buckets' in facet_dict['BodyPartExamined'])
        bucket_list = facet_dict['BodyPartExamined']['buckets']
        self.assertEquals(len(bucket_list), test_vals['BPEX_BUCKET_COUNT'])
        found_bucket = False
        for bucket in bucket_list:
            if bucket['val'] == test_vals['BPEX_TEST_VAL']:
                self.assertEquals(bucket['count'], test_vals['BPEX_TEST_COUNT'])
                found_bucket = True
                break
        self.assertTrue(found_bucket)

        self.assertTrue('Modality' in facet_dict)
        self.assertTrue('missing' in facet_dict['Modality'])
        missing = facet_dict['Modality']['missing']
        self.assertTrue('count' in missing)
        self.assertEquals(missing['count'], test_vals['MISSING_MOD'])
        self.assertTrue('buckets' in facet_dict['Modality'])
        bucket_list = facet_dict['Modality']['buckets']
        self.assertEquals(len(bucket_list), test_vals['MOD_BUCKET_COUNT'])
        found_bucket = False
        for bucket in bucket_list:
            if bucket['val'] == test_vals['MOD_TEST_VAL']:
                found_bucket = True
                self.assertEquals(bucket['count'], test_vals['MOD_TEST_COUNT'])
                break
        self.assertTrue(found_bucket)

        self.assertTrue('Species' in facet_dict)
        self.assertTrue('missing' in facet_dict['Species'])
        missing = facet_dict['Species']['missing']
        self.assertTrue('count' in missing)
        self.assertEquals(missing['count'], test_vals['MISSING_SPEC'])
        self.assertTrue('buckets' in facet_dict['Species'])
        bucket_list = facet_dict['Species']['buckets']
        self.assertEquals(len(bucket_list), test_vals['SPEC_BUCKET_COUNT'])
        found_bucket = False
        for bucket in bucket_list:
            if bucket['val'] == test_vals['SPEC_TEST_VAL']:
                found_bucket = True
                self.assertEquals(bucket['count'], test_vals['SPEC_TEST_COUNT'])
                break
        self.assertTrue(found_bucket)

        self.assertTrue('response' in query_result)
        self.assertEquals(query_result['response']['numFound'], test_vals['TOTAL_SIZE'])
        return

    #
    # Build a payload that asks for the full collection, plus archived response:
    #

    @staticmethod
    def _build_full_collection_payload_and_response():

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

        #
        # Return value of dev SOLR server recorded on 2/29/19
        #

        archived_response = {
            'facets':
                {'BodyPartExamined': {'buckets': [{'count': 6946, 'val': 'BREAST'},
                                                  {'count': 1476, 'val': 'CHEST'},
                                                  {'count': 850,  'val': 'COLON'},
                                                  {'count': 829,  'val': 'LUNG'},
                                                  {'count': 776,  'val': 'BRAIN'},
                                                  {'count': 551,  'val': 'PROSTATE'},
                                                  {'count': 548,  'val': 'HEADNECK'},
                                                  {'count': 318,  'val': 'Left Breast'},
                                                  {'count': 295,  'val': 'KIDNEY'},
                                                  {'count': 248,  'val': 'Right Breast'},
                                                  {'count': 197,  'val': 'ABDOMEN'},
                                                  {'count': 143,  'val': 'OVARY'},
                                                  {'count': 106,  'val': 'BLADDER'},
                                                  {'count': 101,  'val': 'PANCREAS'},
                                                  {'count': 97,   'val': 'LIVER'},
                                                  {'count': 90,   'val': 'MEDIASTINUM'},
                                                  {'count': 58, 'val': 'UTERUS'},
                                                  {'count': 54, 'val': 'CERVIX'},
                                                  {'count': 51, 'val': 'EXTREMITY'},
                                                  {'count': 48, 'val': 'SKULL'},
                                                  {'count': 46, 'val': 'STOMACH'},
                                                  {'count': 32, 'val': 'PHANTOM'},
                                                  {'count': 20, 'val': 'PELVIS'},
                                                  {'count': 17, 'val': 'Phantom'},
                                                  {'count': 16, 'val': 'ESOPHAGUS'},
                                                  {'count': 16, 'val': 'Kidney'},
                                                  {'count': 7, 'val': 'NECK'},
                                                  {'count': 6, 'val': 'THYROID'},
                                                  {'count': 4, 'val': 'HEAD'},
                                                  {'count': 3,'val': 'CHESTABDPELVIS'},
                                                  {'count': 3, 'val': 'RECTUM'},
                                                  {'count': 2, 'val': 'HEART'},
                                                  {'count': 1, 'val': 'CAROTID'},
                                                  {'count': 1, 'val': 'CHEST_TO_PELVIS'},
                                                  {'count': 1, 'val': 'J BRZUSZNA'},
                                                  {'count': 1, 'val': 'J brzuszna'},
                                                  {'count': 1, 'val': 'LEG'},
                                                  {'count': 1, 'val': 'LUMBO-SACRAL SPI'},
                                                  {'count': 1, 'val': 'THORAX_1HEAD_NEC'},
                                                  {'count': 1, 'val': 'TSPINE'}],
                                       'missing': {'count': 720}
                },
                'Modality': {'buckets': [{'count': 6996, 'val': 'MG'},
                                         {'count': 4653, 'val': 'CT'},
                                         {'count': 2257, 'val': 'MR'},
                                         {'count': 526, 'val': 'PT'},
                                         {'count': 86, 'val': 'DX'},
                                         {'count': 46, 'val': 'RTSTRUCT'},
                                         {'count': 41, 'val': 'RTDOSE'},
                                         {'count': 28, 'val': 'SEG'},
                                         {'count': 19, 'val': 'CR'},
                                         {'count': 7, 'val': 'KO'},
                                         {'count': 6, 'val': 'RTPLAN'},
                                         {'count': 5, 'val': 'SC'},
                                         {'count': 5, 'val': 'SR'},
                                         {'count': 4, 'val': 'REG'},
                                         {'count': 2, 'val': 'NM'},
                                         {'count': 1, 'val': 'PR'}],
                              'missing': {'count': 0}
                },
                'Species': {'buckets': [{'count': 14682, 'val': 'H. sapiens'}],
                             'missing': {'count': 0}
                },
                'collection_id': {'buckets': [{'count': 6991, 'val': 'cbis_ddsm'},
                                              {'count': 1010, 'val': 'lidc_idri'},
                                              {'count': 825, 'val': 'ct_colonography'},
                                              {'count': 422, 'val': 'nsclc_radiomics'},
                                              {'count': 346, 'val': 'prostatex'},
                                              {'count': 298, 'val': 'head_neck_pet_ct'},
                                              {'count': 267, 'val': 'tcga_kirc'},
                                              {'count': 262, 'val': 'tcga_gbm'},
                                              {'count': 243, 'val': 'rider_lung_pet_ct'},
                                              {'count': 227, 'val': 'tcga_hnsc'},
                                              {'count': 222, 'val': 'ispy1'},
                                              {'count': 215, 'val': 'hnscc'},
                                              {'count': 211, 'val': 'nsclc_radiogenomics'},
                                              {'count': 199, 'val': 'tcga_lgg'},
                                              {'count': 176, 'val': 'ct_lymph_nodes'},
                                              {'count': 159, 'val': 'lgg_1p19qdeletion'},
                                              {'count': 156, 'val': 'qin_headneck'},
                                              {'count': 143, 'val': 'tcga_ov'},
                                              {'count': 139, 'val': 'tcga_brca'},
                                              {'count': 130, 'val': 'rembrandt'},
                                              {'count': 111, 'val': 'head_neck_cetuximab'},
                                              {'count': 106, 'val': 'tcga_blca'},
                                              {'count': 97, 'val': 'tcga_lihc'},
                                              {'count': 91, 'val': 'prostate_diagnosis'},
                                              {'count': 89, 'val': 'nsclc_radiomics_genomics'},
                                              {'count': 88, 'val': 'breast_diagnosis'},
                                              {'count': 82, 'val': 'pancreas_ct'},
                                              {'count': 70, 'val': 'spie_aapm_lung_ct_challenge'},
                                              {'count': 69, 'val': 'tcga_luad'},
                                              {'count': 65, 'val': 'tcga_ucec'},
                                              {'count': 64, 'val': 'breast_mri_nact_pilot'},
                                              {'count': 64, 'val': 'prostate_3t'},
                                              {'count': 61, 'val': 'lungct_diagnosis'},
                                              {'count': 60, 'val': 'lctsc'},
                                              {'count': 54, 'val': 'tcga_cesc'},
                                              {'count': 51, 'val': 'soft_tissue_sarcoma'},
                                              {'count': 48, 'val': 'mouse_astrocytoma'},
                                              {'count': 47, 'val': 'anti_pd_1_melanoma'},
                                              {'count': 47, 'val': 'qin_lung_ct'},
                                              {'count': 46, 'val': 'tcga_stad'},
                                              {'count': 45, 'val': 'cptac_pda'},
                                              {'count': 39, 'val': 'ivygap'},
                                              {'count': 37, 'val': 'tcga_lusc'},
                                              {'count': 35, 'val': 'cptac_ucec'},
                                              {'count': 33, 'val': 'tcga_kirp'},
                                              {'count': 32, 'val': 'cptac_gbm'},
                                              {'count': 32, 'val': 'mouse_mammary'},
                                              {'count': 32, 'val': 'rider_lung_ct'},
                                              {'count': 31, 'val': 'hnscc_3dct_rt'},
                                              {'count': 28, 'val': 'prostate_fused_mri_pathology'},
                                              {'count': 26, 'val': 'cptac_ccrcc'},
                                              {'count': 25, 'val': 'tcga_coad'},
                                              {'count': 22, 'val': 'cptac_hnscc'},
                                              {'count': 20, 'val': '4d_lung'},
                                              {'count': 20, 'val': 'rider_phantom_pet_ct'},
                                              {'count': 19, 'val': 'rider_neuro_mri'},
                                              {'count': 17, 'val': 'cc_radiomics_phantom'},
                                              {'count': 16, 'val': 'tcga_esca'},
                                              {'count': 15, 'val': 'tcga_kich'},
                                              {'count': 14, 'val': 'tcga_prad'},
                                              {'count': 12, 'val': 'cptac_luad'},
                                              {'count': 10, 'val': 'qin_breast_dce_mri'},
                                              {'count': 10, 'val': 'rider_phantom_mri'},
                                              {'count': 9, 'val': 'mri_dir'},
                                              {'count': 9, 'val': 'naf_prostate'},
                                              {'count': 7, 'val': 'apollo'},
                                              {'count': 7, 'val': 'phantom_fda'},
                                              {'count': 6, 'val': 'tcga_thca'},
                                              {'count': 5, 'val': 'rider_breast_mri'},
                                              {'count': 5, 'val': 'tcga_sarc'},
                                              {'count': 4, 'val': 'cptac_lscc'},
                                              {'count': 3, 'val': 'tcga_read'},
                                              {'count': 2, 'val': 'cptac_cm'},
                                              {'count': 2, 'val': 'qin_pet_phantom'},
                                              {'count': 1, 'val': 'lung_phantom'},
                                              {'count': 1, 'val': 'qiba_ct_1c'}],
                                   'missing': {'count': 0}
                },
            'count': 14682},
            'response': {'docs': [], 'numFound': 14682, 'start': 0}
        }

        #
        # Magic numbers that will change when solr collections are updated
        # Current as of: 2/29/2020
        #
        test_vals = {
            'TOTAL_SIZE': 14682,
            'MISSING_COLL_ID': 0,
            'COLL_ID_BUCKET_COUNT': 76,
            'COLL_ID_TEST_VAL': 'tcga_blca',
            'COLL_ID_TEST_COUNT': 106,

            'MISSING_BPEX': 720,
            'BPEX_BUCKET_COUNT': 40,
            'BPEX_TEST_VAL': 'BREAST',
            'BPEX_TEST_COUNT': 6946,

            'MISSING_MOD': 0,
            'MOD_BUCKET_COUNT': 16,
            'MOD_TEST_VAL': 'CT',
            'MOD_TEST_COUNT': 4653,

            'MISSING_SPEC': 0,
            'SPEC_BUCKET_COUNT': 1,
            'SPEC_TEST_VAL': 'H. sapiens',
            'SPEC_TEST_COUNT': 14682
        }


        return payload, archived_response, test_vals

    #
    # Get the archived response for the full tcga collections, to use for request mocking:
    #

    @staticmethod
    def _get_archived_tcga_image_response():

        archived_result = {'facets': {'BodyPartExamined': {'buckets': [{'count': 461, 'val': 'BRAIN'},
                                         {'count': 295, 'val': 'KIDNEY'},
                                         {'count': 192, 'val': 'HEADNECK'},
                                         {'count': 143, 'val': 'OVARY'},
                                         {'count': 139, 'val': 'BREAST'},
                                         {'count': 106, 'val': 'BLADDER'},
                                         {'count': 97, 'val': 'LIVER'},
                                         {'count': 68, 'val': 'LUNG'},
                                         {'count': 58, 'val': 'UTERUS'},
                                         {'count': 54, 'val': 'CERVIX'},
                                         {'count': 46, 'val': 'STOMACH'},
                                         {'count': 38, 'val': 'CHEST'},
                                         {'count': 25, 'val': 'COLON'},
                                         {'count': 16, 'val': 'ESOPHAGUS'},
                                         {'count': 16, 'val': 'Kidney'},
                                         {'count': 14, 'val': 'PROSTATE'},
                                         {'count': 6, 'val': 'THYROID'},
                                         {'count': 3,
                                          'val': 'CHESTABDPELVIS'},
                                         {'count': 3, 'val': 'RECTUM'},
                                         {'count': 1, 'val': 'LEG'},
                                         {'count': 1, 'val': 'TSPINE'}],
                             'missing': {'count': 46}},
                            'Modality': {'buckets': [{'count': 984, 'val': 'CT'},
                                                     {'count': 813, 'val': 'MR'},
                                                     {'count': 27, 'val': 'PT'},
                                                     {'count': 2, 'val': 'MG'},
                                                     {'count': 2, 'val': 'NM'}],
                                         'missing': {'count': 0}},
                            'Species': {'buckets': [{'count': 1828, 'val': 'H. sapiens'}],
                                        'missing': {'count': 0}},
                            'collection_id': {'buckets': [{'count': 267, 'val': 'tcga_kirc'},
                                                          {'count': 262, 'val': 'tcga_gbm'},
                                                          {'count': 227, 'val': 'tcga_hnsc'},
                                                          {'count': 199, 'val': 'tcga_lgg'},
                                                          {'count': 143, 'val': 'tcga_ov'},
                                                          {'count': 139, 'val': 'tcga_brca'},
                                                          {'count': 106, 'val': 'tcga_blca'},
                                                          {'count': 97, 'val': 'tcga_lihc'},
                                                          {'count': 69, 'val': 'tcga_luad'},
                                                          {'count': 65, 'val': 'tcga_ucec'},
                                                          {'count': 54, 'val': 'tcga_cesc'},
                                                          {'count': 46, 'val': 'tcga_stad'},
                                                          {'count': 37, 'val': 'tcga_lusc'},
                                                          {'count': 33, 'val': 'tcga_kirp'},
                                                          {'count': 25, 'val': 'tcga_coad'},
                                                          {'count': 16, 'val': 'tcga_esca'},
                                                          {'count': 15, 'val': 'tcga_kich'},
                                                          {'count': 14, 'val': 'tcga_prad'},
                                                          {'count': 6, 'val': 'tcga_thca'},
                                                          {'count': 5, 'val': 'tcga_sarc'},
                                                          {'count': 3, 'val': 'tcga_read'}],
                                              'missing': {'count': 0}},
                                             'count': 1828},
                         'response': {'docs': [], 'numFound': 1828, 'start': 0},
                         'responseHeader': {'QTime': 259,
                                            'params': {'json': '{"offset": 0, "query": '
                                                               '"(+collection_id:(\\"tcga_ucec\\" '
                                                               '\\"tcga_thca\\" \\"tcga_stad\\" '
                                                               '\\"tcga_sarc\\" \\"tcga_read\\" '
                                                               '\\"tcga_prad\\" \\"tcga_ov\\" '
                                                               '\\"tcga_lusc\\" \\"tcga_luad\\" '
                                                               '\\"tcga_lihc\\" \\"tcga_lgg\\" '
                                                               '\\"tcga_kirp\\" \\"tcga_kirc\\" '
                                                               '\\"tcga_kich\\" \\"tcga_hnsc\\" '
                                                               '\\"tcga_gbm\\" \\"tcga_esca\\" '
                                                               '\\"tcga_coad\\" \\"tcga_cesc\\" '
                                                               '\\"tcga_brca\\" \\"tcga_blca\\"))", '
                                                               '"params": {"debugQuery": "off"}, '
                                                               '"limit": 0, "facet": '
                                                               '{"BodyPartExamined": {"limit": -1, '
                                                               '"field": "BodyPartExamined", '
                                                               '"missing": true, "type": "terms"}, '
                                                               '"Modality": {"limit": -1, "field": '
                                                               '"Modality", "missing": true, "type": '
                                                               '"terms"}, "collection_id": {"limit": '
                                                               '-1, "domain": {"excludeTags": "f0"}, '
                                                               '"field": "collection_id", "missing": '
                                                               'true, "type": "terms"}, "Species": '
                                                               '{"limit": -1, "field": "Species", '
                                                               '"missing": true, "type": "terms"}}, '
                                                               '"filter": '
                                                               '["{!tag=f0}(+collection_id:(\\"tcga_ucec\\" '
                                                               '\\"tcga_thca\\" \\"tcga_stad\\" '
                                                               '\\"tcga_sarc\\" \\"tcga_read\\" '
                                                               '\\"tcga_prad\\" \\"tcga_ov\\" '
                                                               '\\"tcga_lusc\\" \\"tcga_luad\\" '
                                                               '\\"tcga_lihc\\" \\"tcga_lgg\\" '
                                                               '\\"tcga_kirp\\" \\"tcga_kirc\\" '
                                                               '\\"tcga_kich\\" \\"tcga_hnsc\\" '
                                                               '\\"tcga_gbm\\" \\"tcga_esca\\" '
                                                               '\\"tcga_coad\\" \\"tcga_cesc\\" '
                                                               '\\"tcga_brca\\" \\"tcga_blca\\"))", '
                                                               '"{!collapse field=PatientID}"], '
                                                               '"fields": ["project_short_name", '
                                                               '"project_name", "disease_code", '
                                                               '"gender", "vital_status", "race", '
                                                               '"ethnicity", "age_at_diagnosis", '
                                                               '"pathologic_stage", '
                                                               '"tumor_tissue_site", "country", '
                                                               '"histological_type", '
                                                               '"neoplasm_histologic_grade", "bmi", '
                                                               '"sample_type", "sample_type_name"]}'},
                                            'status': 0}}
        return archived_result

    #
    # Get the archived response for the full tcga clinical collections, to use for request mocking:
    #

    @staticmethod
    def _get_archived_tcga_clinical_response():

        archived_result =  {'facets': {'age_at_diagnosis:* to 10': {'count': 0},
                 'age_at_diagnosis:10 to 20': {'count': 6},
                 'age_at_diagnosis:20 to 30': {'count': 53},
                 'age_at_diagnosis:30 to 40': {'count': 120},
                 'age_at_diagnosis:40 to 50': {'count': 238},
                 'age_at_diagnosis:50 to 60': {'count': 456},
                 'age_at_diagnosis:60 to 70': {'count': 489},
                 'age_at_diagnosis:70 to 80': {'count': 339},
                 'age_at_diagnosis:80 to *': {'count': 79},
                 'age_at_diagnosis:None': {'count': 40},
                 'bmi:normal': {'count': 108},
                 'bmi:obese': {'count': 127},
                 'bmi:overweight': {'count': 111},
                 'bmi:underweight': {'count': 11},
                 'count': 1820,
                 'country': {'buckets': [{'count': 718, 'val': 'United States'},
                                         {'count': 144, 'val': 'Brazil'},
                                         {'count': 62, 'val': 'Canada'},
                                         {'count': 31, 'val': 'United Kingdom'},
                                         {'count': 1, 'val': 'Sao Paulo'}],
                             'missing': {'count': 864}},
                 'disease_code': {'buckets': [{'count': 267, 'val': 'KIRC'},
                                              {'count': 262, 'val': 'GBM'},
                                              {'count': 226, 'val': 'HNSC'},
                                              {'count': 197, 'val': 'LGG'},
                                              {'count': 142, 'val': 'OV'},
                                              {'count': 139, 'val': 'BRCA'},
                                              {'count': 106, 'val': 'BLCA'},
                                              {'count': 97, 'val': 'LIHC'},
                                              {'count': 69, 'val': 'LUAD'},
                                              {'count': 65, 'val': 'UCEC'},
                                              {'count': 54, 'val': 'CESC'},
                                              {'count': 46, 'val': 'STAD'},
                                              {'count': 37, 'val': 'LUSC'},
                                              {'count': 33, 'val': 'KIRP'},
                                              {'count': 21, 'val': 'COAD'},
                                              {'count': 16, 'val': 'ESCA'},
                                              {'count': 15, 'val': 'KICH'},
                                              {'count': 14, 'val': 'PRAD'},
                                              {'count': 6, 'val': 'THCA'},
                                              {'count': 5, 'val': 'SARC'},
                                              {'count': 3, 'val': 'READ'}],
                                  'missing': {'count': 0}},
                 'ethnicity': {'buckets': [{'count': 1341,
                                            'val': 'NOT HISPANIC OR LATINO'},
                                           {'count': 63,
                                            'val': 'HISPANIC OR LATINO'}],
                               'missing': {'count': 416}},
                 'gender': {'buckets': [{'count': 894, 'val': 'MALE'},
                                        {'count': 886, 'val': 'FEMALE'}],
                            'missing': {'count': 40}},
                 'histological_type': {'buckets': [{'count': 267,
                                                    'val': 'Kidney Clear Cell Renal '
                                                           'Carcinoma'},
                                                   {'count': 224,
                                                    'val': 'Head & Neck Squamous '
                                                           'Cell Carcinoma'},
                                                   {'count': 206,
                                                    'val': 'Untreated primary (de '
                                                           'novo) GBM'},
                                                   {'count': 142,
                                                    'val': 'Serous '
                                                           'Cystadenocarcinoma'},
                                                   {'count': 111,
                                                    'val': 'Infiltrating Ductal '
                                                           'Carcinoma'},
                                                   {'count': 106,
                                                    'val': 'Muscle invasive '
                                                           'urothelial carcinoma '
                                                           '(pT2 or above)'},
                                                   {'count': 95,
                                                    'val': 'Hepatocellular '
                                                           'Carcinoma'},
                                                   {'count': 85,
                                                    'val': 'Oligodendroglioma'},
                                                   {'count': 64,
                                                    'val': 'Astrocytoma'},
                                                   {'count': 52,
                                                    'val': 'Glioblastoma Multiforme '
                                                           '(GBM)'},
                                                   {'count': 47,
                                                    'val': 'Oligoastrocytoma'},
                                                   {'count': 46,
                                                    'val': 'Endometrioid '
                                                           'endometrial '
                                                           'adenocarcinoma'},
                                                   {'count': 42,
                                                    'val': 'Cervical Squamous Cell '
                                                           'Carcinoma'},
                                                   {'count': 33,
                                                    'val': 'Kidney Papillary Renal '
                                                           'Cell Carcinoma'},
                                                   {'count': 27,
                                                    'val': 'Lung Squamous Cell '
                                                           'Carcinoma- Not '
                                                           'Otherwise Specified '
                                                           '(NOS)'},
                                                   {'count': 22,
                                                    'val': 'Lung Adenocarcinoma- '
                                                           'Not Otherwise Specified '
                                                           '(NOS)'},
                                                   {'count': 20,
                                                    'val': 'Infiltrating Lobular '
                                                           'Carcinoma'},
                                                   {'count': 18,
                                                    'val': 'Colon Adenocarcinoma'},
                                                   {'count': 17,
                                                    'val': 'Serous endometrial '
                                                           'adenocarcinoma'},
                                                   {'count': 17,
                                                    'val': 'Stomach, Intestinal '
                                                           'Adenocarcinoma, Not '
                                                           'Otherwise Specified '
                                                           '(NOS)'},
                                                   {'count': 17,
                                                    'val': 'Stomach, Intestinal '
                                                           'Adenocarcinoma, Tubular '
                                                           'Type'},
                                                   {'count': 15,
                                                    'val': 'Kidney Chromophobe'},
                                                   {'count': 14,
                                                    'val': 'Esophagus Squamous Cell '
                                                           'Carcinoma'},
                                                   {'count': 14,
                                                    'val': 'Prostate Adenocarcinoma '
                                                           'Acinar Type'},
                                                   {'count': 12,
                                                    'val': 'Mucinous Adenocarcinoma '
                                                           'of Endocervical Type'},
                                                   {'count': 7,
                                                    'val': 'Squamous Cell '
                                                           'Carcinoma, Not '
                                                           'Otherwise Specified'},
                                                   {'count': 6,
                                                    'val': 'Lung Adenocarcinoma '
                                                           'Mixed Subtype'},
                                                   {'count': 5,
                                                    'val': 'Classical/Usual '
                                                           '(Papillary NOS)'},
                                                   {'count': 5,
                                                    'val': 'Other, specify'},
                                                   {'count': 4,
                                                    'val': 'Stomach Adenocarcinoma, '
                                                           'Signet Ring Type'},
                                                   {'count': 4,
                                                    'val': 'Stomach, '
                                                           'Adenocarcinoma, Not '
                                                           'Otherwise Specified '
                                                           '(NOS)'},
                                                   {'count': 3,
                                                    'val': 'Colon Mucinous '
                                                           'Adenocarcinoma'},
                                                   {'count': 3,
                                                    'val': 'Lung Basaloid Squamous '
                                                           'Cell Carcinoma'},
                                                   {'count': 3,
                                                    'val': 'Rectal Adenocarcinoma'},
                                                   {'count': 3,
                                                    'val': 'Treated primary GBM'},
                                                   {'count': 2,
                                                    'val': 'Esophagus '
                                                           'Adenocarcinoma, NOS'},
                                                   {'count': 2,
                                                    'val': 'Head & Neck Squamous '
                                                           'Cell Carcinoma Basaloid '
                                                           'Type'},
                                                   {'count': 2,
                                                    'val': 'Leiomyosarcoma (LMS)'},
                                                   {'count': 2,
                                                    'val': 'Malignant Peripheral '
                                                           'Nerve Sheath Tumors '
                                                           '(MPNST)'},
                                                   {'count': 2,
                                                    'val': 'Mixed Histology (please '
                                                           'specify)'},
                                                   {'count': 2,
                                                    'val': 'Mixed serous and '
                                                           'endometrioid'},
                                                   {'count': 2,
                                                    'val': 'Stomach, Intestinal '
                                                           'Adenocarcinoma, '
                                                           'Mucinous Type'},
                                                   {'count': 1,
                                                    'val': 'Hepatocholangiocarcinoma '
                                                           '(Mixed)'},
                                                   {'count': 1,
                                                    'val': 'Liver - Fibrolamellar '
                                                           'hepatocellular '
                                                           'carcinoma'},
                                                   {'count': 1,
                                                    'val': 'Lung Mucinous '
                                                           'Adenocarcinoma'},
                                                   {'count': 1,
                                                    'val': 'Lung Papillary '
                                                           'Adenocarcinoma'},
                                                   {'count': 1,
                                                    'val': 'Medullary Carcinoma'},
                                                   {'count': 1,
                                                    'val': 'Mucinous (Colloid) '
                                                           'Carcinoma'},
                                                   {'count': 1,
                                                    'val': 'Stomach, '
                                                           'Adenocarcinoma, Diffuse '
                                                           'Type'},
                                                   {'count': 1,
                                                    'val': 'Stomach, Intestinal '
                                                           'Adenocarcinoma, '
                                                           'Papillary Type'},
                                                   {'count': 1,
                                                    'val': 'Thyroid Papillary '
                                                           'Carcinoma - Follicular '
                                                           '(>= 99% follicular '
                                                           'patterned)'},
                                                   {'count': 1,
                                                    'val': 'Undifferentiated '
                                                           'Pleomorphic Sarcoma '
                                                           '(UPS)'}],
                                       'missing': {'count': 40}},
                 'neoplasm_histologic_grade': {'buckets': [{'count': 499,
                                                            'val': 'G3'},
                                                           {'count': 481,
                                                            'val': 'G2'},
                                                           {'count': 111,
                                                            'val': 'High Grade'},
                                                           {'count': 57,
                                                            'val': 'G1'},
                                                           {'count': 44,
                                                            'val': 'G4'},
                                                           {'count': 22,
                                                            'val': 'GX'}],
                                               'missing': {'count': 606}},
                 'pathologic_stage': {'buckets': [{'count': 249, 'val': 'Stage I'},
                                                  {'count': 157, 'val': 'Stage III'},
                                                  {'count': 121, 'val': 'Stage II'},
                                                  {'count': 108, 'val': 'Stage IVA'},
                                                  {'count': 95, 'val': 'Stage IV'},
                                                  {'count': 73, 'val': 'Stage IIA'},
                                                  {'count': 54, 'val': 'Stage IIIA'},
                                                  {'count': 47, 'val': 'Stage IIB'},
                                                  {'count': 29, 'val': 'Stage IIIB'},
                                                  {'count': 21, 'val': 'Stage IIIC'},
                                                  {'count': 19, 'val': 'Stage IA'},
                                                  {'count': 15, 'val': 'Stage IB'},
                                                  {'count': 6, 'val': 'Stage IVB'}],
                                      'missing': {'count': 826}},
                 'project_name': {'buckets': [{'count': 267,
                                               'val': 'Kidney Renal Clear Cell '
                                                      'Carcinoma'},
                                              {'count': 262,
                                               'val': 'Glioblastoma Multiforme'},
                                              {'count': 226,
                                               'val': 'Head and Neck Squamous Cell '
                                                      'Carcinoma'},
                                              {'count': 197,
                                               'val': 'Brain Lower Grade Glioma'},
                                              {'count': 142,
                                               'val': 'Ovarian Serous '
                                                      'Cystadenocarcinoma'},
                                              {'count': 139,
                                               'val': 'Breast Invasive Carcinoma'},
                                              {'count': 106,
                                               'val': 'Bladder Urothelial '
                                                      'Carcinoma'},
                                              {'count': 97,
                                               'val': 'Liver Hepatocellular '
                                                      'Carcinoma'},
                                              {'count': 69,
                                               'val': 'Lung Adenocarcinoma'},
                                              {'count': 65,
                                               'val': 'Uterine Corpus Endometrial '
                                                      'Carcinoma'},
                                              {'count': 54,
                                               'val': 'Cervical Squamous Cell '
                                                      'Carcinoma and Endocervical '
                                                      'Adenocarcinoma'},
                                              {'count': 46,
                                               'val': 'Stomach Adenocarcinoma'},
                                              {'count': 37,
                                               'val': 'Lung Squamous Cell '
                                                      'Carcinoma'},
                                              {'count': 33,
                                               'val': 'Kidney Renal Papillary Cell '
                                                      'Carcinoma'},
                                              {'count': 21,
                                               'val': 'Colon Adenocarcinoma'},
                                              {'count': 16,
                                               'val': 'Esophageal Carcinoma'},
                                              {'count': 15,
                                               'val': 'Kidney Chromophobe'},
                                              {'count': 14,
                                               'val': 'Prostate Adenocarcinoma'},
                                              {'count': 6,
                                               'val': 'Thyroid Carcinoma'},
                                              {'count': 5, 'val': 'Sarcoma'},
                                              {'count': 3,
                                               'val': 'Rectum Adenocarcinoma'}],
                                  'missing': {'count': 0}},
                 'project_short_name': {'buckets': [{'count': 267,
                                                     'val': 'TCGA-KIRC'},
                                                    {'count': 262,
                                                     'val': 'TCGA-GBM'},
                                                    {'count': 226,
                                                     'val': 'TCGA-HNSC'},
                                                    {'count': 197,
                                                     'val': 'TCGA-LGG'},
                                                    {'count': 142, 'val': 'TCGA-OV'},
                                                    {'count': 139,
                                                     'val': 'TCGA-BRCA'},
                                                    {'count': 106,
                                                     'val': 'TCGA-BLCA'},
                                                    {'count': 97,
                                                     'val': 'TCGA-LIHC'},
                                                    {'count': 69,
                                                     'val': 'TCGA-LUAD'},
                                                    {'count': 65,
                                                     'val': 'TCGA-UCEC'},
                                                    {'count': 54,
                                                     'val': 'TCGA-CESC'},
                                                    {'count': 46,
                                                     'val': 'TCGA-STAD'},
                                                    {'count': 37,
                                                     'val': 'TCGA-LUSC'},
                                                    {'count': 33,
                                                     'val': 'TCGA-KIRP'},
                                                    {'count': 21,
                                                     'val': 'TCGA-COAD'},
                                                    {'count': 16,
                                                     'val': 'TCGA-ESCA'},
                                                    {'count': 15,
                                                     'val': 'TCGA-KICH'},
                                                    {'count': 14,
                                                     'val': 'TCGA-PRAD'},
                                                    {'count': 6, 'val': 'TCGA-THCA'},
                                                    {'count': 5, 'val': 'TCGA-SARC'},
                                                    {'count': 3,
                                                     'val': 'TCGA-READ'}],
                                        'missing': {'count': 0}},
                 'race': {'buckets': [{'count': 1501, 'val': 'WHITE'},
                                      {'count': 155,
                                       'val': 'BLACK OR AFRICAN AMERICAN'},
                                      {'count': 45, 'val': 'ASIAN'},
                                      {'count': 7,
                                       'val': 'AMERICAN INDIAN OR ALASKA NATIVE'},
                                      {'count': 1,
                                       'val': 'NATIVE HAWAIIAN OR OTHER PACIFIC '
                                              'ISLANDER'}],
                          'missing': {'count': 111}},
                 'sample_type': {'buckets': [{'count': 1814, 'val': '01'},
                                             {'count': 5, 'val': '06'},
                                             {'count': 1, 'val': '11'}],
                                 'missing': {'count': 0}},
                 'sample_type_name': {'buckets': [{'count': 1814,
                                                   'val': 'Primary solid Tumor'},
                                                  {'count': 5, 'val': 'Metastatic'},
                                                  {'count': 1,
                                                   'val': 'Solid Tissue Normal'}],
                                      'missing': {'count': 0}},
                 'tumor_tissue_site': {'buckets': [{'count': 315, 'val': 'Kidney'},
                                                   {'count': 260, 'val': 'Brain'},
                                                   {'count': 226,
                                                    'val': 'Head and Neck'},
                                                   {'count': 197,
                                                    'val': 'Central nervous system'},
                                                   {'count': 142, 'val': 'Ovary'},
                                                   {'count': 139, 'val': 'Breast'},
                                                   {'count': 106, 'val': 'Bladder'},
                                                   {'count': 97, 'val': 'Liver'},
                                                   {'count': 68, 'val': 'Lung'},
                                                   {'count': 65,
                                                    'val': 'Endometrial'},
                                                   {'count': 54, 'val': 'Cervical'},
                                                   {'count': 46, 'val': 'Stomach'},
                                                   {'count': 21, 'val': 'Colon'},
                                                   {'count': 16, 'val': 'Esophagus'},
                                                   {'count': 14, 'val': 'Prostate'},
                                                   {'count': 6, 'val': 'Thyroid'},
                                                   {'count': 3, 'val': 'Rectum'},
                                                   {'count': 1,
                                                    'val': 'Chest - Other (please '
                                                           'specify'},
                                                   {'count': 1,
                                                    'val': 'Lower Extremity - Lower '
                                                           'leg/calf'},
                                                   {'count': 1,
                                                    'val': 'Lower Extremity - '
                                                           'Thigh/knee'},
                                                   {'count': 1,
                                                    'val': 'Lower abdominal/Pelvic '
                                                           '- Pelvic'},
                                                   {'count': 1,
                                                    'val': 'Superficial Trunk - '
                                                           'Back'}],
                                       'missing': {'count': 40}},
                 'vital_status': {'buckets': [{'count': 1030, 'val': 'Alive'},
                                              {'count': 749, 'val': 'Dead'}],
                                  'missing': {'count': 41}}},
                  'response': {'docs': [], 'numFound': 1820, 'start': 0},
                  'responseHeader': {'QTime': 2,
                                     'params': {'json': '{"limit": 0, "facet": {"bmi:normal": '
                                                        '{"q": "bmi:[18.5 TO 25}", "field": '
                                                        '"bmi", "limit": -1, "type": "query"}, '
                                                        '"age_at_diagnosis:60 to 70": {"q": '
                                                        '"age_at_diagnosis:[60 TO 70}", '
                                                        '"field": "age_at_diagnosis", "limit": '
                                                        '-1, "type": "query"}, '
                                                        '"project_short_name": {"missing": '
                                                        'true, "field": "project_short_name", '
                                                        '"limit": -1, "type": "terms"}, '
                                                        '"vital_status": {"missing": true, '
                                                        '"field": "vital_status", "limit": -1, '
                                                        '"type": "terms"}, "tumor_tissue_site": '
                                                        '{"missing": true, "field": '
                                                        '"tumor_tissue_site", "limit": -1, '
                                                        '"type": "terms"}, "bmi:underweight": '
                                                        '{"q": "bmi:[* TO 18.5}", "field": '
                                                        '"bmi", "limit": -1, "type": "query"}, '
                                                        '"disease_code": {"missing": true, '
                                                        '"field": "disease_code", "limit": -1, '
                                                        '"type": "terms"}, "age_at_diagnosis:50 '
                                                        'to 60": {"q": "age_at_diagnosis:[50 TO '
                                                        '60}", "field": "age_at_diagnosis", '
                                                        '"limit": -1, "type": "query"}, "race": '
                                                        '{"missing": true, "field": "race", '
                                                        '"limit": -1, "type": "terms"}, '
                                                        '"age_at_diagnosis:* to 10": {"q": '
                                                        '"age_at_diagnosis:[* TO 10}", "field": '
                                                        '"age_at_diagnosis", "limit": -1, '
                                                        '"type": "query"}, "gender": '
                                                        '{"missing": true, "field": "gender", '
                                                        '"limit": -1, "type": "terms"}, '
                                                        '"project_name": {"missing": true, '
                                                        '"field": "project_name", "limit": -1, '
                                                        '"type": "terms"}, "age_at_diagnosis:70 '
                                                        'to 80": {"q": "age_at_diagnosis:[70 TO '
                                                        '80}", "field": "age_at_diagnosis", '
                                                        '"limit": -1, "type": "query"}, '
                                                        '"histological_type": {"missing": true, '
                                                        '"field": "histological_type", "limit": '
                                                        '-1, "type": "terms"}, '
                                                        '"sample_type_name": {"missing": true, '
                                                        '"field": "sample_type_name", "limit": '
                                                        '-1, "type": "terms"}, '
                                                        '"age_at_diagnosis:30 to 40": {"q": '
                                                        '"age_at_diagnosis:[30 TO 40}", '
                                                        '"field": "age_at_diagnosis", "limit": '
                                                        '-1, "type": "query"}, '
                                                        '"age_at_diagnosis:None": {"q": '
                                                        '"-age_at_diagnosis:[* TO *]", "field": '
                                                        '"age_at_diagnosis", "limit": -1, '
                                                        '"type": "query"}, "pathologic_stage": '
                                                        '{"missing": true, "field": '
                                                        '"pathologic_stage", "limit": -1, '
                                                        '"type": "terms"}, "bmi:obese": {"q": '
                                                        '"bmi:[30 TO *]", "field": "bmi", '
                                                        '"limit": -1, "type": "query"}, '
                                                        '"age_at_diagnosis:40 to 50": {"q": '
                                                        '"age_at_diagnosis:[40 TO 50}", '
                                                        '"field": "age_at_diagnosis", "limit": '
                                                        '-1, "type": "query"}, "sample_type": '
                                                        '{"missing": true, "field": '
                                                        '"sample_type", "limit": -1, "type": '
                                                        '"terms"}, "country": {"missing": true, '
                                                        '"field": "country", "limit": -1, '
                                                        '"type": "terms"}, '
                                                        '"neoplasm_histologic_grade": '
                                                        '{"missing": true, "field": '
                                                        '"neoplasm_histologic_grade", "limit": '
                                                        '-1, "type": "terms"}, '
                                                        '"bmi:overweight": {"q": "bmi:[25 TO '
                                                        '30}", "field": "bmi", "limit": -1, '
                                                        '"type": "query"}, "age_at_diagnosis:20 '
                                                        'to 30": {"q": "age_at_diagnosis:[20 TO '
                                                        '30}", "field": "age_at_diagnosis", '
                                                        '"limit": -1, "type": "query"}, '
                                                        '"age_at_diagnosis:10 to 20": {"q": '
                                                        '"age_at_diagnosis:[10 TO 20}", '
                                                        '"field": "age_at_diagnosis", "limit": '
                                                        '-1, "type": "query"}, '
                                                        '"age_at_diagnosis:80 to *": {"q": '
                                                        '"age_at_diagnosis:[80 TO *]", "field": '
                                                        '"age_at_diagnosis", "limit": -1, '
                                                        '"type": "query"}, "ethnicity": '
                                                        '{"missing": true, "field": '
                                                        '"ethnicity", "limit": -1, "type": '
                                                        '"terms"}}, "query": "*:*", "filter": '
                                                        '["{!join from=PatientID '
                                                        'fromIndex=tcia_images '
                                                        'to=case_barcode}{!tag=f0}(+collection_id:(\\"tcga_ucec\\" '
                                                        '\\"tcga_thca\\" \\"tcga_stad\\" '
                                                        '\\"tcga_sarc\\" \\"tcga_read\\" '
                                                        '\\"tcga_prad\\" \\"tcga_ov\\" '
                                                        '\\"tcga_lusc\\" \\"tcga_luad\\" '
                                                        '\\"tcga_lihc\\" \\"tcga_lgg\\" '
                                                        '\\"tcga_kirp\\" \\"tcga_kirc\\" '
                                                        '\\"tcga_kich\\" \\"tcga_hnsc\\" '
                                                        '\\"tcga_gbm\\" \\"tcga_esca\\" '
                                                        '\\"tcga_coad\\" \\"tcga_cesc\\" '
                                                        '\\"tcga_brca\\" \\"tcga_blca\\"))", '
                                                        '"{!collapse field=case_barcode}"], '
                                                        '"offset": 0, "params": {"debugQuery": '
                                                        '"off"}}'},
                                     'status': 0}}

        return archived_result

    #
    # Call the real Solr server with payload, to gather up a response to archive:
    #

    def _call_real_solr_data(self):

        payload, response, _ = TestSolr._build_full_collection_payload_and_response()

        query_uri = "{}{}/query".format(self.SOLR_URI, "tcia_images")

        query_result = {}

        try:
            query_response = requests.post(query_uri, data=json.dumps(payload),
                                           headers={'Content-type': 'application/json'},
                                           auth=(self.SOLR_LOGIN, self.SOLR_PASSWORD),
                                           verify=self.SOLR_CERT)
            print("Back from solr {}".format(query_uri))
            print("request")
            print(query_response.request.body)
            print("request headers")
            print(query_response.request.headers)
            print("response")
            print(query_response)
            pp = pprint.PrettyPrinter()
            pp.pprint(query_response.json())
            self.assertEquals(query_response.status_code, 200)
        except Exception as e:
            self.fail("Test caught exception {}".format(str(e)))
        return

