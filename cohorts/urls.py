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
from __future__ import absolute_import

from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^$',                                      views.cohorts_list, name='cohort_list'),
    url(r'^public',                                 views.public_cohort_list, name='public_cohort_list'),
    url(r'^new_cohort/',                            views.cohort_detail, name='cohort'),
    url(r'^new_cohort/barcodes/',                   views.cohort_detail, name='cohort_barcodes'),
    url(r'^validate_barcodes/',                     views.validate_barcodes, name='validate_barcodes'),
    url(r'^(?P<cohort_id>\d+)/$',                   views.cohort_detail, name='cohort_details'),
    url(r'^filelist/(?P<cohort_id>\d+)/$',          views.cohort_filelist, name='cohort_filelist'),
    url(r'^filelist/(?P<cohort_id>\d+)/panel/(?P<panel_type>[A-Za-z]+)/$',
                                                    views.cohort_filelist, name='cohort_filelist_panel'),
    url(r'^filelist_ajax/(?P<cohort_id>\d+)/$',     views.cohort_filelist_ajax, name='cohort_filelist_ajax'),
    url(r'^filelist_ajax/(?P<cohort_id>\d+)/panel/(?P<panel_type>[A-Za-z]+)/$',
                                                    views.cohort_filelist_ajax, name='cohort_filelist_ajax_panel'),
    url(r'^save_cohort/',                           views.save_cohort, name='save_cohort'),
    url(r'^export/(?P<cohort_id>\d+)/(?P<export_type>cohort|file_manifest)/$',
                                                    views.export_data, name='export_data'),
    url(r'^save_cohort_from_plot/',                 views.save_cohort_from_plot, name='save_cohort_from_plot'),
    url(r'^delete_cohort/',                         views.delete_cohort, name='delete_cohort'),
    url(r'^clone_cohort/(?P<cohort_id>\d+)/',       views.clone_cohort, name='clone_cohort'),
    url(r'^share_cohort/$',                         views.share_cohort, name='share_cohorts'),
    url(r'^share_cohort/(?P<cohort_id>\d+)/',       views.share_cohort, name='share_cohort'),
    url(r'^unshare_cohort/$',                       views.unshare_cohort, name='unshare_cohorts'),
    url(r'^unshare_cohort/(?P<cohort_id>\d+)/',     views.unshare_cohort, name='unshare_cohort'),
    url(r'^set_operation/',                         views.set_operation, name='set_operation'),
    url(r'^save_cohort_comment/',                   views.save_comment, name='save_cohort_comment'),
    url(r'^download_filelist/(?P<cohort_id>\d+)/',  views.streaming_csv_view, name='download_filelist'),
    url(r'^download_ids/(?P<cohort_id>\d+)/',       views.cohort_samples_cases, name='download_ids'),

    url(r'^get_metadata_ajax/$',                                        views.get_metadata, name='metadata_count_ajax'),
    url(r'^filter_panel/(?P<program_id>\d+)/$',                         views.get_cohort_filter_panel, name='cohort_filter_panel'),
    url(r'^(?P<cohort_id>\d+)/filter_panel/(?P<program_id>\d+)/$',      views.get_cohort_filter_panel, name='cohort_filter_panel')
]
