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
    url(r'^$', views.cohorts_list, name='cohort_list'),
    url(r'^api/$', views.cohort_list_api, name='cohort_list_api'),
    url(r'^api/preview/$', views.cohort_preview_api, name='cohort_preview_api'),
    url(r'^api/preview/manifest/$', views.cohort_preview_manifest_api, name='cohort_preview_manifest_api'),
    url(r'^(?P<cohort_id>\d+)/$', views.cohort_detail, name='cohort_details'),
    url(r'^api/(?P<cohort_id>\d+)/$', views.cohort_detail_api, name='cohort_detail_api'),
    url(r'^api/(?P<cohort_id>\d+)/manifest/$', views.cohort_manifest_api, name='cohort_manifest_api'),
    url(r'^save_cohort/', views.save_cohort, name='save_cohort'),
    url(r'^api/save_cohort/', views.save_cohort_api, name='save_cohort_api'),
    url(r'^delete_cohort/', views.delete_cohort, name='delete_cohort'),
    url(r'^api/delete_cohort/', views.delete_cohort_api, name='delete_cohort_api'),
    url(r'^clone_cohort/(?P<cohort_id>\d+)/', views.clone_cohort, name='clone_cohort'),
    url(r'^share_cohort/$', views.share_cohort, name='share_cohorts'),
    url(r'^share_cohort/(?P<cohort_id>\d+)/', views.share_cohort, name='share_cohort'),
    url(r'^unshare_cohort/$', views.unshare_cohort, name='unshare_cohorts'),
    url(r'^unshare_cohort/(?P<cohort_id>\d+)/', views.unshare_cohort, name='unshare_cohort'),
    url(r'^set_operation/', views.set_operation, name='set_operation'),
    url(r'^save_cohort_comment/', views.save_comment, name='save_cohort_comment'),
    url(r'^download_manifest/(?P<cohort_id>\d+)/', views.download_cohort_manifest, name='cohort_manifest'),
    url(r'^download_ids/(?P<cohort_id>\d+)/', views.cohort_uuids, name='download_ids'),
    url(r'^get_metadata_ajax/$', views.get_metadata, name='metadata_count_ajax')
]
