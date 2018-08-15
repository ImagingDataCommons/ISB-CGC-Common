"""
Copyright 2017-2018, Institute for Systems Biology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import logging

from allauth.account import views as account_views
from allauth.socialaccount.models import SocialAccount
from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.http import JsonResponse
from django.shortcuts import render, redirect
from django.core.urlresolvers import reverse
from django.views.decorators.csrf import csrf_protect
from google_helpers.stackdriver import StackDriverLogger
from google_helpers.bigquery.service import get_bigquery_service
from google_helpers.directory_service import get_directory_resource
from google_helpers.resourcemanager_service import get_special_crm_resource
from google_helpers.storage_service import get_storage_resource
from google_helpers.bigquery.bq_support import BigQuerySupport
from googleapiclient.errors import HttpError
from django.contrib.auth.models import User
from models import *
from projects.models import User_Data_Tables
from django.utils.html import escape
from sa_utils import verify_service_account, register_service_account, \
                     unregister_all_gcp_sa, unregister_sa, service_account_dict, \
                     do_nih_unlink, deactivate_nih_add_to_open, controlled_auth_datasets

from dcf_support import service_account_info_from_dcf_for_project

from json import loads as json_loads

logger = logging.getLogger('main_logger')

OPEN_ACL_GOOGLE_GROUP = settings.OPEN_ACL_GOOGLE_GROUP
SERVICE_ACCOUNT_LOG_NAME = settings.SERVICE_ACCOUNT_LOG_NAME
SERVICE_ACCOUNT_BLACKLIST_PATH = settings.SERVICE_ACCOUNT_BLACKLIST_PATH
GOOGLE_ORG_WHITELIST_PATH = settings.GOOGLE_ORG_WHITELIST_PATH
MANAGED_SERVICE_ACCOUNTS_PATH = settings.MANAGED_SERVICE_ACCOUNTS_PATH

@login_required
def extended_logout_view(request):

    response = None
    try:
        # deactivate NIH_username entry if exists
        user = User.objects.get(id=request.user.id)
        deactivate_nih_add_to_open(request.user.id, user.email)
        response = account_views.logout(request)

    except ObjectDoesNotExist as e:
        logger.error("[ERROR] User with ID of {} not found!".format(str(request.user.id)))
        logger.exception(e)
        messages.error(request, "There was an error while attempting to log out - please contact the administrator.")
        return redirect(reverse('landing_page'))
    except Exception as e:
        logger.error("[ERROR] While attempting to log out:")
        logger.exception(e)
        messages.error(request,"There was an error while attempting to log out - please contact the administrator.")
        return redirect(reverse('user_detail', args=[request.user.id]))
    return response

@login_required
def unlink_accounts(request):
    user_id = request.user.id

    try:
        message = do_nih_unlink(user_id)
        if message:
            messages.error(request, message)
            return redirect(reverse('user_detail', args=[user_id]))

    except Exception as e:
        logger.error("[ERROR] While unlinking accounts:")
        logger.exception(e)
        messages.error(request, 'There was an error when attempting to unlink your NIH user account - please contact the administrator.')

    # redirect to user detail page
    return redirect(reverse('user_detail', args=[user_id]))


# GCP RELATED VIEWS

'''
Returns page that has user Google Cloud Projects
'''
@login_required
def user_gcp_list(request, user_id):
    context = {}
    template = 'GenespotRE/user_gcp_list.html'

    try:
        if int(request.user.id) == int(user_id):

            try:
                user = User.objects.get(id=user_id)
                gcp_list = GoogleProject.objects.filter(user=user, active=1)
                social_account = SocialAccount.objects.get(user_id=user_id)

                user_details = {
                    'date_joined': user.date_joined,
                    'email': user.email,
                    'extra_data': social_account.extra_data,
                    'first_name': user.first_name,
                    'id': user.id,
                    'last_login': user.last_login,
                    'last_name': user.last_name
                }

                context = {'user': user, 'user_details': user_details, 'gcp_list': gcp_list}

            except (MultipleObjectsReturned, ObjectDoesNotExist) as e:
                logger.error("[ERROR] While fetching user GCP list: ")
                logger.exception(e)
                messages.error(request,"There was an error while attempting to list your Google Cloud Projects - please contact the administrator.")

        else:
            messages.error(request,"You are not allowed to view that user's Google Cloud Project list.")
            logger.warn("[WARN] While trying to view a user GCP list, saw mismatched IDs. Request ID: {}, GCP list requested: {}".format(str(request.user.id),str(user_id)))
            template = '403.html'
    except Exception as e:
        logger.error("[ERROR] While trying to view the GCP list:")
        logger.exception(e)
        messages.error(request,"There was an error while attempting to list your Google Cloud Projects - please contact the administrator.")
        template = '500.html'

    return render(request, template, context)


@login_required
def verify_gcp(request, user_id):
    status = None
    response = {}
    try:
        gcp = None
        gcp_id = request.GET.get('gcp-id', None)
        is_refresh = bool(request.GET.get('is_refresh', '')=='true')

        try:
            gcp = GoogleProject.objects.get(project_id=gcp_id, active=1)
            # Can't register the same GCP twice - return immediately
            if not is_refresh:
                return JsonResponse({'message': 'A Google Cloud Project with the project ID {} has already been registered.'.format(gcp_id)}, status='500')
        except ObjectDoesNotExist:
            if is_refresh:
                return JsonResponse({'message': 'GCP ID {} does not exist and so cannot be refreshed'.format(str(gcp_id))}, status='500')

        crm_service = get_special_crm_resource()
        iam_policy = crm_service.projects().getIamPolicy(
            resource=gcp_id, body={}).execute()
        bindings = iam_policy['bindings']
        roles = {}
        user = User.objects.get(id=user_id)
        user_found = False

        for val in bindings:
            role = val['role']
            members = val['members']

            for member in members:
                if member.startswith('user:'):
                    email = member.split(':')[1]
                    if email not in roles:
                        roles[email] = {}
                        roles[email]['roles'] = []
                        roles[email]['registered_user'] = bool(User.objects.filter(email=email).first())
                    if user.email.lower() == email.lower():
                        user_found = True
                    roles[email]['roles'].append(role)

        if not user_found:
            status='403'
            logger.error("[ERROR] While attempting to {} GCP ID {}: ".format("register" if not is_refresh else "refresh",gcp_id))
            logger.error("User {} was not found on GCP {}'s IAM policy.".format(user.email,gcp_id))
            response['message'] = 'Your user email ({}) was not found in GCP {}. You may not {} a project you do not belong to.'.format(user.email,gcp_id,"register" if not is_refresh else "refresh")
            if is_refresh:
                gcp.user.set(gcp.user.all().exclude(id=user.id))
                gcp.save()
                response['redirect']=reverse('user_gcp_list',kwargs={'user_id': user.id})
        else:
            response = {'roles': roles,'gcp_id': gcp_id}
            status='200'

    except Exception as e:
        if type(e) is HttpError:
            logger.error("[ERROR] While trying to access IAM policies for GCP ID {}:".format(gcp_id))
            response['message'] = 'There was an error accessing this project. Please verify that you have entered the correct Google Cloud Project ID--not the Number or the Name--and set the permissions correctly.'
            status = '403'
        else:
            logger.error("[ERROR] While trying to verify GCP ID {}:".format(gcp_id))
            response['message'] = 'There was an error while attempting to verify this project. Please verify that you have entered the correct Google Cloud Project ID--not the Number or the Name--and set the permissions correctly.'
            status = '500'
        logger.exception(e)

    return JsonResponse(response, status=status)


@login_required
def register_gcp(request, user_id):
    is_refresh = False

    redirect_view = 'user_gcp_list'
    args={'user_id': request.user.id}

    try:
        # log the reports using Cloud logging API
        st_logger = StackDriverLogger.build_from_django_settings()
        log_name = SERVICE_ACCOUNT_LOG_NAME

        if request.POST:
            crm_service = get_special_crm_resource()

            project_id = request.POST.get('gcp_id', None)
            register_users = request.POST.getlist('register_users')
            is_refresh = bool(request.POST.get('is_refresh', '') == 'true')

            project = crm_service.projects().get(projectId=project_id).execute()

            project_name = project['name']

            gcp_users = User.objects.filter(email__in=register_users)

            if not user_id:
                raise Exception("User ID not provided.")
            elif not project_id:
                raise Exception("Project ID not provided.")
            elif not len(register_users) or not gcp_users.count():
                # A set of users to register or refresh is required
                msg = "[STATUS] No registered user set found for GCP {} of project {}; {} aborted.".format(
                    "refresh" if is_refresh else "registration",project_id,"refresh" if is_refresh else "registration")
                logger.warn(msg)
                st_logger.write_text_log_entry(log_name,msg)
                messages.error(request, "The registered user set was empty, so the project could not be {}.".format("refreshed" if is_refresh else "registered"))
                return redirect('user_gcp_list', user_id=request.user.id)
            else:
                try:
                    gcp = GoogleProject.objects.get(project_id=project_id,
                                                    active=1)
                    if not is_refresh:
                        messages.info(request, "A Google Cloud Project with the id {} already exists.".format(project_id))

                except ObjectDoesNotExist:
                    gcp,created = GoogleProject.objects.update_or_create(
                        project_name=project_name,project_id=project_id,
                        defaults={
                           'big_query_dataset': '',
                           'active': 1
                        }
                    )
                    gcp.save()
                    if not created:
                        msg="[STATUS] User {} has re-registered GCP {}".format(User.objects.get(id=user_id).email,project_id)
                        logger.info(msg)
                        st_logger.write_text_log_entry(log_name,msg)

            if is_refresh:
                if project_name != gcp.project_name:
                    gcp.project_name = project_name

                users_to_add = gcp_users.exclude(id__in=gcp.user.all())
                users_to_remove = gcp.user.all().exclude(id__in=gcp_users)
                if len(users_to_add):
                    msg = "The following user{} added to GCP {}: {}".format(
                        ("s were" if len(users_to_add) > 1 else " was"),
                        project_id,
                        ", ".join(users_to_add.values_list('email',flat=True)))
                else:
                    msg = "There were no new users to add to GCP {}.".format(project_id)
                if len(users_to_remove):
                    msg += " The following user{} removed from GCP {}: {}".format(
                        ("s were" if len(users_to_remove) > 1 else " was"),
                        project_id,
                        ", ".join(users_to_remove.values_list('email',flat=True)))
                else:
                    msg += " There were no users to remove from GCP {}.".format(project_id)

                messages.info(request, msg)

            gcp.user.set(gcp_users)
            gcp.save()

            if not gcp.user.all().count():
                raise Exception("GCP {} has no users!".format(project_id))

            if request.POST.get('detail','') == 'true':
                redirect_view = 'gcp_detail'
                args['gcp_id'] = gcp.id

            return redirect(reverse(redirect_view, kwargs=args))

        return render(request, 'GenespotRE/register_gcp.html', {})

    except Exception as e:
        logger.error("[ERROR] While {} a Google Cloud Project:".format("refreshing" if is_refresh else "registering"))
        logger.exception(e)
        messages.error(request, "There was an error while attempting to register/refresh this Google Cloud Project - please contact the administrator.")

    return redirect(reverse(redirect_view, kwargs=args))



@login_required
def gcp_detail(request, user_id, gcp_id):
    try:
        logger.info("[INFO] gcp_detail {}:".format(gcp_id))
        context = {}
        context['gcp'] = GoogleProject.objects.get(id=gcp_id, active=1)

        logger.info("[INFO] Listing SAs for GCP {}:".format(gcp_id))
        if settings.SA_VIA_DCF:
            context['sa_list'] = []
            sa_info, messages = service_account_info_from_dcf_for_project(user_id, gcp_id)
            if messages:
                for message in messages:
                    messages.error(request, message)
                return render(request, 'GenespotRE/gcp_detail.html', context)

            for sa_dict in sa_info:
                sa_data = {}
                context['sa_list'].append(sa_data)
                sa_data['name'] = sa_dict['sa_name']
                now_time = pytz.utc.localize(datetime.datetime.utcnow())
                exp_time = datetime.datetime.fromtimestamp(sa_data['sa_exp'])
                sa_data['is_expired'] = exp_time < now_time
                sa_data['authorized_date'] = exp_time + datetime.timedelta(days=-7)
                auth_names = []
                auth_ids = []
                sa_data['num_auth'] = len(sa_data['sa_dataset_ids'])
                logger.info("[INFO] Listing ADs for GCP {} {}:".format(gcp_id, len(sa_data['sa_dataset_ids'])))
                for auth_data in sa_data['sa_dataset_ids']:
                    logger.info("[INFO] AD {}:".format(gcp_id, str(auth_data)))
                    protected_dataset = AuthorizedDataset.objects.get(whitelist_id=auth_data)
                    auth_names.append(protected_dataset.name)
                    auth_ids.append(str(protected_dataset.id))
                sa_data['auth_dataset_names'] = ', '.join(auth_names)
                sa_data['auth_dataset_ids'] = ', '.join(auth_ids)

            #  for sa in sa_list:
            #    ret_entry = {
            #         'gcp_id': sa['google_project_id'],
            #        'sa_dataset_ids': sa['project_access'],
            #         'sa_name': sa['service_account_email'],
            #         'sa_exp': sa['project_access_exp']
            #     }
            #      retval.append(ret_entry)

        else:
            context['sa_list'] = []

            #google_project = models.ForeignKey(GoogleProject, null=False)
            #service_account = models.CharField(max_length=1024, null=False)
            #active = models.BooleanField(default=False, null=False)
            #authorized_date = models.DateTimeField(auto_now=True)

            active_sas = context['gcp'].active_service_accounts()
            for service_account in active_sas:
                logger.info("[INFO] Listing SA {}:".format(service_account.service_account))
                auth_datasets = service_account.get_auth_datasets()
                sa_data = {}
                context['sa_list'].append(sa_data)
                sa_data['name'] = service_account.service_account
                sa_data['is_expired'] = service_account.is_expired()
                sa_data['authorized_date'] = service_account.authorized_date
                auth_names = []
                auth_ids = []
                sa_data['num_auth'] = len(auth_datasets)
                logger.info("[INFO] Listing ADs for GCP {} {}:".format(gcp_id, len(auth_datasets)))
                for auth_data in auth_datasets:
                    auth_names.append(auth_data.name)
                    auth_ids.append(str(auth_data.id))
                sa_data['auth_dataset_names'] = ', '.join(auth_names)
                sa_data['auth_dataset_ids'] = ', '.join(auth_ids)

            # We should get back all service accounts, even ones that have expired (I hope). Note we no longer should be
            # getting back "inactive" service accounts; that is for DCF to sort out and manage internally.
            #


        # we need:
        #
        # service_account.get_auth_datasets
        # dataset names, separated by ","
        # if we have auth datasets and they are expired, want the authorized_date as: 'M d, Y, g:i a'
        # dataset ids, separated by ", "
        logger.info("[INFO] Render!! {}:".format(gcp_id))

    except Exception as e:
        logger.error("[ERROR] While detailing a GCP: ")
        logger.exception(e)
        messages.error(request,
                       "Encountered an error while trying to detail this Google Cloud Project - please contact the administrator.")


    return render(request, 'GenespotRE/gcp_detail.html', context)


@login_required
def user_gcp_delete(request, user_id, gcp_id):

    try:
        if request.POST:
            user = User.objects.get(id=user_id)
            logger.info("[STATUS] User {} is unregistering GCP {}".format(user.email,gcp_id))
            gcp = GoogleProject.objects.get(id=gcp_id, active=1)
            unregister_all_gcp_sa(user_id, gcp_id)
            gcp.user.clear()
            gcp.active=False
            gcp.save()
    except Exception as e:
        logger.error("[ERROR] While deleting a GCP: ")
        logger.exception(e)
        messages.error(request, "Encountered an error while trying to delete this Google Cloud Project - please contact the administrator.")

    return redirect('user_gcp_list', user_id=request.user.id)


@login_required
def verify_sa(request, user_id):
    status = None
    result = None
    try:
        st_logger = StackDriverLogger.build_from_django_settings()

        if request.POST.get('gcp_id'):
            user_email = request.user.email
            gcp_id = request.POST.get('gcp_id')
            user_sa = request.POST.get('user_sa')
            datasets = request.POST.getlist('datasets')
            is_refresh = bool(request.POST.get('is_refresh') == 'true')
            is_adjust = bool(request.POST.get('is_adjust') == 'true')
            remove_all = bool(request.POST.get('select-datasets') == 'remove')

            # If we have received a 'remove all' request, there's nothing to verify, so set the datasets to empty
            if remove_all:
                datasets = []

            result = verify_service_account(gcp_id, user_sa, datasets, user_email, is_refresh, is_adjust, remove_all)

            if 'message' in result.keys():
                status = '400'
                st_logger.write_struct_log_entry(SERVICE_ACCOUNT_LOG_NAME, {'message': '{}: For user {}, {}'.format(user_sa, user_email, result['message'])})
                # Users attempting to refresh a project they're not on go back to their GCP list (because this GCP was probably removed from it)
                if 'redirect' in result.keys():
                    result['redirect'] = reverse('user_gcp_list', kwargs={'user_id': request.user.id})
                if 'user_not_found' in result.keys():
                    gcp = GoogleProject.objects.get(project_id=gcp_id, active=1)
                    user=User.objects.get(id=request.user.id)
                    gcp.user.set(gcp.user.all().exclude(id=user.id))
                    gcp.save()
            else:
                if result['all_user_datasets_verified']:
                    st_logger.write_struct_log_entry(SERVICE_ACCOUNT_LOG_NAME, {'message': '{}: Service account was successfully verified for user {}.'.format(user_sa,user_email)})
                else:
                    st_logger.write_struct_log_entry(SERVICE_ACCOUNT_LOG_NAME, {'message': '{}: Service account was not successfully verified for user {}.'.format(user_sa,user_email)})
                result['user_sa'] = user_sa
                result['datasets'] = datasets
                status = '200'
        else:
            result = {'message': 'There was no Google Cloud Project provided.'}
            status = '404'
    except Exception as e:
        logger.error("[ERROR] While verifying Service Accounts: ")
        logger.exception(e)
        result = {'message': 'There was an error while trying to verify this service account. Please contact an administrator.'}
        status = '500'

    return JsonResponse(result, status=status)


@login_required
def register_sa(request, user_id):
    st_logger = StackDriverLogger.build_from_django_settings()

    try:
        # This is a Service Account dataset adjustment or an initial load of the service account registration page
        if request.GET.get('sa_name') or request.GET.get('gcp_id'):
            template = 'GenespotRE/register_sa.html'
            context = {
                'authorized_datasets': controlled_auth_datasets()
            }

            if request.GET.get('sa_name'):
                template = 'GenespotRE/adjust_sa.html'
                sa_dict, sa_msgs = service_account_dict(user_id, request.GET.get('sa_id'))
                # FIXME!!! What to do next if there is an error message (Coming from DCF)??
                if sa_msgs:
                    for sa_msg in sa_msgs:
                        messages.error(request, sa_msg)

                context['gcp_id'] = sa_dict['gcp_id']
                context['sa_dataset_ids'] = sa_dict['sa_dataset_ids']
                context['sa_name'] = sa_dict['sa_name']

            else:
                gcp_id = escape(request.GET.get('gcp_id'))
                crm_service = get_special_crm_resource()
                gcp = crm_service.projects().get(
                    projectId=gcp_id).execute()
                context['gcp_id'] = gcp_id
                context['default_sa_id'] = gcp['projectNumber']+'-compute@developer.gserviceaccount.com'

            return render(request, template, context)

        # This is an attempt to formally register (or refresh) the service account, post verification
        elif request.POST.get('gcp_id'):
            user_email = request.user.email
            gcp_id = request.POST.get('gcp_id')
            user_sa = request.POST.get('user_sa')
            datasets = request.POST.get('datasets').split(',')
            is_refresh = bool(request.POST.get('is_refresh') == 'true')
            is_adjust = bool(request.POST.get('is_adjust') == 'true')
            remove_all = bool(request.POST.get('remove_all') == 'true')
            err_msgs = register_service_account(user_email, gcp_id, user_sa, datasets, is_refresh, is_adjust, remove_all)

            for msg_tuple in err_msgs:
                if msg_tuple[1] == 'error':
                    messages.error(request, msg_tuple[0])
                elif msg_tuple[1] == 'warning':
                    messages.warning(request, msg_tuple[0])
                else:
                    logger.error("[ERROR] Unimplemented message level: {}, {}".format(msg_tuple[1], msg_tuple[0]))

            return redirect('user_gcp_list', user_id=user_id)
        else:
            messages.error(request, 'There was no Google Cloud Project provided.', 'warning')
            return redirect('user_gcp_list', user_id=user_id)

    except Exception as e:
        logger.error("[ERROR] While registering a Service Account: ")
        logger.exception(e)
        messages.error(request, "Unable to register this Service Account - please contact the administrator.")
        return redirect('user_gcp_list', user_id=user_id)


@login_required
def delete_sa(request, user_id, sa_name):
    # FIXME: No longer have an SA_ID to work with
    try:
        if request.POST:
            unregister_sa(user_id, sa_name)
    except Exception as e:
        logger.error("[ERROR] While trying to unregister Service Account {}: ".format(sa_name))
        logger.exception(e)
        messages.error(request, "Encountered an error while trying to remove this service account - please contact the administrator.")

    return redirect('user_gcp_list', user_id=user_id)


@login_required
def register_bucket(request, user_id, gcp_id):
    st_logger = StackDriverLogger.build_from_django_settings()

    if request.POST:
        bucket_name = request.POST.get('bucket_name', None)
        gcp = GoogleProject.objects.get(id=gcp_id, active=1)
        found_bucket = False

        # Check bucketname not null
        if not bucket_name:
            messages.error(request, 'There was no bucket name provided.')
            return redirect('gcp_detail', user_id=user_id, gcp_id=gcp_id)
        else:
            try:
                bucket = Bucket.objects.get(bucket_name=bucket_name)
                if bucket.google_project.project_id != gcp.project_id:
                    messages.error(
                        request,
                        "A bucket with the name {} has already been registered under a different project.".format(escape(bucket_name)) +
                        " If you feel you've received this message in error, please contact the administrator."
                    )
                else:
                    messages.error(
                        request,
                        "A bucket with the name {} has already been registered under project {}.".format(escape(bucket_name),gcp.project_id) +
                        " Buckets can only be registered to a project once. If you feel you've received this message in error, please contact the administrator."
                    )
                return redirect('gcp_detail', user_id=user_id, gcp_id=gcp_id)
            except MultipleObjectsReturned:
                messages.error(
                    request,
                    "More than one bucket with the name {} has already been registered.".format(escape(bucket_name)) +
                    " Buckets can only be registered once."
                )
                return redirect('gcp_detail', user_id=user_id, gcp_id=gcp_id)
            except ObjectDoesNotExist:
                pass

        # Check that bucket is in project
        try:
            storage_service = get_storage_resource()
            buckets = storage_service.buckets().list(project=gcp.project_id).execute()

            if 'items' in buckets.keys():
                bucket_list = buckets['items']

                for bucket in bucket_list:
                    if bucket['name'] == bucket_name:
                        found_bucket = True

                if found_bucket:
                    bucket = Bucket(google_project=gcp, bucket_name=bucket_name)
                    bucket.save()
                else:
                    messages.error(request, 'The bucket, {0}, was not found in the Google Cloud Project, {1}.'.format(
                        bucket_name, gcp.project_id))

        except HttpError as e:
            if e.resp.status == 403:
                messages.error(request, 'Access to the bucket {0} in Google Cloud Project {1} was denied.'.format(
                    bucket_name, gcp.project_id))
            elif e.resp.get('content-type', '').startswith('application/json'):
                err_val = json_loads(e.content).get('error')
                if err_val:
                    e_message = err_val.get('message')
                else:
                    e_message = "HTTP error {0}".format(str(e.resp.status))
                messages.error(request,
                               'Error returned trying to access bucket {0} in Google Cloud Project {1}: {2}.'.format(
                                bucket_name, gcp.project_id, e_message))
            else:
                messages.error(request, 'There was an unknown error processing this request.')

            st_logger.write_struct_log_entry(SERVICE_ACCOUNT_LOG_NAME, {
                'message': '{0}: There was an error accessing the Google Cloud Project bucket list.'.format(
                    str(gcp.project_id))})
            logger.info(e)

    return redirect('gcp_detail', user_id=user_id, gcp_id=gcp_id)


@login_required
def delete_bucket(request, user_id, bucket_id):
    if request.POST:
        gcp_id = request.POST.get('gcp_id')
        bucket = Bucket.objects.get(id=bucket_id)

        # Check to make sure it's not being used by user data
        user_data_tables = User_Data_Tables.objects.filter(google_bucket_id=bucket_id, project__active=True)
        if len(user_data_tables):
            messages.error(request, 'The bucket, {0}, is being used for uploaded program data. Please delete the program(s) before deleting this bucket. This includes any programs uploaded by other users to ths same bucket.'.format(bucket.bucket_name))
        else:
            bucket.delete()
        return redirect('gcp_detail', user_id=user_id, gcp_id=gcp_id)
    return redirect('user_gcp_list', user=user_id)


@login_required
def register_bqdataset(request, user_id, gcp_id):
    if request.POST:
        bqdataset_name = request.POST.get('bqdataset_name', None)
        gcp = GoogleProject.objects.get(id=gcp_id, active=1)
        found_dataset = False

        # Check bqdatasetname not null
        if not bqdataset_name:
            messages.error(request, 'There was no dataset name provided.')
            return redirect('gcp_detail', user_id=user_id, gcp_id=gcp_id)
        else:
            bqdataset_name = bqdataset_name.strip()
            try:
                BqDataset.objects.get(dataset_name=bqdataset_name,google_project=gcp)
                messages.error(request,"A dataset with the name {} has already been registered for project {}.".format(escape(bqdataset_name),gcp.project_id))
                return redirect('gcp_detail', user_id=user_id, gcp_id=gcp_id)
            except MultipleObjectsReturned:
                messages.error(request, "Multiple datasets with the name {} have already been registered for project {}.".format(
                    escape(bqdataset_name),gcp.project_id))
                return redirect('gcp_detail', user_id=user_id, gcp_id=gcp_id)
            except ObjectDoesNotExist:
                pass

        # Check that bqdataset is in project
        try:
            bq_service = get_bigquery_service()
            datasets = bq_service.datasets().list(projectId=gcp.project_id).execute()

            if 'datasets' in datasets.keys():
                dataset_list = datasets['datasets']

                for dataset in dataset_list:
                    if dataset['datasetReference']['datasetId'] == bqdataset_name:
                        found_dataset = True

            if found_dataset:
                bqdataset = BqDataset(google_project=gcp, dataset_name=bqdataset_name)
                bqdataset.save()
            else:
                messages.error(request, 'The dataset, {0}, was not found in the Google Cloud Project, {1}.'.format(
                    bqdataset_name, gcp.project_id))

        except HttpError as e:
            st_logger = StackDriverLogger.build_from_django_settings()
            messages.error(request, 'There was an unknown error processing this request.')
            st_logger.write_struct_log_entry(SERVICE_ACCOUNT_LOG_NAME, {
                'message': '{0}: There was an error accessing the Google Cloud Project dataset list.'.format(
                    str(gcp.project_id))})
            logger.info(e)

    return redirect('gcp_detail', user_id=user_id, gcp_id=gcp_id)


@login_required
def delete_bqdataset(request, user_id, bqdataset_id):

    if request.POST:
        gcp_id = request.POST.get('gcp_id')
        bqdataset = BqDataset.objects.get(id=bqdataset_id)

        # Check to make sure it's not being used by user data
        user_data_tables = User_Data_Tables.objects.filter(google_bq_dataset_id=bqdataset_id, project__active=True)
        if len(user_data_tables):
            messages.error(request,
                           'The dataset, {0}, is being used for uploaded program data. Please delete the program(s) before unregistering this dataset. This includes any programs uploaded by other users to ths same dataset.'.format(
                               bqdataset.dataset_name))
        else:
            bqdataset.delete()
        return redirect('gcp_detail', user_id=user_id, gcp_id=gcp_id)
    return redirect('user_gcp_list', user_id=user_id)


@login_required
@csrf_protect
def get_user_buckets(request, user_id):
    result = None
    status = '200'

    try:
        if int(request.user.id) != int(user_id):
            raise Exception("User {} is not the owner of the requested GCS buckets.".format(str(request.user.id)))

        req_user = User.objects.get(id=request.user.id)

        result = {
            'status': '',
            'data': {
                'projects': []
            }
        }

        gcps = GoogleProject.objects.filter(user=req_user, active=1)

        if not gcps.count():
            status = '500'
            result = {
                'status': 'error',
                'msg': "We couldn't find any Google Cloud Projects registered for you. Please register at least one "
                    + "project and GCS bucket before attempting to export."
            }
            logger.info("[STATUS] No registered GCPs found for user {} (ID: {}).".format(req_user.email, str(req_user.id)))
        else:
            for gcp in gcps:
                this_proj = {
                    'buckets': [x.bucket_name for x in gcp.bucket_set.all()],
                    'name': gcp.project_id
                }
                if len(this_proj['buckets']):
                    result['data']['projects'].append(this_proj)

            if not len(result['data']['projects']):
                status = '500'
                result = {
                    'status': 'error',
                    'msg': "No registered GCS buckets were found in your Google Cloud Projects. Please register "
                        + "at least one bucket in one of your projects before attempting to export."
                }
                logger.info(
                    "[STATUS] No registered buckets were found for user {} (ID: {}).".format(req_user.email,str(req_user.id)))
            else:
                status = '200'
                result['status'] = 'success'


    except Exception as e:
        logger.error("[ERROR] While retrieving user {}'s registered GCS buckets:".format(str(user_id)))
        logger.exception(e)
        result = {'message': "There was an error while retrieving your GCS buckets--please contact the administrator.".format(str(request.user.id)), 'status': 'error'}
        status='500'

    return JsonResponse(result, status=status)


@login_required
def get_user_datasets(request,user_id):

    result = None
    status = '200'

    try:
        if int(request.user.id) != int(user_id):
            raise Exception("User {} is not the owner of the requested datasets.".format(str(request.user.id)))

        req_user = User.objects.get(id=request.user.id)

        result = {
            'status': '',
            'data': {
                'projects': []
            }
        }

        gcps = GoogleProject.objects.filter(user=req_user, active=1)

        if not gcps.count():
            status = '500'
            result = {
                'status': 'error',
                'msg': "We couldn't find any Google Cloud Projects registered for you. Please register at least one "
                    + "project and BigQuery dataset before attempting to export your cohort."
            }
            logger.info("[STATUS] No registered GCPs found for user {} (ID: {}).".format(req_user.email, str(req_user.id)))
        else:
            for gcp in gcps:
                bqds = [x.dataset_name for x in gcp.bqdataset_set.all()]

                this_proj = {
                    'datasets': {},
                    'name': gcp.project_id
                }
                bqs = BigQuerySupport(gcp.project_id, None, None)
                bq_tables = bqs.get_tables()
                for table in bq_tables:
                    if table['dataset'] in bqds:
                        if table['dataset'] not in this_proj['datasets']:
                            this_proj['datasets'][table['dataset']] = []
                        if table['table_id']:
                            this_proj['datasets'][table['dataset']].append(table['table_id'])
                if len(this_proj['datasets']):
                    result['data']['projects'].append(this_proj)

            if not len(result['data']['projects']):
                status = '500'
                result = {
                    'status': 'error',
                    'msg': "No registered BigQuery datasets were found in your Google Cloud Projects. Please register "
                        + "at least one dataset in one of your projects before attempting to export your cohort."
                }
                logger.info(
                    "[STATUS] No registered datasets were found for user {} (ID: {}).".format(req_user.email,str(req_user.id)))
            else:
                status = '200'
                result['status'] = 'success'


    except Exception as e:
        logger.error("[ERROR] While retrieving user {}'s registered BQ datasets and tables:".format(str(user_id)))
        logger.exception(e)
        result = {'message': "There was an error while retrieving your datasets and BQ tables. Please contact the administrator.".format(str(request.user.id)), 'status': 'error'}
        status='500'

    return JsonResponse(result, status=status)
