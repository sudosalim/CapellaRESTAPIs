# -*- coding: utf-8 -*-
# Generic/Built-in
import base64
import json
import logging

from ..common.CapellaAPI_v4 import CommonCapellaAPI
from ..lib.CapellaAPIRequests import CapellaAPIRequests


class ClusterOperationsAPIs(CapellaAPIRequests):

    def __init__(self, url, secret, access, bearer_token):
        super(ClusterOperationsAPIs, self).__init__(
            url, secret, access, bearer_token)
        self.cluster_ops_API_log = logging.getLogger(__name__)
        organization_endpoint = "/v4/organizations"
        self.cluster_endpoint = organization_endpoint + \
            "/{}/projects/{}/clusters"
        self.allowedCIDR_endpoint = organization_endpoint + \
            "/{}/projects/{}/clusters/{}/allowedcidrs"
        self.db_user_endpoint = organization_endpoint + \
            "/{}/projects/{}/clusters/{}/users"
        self.bucket_endpoint = organization_endpoint + \
            "/{}/projects/{}/clusters/{}/buckets"
        self.scope_endpoint = organization_endpoint + \
            "/{}/projects/{}/clusters/{}/buckets/{}/scopes"
        self.collection_endpoint = organization_endpoint + \
            "/{}/projects/{}/clusters/{}/buckets/{}/scopes/{}/collections"
        self.backups_endpoint = organization_endpoint + \
            "/{}/projects/{}/clusters/{}/backups"
        self.backup_schedule_endpoint = organization_endpoint + \
            "/{}/projects/{}/clusters/{}/buckets/{}/backup/schedules"
        self.sample_bucket_endpoint = organization_endpoint + \
            "/{}/projects/{}/clusters/{}/sampleBuckets"
        self.org_appservice_api = organization_endpoint + "/{}/appservices"
        self.cluster_appservice_api = self.cluster_endpoint + "/{}/appservices"
        self.cluster_on_off_schedule_endpoint = self.cluster_endpoint + \
            "/{}/onOffSchedule"
        self.cluster_on_off_endpoint = self.cluster_endpoint + "/{}/activationState"
        self.appservice_on_off_endpoint = self.cluster_appservice_api + "/{}/activationState"
        self.alerts_endpoint = organization_endpoint + "/{}/projects/{}/alertIntegrations"
        self.test_alert_endpoint = self.alerts_endpoint[:-1] + "Test"
        self.audit_log_exports_endpoint = self.cluster_endpoint + "/{}/auditLogExports"
        self.audit_log_endpoint = self.cluster_endpoint + "/{}/auditLog"
        self.audit_log_events_endpoint = self.cluster_endpoint + "/{}/auditLogEvents"
        self.bucket_storage_migration_endpoint = (
            self.cluster_endpoint + "/{}/bucketStorageMigration"
        )

    """
    Method to restore the backup with backupId under cluster, project and organization mentioned.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
    - Organization Owner
    - Project Owner
    - Project Manager
    - Project Viewer
    - Database Data Reader/Writer
    - Database Data Reader
    :param organizationId (str) Organization ID under which the backup has to be created.
    :param projectId (str) Project ID under which the backup has to be created.
    :param targetClusterID (str) The ID of the target cluster to restore to.
    :param sourceClusterID (str) The ID of the source cluster the restore is based on.
    :param backupID (str) The backup record ID that contains the backup to restore from.
    :param services (str) Items Enum: "data" "query"
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def restore_backup(
            self,
            organizationId,
            projectId,
            targetClusterID,
            sourceClusterID,
            backupID,
            services,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Restore Backup in project {} in organization {}".format(
                projectId, organizationId))
        params = {
            "targetClusterID": targetClusterID,
            "sourceClusterID": sourceClusterID,
            "backupID": backupID,
            "services": services,
        }
        for k, v in kwargs.items():
            params[k] = v

        resp = self.capella_api_post(
            self.backups_endpoint.format(
                organizationId, projectId, sourceClusterID) + '/' + backupID + '/restore', params, headers)
        return resp

    """
    Method to delete the backup with backupId under cluster, project and organization mentioned.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
    - Organization Owner
    - Project Owner
    :param organizationId (str) Organization ID under which the backup has to be created.
    :param projectId (str) Project ID under which the backup has to be created.
    :param clusterId (str) Cluster ID under which the backup has to be created.
    :param backupId (str) Backup ID which the backup has to be fetched.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def delete_backup(
            self,
            organizationId,
            projectId,
            clusterId,
            backupId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Delete Backup in project {} in organization {}".format(
                projectId, organizationId))
        params = {}
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_del(
            self.backups_endpoint.format(
                organizationId, projectId, clusterId) + '/' + backupId, params, headers)
        return resp

    """
    Method to get the backup under cluster, project and organization mentioned.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
    - Organization Owner
    - Project Owner
    :param organizationId (str) Organization ID under which the backup has to be created.
    :param projectId (str) Project ID under which the backup has to be created.
    :param clusterId (str) Cluster ID under which the backup has to be created.
    :param backupId (str) Backup ID which the backup has to be fetched.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def get_backup(
            self,
            organizationId,
            projectId,
            clusterId,
            backupId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Get Backup in project {} in organization {}".format(
                projectId, organizationId))
        params = {}
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_get(
            self.backups_endpoint.format(
                organizationId, projectId, clusterId) + '/' + backupId, params, headers)
        return resp

    """
    Method to get list all the backups under cluster, project and organization mentioned.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
    - Organization Owner
    - Project Owner
    :param organizationId (str) Organization ID under which the backup has to be created.
    :param projectId (str) Project ID under which the backup has to be created.
    :param clusterId (str) Cluster ID under which the backup has to be created.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def list_backups(
            self,
            organizationId,
            projectId,
            clusterId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "List Backup in project {} in organization {}".format(
                projectId, organizationId))
        params = {}
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_get(
            self.backups_endpoint.format(
                organizationId, projectId, clusterId), params, headers)
        return resp

    """
    Method creates a backup under bucket, cluster, project and organization mentioned.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
    - Organization Owner
    - Project Owner
    :param organizationId (str) Organization ID under which the backup has to be created.
    :param projectId (str) Project ID under which the backup has to be created.
    :param clusterId (str) Cluster ID under which the backup has to be created.
    :param bucketId (str) Bucket ID under which the backup has to be created.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def create_backup(
            self,
            organizationId,
            projectId,
            clusterId,
            bucketId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Creating Backup in project {} in organization {}".format(
                projectId, organizationId))
        params = {}
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_post(
            self.backups_endpoint.format(
                organizationId, projectId, "{}/buckets/{}".format(clusterId, bucketId)), params, headers)
        return resp

    """
    Method creates a cluster under project and organization mentioned.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
    - Organization Owner
    - Project Owner
    - Project Manager
    :param organizationId (str) Organization ID under which the cluster has to be created.
    :param projectId (str) Project ID under which the cluster has to be created.
    :param name (str) Name of the cluster to be created. Max length 256 characters.
    :param description (str) Description of the cluster. Optional. Max length 1024 characters.
    :param cloudProvider (object) The cloud provider where the cluster will be hosted.
    {
        :param type (str) Cloud provider type, either 'aws', 'gcp', or 'azure'.
        :param region (str) Cloud provider region
        :param cidr (str) CIDR block for Cloud Provider.
    }
    :param couchbaseServer (object)
    {
        :param version(str) Version of the Couchbase Server to be installed in the cluster, should be greater than 7.1.
    }
    :param serviceGroups ([object])
    [{
        :param node (object)
        {
            :param compute (object)
            {
                :param cpu (int) CPU units (cores).
                :param ram (int) RAM units (GB).
            }
            :param disk (object)
            {
                :param storage (int) >=50. Storage in GB.
                :param type (str) Type of disk
                    AWS - "gp3", "io2"
                    GCP - "pd-ssd"
                    azure - "p6" "p10" "p15" "p20" "p30" "p40" "p50" "p60" "ultra"
                :param iops (int) For AWS and Azure only.
                :param autoExpansion (bool) Auto-expansion option. Only supported for AWS and GCP.
            }
        }
        :param numOfNodes (int) Number of nodes. Min value - 3 Max Value - 27
        :param services ([object])
        [{
            :param type (str) Enum: "query" "index" "data" "search" "analytics" "eventing"
        }]
    }]
    :param availability (object)
    {
        :param type (str) Availability zone type, either 'single' or 'multi'.
    }
    :param trial (bool) Specify if the cluster is for a trial or not. True for trial cluster.
    :param support (object)
    {
        :param plan (str) Plan type, either "basic" "developer pro" "enterprise"
        :param timezone (str) The standard timezone for the cluster. Should be the TZ identifier.
        Enum: "ET" "GMT" "IST" "PT"
    }
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def create_cluster(
            self,
            organizationId,
            projectId,
            name,
            cloudProvider,
            couchbaseServer=None,
            serviceGroups=None,
            availability=None,
            support=None,
            description="",
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Creating Cluster {} in project {} in organization {}".format(
                name, projectId, organizationId))
        params = {
            "name": name,
            "cloudProvider": cloudProvider
        }
        if couchbaseServer:
            params["couchbaseServer"] = couchbaseServer
        if serviceGroups:
            params["serviceGroups"] = serviceGroups
        if availability:
            params["availability"] = availability
        if support:
            params["support"] = support
        if description:
            params["description"] = description

        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_post(
            self.cluster_endpoint.format(
                organizationId, projectId), params, headers)
        return resp

    """
    Method fetches all the clusters under a project.
    Returned set of clusters is reduced to what the caller has access to view.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
        Project Viewer
        Database Data Reader/Writer
        Database Data Reader
    :param organizationId (str) Organization ID for which the cluster have to be listed.
    :param projectId (str) Project ID for which the cluster has to be listed.
    :param page (int) Sets what page you would like to view
    :param perPage (int) Sets how many results you would like to have on each page
    :param sortBy ([string]) Sets order of how you would like to sort results and also the key you would like to order by
                             Example: sortBy=name
    :param sortDirection (str) The order on which the items will be sorted. Accepted Values - asc / desc
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def list_clusters(
            self,
            organizationId,
            projectId,
            page=None,
            perPage=None,
            sortBy=None,
            sortDirection=None,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "List all the cluster for project {} in organization {}".format(
                projectId, organizationId))
        params = {}
        if page:
            params["page"] = page
        if perPage:
            params["perPage"] = perPage
        if perPage:
            params["sortBy"] = sortBy
        if perPage:
            params["sortDirection"] = sortDirection

        for k, v in kwargs.items():
            params[k] = v

        resp = self.capella_api_get(
            self.cluster_endpoint.format(
                organizationId, projectId), params, headers)
        return resp

    """
    Method fetches info of the required cluster.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
        Project Viewer
        Database Data Reader/Writer
        Database Data Reader
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster whose info has to be fetched.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def fetch_cluster_info(
            self,
            organizationId,
            projectId,
            clusterId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Fetching cluster info for {} in project {} in organization {}".format(
                clusterId, projectId, organizationId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get(
            "{}/{}".format(
                self.cluster_endpoint.format(
                    organizationId,
                    projectId),
                clusterId),
            params,
            headers)
        return resp

    """
    Method to update cluster config.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster whose config has to be updated.
    :param name (str) Name of the cluster to be created. Max length 256 characters.
    :param description (str) Description of the cluster. Optional. Max length 1024 characters.
    :param support (object)
    {
        :param plan (str) Plan type, either "basic" "developer pro" "enterprise"
        :param timezone (str) The standard timezone for the cluster. Should be the TZ identifier.
        Enum: "ET" "GMT" "IST" "PT"
    }
    :param serviceGroups ([object])
    [{
        :param node (object)
        {
            :param compute (object)
            {
                :param cpu (int) CPU units (cores).
                :param ram (int) RAM units (GB).
            }
            :param disk (object)
            {
                :param storage (int) >=50. Storage in GB.
                :param type (str) Type of disk
                    AWS - "gp3", "io2"
                    GCP - "pd-ssd"
                    azure - "p6" "p10" "p15" "p20" "p30" "p40" "p50" "p60" "ultra"
                :param iops (int) For AWS and Azure only.
                :param autoExpansion (bool) Auto-expansion option. Only supported for AWS and GCP.
            }
        }
        :param numOfNodes (int) Number of nodes. Min value - 3 Max Value - 27
        :param services ([object])
        [{
            :param type (str) Enum: "query" "index" "data" "search" "analytics" "eventing"
        }]
    }]
    :param ifmatch (bool) Is set to true then it uses a precondition header that specifies the entity tag of a resource.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def update_cluster(
            self,
            organizationId,
            projectId,
            clusterId,
            name,
            description,
            support,
            serviceGroups,
            ifmatch,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Updating cluster {} in project {} in organization {}".format(
                clusterId, projectId, organizationId))
        params = {
            "name": name,
            "description": description,
            "support": support,
            "serviceGroups": serviceGroups
        }
        if ifmatch:
            if not headers:
                headers = {}
            result = self.fetch_cluster_info(
                organizationId, projectId, clusterId)
            version_id = result.json()["audit"]["version"]
            headers["If-Match"] = "Version: {}".format(version_id)

        for k, v in kwargs.items():
            params[k] = v

        resp = self.capella_api_put(
            "{}/{}".format(
                self.cluster_endpoint.format(
                    organizationId,
                    projectId),
                clusterId),
            params,
            headers)
        return resp

    """
    Method deletes the required cluster.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster which has to be deleted.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def delete_cluster(
            self,
            organizationId,
            projectId,
            clusterId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Deleting cluster {} in project {} in organization {}".format(
                clusterId, projectId, organizationId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_del(
            "{}/{}".format(
                self.cluster_endpoint.format(
                    organizationId,
                    projectId),
                clusterId),
            params,
            headers)
        return resp

    """
    Switches on the cluster.

    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster which has to be switched on.
    :param turnOnLinkedAppService (bool) To turn on the related App Services inside the cluster as well or not
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def switch_cluster_on(self, organizationId, projectId, clusterId,
                          turnOnLinkedAppService, headers=None, **kwargs):
        self.cluster_ops_API_log.info(
            "Switching on Cluster {} in project {} in organization {}".format(
                clusterId, projectId, organizationId))

        params = {
            "turnOnLinkedAppService": turnOnLinkedAppService,
        }
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_post(
            self.cluster_on_off_endpoint.format(
                organizationId, projectId, clusterId), params, headers)
        return resp

    """
    Switches off the cluster.
    Turning off your database turns off the compute for your cluster but the storage remains.
    All of the data, schema (buckets, scopes, and collections), and indexes remain, as well as cluster configuration, including users and allow lists.

    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster which has to be switched off.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def switch_cluster_off(self, organizationId, projectId, clusterId,
                           headers=None, **kwargs):
        self.cluster_ops_API_log.info(
            "Switching off Cluster {} in project {} in organization {}".format(
                clusterId, projectId, organizationId))

        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_del(
            self.cluster_on_off_endpoint.format(
                organizationId, projectId, clusterId), params, headers)
        return resp

    """
    This provides the means to add a new cluster On/Off schedule. Based on this the cluster is turned on or off for the whole week based on the specified day

    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager

    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) ID of the cluster which has to be scheduled.
    :param timezone (str) Timezone for the schedule
    :param days (list) [
        :object {
            :param state (str) The scheduled state for the cluster
            :param day (str) The scheduled day for that state
            :param from (object) {
                :param hour (int)
                :param minute (int)
            }
            :param to (object) {
                :param hour (int)
                :param minute (int)
            }
        }
    ]
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def create_cluster_on_off_schedule(
            self,
            organizationId,
            projectId,
            clusterId,
            timezone,
            days,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info("Adding on/off schedule in cluster {} "
                                      .format(clusterId))
        params = {
            "timezone": timezone,
            "days": days,
        }
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_post(
            self.cluster_on_off_schedule_endpoint.format(
                organizationId, projectId, clusterId), params, headers)
        return resp

    """
    Fetches the details of the On/Off schedule for the given cluster.

    In order to access this endpoint, the provided API key must have at least one of the following roles:
        Organization Owner
        Project Owner
        Project Manager

    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) ID of the cluster which has to be scheduled.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def fetch_cluster_on_off_schedule(
            self,
            organizationId,
            projectId,
            clusterId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info("Fetching on/off schedule in cluster {}"
                                      .format(clusterId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get(
            self.cluster_on_off_schedule_endpoint.format(
                organizationId, projectId, clusterId), params, headers)
        return resp

    """
    This provides the means to update an existing cluster On/Off schedule.

    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager

    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) ID of the cluster which has to be scheduled.
    :param timezone (str) Timezone for the schedule
    :param days (list) [
        :object {
            :param state (str) The scheduled state for the cluster
            :param day (str) The scheduled day for that state
            :param from (object) {
                :param hour (int)
                :param minute (int)
            }
            :param to (object) {
                :param hour (int)
                :param minute (int)
            }
        }
    ]
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def update_cluster_on_off_schedule(
            self,
            organizationId,
            projectId,
            clusterId,
            timezone,
            days,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info("Updating on/off schedule in cluster {}"
                                      .format(clusterId))
        params = {
            "timezone": timezone,
            "days": days,
        }
        for k, v in kwargs.items():
            params[k] = v

        resp = self.capella_api_put(
            self.cluster_on_off_schedule_endpoint.format(
                organizationId, projectId, clusterId), params, headers)
        return resp

    """
    Fetches the details of the On/Off schedule for the given cluster.

    In order to access this endpoint, the provided API key must have at least one of the following roles:
        Organization Owner
        Project Owner
        Project Manager

    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) ID of the cluster which has to be scheduled.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def delete_cluster_on_off_schedule(
            self,
            organizationId,
            projectId,
            clusterId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info("Deleting on/off schedule in cluster {}"
                                      .format(clusterId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_del(
            self.cluster_on_off_schedule_endpoint.format(
                organizationId, projectId, clusterId), params, headers)
        return resp

    """
    Method creates an alert inside a project.
    :param organizationId (str) Organization ID under which the alert is present.
    :param projectId (str) Project ID under which the alert is present.
    :param kind (str) Type of alert integration, currently only supports the type "webhook".
    :param name (str) Name to be added by user about the alert integration. [optional]
    :param config (obj) Configuration details of the alert integration, currently only contains the object webhook.
        :param method (str) Type of Rest API to be send to the user for the alert notification. (POST or PUT)
        :param url ($url) The URL of the customer to which the alert notification API call must be sent.
        :param headers (obj) HTTP headers to be present in the alert API call to the customer. [optional]
            :param headerValue (str) key value pair of the headers to be present in the API call
        :param webhook (obj)
            :param token (str) Authorisation header token to be used in the POST/PUT API alert send to the customer though the webhook integration.
            :param basicAuth (obj) Authorisation header username and password to be used in the POST/PUT API alert send to the customer though the webhook integration.
                :param userId (str)
                :param password (str)
            :param exclude (obj) Contains 2 arrays named appServices and clusters to list what appServices and clusters to not send alerts about. [Optional]
                :param appServices (list) The appServices that the alert shall ignore
                    :param appServiceId (UUID) All the IDs to be excluded by the alert
                :param clusters (list) The clusters that the alert shall ignore
                    :param clusterID (UUID) All the IDs to be excluded by the alert
    :param headers (dict) Headers to be sent with the API call. (NOTE THAT THIS PARAM IS DIFFERENT TO THE HEADER PARAM BEING SENT IN THE BODY OF THE ALERT IN THE CONFIG OBJECT)
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.

    NOTES:
    Either token or basicAuth is required. * Not both! *
    Headers are optional HTTP headers that can be added (support for up to 20).
    The method can be one of POST or PUT
    The url must be HTTPS
    """
    def create_alert(
            self,
            organizationId,
            projectId,
            kind,
            config,
            name=None,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info("Creating an alert inside the project {} inside the organization {}.".format(projectId, organizationId))

        params = {
            "kind": kind,
            "config": config,
        }
        if name:
            params["name"] = name
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_post(self.alerts_endpoint.format(
                organizationId, projectId),
            params, headers)
        return resp

    """
    Method fetches an alert's information, configuration and state.
    :param organizationId (str) Organization ID under which the alert is present.
    :param projectId (str) Project ID under which the alert is present.
    :param alertId (str) ID of the alert to be fetched.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def fetch_alert_info(
            self,
            organizationId,
            projectId,
            alertId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info("Fetching the alert {} inside the project {} inside the organization {}.".format(alertId, projectId, organizationId))

        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get("{}/{}".format(self.alerts_endpoint.format(
                organizationId, projectId), alertId),
            params, headers)
        return resp

    """
    Method lists all alerts inside an organization.
    :param organizationId (str) Organization ID under which the alert is present.
    :param projectId (str) Project ID under which the alert is present.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def list_alerts(
            self,
            organizationId,
            projectId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info("List alert inside project {} the organization {}.".format(projectId, organizationId))

        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get(self.alerts_endpoint.format(
                organizationId, projectId),
            params, headers)
        return resp

    """
    Method deletes an alert inside a project.
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the alert is present.
    :param alertId (str) ID of the alert to be deleted.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def delete_alert(
            self,
            organizationId,
            projectId,
            alertId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info("Deleting an alert inside the project {} inside the organization {}.".format(projectId, organizationId))

        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_del("{}/{}".format(self.alerts_endpoint.format(
                organizationId, projectId), alertId),
            params, headers)
        return resp

    """
    Method updates an alert inside a project.
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param alertId (str) ID of the alert to be updated.
    :param kind (str) Type of alert integration, currently only supports the type "webhook".
    :param name (str) Name to be added by user about the alert integration. [optional]
    :param config (obj) Configuration details of the alert integration, currently only contains the object webhook.
        :param method (str) Type of Rest API to be send to the user for the alert notification. (POST or PUT)
        :param url ($url) The URL of the customer to which the alert notification API call must be sent.
        :param headers (obj) HTTP headers to be present in the alert API call to the customer. [optional]
            :param headerValue (str) key value pair of the headers to be present in the API call
        :param webhook (obj)
            :param token (str) Authorisation header token to be used in the POST/PUT API alert send to the customer though the webhook integration.
            :param basicAuth (obj) Authorisation header username and password to be used in the POST/PUT API alert send to the customer though the webhook integration.
                :param userId (str)
                :param password (str)
            :param exclude (obj) Contains 2 arrays named appServices and clusters to list what appServices and clusters to not send alerts about. [Optional]
                :param appServices (list) The appServices that the alert shall ignore
                    :param appServiceId (UUID) All the IDs to be excluded by the alert
                :param clusters (list) The clusters that the alert shall ignore
                    :param clusterID (UUID) All the IDs to be excluded by the alert
    :param headers (dict) Headers to be sent with the API call. (NOTE THAT THIS PARAM IS DIFFERENT TO THE HEADER PARAM BEING SENT IN THE BODY OF THE ALERT IN THE CONFIG OBJECT)
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.

    NOTES:
    Either token or basicAuth is required. * Not both! *
    Headers are optional HTTP headers that can be added (support for up to 20).
    The method can be one of POST or PUT
    The url must be HTTPS
    """
    def update_alert(
            self,
            organizationId,
            projectId,
            alert_id,
            kind,
            config,
            name=None,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info("Updating the alert {} inside the project {} inside the organization {}.".format(alert_id, projectId, organizationId))

        params = {
            "kind": kind,
            "config": config,
        }
        if name:
            params["name"] = name
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_put("{}/{}".format(self.alerts_endpoint.format(
                organizationId, projectId), alert_id),
            params, headers)
        return resp

    """
    Method tests all alerts inside an organization.
    :param organizationId (str) Organization ID under which the alert is present.
    :param projectId (str) Project ID under which the alert is present.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def test_alert(
            self,
            organizationId,
            projectId,
            config,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info("Test alert inside project {} the organization {}.".format(projectId, organizationId))

        params = {
            "config": config,
        }
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_post(self.test_alert_endpoint.format(
                organizationId, projectId),
            params, headers)
        return resp

    """
    Updates the audit log configuration for the cluster.
    In order to access this endpoint, the provided API key must have at least one of the following roles:
        Organization Owner
        Project Owner
        Project Manager

    :param organizationId (str) Organization ID under which the project is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID for which the audit log is being created.
    :param auditEnabled (bool)
    :param disabledUsers (list)
        :param (obj)
            :param domain (str)
            :param name (str)
    :param enabledEventIDs (list)
        :param eventId (int)
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def update_audit_log(
            self,
            organizationId,
            projectId,
            clusterId,
            auditEnabled,
            disabledUsers,
            enabledEventIDs,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info("Creating Audit Log inside cluster {} inside project {} inside organization {}".format(clusterId, projectId, organizationId))

        params = {
            "auditEnabled": auditEnabled,
            "disabledUsers": disabledUsers,
            "enabledEventIDs": enabledEventIDs
        }
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_put(self.audit_log_endpoint.format(
            organizationId, projectId, clusterId),
            params, headers)
        return resp

    """
    Fetches information on whether audit logging is enabled, and which event IDs are enabled.
    To learn more about cluster audit logs, please refer to audit management.
    In order to access this endpoint, the provided API key must have at least one of the following roles :
        Organization Owner
        Project Owner
        Project Manager
        Project Viewer
        Database Data Reader/Writer
        Database Data Reader

    :param organizationId (str) Organization ID under which the project is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID for which the audit log is requested.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def fetch_audit_log_info(
            self,
            organizationId,
            projectId,
            clusterId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info("Fetching Audit Logging inside cluster {} inside project {} inside organization {}".format(clusterId, projectId, organizationId))

        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get(self.audit_log_endpoint.format(
                organizationId, projectId, clusterId),
            params, headers)
        return resp

    """
    Retrieves a list of audit event IDs. The list of filterable event IDs can be specified while configuring audit log for cluster.
    In order to access this endpoint, the provided API key must have at least one of the following roles :
        Organization Owner
        Project Owner
        Project Manager
        Project Viewer
        Database Data Reader/Writer
        Database Data Reader

    :param organizationId (str) Organization ID under which the project is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID for which the audit log events are to be fetched.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def fetch_audit_log_event_info(
            self,
            organizationId,
            projectId,
            clusterId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info("Fetching Audit Log Events info inside cluster {} inside project {} inside organization {}".format(clusterId, projectId, organizationId))

        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get(self.audit_log_events_endpoint.format(
                organizationId, projectId, clusterId),
            params, headers)
        return resp

    """
    Creates a new audit log export job.
    Audit Logs for the last 30 days can be requested, otherwise they are purged. A pre-signed URL to a s3 bucket location is returned, which is used to download these audit logs.
    In order to access this endpoint, the provided API key must have at least one of the following roles:
        Organization Owner
        Project Owner
        Project Manager

    :param organizationId (str) Organization ID under which the project is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID for which the audit log is being created.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def create_audit_log_export(
            self,
            organizationId,
            projectId,
            clusterId,
            start,
            end,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info("Creating Audit Log inside cluster {} inside project {} inside organization {}".format(clusterId, projectId, organizationId))

        params = {
            "start": start,
            "end": end
        }
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_post(self.audit_log_exports_endpoint.format(
            organizationId, projectId, clusterId),
            params, headers)
        return resp

    """
    Fetches the status of a single audit log export job.
    It will show the pre-signed URL if the export was successful, a failure error if it was unsuccessful or a message saying no audit logs available if there were no audit logs found during the given timeframe.
    In order to access this endpoint, the provided API key must have at least one of the following roles:
        Organization Owner
        Project Owner
        Project Manager
        Project Viewer
        Database Data Reader/Writer
        Database Data Reader

    :param organizationId (str) Organization ID under which the project is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID for which the audit log is requested.
    :param auditLogId (str) The ID of the audit log which is being fetched.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def fetch_audit_log_export_info(
            self,
            organizationId,
            projectId,
            clusterId,
            auditLogId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info("Fetching Audit Log {} inside cluster {} inside project {} inside organization {}".format(auditLogId, clusterId, projectId, organizationId))

        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get("{}/{}".format(self.audit_log_exports_endpoint.format(
                organizationId, projectId, clusterId), auditLogId),
            params, headers)
        return resp

    """
    Fetches the status of a all audit log export jobs inside a cluster.
    It will show the pre-signed URL if the export was successful, a failure error if it was unsuccessful or a message saying no audit logs available if there were no audit logs found during the given timeframe.
    In order to access this endpoint, the provided API key must have at least one of the following roles:
        Organization Owner
        Project Owner
        Project Manager
        Project Viewer
        Database Data Reader/Writer
        Database Data Reader

    :param organizationId (str) Organization ID under which the project is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID for which the audit log is requested.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def list_audit_log_exports(
            self,
            organizationId,
            projectId,
            clusterId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info("Listing Audit Logs inside cluster {} inside project {} inside organization {}".format(clusterId, projectId, organizationId))

        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get(self.audit_log_exports_endpoint.format(
                organizationId, projectId, clusterId),
            params, headers)
        return resp

    """
    Method downloads the cluster certificate
    In order to access this endpoint, the provided API key must have at least one of the following
    roles:
         Organization Owner
         Project Owner
    Couchbase Capella supports the use of x.509 certificates, for clients and servers. This
    ensures that only approved users, applications, machines, and endpoints have access to system
    resources.
    Consequently, the mechanism can be used by Couchbase SDK clients to access Couchbase Services,
    and by source clusters that use XDCR to replicate data to target clusters. Clients can verify
    the identity of Couchbase Capella, thereby ensuring that they are not exchanging data with a
    rogue entity.
    Get Certificate
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster which has to be deleted.
    """
    def get_cluster_certificate(self, organization_id, project_id, cluster_id, headers=None,
                                **kwargs):
        self.cluster_ops_API_log.info(
            "Downloading certificate for cluster {} in project {} in organization {}".format(
                cluster_id, project_id, organization_id))
        if kwargs:
            params = kwargs
        else:
            params = None
        capella_api_response = self.capella_api_get('{}/{}/certificates'.format(
            self.cluster_endpoint.format(organization_id, project_id), cluster_id),
            params=params, headers=headers)
        return capella_api_response

    """
    Method adds a CIDR to allowed CIDRs list of the specified cluster.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster to which CIDR has to be added.
    :param cidr (str) The trusted CIDR to allow connections from.
    :param comment (str) A short description about the allowed CIDR.
    :param expiresAt (str) An RFC3339 timestamp determining when the allowed CIDR should expire.
    If this field is empty/omitted then the allowed CIDR is permanent and will never automatically expire.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def add_CIDR_to_allowed_CIDRs_list(
            self,
            organizationId,
            projectId,
            clusterId,
            cidr,
            comment="",
            expiresAt="",
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Adding {} CIDR block to {} cluster allowed CIDR list".format(
                cidr, clusterId))
        params = {
            "cidr": cidr
        }
        if comment:
            params["comment"] = comment
        if expiresAt:
            params["expiresAt"] = expiresAt
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_post(self.allowedCIDR_endpoint.format(
            organizationId, projectId, clusterId), params, headers)
        return resp

    """
    Method fetches all the allowed CIDRs for a given cluster.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
        Project Viewer
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster for which the allowed CIDRs list is to be fetched.
    :param page (int) Sets what page you would like to view
    :param perPage (int) Sets how many results you would like to have on each page
    :param sortBy ([string]) Sets order of how you would like to sort results and also the key you would like to order by
                             Example: sortBy=name
    :param sortDirection (str) The order on which the items will be sorted. Accepted Values - asc / desc
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def list_allowed_CIDRs(
            self,
            organizationId,
            projectId,
            clusterId,
            page=None,
            perPage=None,
            sortBy=None,
            sortDirection=None,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "List all the allowed CIDRs for cluster {}".format(clusterId))
        params = {}
        if page:
            params["page"] = page
        if perPage:
            params["perPage"] = perPage
        if perPage:
            params["sortBy"] = sortBy
        if perPage:
            params["sortDirection"] = sortDirection

        for k, v in kwargs.items():
            params[k] = v

        resp = self.capella_api_get(self.allowedCIDR_endpoint.format(
            organizationId, projectId, clusterId), params, headers)
        return resp

    """
    Method fetches info of the required allowed CIDR ID.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
        Project Viewer
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the allowed CIDR ID is present.
    :param allowedCidrId (str) The GUID4 ID of the allowed CIDR.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def fetch_allowed_CIDR_info(
            self,
            organizationId,
            projectId,
            clusterId,
            allowedCidrId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Fetching allowed CIDR info for {} in cluster {}".format(
                allowedCidrId, clusterId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get(
            "{}/{}".format(
                self.allowedCIDR_endpoint.format(
                    organizationId,
                    projectId,
                    clusterId),
                allowedCidrId),
            params,
            headers)
        return resp

    """
    Method deletes specified CIDR ID from allowed CIDR list of the cluster.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the allowed CIDR ID is present.
    :param allowedCidrId (str) The GUID4 ID of the allowed CIDR which is to be deleted.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def delete_allowed_CIDR(
            self,
            organizationId,
            projectId,
            clusterId,
            allowedCidrId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Deleting allowed CIDR {} from cluster {}".format(
                allowedCidrId, clusterId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_del(
            "{}/{}".format(
                self.allowedCIDR_endpoint.format(
                    organizationId,
                    projectId,
                    clusterId),
                allowedCidrId),
            params,
            headers)
        return resp

    """
    Method creates a new database user for a cluster.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster for which the database user is to be created.
    :param name (str) Username for the database credential (2-256 characters).
    :param password (str) A password associated with the database credential.
    If this field is left empty, a password will be auto-generated.
    :param access ([object])
    [{
        :param privileges ([str]) The list of privileges granted on the resources. read/write
        :param resources (object) The resources for which access will be granted on.
        {
            :param buckets ([object])
            [{
                :param name (str) The name of the bucket.
                :param scopes ([object])
                [{
                    :param name (str) The name of the scope.
                    :param collections ([str]) The collections under a scope.
                }]
            }]
        }
    }]
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def create_database_user(
            self,
            organizationId,
            projectId,
            clusterId,
            name,
            access,
            password="",
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Creating Database User {} in cluster {}".format(
                name, clusterId))
        params = {
            "name": name,
            "access": access
        }
        if password:
            params["password"] = password
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_post(self.db_user_endpoint.format(
            organizationId, projectId, clusterId), params, headers)
        return resp

    """
    Method fetches all the database users for a given cluster.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
        Project Viewer
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster for which the database users list is to be fetched.
    :param page (int) Sets what page you would like to view
    :param perPage (int) Sets how many results you would like to have on each page
    :param sortBy ([string]) Sets order of how you would like to sort results and also the key you would like to order by
                             Example: sortBy=name
    :param sortDirection (str) The order on which the items will be sorted. Accepted Values - asc / desc
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def list_database_users(
            self,
            organizationId,
            projectId,
            clusterId,
            page=None,
            perPage=None,
            sortBy=None,
            sortDirection=None,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "List all the database users for cluster {}".format(clusterId))
        params = {}
        if page:
            params["page"] = page
        if perPage:
            params["perPage"] = perPage
        if perPage:
            params["sortBy"] = sortBy
        if perPage:
            params["sortDirection"] = sortDirection

        for k, v in kwargs.items():
            params[k] = v

        resp = self.capella_api_get(self.db_user_endpoint.format(
            organizationId, projectId, clusterId), params, headers)
        return resp

    """
    Method fetches info of the required database user ID.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
        Project Viewer
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the database user ID is present.
    :param userId (str) The GUID4 ID of the database user.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def fetch_database_user_info(
            self,
            organizationId,
            projectId,
            clusterId,
            userId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Fetching Database user info for {} present in cluster {}".format(
                userId, clusterId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get(
            "{}/{}".format(
                self.db_user_endpoint.format(
                    organizationId,
                    projectId,
                    clusterId),
                userId),
            params,
            headers)
        return resp

    """
    Method updates the access of database user ID.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the database user ID is present.
    :param userId (str) The GUID4 ID of the database user.
    :param access ([object])
    [{
        :param privileges ([str]) The list of privileges granted on the resources. read/write
        :param resources (object) The resources for which access will be granted on.
        {
            :param buckets ([object])
            [{
                :param name (str) The name of the bucket.
                :param scopes ([object])
                [{
                    :param name (str) The name of the scope.
                    :param collections ([str]) The collections under a scope.
                }]
            }]
        }
    }]
    :param ifmatch (bool) Is set to true then it uses a precondition header that specifies the entity tag of a resource.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def update_database_user(
            self,
            organizationId,
            projectId,
            clusterId,
            userId,
            access,
            ifmatch,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Updating database user {} in cluster {}".format(
                userId, clusterId))
        params = {
            "access": access
        }
        if ifmatch:
            if not headers:
                headers = {}
            result = self.fetch_database_user_info(
                organizationId, projectId, clusterId, userId)
            version_id = result.json()["audit"]["version"]
            headers["If-Match"] = "Version: {}".format(version_id)

        for k, v in kwargs.items():
            params[k] = v

        resp = self.capella_api_put(
            "{}/{}".format(
                self.db_user_endpoint.format(
                    organizationId,
                    projectId,
                    clusterId),
                userId),
            params,
            headers)
        return resp

    """
    Method deletes specified user ID from the cluster.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the allowed CIDR ID is present.
    :param userId (str) The GUID4 ID of the database user which is to be deleted.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def delete_database_user(
            self,
            organizationId,
            projectId,
            clusterId,
            userId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Deleting database user {} from cluster {}".format(
                userId, clusterId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_del(
            "{}/{}".format(
                self.db_user_endpoint.format(
                    organizationId,
                    projectId,
                    clusterId),
                userId),
            params,
            headers)
        return resp

    """
    Method enables clients to load predefined sample data into a cluster by selecting from three available options:
        - travel-sample
        - gamesim-sample
        - beer-sample
    Upon a successful request, a new bucket will be created within the cluster, and it will be populated with the chosen sample data.

    In order to access this endpoint, the provided API key must have at least one of the following roles:
        - Organization Owner
        - Project Owner
        - Project Manager
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster which has the sample bucket.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def create_sample_bucket(
            self,
            organizationId,
            projectId,
            clusterId,
            sampleBucket,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Loading Sample Bucket {} into cluster {} in project {} in "
            "organization {}".format(
                sampleBucket, clusterId, projectId, organizationId))
        params = {
            "name": sampleBucket,
        }
        for k, v in kwargs.items():
            params[k] = v

        resp = self.capella_api_post(self.sample_bucket_endpoint.format(
                organizationId, projectId, clusterId),
            params, headers)
        return resp

    """
    Method Lists configurations of all the sample buckets under a cluster.
    In order to access this endpoint, the provided API key must have at least one of the following roles:
        - Organization Owner
        - Project Owner
        - Project Manager
        - Project Viewer
        - Database Data Reader/Writer
        - Database Data Reader
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster which has the sample bucket.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def list_sample_buckets(
            self,
            organizationId,
            projectId,
            clusterId,
            page=None,
            perPage=None,
            sortBy=None,
            sortDirection=None,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Listing all Sample Buckets in cluster {} in project {} in "
            "organization {}".format(
                clusterId, projectId, organizationId))
        params = {}
        if page:
            params["page"] = page
        if perPage:
            params["perPage"] = perPage
        if perPage:
            params["sortBy"] = sortBy
        if perPage:
            params["sortDirection"] = sortDirection

        for k, v in kwargs.items():
            params[k] = v

        resp = self.capella_api_get(self.sample_bucket_endpoint.format(
            organizationId, projectId, clusterId), params, headers)
        return resp

    """
    Method Fetches the configuration of the given sample bucket.
    In order to access this endpoint, the provided API key must have at least one of the following roles:
        - Organization Owner
        - Project Owner
        - Project Manager
        - Project Viewer
        - Database Data Reader/Writer
        - Database Data Reader
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster which has the sample bucket.
    :param sampleBucket (str) Name of the Sample Bucket which has to be fetched.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def fetch_sample_bucket(
            self,
            organizationId,
            projectId,
            clusterId,
            sampleBucket,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Fetching sample bucket info for {} present in cluster {}".format(
                sampleBucket, clusterId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get("{}/{}".format(
            self.sample_bucket_endpoint.format(
                organizationId, projectId, clusterId), sampleBucket),
            params, headers)
        return resp

    """
    Method Deletes an existing bucket which was loaded with sample data.
    In order to access this endpoint, the provided API key must have at least one of the following roles:
        - Organization Owner
        - Project Owner
        - Project Manager
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster which has the sample bucket.
    :param sampleBucket (str) Name of the Sample Bucket which has to be deleted.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def delete_sample_bucket(
            self,
            organizationId,
            projectId,
            clusterId,
            sampleBucket,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Deleting Sample Bucket {} from cluster {} in project {} "
            "in organization {}".format(
                sampleBucket, clusterId, projectId, organizationId))
        if kwargs:
            params = kwargs
        else:
            params = None

        resp = self.capella_api_del("{}/{}".format(
            self.sample_bucket_endpoint.format(
                organizationId, projectId, clusterId), sampleBucket),
            params, headers)
        return resp

    """
    Method creates a new bucket in a cluster.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster in which the bucket is to be created.

    :param name (str) Name of the bucket. This field cannot be changed later.
        The name should be according to the following rules-
        1. Characters used for the name should be in the ranges of A-Z, a-z, and 0-9; plus the underscore,
        period, dash, and percent characters.
        2. The name can be a maximum of 100 characters in length.
        3. The name cannot have 0 characters or empty. Minimum length of name is 1.
        4. The name cannot start with a . (period).

    :param type (str) Type of the bucket. If selected Ephemeral, it is not eligible for imports
    or App Endpoints creation. This field cannot be changed later. The options may also be referred to as
    Memory and Disk (Couchbase), Memory Only (Ephemeral).
        Default: "couchbase"
        Accepted Values: "couchbase" "ephemeral"

    :param storageBackend (str) The storage engine to be assigned to and used by the bucket.
    The minimum memory required for Couchstore is 100 MiB, and the minimum memory required for Magma is 1 GiB.
    This field cannot be changed later.
        Default: "couchstore"
        Accepted Values: "couchstore" "magma"

    :param memoryAllocationInMb (int) The amount of memory to allocate for the bucket memory in MiB.
    The maximum limit is dependent on the allocation of the KV service. Min Value 100.

    :param bucketConflictResolution (str) The means in which conflicts are resolved during replication.
    This field cannot be changed later. This field might be referred to as conflictResolution in some places
    and seqno and lww might be referred as sequence Number and Timestamp respectively.
        Default: "seqno"
        Accepted Values: "seqno" "lww"

    :param durabilityLevel (str) The minimum level at which all writes to the Couchbase bucket must occur.
        Default: "none"
        Accepted Values: "none" "majority" "majorityAndPersistActive" "persistToMajority"

    :param replicas (int) The number of replicas for the bucket.
        Default: 1
        Accepted Values: 1 2 3

    :param flush (bool) Determines whether flushing is enabled on the bucket.
    Enable Flush to delete all items in this bucket at the earliest opportunity.
    Disable Flush to avoid inadvertent data loss.
        Default: false

    :param timeToLiveInSeconds (int) Specify the time to live (TTL) value in seconds. This is the maximum time
    to live for items in the bucket. If specified as 0, TTL is disabled.

    :param evictionPolicy (str) This value should only be used when creating ephemeral buckets. This is also
    known as Ejection Policy at various places. Ejection is the policy which Capella will adopt to prevent
    data loss due to memory exhaustion.
        Accepted Values: "valueOnly" "fullEviction" "noEviction" "nruEviction"

    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def create_bucket(
            self,
            organizationId,
            projectId,
            clusterId,
            name,
            type,
            storageBackend,
            memoryAllocationInMb,
            bucketConflictResolution,
            durabilityLevel,
            replicas,
            flush,
            timeToLiveInSeconds,
            priority=None,
            evictionPolicy="",
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Creating bucket {} in cluster {}".format(
                name, clusterId))
        params = {
            "name": name,
            "type": type,
            "storageBackend": storageBackend,
            "memoryAllocationInMb": memoryAllocationInMb,
            "bucketConflictResolution": bucketConflictResolution,
            "durabilityLevel": durabilityLevel,
            "replicas": replicas,
            "flush": flush,
            "timeToLiveInSeconds": timeToLiveInSeconds,
        }
        if priority is not None:
            params["priority"] = priority
        if evictionPolicy:
            params["evictionPolicy"] = evictionPolicy
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_post(
            self.bucket_endpoint.format(
                organizationId,
                projectId,
                clusterId),
            params,
            headers)
        return resp

    """
    Method fetches all the database users for a given cluster.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
        Project Viewer
        Database Data Reader/Writer
        Database Data Reader
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster for which the bucket list is to be fetched.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def list_buckets(
            self,
            organizationId,
            projectId,
            clusterId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "List all the buckets in the cluster {}".format(clusterId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get(self.bucket_endpoint.format(
            organizationId, projectId, clusterId), params, headers)
        return resp

    """
    Method fetches info of the required bucket.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
        Project Viewer
        Database Data Reader/Writer
        Database Data Reader
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the bucket is present.
    :param bucketId (str) The ID of the bucket. It is the URL-compatible base64 encoding of the bucket name.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def fetch_bucket_info(
            self,
            organizationId,
            projectId,
            clusterId,
            bucketId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Fetching bucket info for {} present in cluster {}".format(
                bucketId, clusterId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get("{}/{}".format(self.bucket_endpoint.format(
            organizationId, projectId, clusterId), bucketId), params, headers)
        return resp

    """
    Method updates the config of an existing bucket.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the database user ID is present.
    :param bucketId (str) The ID of the bucket. It is the URL-compatible base64 encoding of the bucket name.

    :param memoryAllocationInMb (int) The amount of memory to allocate for the bucket memory in MiB.
    The maximum limit is dependent on the allocation of the KV service. Min Value 100.

    :param durabilityLevel (str) The minimum level at which all writes to the Couchbase bucket must occur.
        Default: "none"
        Accepted Values: "none" "majority" "majorityAndPersistActive" "persistToMajority"

    :param replicas (int) The number of replicas for the bucket.
        Default: 1
        Accepted Values: 1 2 3

    :param flush (bool) Determines whether flushing is enabled on the bucket.
    Enable Flush to delete all items in this bucket at the earliest opportunity.
    Disable Flush to avoid inadvertent data loss.
        Default: false

    :param timeToLiveInSeconds (int) Specify the time to live (TTL) value in seconds. This is the maximum time
    to live for items in the bucket. If specified as 0, TTL is disabled.

    :param priority (int) Specifies the ranking of the bucket amongst
    other buckets, ranges from a value between (0, 1000), both inclusive.

    :param ifmatch (bool) Is set to true then it uses a precondition header that specifies the entity tag of a resource.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def update_bucket_config(
            self,
            organizationId,
            projectId,
            clusterId,
            bucketId,
            memoryAllocationInMb,
            durabilityLevel,
            replicas,
            flush,
            timeToLiveInSeconds,
            ifmatch,
            priority=None,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Updating bucket {} in cluster {}".format(
                bucketId, clusterId))
        params = {
            "memoryAllocationInMb": memoryAllocationInMb,
            "durabilityLevel": durabilityLevel,
            "replicas": replicas,
            "flush": flush,
            "timeToLiveInSeconds": timeToLiveInSeconds,
            "priority": priority
        }
        if priority:
            params["priority"] = priority

        if ifmatch:
            if not headers:
                headers = {}
            result = self.fetch_bucket_info(
                organizationId, projectId, clusterId, bucketId)
            version_id = result.json()["audit"]["version"]
            headers["If-Match"] = "Version: {}".format(version_id)

        for k, v in kwargs.items():
            params[k] = v

        resp = self.capella_api_put("{}/{}".format(self.bucket_endpoint.format(
            organizationId, projectId, clusterId), bucketId), params, headers)
        return resp

    """
    Method deletes specified bucket from the cluster.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the bucket is present.
    :param bucketId (str) The ID of the bucket. It is the URL-compatible base64 encoding of the bucket name.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def delete_bucket(
            self,
            organizationId,
            projectId,
            clusterId,
            bucketId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Deleting bucket {} in cluster {}".format(
                bucketId, clusterId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_del("{}/{}".format(self.bucket_endpoint.format(
            organizationId, projectId, clusterId), bucketId), params, headers)
        return resp

    """
    Method create's a scope in the specified bucket in the cluster.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the bucket is present.
    :param bucketId (str) The ID of the bucket. It is the URL-compatible base64 encoding of the bucket name.
    :param name (str) The name of the scope.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def create_scope(
            self,
            organizationId,
            projectId,
            clusterId,
            bucketId,
            name,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Creating scope {} in bucket {} in cluster {}".format(
                name, bucketId, clusterId))
        params = {
            "name": name
        }
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_post(self.scope_endpoint.format(
            organizationId, projectId, clusterId, bucketId), params, headers)
        return resp

    """
    Method list's all the scopes in the specified bucket of the cluster.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the bucket is present.
    :param bucketId (str) The ID of the bucket. It is the URL-compatible base64 encoding of the bucket name.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def list_scopes(
            self,
            organizationId,
            projectId,
            clusterId,
            bucketId,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "List all the scopes for bucket {} in cluster {}".format(
                bucketId, clusterId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get(self.scope_endpoint.format(
            organizationId, projectId, clusterId, bucketId), params, headers)
        return resp

    """
    Method fetches the info of the specified scope of a bucket.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the bucket is present.
    :param bucketId (str) The ID of the bucket. It is the URL-compatible base64 encoding of the bucket name.
    :param scopeName (str) The name of the scope whose info is to be fetched.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def fetch_scope_info(
            self,
            organizationId,
            projectId,
            clusterId,
            bucketId,
            scopeName,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Fetching scope info for {} in bucket {} in cluster {}".format(
                scopeName, bucketId, clusterId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get(
            "{}/{}".format(
                self.scope_endpoint.format(
                    organizationId,
                    projectId,
                    clusterId,
                    bucketId),
                scopeName),
            params,
            headers)
        return resp

    """
    Method deletes the specified scope from the bucket.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the bucket is present.
    :param bucketId (str) The ID of the bucket. It is the URL-compatible base64 encoding of the bucket name.
    :param scopeName (str) The name of the scope which has to be deleted.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def delete_scope(
            self,
            organizationId,
            projectId,
            clusterId,
            bucketId,
            scopeName,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Deleting scope {} in bucket {} in cluster {}".format(
                scopeName, bucketId, clusterId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_del(
            "{}/{}".format(
                self.scope_endpoint.format(
                    organizationId,
                    projectId,
                    clusterId,
                    bucketId),
                scopeName),
            params,
            headers)
        return resp

    """
    Method create's a collection in the specified scope of a bucket in the cluster.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the bucket is present.
    :param bucketId (str) The ID of the bucket. It is the URL-compatible base64 encoding of the bucket name.
    :param scopeName (str) The name of the scope under which the collection has to be created.
    :param name (str) The name of the scope.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def create_collection(self, organizationId, projectId, clusterId, bucketId,
                          scopeName, name, maxTTL=-1, headers=None, **kwargs):
        self.cluster_ops_API_log.info(
            "Creating collection {} in scope {} in bucket {} in cluster {}".format(
                name, scopeName, bucketId, clusterId))
        params = {
            "name": name
        }
        if maxTTL >= 0:
            params["maxTTL"] = maxTTL
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_post(
            self.collection_endpoint.format(
                organizationId,
                projectId,
                clusterId,
                bucketId,
                scopeName),
            params,
            headers)
        return resp

    """
    Method list's all the collections in the specified scope in the specified bucket of the cluster.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the bucket is present.
    :param bucketId (str) The ID of the bucket. It is the URL-compatible base64 encoding of the bucket name.
    :param scopeName (str) The name of the scope for which the collection has to be listed.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def list_collections(
            self,
            organizationId,
            projectId,
            clusterId,
            bucketId,
            scopeName,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "List all the collections in the scope {} in bucket {} in cluster {}".format(
                scopeName, bucketId, clusterId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get(
            self.collection_endpoint.format(
                organizationId,
                projectId,
                clusterId,
                bucketId,
                scopeName),
            params,
            headers)
        return resp

    """
    Method fetches the info of the specified collection in the scope of a bucket.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the bucket is present.
    :param bucketId (str) The ID of the bucket. It is the URL-compatible base64 encoding of the bucket name.
    :param scopeName (str) The name of the scope under which the collection is present.
    :param collectionName (str) Name of the collection whose info has to be fetched.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def fetch_collection_info(
            self,
            organizationId,
            projectId,
            clusterId,
            bucketId,
            scopeName,
            collectionName,
            headers=None,
            **kwargs):
        self.cluster_ops_API_log.info(
            "Fetching info for the collection {} in scope {} in bucket {} in cluster {}".format(
                collectionName, scopeName, bucketId, clusterId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get(
            "{}/{}".format(
                self.collection_endpoint.format(
                    organizationId,
                    projectId,
                    clusterId,
                    bucketId,
                    scopeName),
                collectionName),
            params,
            headers)
        return resp

    """
    This method is used to change the maxTTL for a collection.
    It requires 'maxTTL' param in its body under the PATCH request since that is the only parameter being mutated.
     - range = (-1 <= maxTTL <= 2147483647) for CB cluster v7.6 or above,
     - else: range = (-0 <= maxTTL <= 2147483647)
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the bucket is present.
    :param bucketId (str) The ID of the bucket. It is the URL-compatible base64 encoding of the bucket name.
    :param scopeName (str) The name of the scope under which the collection is present.
    :param collectionName (str) Name of the collection whose info has to be fetched.
    :param maxTTL (int) The time limit for the collection to live.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def update_collection(
            self,
            organizationId,
            projectId,
            clusterId,
            bucketId,
            scopeName,
            collectionName,
            maxTTL,
            headers=None,
            **kwargs):
        params = {
            "maxTTL": maxTTL
        }
        for k, v in kwargs.items():
            params[k] = v

        resp = self.capella_api_put("{}/{}".format(
            self.collection_endpoint.format(
                organizationId, projectId, clusterId, bucketId, scopeName),
            collectionName), params, headers)
        return resp

    """
    Method deletes the specified collection in the scope.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the bucket is present.
    :param bucketId (str) The ID of the bucket. It is the URL-compatible base64 encoding of the bucket name.
    :param scopeName (str) The name of the scope under which the collection is present.
    :param collectionName (str) Name of the collection which is to be deleted.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def delete_collection(self, organizationId, projectId, clusterId, bucketId,
                          scopeName, collectionName, headers=None, **kwargs):
        self.cluster_ops_API_log.info(
            "Deleting the collection {} in scope {} in bucket {} in cluster {}".format(
                collectionName, scopeName, bucketId, clusterId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_del(
            "{}/{}".format(
                self.collection_endpoint.format(
                    organizationId,
                    projectId,
                    clusterId,
                    bucketId,
                    scopeName),
                collectionName),
            params,
            headers)
        return resp

    """
    Creates a scheduled backup for a bucket.
    In order to access this endpoint, the provided API key must have at least one of the following roles:
        Organization Owner
        Project Own
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the bucket is present.
    :param bucketId (str) The ID of the bucket. It is the URL-compatible base64 encoding of the bucket name.
    :param dayOfWeek (str) Day of the week for the backup.
    :param startAt  (int) Start at hour (in 24-Hour format).
    :param incrementalEvery  (int) Interval in hours for incremental backup.
    :param retentionTime  (str) Retention time in days.
    :param costOptimizedRetention  (bool) Optimize backup retention to reduce total cost of ownership (TCO). This gives the option to keep all but the last backup cycle of the month for thirty days; the last cycle will be kept for the defined retention period
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def create_backup_schedule(self, organizationId, projectId, clusterId, bucketId,
                               type, dayOfWeek, startAt, incrementalEvery,
                               retentionTime, costOptimizedRetention, headers=None,
                               **kwargs):

        params = {
            "type":  type,
            "weeklySchedule": {
            "dayOfWeek": dayOfWeek,
            "startAt": startAt,
            "incrementalEvery": incrementalEvery,
            "retentionTime": retentionTime,
            "costOptimizedRetention": costOptimizedRetention
            }
        }

        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_post(
            self.backup_schedule_endpoint.format(
                organizationId,
                projectId,
                clusterId,
                bucketId),
            params,
            headers)
        return resp

    """
    Fetched the backup schedule for a bucket in a cluster.
    In order to access this endpoint, the provided API key must have at least one of the following roles:
        Organization Owner
        Project Own
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the bucket is present.
    :param bucketId (str) The ID of the bucket. It is the URL-compatible base64 encoding of the bucket name.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def get_backup_schedule(self, organizationId, projectId, clusterId, bucketId,
                            headers=None, **kwargs):
        if kwargs:
            params = kwargs
        else:
            params = None

        resp = self.capella_api_get(
            self.backup_schedule_endpoint.format(
                organizationId,
                projectId,
                clusterId,
                bucketId),
            params,
            headers
        )

        return resp

    """
    Updates an existing backup schedule.
    In order to access this endpoint, the provided API key must have at least one of the following roles:
        Organization Owner
        Project Own
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the bucket is present.
    :param bucketId (str) The ID of the bucket. It is the URL-compatible base64 encoding of the bucket name.
    :param dayOfWeek (str) Day of the week for the backup.
    :param startAt  (int) Start at hour (in 24-Hour format).
    :param incrementalEvery  (int) Interval in hours for incremental backup.
    :param retentionTime  (str) Retention time in days.
    :param costOptimizedRetention  (bool) Optimize backup retention to reduce total cost of ownership (TCO). This gives the option to keep all but the last backup cycle of the month for thirty days; the last cycle will be kept for the defined retention period
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def update_backup_schedule(self, organizationId, projectId, clusterId, bucketId,
                               type, dayOfWeek, startAt, incrementalEvery,
                               retentionTime, costOptimizedRetention, headers=None,
                               **kwargs):
        params = {
            "type":  type,
            "weeklySchedule": {
            "dayOfWeek": dayOfWeek,
            "startAt": startAt,
            "incrementalEvery": incrementalEvery,
            "retentionTime": retentionTime,
            "costOptimizedRetention": costOptimizedRetention
            }
        }

        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_put(
            self.backup_schedule_endpoint.format(
                organizationId,
                projectId,
                clusterId,
                bucketId),
            params,
            headers)
        return resp

    """
    Deletes an existing backup schedule
    In order to access this endpoint, the provided API key must have at least one of the following roles:
        Organization Owner
        Project Own
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster under which the bucket is present.
    :param bucketId (str) The ID of the bucket. It is the URL-compatible base64 encoding of the bucket name.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """

    def delete_backup_schedule(self, organizationId, projectId, clusterId, bucketId,
                               headers=None, **kwargs):
        if kwargs:
            params = kwargs
        else:
            params = None

        resp = self.capella_api_del(
            self.backup_schedule_endpoint.format(
                organizationId,
                projectId,
                clusterId,
                bucketId),
            params,
            headers
        )

        return resp

    def list_appservices(self, tenant_id, page=None, perPage=None, sortBy=None, sortDirection=None, projectId=None, headers=None, **kwargs):
        """
        Lists all the clusters under the organization.
        In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        -Organization Owner
        -Project Owner
        -Project Manager
        -Project Viewer
        -Database Data Reader/Writer
        -Database Data Reader
        :param tenant_id: Organization id
        :param page (int) Sets what page you would like to view
        :param perPage (int) Sets how many results you would like to have on each page
        :param sortBy ([string]) Sets order of how you would like to sort results and also the key you would like to order by
                             Example: sortBy=name
        :param sortDirection (str) The order on which the items will be sorted. Accepted Values - asc / desc
        :param projectId (str) The GUID4 ID of the project.
        """
        url = self.org_appservice_api.format(tenant_id)
        params = {}
        if page:
            params["page"] = page
        if perPage:
            params["perPage"] = perPage
        if perPage:
            params["sortBy"] = sortBy
        if perPage:
            params["sortDirection"] = sortDirection
        if projectId:
            params["projectId"] = projectId
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_get(url, params=params, headers=headers)
        return resp

    def create_appservice(self, tenant_id, project_id, cluster_id, appservice_name, compute, nodes=None, version=None, description="", headers=None, **kwargs):
        """
        Creates a new App Service.
        In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        -Organization Owner
        -Project Owner
        :param organizationId (str) Organization ID under which the app service is present.
        :param projectId (str) Project ID under which the app service is present.
        :param clusterId (str) ID of the cluster under which the app service is present.
        :param appservice_name: Name of the appservice.
        :param compute (obj)
            :param cpu: Number of cpu CORES in appservices. Accepted values = [2, 4, 8, 16, 26]
            :param ram: Size of ram (in GBs. Accepted values = [4, 8, 16, 32, 72])
        :param description: Description of the appservice (optional, can be empty)
        :param nodes: Number of nodes in appservices (optional, Number of nodes configured for the App Service. The number of nodes can range from 2 to 12.)
        :param version: Version of appservice to deploy (optional, if not given, defaults to the latest version)
        :param headers (dict) Headers to be sent with the API call.
        :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
        """
        url = self.cluster_appservice_api.format(tenant_id, project_id, cluster_id)
        params = {
            "name": appservice_name,
            "compute": compute
        }
        if description:
            params["description"] = description
        if nodes:
            params["nodes"] = nodes
        if version:
            params["version"] = version

        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_post(url, params, headers)
        return resp

    def delete_appservice(self, tenant_id, project_id, cluster_id, appservice_id, headers=None, **kwargs):
        """
        Deletes an existing App Service.
        In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        -Organization Owner
        -Project Owner
        -Project Manager
        :param organizationId (str) Organization ID under which the app service is present.
        :param projectId (str) Project ID under which the app service is present.
        :param clusterId (str) ID of the cluster under which the app service is present.
        :param appservice_id: ID of the app service.
        :param headers (dict) Headers to be sent with the API call.
        :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
        """
        url = (self.cluster_appservice_api + "/{}").format(tenant_id, project_id, cluster_id, appservice_id)
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_del(url, request_body=params, headers=headers)
        return resp

    def get_appservice(self, tenant_id, project_id, cluster_id, appservice_id, headers=None, **kwargs):
        """
        Fetches the details of the given App Service.
        In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        -Organization Owner
        -Project Owner
        -Project Manager
        -Project Viewer
        -Database Data Reader/Writer
        -Database Data Reader
        """
        url = (self.cluster_appservice_api + "/{}").format(tenant_id, project_id, cluster_id, appservice_id)
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get(url, params=params, headers=headers)
        return resp

    def get_cluster(self, tenant_id, project_id, cluster_id, headers=None, **kwargs):
        """
        Fetches the details of the given Cluster.
        In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        -Organization Owner
        -Project Owner
        -Project Manager
        -Project Viewer
        -Database Data Reader/Writer
        -Database Data Reader
        """
        url = (self.cluster_endpoint + "/{}").format(tenant_id, project_id, cluster_id)
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_get(url, params=params, headers=headers)
        return resp

    def update_appservices(self, tenant_id, project_id, cluster_id, appservice_id, nodes, cpu, ram, headers=None, **kwargs):
        """
        Updates an existing App Service.
        In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        -Organization Owner
        -Project Owner
        -Project Manager
        :param tenant_id:
        :param project_id:
        :param cluster_id:
        :param appservice_name: Name of the appservice
        :param nodes: Number of nodes in appservices
        :param cpu: Number of cpus in appservices
        :param ram: Size of ram
        :param If-Match: A precondition header that specifies the entity tag of a resource.
        """
        url = (self.cluster_appservice_api + "/{}").format(tenant_id, project_id, cluster_id, appservice_id)
        params = {
            "nodes": nodes,
            "compute": {
                "cpu": cpu,
                "ram": ram
            }
        }
        for k, v in kwargs.items():
            params[k] = v
        resp = self.capella_api_put(url, params, headers=headers)
        return resp

    """
    Switches on the App Service.
    In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
        Organization Owner
        Project Owner
        Project Manager
    :param organizationId (str) Organization ID under which the cluster is present.
    :param projectId (str) Project ID under which the cluster is present.
    :param clusterId (str) Cluster ID of the cluster which has the sample bucket.
    :param appServiceId (str) Name of the App Service which has to be switched on.
    :param headers (dict) Headers to be sent with the API call.
    :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
    """
    def switch_app_service_on(self, organizationId, projectId, clusterId,
                              appServiceId, headers=None, **kwargs):
        self.cluster_ops_API_log.info(
            "Switching on App Service {} in cluster {} in project {} in "
            "organization {}".format(appServiceId, clusterId, projectId,
                                     organizationId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_post(
            self.appservice_on_off_endpoint.format(
                organizationId, projectId, clusterId, appServiceId),
            params, headers)
        return resp

    """
        Switches off the App Service.
        In order to access this endpoint, the provided API key must have at least one of the roles referenced below:
            Organization Owner
            Project Owner
            Project Manager
        :param organizationId (str) Organization ID under which the cluster is present.
        :param projectId (str) Project ID under which the cluster is present.
        :param clusterId (str) Cluster ID of the cluster which has the sample bucket.
        :param appServiceId (str) Name of the App Service which has to be switched off.
        :param headers (dict) Headers to be sent with the API call.
        :param kwargs (dict) Do not use this under normal circumstances. This is only to test negative scenarios.
        """
    def switch_app_service_off(self, organizationId, projectId, clusterId,
                               appServiceId, headers=None, **kwargs):
        self.cluster_ops_API_log.info(
            "Switching off App Service {} in cluster {} in project {} in "
            "organization {}".format(appServiceId, clusterId, projectId,
                                     organizationId))
        if kwargs:
            params = kwargs
        else:
            params = None
        resp = self.capella_api_del(
            self.appservice_on_off_endpoint.format(
                organizationId, projectId, clusterId, appServiceId),
            params, headers)
        return resp

    def start_online_storage_migration(
        self,
        organization_id: str,
        project_id: str,
        cluster_id: str,
        buckets,  # str | list[str],
        headers=None,
    ):
        """Initiate online migration of bucket storage backend.

        Parameters:
            - buckets: A bucket name or list of buckets to migrate.
        """
        if isinstance(buckets, str):
            buckets = [buckets]

        params = {"buckets": buckets}
        resp = self.capella_api_put(
            self.bucket_storage_migration_endpoint.format(
                organization_id, project_id, cluster_id
            ),
            params,
            headers,
        )

        return resp


class CapellaAPI(CommonCapellaAPI):

    def __init__(self, url, secret, access, user, pwd, bearer_token,
                 TOKEN_FOR_INTERNAL_SUPPORT=None):
        """
        Making explicit call to init function of inherited classes because the init params differ.
        """
        super(CapellaAPI, self).__init__(
            url=url, secret=secret, access=access, user=user, pwd=pwd,
            bearer_token=bearer_token,
            TOKEN_FOR_INTERNAL_SUPPORT=TOKEN_FOR_INTERNAL_SUPPORT)
        self.cluster_ops_apis = ClusterOperationsAPIs(
            url, secret, access, bearer_token)
        self.capellaAPI_log = logging.getLogger(__name__)

    def set_logging_level(self, level):
        self.capellaAPI_log.setLevel(level)

    # Cluster methods
    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.list_clusters

    def get_clusters(self, params=None):
        capella_api_response = self.capella_api_get('/v3/clusters', params)
        return (capella_api_response)
    """

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.fetch_cluster_info

    def get_cluster_info(self, cluster_id):
        capella_api_response = self.capella_api_get('/v3/clusters/' + cluster_id)

        return (capella_api_response)
    """

    def get_cluster_status(self, cluster_id):
        capella_api_response = self.capella_api_get(
            '/v3/clusters/' + cluster_id + '/status')

        return (capella_api_response)

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.create_cluster

    def create_cluster(self, cluster_configuration):
        capella_api_response = self.capella_api_post('/v3/clusters', cluster_configuration)

        return (capella_api_response)
    """

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.update_cluster
    def update_cluster_servers(self, cluster_id, new_cluster_server_configuration):
        capella_api_response = self.capella_api_put('/v3/clusters' + '/' + cluster_id + '/servers',
                                                    new_cluster_server_configuration)

        return (capella_api_response)
    """

    def get_cluster_servers(self, cluster_id):
        response_dict = None

        capella_api_response = self.get_cluster_info(True, cluster_id)
        # Did we get the info back ?
        if capella_api_response.status_code == 200:
            # Do we have JSON response ?
            if capella_api_response.headers['content-type'] == 'application/json':
                # Is there anything in it?
                # We use response.text as this is a string
                # response.content is in bytes which we use for json.loads
                if len(capella_api_response.text) > 0:
                    response_dict = capella_api_response.json()['place']

        # return just the servers bit
        return (response_dict)

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.delete_cluster
    def delete_cluster(self, cluster_id):
        capella_api_response = self.capella_api_del('/v3/clusters' + '/' + cluster_id)
        return (capella_api_response)
    """

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.list_database_users
    def get_cluster_users(self, cluster_id):
        capella_api_response = self.capella_api_get('/v3/clusters' + '/' + cluster_id +
                                                    '/users')
        return (capella_api_response)
    """

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.delete_database_user
    def delete_cluster_user(self, cluster_id, cluster_user):
        capella_api_response = self.capella_api_del('/v3/clusters' + '/' + cluster_id +
                                                    '/users/' + cluster_user)
        return (capella_api_response)
    """

    """
    Cluster Certificate
    Method Deprecated
    New Method - CLusterOperationsAPIs.get_cluster_certificate
    def get_cluster_certificate(self, cluster_id):
        capella_api_response = self.capella_api_get(
            '/v3/clusters' + '/' + cluster_id + '/certificate')
        return (capella_api_response)
    """

    # Cluster buckets
    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.list_buckets
    def get_cluster_buckets(self, cluster_id):
        capella_api_response = self.capella_api_get('/v2/clusters' + '/' + cluster_id +
                                                    '/buckets')
        return (capella_api_response)
    """

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.create_bucket
    def create_cluster_bucket(self, cluster_id, bucket_configuration):
        capella_api_response = self.capella_api_post('/v2/clusters' + '/' + cluster_id +
                                                     '/buckets', bucket_configuration)
        return (capella_api_response)
    """

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.update_bucket_config
    def update_cluster_bucket(self, cluster_id, bucket_id, new_bucket_configuration):
        capella_api_response = self.capella_api_put('/v2/clusters' + '/' + cluster_id +
                                                    '/buckets/' + bucket_id, new_bucket_configuration)
        return (capella_api_response)
    """

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.delete_bucket
    def delete_cluster_bucket(self, cluster_id, bucket_configuration):
        capella_api_response = self.capella_api_del('/v2/clusters' + '/' + cluster_id +
                                                    '/buckets', bucket_configuration)
        return (capella_api_response)
    """

    # Cluster Allow lists
    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.list_allowed_CIDRs
    def get_cluster_allowlist(self, cluster_id):
        capella_api_response = self.capella_api_get('/v2/clusters' + '/' + cluster_id +
                                                    '/allowlist')
        return (capella_api_response)
    """

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.delete_allowed_CIDR
    def delete_cluster_allowlist(self, cluster_id, allowlist_configuration):
        capella_api_response = self.capella_api_del('/v2/clusters' + '/' + cluster_id +
                                                    '/allowlist', allowlist_configuration)
        return (capella_api_response)
    """

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.add_CIDR_to_allowed_CIDRs_list
    def create_cluster_allowlist(self, cluster_id, allowlist_configuration):
        capella_api_response = self.capella_api_post('/v2/clusters' + '/' + cluster_id +
                                                     '/allowlist', allowlist_configuration)
        return (capella_api_response)
    """

    def update_cluster_allowlist(
            self,
            cluster_id,
            new_allowlist_configuration):
        capella_api_response = self.capella_api_put(
            '/v2/clusters' + '/' + cluster_id + '/allowlist',
            new_allowlist_configuration)
        return (capella_api_response)

    # Cluster user
    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.create_database_user
    def create_cluster_user(self, cluster_id, cluster_user_configuration):
        capella_api_response = self.capella_api_post('/v3/clusters' + '/' + cluster_id +
                                                     '/users', cluster_user_configuration)
        return (capella_api_response)
    """

    # Capella Users
    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.list_database_users
    def get_users(self):
        capella_api_response = self.capella_api_get('/v2/users?perPage=' + str(self.perPage))
        return (capella_api_response)
    """

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.create_bucket
    def create_bucket(self, tenant_id, project_id, cluster_id,
                      bucket_params):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        url = '{}/buckets'.format(url)
        default = {"name": "default", "bucketConflictResolution": "seqno",
                   "memoryAllocationInMb": 100, "flush": False, "replicas": 0,
                   "durabilityLevel": "none", "timeToLive": None}
        default.update(bucket_params)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(default))
        return resp
    """

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.list_buckets
    def get_buckets(self, tenant_id, project_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        url = '{}/buckets'.format(url)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp
    """

    def flush_bucket(self, tenant_id, project_id, cluster_id, bucket_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        url = url + "/buckets/" + bucket_id + "/flush"
        resp = self.do_internal_request(url, method="POST")
        return resp

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.delete_bucket
    def delete_bucket(self, tenant_id, project_id, cluster_id,
                      bucket_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        url = '{}/buckets/{}'.format(url, bucket_id)
        resp = self.do_internal_request(url, method="DELETE")
        return resp
    """

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.update_bucket_config
    def update_bucket_settings(self, tenant_id, project_id, cluster_id,
                               bucket_id, bucket_params):
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/buckets/{}" \
            .format(self.internal_url, tenant_id, project_id,
                    cluster_id, bucket_id)
        resp = self.do_internal_request(url, method="PUT", params=json.dumps(bucket_params))
        return resp
    """

    def jobs(self, project_id, tenant_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        url = '{}/jobs'.format(url)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.fetch_cluster_info
    def get_cluster_internal(self, tenant_id, project_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)

        resp = self.do_internal_request(url, method="GET",
                                        params='')
        return resp
    """

    def get_nodes(self, tenant_id, project_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        url = '{}/nodes'.format(url)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.list_database_users
    def get_db_users(self, tenant_id, project_id, cluster_id,
                     page=1, limit=100):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        url = url + '/users?page=%s&perPage=%s' % (page, limit)
        resp = self.do_internal_request(url, method="GET")
        return resp
    """

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.delete_database_user
    def delete_db_user(self, tenant_id, project_id, cluster_id, user_id):
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/users/{}" \
            .format(self.internal_url, tenant_id, project_id, cluster_id,
                    user_id)
        resp = self.do_internal_request(url, method="DELETE",
                                        params='')
        return resp
    """

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.create_database_user
    def create_db_user(self, tenant_id, project_id, cluster_id,
                       user, pwd):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        body = {"name": user, "password": pwd,
                "permissions": {"data_reader": {}, "data_writer": {}}}
        url = '{}/users'.format(url)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(body))
        return resp
    """

    def allow_my_ip(self, tenant_id, project_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self._urllib_request("https://ifconfig.me", method="GET")
        if resp.status_code != 200:
            raise Exception("Fetch public IP failed!")
        body = {"create": [{"cidr": "{}/32".format(resp.content.decode()),
                            "comment": ""}]}
        url = '{}/allowlists-bulk'.format(url)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(body))
        return resp

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.add_CIDR_to_allowed_CIDRs_list
    def add_allowed_ips(self, tenant_id, project_id, cluster_id, ips):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        body = {
            "create": [
                {"cidr": "{}/32".format(ip), "comment": ""} for ip in ips
            ]
        }
        url = '{}/allowlists-bulk'.format(url)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(body))
        return resp
    """

    def load_sample_bucket(self, tenant_id, project_id, cluster_id,
                           bucket_name):
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/buckets/samples" \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        param = {'name': bucket_name}
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(param))
        return resp

    def create_cluster_customAMI(self, tenant_id, config):
        '''
        #Sample Config
        config = {"cidr": "10.0.64.0/20",
          "name": "a_customAMI",
          "description": "",
          "overRide": {"token": "TOKEN_FOR_INTERNAL_SUPPORT",
                       "image": "couchbase-cloud-server-7.2.0-qe",
                       "server": "7.1.0"},
          "projectId": "e51ce483-d067-4d4e-9a66-d0583b9d543e",
          "provider": "hostedAWS",
          "region": "us-east-1",
          "singleAZ": False, "server": None,
          "specs": [
              {"count": 3,
               "services": [{"type": "fts"}, {"type": "index"}, {"type": "kv"}, {"type": "n1ql"}],
               "compute": {"type": "r5.2xlarge", "cpu": 0, "memoryInGb": 0},
               "disk": {"type": "gp3", "sizeInGb": 50, "iops": 3000}}],
          "package": "enterprise"
          }
          '''
        url = '{}/v2/organizations/{}/clusters/deploy'.format(
            self.internal_url, tenant_id)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(config))
        return resp

    def get_deployment_options(self, tenant_id):
        """
        Get deployment options, including a suggested CIDR for deploying a
        cluster.

        Example use:

        ```
        resp = client.get_deployment_options(tenant_id)
        suggestedCidr = resp.json().get('suggestedCidr')
        ```
        """
        url = '{}/v2/organizations/{}/clusters/deployment-options' \
            .format(self.internal_url, tenant_id)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def create_eventing_function(
            self,
            cluster_id,
            name,
            body,
            function_scope=None):
        url = '{}/v2/databases/{}/proxy/_p/event/api/v1/functions/{}'.format(
            self.internal_url, cluster_id, name)

        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])

        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(body))
        return resp

    def __set_eventing_function_settings(
            self, cluster_id, name, body, function_scope=None):
        url = '{}/v2/databases/{}/proxy/_p/event/api/v1/functions/{}/settings'.format(
            self.internal_url, cluster_id, name)

        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])

        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(body))
        return resp

    def pause_eventing_function(self, cluster_id, name, function_scope=None):
        body = {
            "processing_status": False,
            "deployment_status": True,
        }
        return self.__set_eventing_function_settings(
            cluster_id, name, body, function_scope)

    def resume_eventing_function(self, cluster_id, name, function_scope=None):
        body = {
            "processing_status": True,
            "deployment_status": True,
        }
        return self.__set_eventing_function_settings(
            cluster_id, name, body, function_scope)

    def deploy_eventing_function(self, cluster_id, name, function_scope=None):
        body = {
            "deployment_status": True,
            "processing_status": True,
        }
        return self.__set_eventing_function_settings(
            cluster_id, name, body, function_scope)

    def undeploy_eventing_function(
            self,
            cluster_id,
            name,
            function_scope=None):
        body = {
            "deployment_status": False,
            "processing_status": False
        }
        return self.__set_eventing_function_settings(
            cluster_id, name, body, function_scope)

    def get_composite_eventing_status(self, cluster_id):
        url = '{}/v2/databases/{}/proxy/_p/event/api/v1/status'.format(
            self.internal_url, cluster_id)

        resp = self.do_internal_request(url, method="GET")
        return resp

    def get_all_eventing_stats(self, cluster_id, seqs_processed=False):
        url = '{}/v2/databases/{}/proxy/_p/event/api/v1/stats'.format(
            self.internal_url, cluster_id)

        if seqs_processed:
            url += "?type=full"

        resp = self.do_internal_request(url, method="GET")
        return resp

    def delete_eventing_function(self, cluster_id, name, function_scope=None):
        url = '{}/v2/databases/{}/proxy/_p/event/deleteAppTempStore/?name={}'.format(
            self.internal_url, cluster_id, name)

        if function_scope is not None:
            url += "&bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        resp = self.do_internal_request(url, method="GET")
        return resp

    def create_private_network(self, tenant_id, project_id, cluster_id,
                               private_network_params):
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/virtualnetworks" \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(
            url, method="POST", params=json.dumps(private_network_params))
        return resp

    def get_private_network(self, tenant_id, project_id, cluster_id,
                            private_network_id):
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/virtualnetworks/{}" .format(
            self.internal_url, tenant_id, project_id, cluster_id, private_network_id)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def update_specs(self, tenant_id, project_id, cluster_id, specs):
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/specs" \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(specs))
        return resp

    def restore_from_backup(
            self,
            tenant_id,
            project_id,
            cluster_id,
            bucket_name):
        """
        method used to restore from the backup
        :param tenant_id:
        :param project_id:
        :param cluster_id:
        :param bucket_name:
        :return: response object
        """
        payload = {
            "sourceClusterId": cluster_id,
            "targetClusterId": cluster_id,
            "options": {
                "services": [
                    "data",
                    "query",
                    "index",
                    "search"],
                "filterKeys": "",
                "filterValues": "",
                "mapData": "",
                "includeData": "",
                "excludeData": "",
                "autoCreateBuckets": True,
                "autoRemoveCollections": True,
                "forceUpdates": True}}
        bucket_id = self.get_backups_bucket_id(
            tenant_id=tenant_id,
            project_id=project_id,
            cluster_id=cluster_id,
            bucket_name=bucket_name)
        url = r"{}/v2/organizations/{}/projects/{}/clusters/{}/buckets/{}/restore" \
            .format(self.internal_url, tenant_id, project_id, cluster_id, bucket_id)
        resp = self.do_internal_request(
            url, method="POST", params=json.dumps(payload))
        return resp

    def get_cluster_id(self, cluster_name):
        return self._get_meta_data(cluster_name=cluster_name)['id']

    def get_bucket_id(self, cluster_name, project_name, bucket_name):
        tenant_id, project_id, cluster_id = self.get_tenant_id(), self.get_project_id(
            project_name), self.get_cluster_id(cluster_name=cluster_name)
        resp = self.get_buckets(tenant_id, project_id, cluster_id)
        if resp.status_code != 200:
            raise Exception("Response when trying to fetch buckets.")
        buckets = json.loads(resp.content)['buckets']['data']
        for bucket in buckets:
            if bucket['data']['name'] == bucket_name:
                return bucket['data']['id']

    def get_tenant_id(self):
        return json.loads(self.get_clusters().content)['data']['tenantId']

    def get_project_id(self, cluster_name):
        return self._get_meta_data(cluster_name=cluster_name)['projectId']

    def _get_meta_data(self, cluster_name):
        all_clusters = json.loads(self.get_clusters().content)['data']
        for cluster in all_clusters['items']:
            if cluster['name'] == cluster_name:
                return cluster

    def get_restores(self, tenant_id, project_id, cluster_id, bucket_name):
        """
        method used to obtain list of restores of a given bucket.
        :param tenant_id:
        :param project_id:
        :param cluster_id:
        :param bucket_name:
        :return: response object
        """

        bucket_id = base64.urlsafe_b64encode(bucket_name.encode()).decode()

        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/buckets/{}/restore".format(
            self.internal_url, tenant_id, project_id, cluster_id, bucket_id)

        resp = self.do_internal_request(url, method="GET")
        return resp

    def get_backups(self, tenant_id, project_id, cluster_id):
        """
        method to obtain a list of the current backups from backups tab
        :param tenant_id:
        :param project_id:
        :param cluster_id:
        :return: response object
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/backups".format(
            self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def get_backups_bucket_id(
            self,
            tenant_id,
            project_id,
            cluster_id,
            bucket_name):
        """
        method to obtain a list of the current backups from backups tab
        :param tenant_id:
        :param project_id:
        :param cluster_id:
        :param bucket_name:
        :return: response object
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/backups".format(
            self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="GET").content
        for bucket in json.loads(resp)['data']:
            if bucket['data']['bucket'] == bucket_name:
                return bucket['data']['bucketId']

    def backup_now(self, tenant_id, project_id, cluster_id, bucket_name):
        """
        method to trigger an on-demand backup
        :param tenant_id:
        :param project_id:
        :param cluster_id:
        :param bucket_name:
        :return: response object
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/backup".format(
            self.internal_url, tenant_id, project_id, cluster_id)
        payload = {"bucket": bucket_name}
        resp = self.do_internal_request(
            url, method="POST", params=json.dumps(payload))
        return resp

    def list_all_bucket_backups(
            self,
            tenant_id,
            project_id,
            cluster_id,
            bucket_id):
        """
        method to obtain the list of backups of a bucket
        :param tenant_id:
        :param project_id:
        :param cluster_id:
        :param bucket_id:
        :return: response object
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/buckets/{}/backups" \
            .format(self.internal_url, tenant_id, project_id, cluster_id, bucket_id)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def begin_export(self, tenant_id, project_id, cluster_id, backup_id):
        """
        method to begin an export
        :param tenant_id:
        :param project_id:
        :param cluster_id:
        :param backup_id:
        :return: response object
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/backups/{}/export" \
            .format(self.internal_url, tenant_id, project_id, cluster_id, backup_id)
        resp = self.do_internal_request(url, method="POST")
        return resp

    def export_status(self, tenant_id, project_id, cluster_id, bucket_id):
        """
        method to query what exports are queued, executing and finished
        :param tenant_id:
        :param project_id:
        :param cluster_id:
        :param bucket_id:
        :return: response object
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/buckets/{}/exports?page=1&perPage=25" \
            .format(self.internal_url, tenant_id, project_id, cluster_id, bucket_id)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def generate_export_link(
            self,
            tenant_id,
            project_id,
            cluster_id,
            export_id):
        """
        method to generate a pre-signed link for the given export
        :param tenant_id:
        :param project_id:
        :param cluster_id:
        :param export_id:
        :return: response object
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/exports/{}/link" \
            .format(self.internal_url, tenant_id, project_id, cluster_id, export_id)
        resp = self.do_internal_request(url, method="POST")
        return resp

    def invite_new_user(self, tenant_id, email, password):
        "Invite a new user to the tenant"
        config = {
            "email": email,
            "name": email,
            "password": password,
            "roles": ["organizationOwner"]
        }
        url = "{}/v2/organizations/{}/users" \
            .format(self.internal_url, tenant_id)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(config))
        return resp

    def invite_new_user_with_config(self, tenant_id, config):
        "Invite a new user to the tenant, with the config already provided"
        url = "{}/v2/organizations/{}/users" \
            .format(self.internal_url, tenant_id)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(config))
        return resp


    def verify_email(self, token):
        """
        Verify an email invitation.

        Example use:

        ```
        token = "email-verify-token"
        resp = client.verify_email(token)
        jwt = resp.json()["jwt"]
        ```
        """
        url = "{}/emails/verify/{}".format(self.internal_url, token)
        resp = self.do_internal_request(url, method="POST")
        return resp

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.delete_user
    def remove_user(self, tenant_id, user_id):
        url = "{}/tenants/{}/users/{}".format(self.internal_url, tenant_id, user_id)
        resp = self.do_internal_request(url, method="DELETE")
        return resp
    """

    def create_xdcr_replication(
            self,
            tenant_id,
            project_id,
            cluster_id,
            payload):
        """
        Create a new XDCR replication

        Sample payload:
        {
            "direction": "one-way",
            "sourceBucket": "YnVja2V0LTE=",
            "target": {
                "cluster": "21a51ea3-4fc6-42ee-90f3-d26334fc3ace",
                "bucket":"YnVja2V0LTE=",
                "scopes": [
                    {
                        "source": "scope-1",
                        "target": "target-scope-1",
                        "collections": [
                            {
                                "source": "coll-1",
                                "target": "target-coll-1"
                            }
                        ]
                    }
                ]
            },
            "settings": {
                "filterExpression": "REGEXP_CONTAINS(country, \"France\")",
                "priority": "medium",
                "networkUsageLimit": 500
            }
        }
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/xdcr" \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(payload))
        return resp

    def list_cluster_replications(self, tenant_id, project_id, cluster_id):
        """
        Get all XDCR replications for a cluster
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/xdcr" \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def get_replication(
            self,
            tenant_id,
            project_id,
            cluster_id,
            replication_id):
        """
        Get a specific XDCR replication for a cluster
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/xdcr/{}" .format(
            self.internal_url, tenant_id, project_id, cluster_id, replication_id)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def delete_replication(
            self,
            tenant_id,
            project_id,
            cluster_id,
            replication_id):
        """
        Delete an XDCR replication
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/xdcr/{}" .format(
            self.internal_url, tenant_id, project_id, cluster_id, replication_id)
        resp = self.do_internal_request(url, method="DELETE")
        return resp

    def pause_replication(
            self,
            tenant_id,
            project_id,
            cluster_id,
            replication_id):
        """
        Pause an XDCR replication
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/xdcr/{}/pause" .format(
            self.internal_url, tenant_id, project_id, cluster_id, replication_id)
        resp = self.do_internal_request(url, method="POST")
        return resp

    def start_replication(
            self,
            tenant_id,
            project_id,
            cluster_id,
            replication_id):
        """
        Start an XDCR replication
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/xdcr/{}/start" .format(
            self.internal_url, tenant_id, project_id, cluster_id, replication_id)
        resp = self.do_internal_request(url, method="POST")
        return resp

    def create_sgw_backend(self, tenant_id, config):
        """
        Create a SyncGateway backend (app services) for a cluster

        Sample config:
        {
            "clusterId": "a2b3dfbb-6e88-4309-a4c1-ea3184d95321",
            "name": "my-sync-gateway-backend",
            "description": "sgw backend that drives my amazing app",
            "SyncGatewaySpecs": {
                "desired_capacity": 1,
                "compute": {
                    "type": "c5.large",
                    "cpu": 2
                    "memoryInGb": 4
                }
            }
        }
        """
        url = '{}/v2/organizations/{}/backends'.format(
            self.internal_url, tenant_id)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(config))
        return resp

    def get_sgw_backend(self, tenant_id, project_id, cluster_id, backend_id):
        """
        Get details about a SyncGateway backend for a cluster
        """
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}' .format(
            self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def delete_sgw_backend(
            self,
            tenant_id,
            project_id,
            cluster_id,
            backend_id):
        """
        Delete a SyncGateway backend
        """
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}' .format(
            self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self.do_internal_request(url, method="DELETE")
        return resp

    def create_sgw_database(
            self,
            tenant_id,
            project_id,
            cluster_id,
            backend_id,
            config):
        """
        Create a SyncGateway database (app endpoint)

        Sample config:
        {
            "name": "sgw-1",
            "sync": "",
            "bucket": "bucket-1",
            "delta_sync": false,
            "import_filter": ""
        }
        """
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases' \
            .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(config))
        return resp

    def get_sgw_databases(self, tenant_id, project_id, cluster_id, backend_id):
        "Get a list of all available sgw databases (app endpoints)"
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases' \
            .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def resume_sgw_database(
            self,
            tenant_id,
            project_id,
            cluster_id,
            backend_id,
            db_name):
        "Resume the sgw database (app endpoint)"
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/online' \
            .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="POST")
        return resp

    def pause_sgw_database(
            self,
            tenant_id,
            project_id,
            cluster_id,
            backend_id,
            db_name):
        "Resume the sgw database (app endpoint)"
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/offline' \
            .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="POST")
        return resp

    def allow_my_ip_sgw(self, tenant_id, project_id, cluster_id, backend_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/allowip' \
            .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self._urllib_request("https://ifconfig.me", method="GET")
        if resp.status_code != 200:
            raise Exception("Fetch public IP failed!")
        body = {"cidr": "{}/32".format(resp.content.decode()), "comment": ""}
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(body))
        return resp

    def add_allowed_ip_sgw(
            self,
            tenant_id,
            project_id,
            cluster_id,
            backend_id,
            ip):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/allowip' \
            .format(self.internal_url, tenant_id, project_id, backend_id, cluster_id)
        body = {"cidr": "{}/32".format(ip), "comment": ""}
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(body))
        return resp

    def update_sync_function_sgw(
            self,
            tenant_id,
            project_id,
            cluster_id,
            backend_id,
            db_name,
            config):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/sync' \
            .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(config))
        return resp

    def add_app_role_sgw(
            self,
            tenant_id,
            project_id,
            cluster_id,
            backend_id,
            db_name,
            config):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/roles' \
            .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(config))
        return resp

    def add_user_sgw(
            self,
            tenant_id,
            project_id,
            cluster_id,
            backend_id,
            db_name,
            config):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/users' \
            .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(config))
        return resp

    def add_admin_user_sgw(
            self,
            tenant_id,
            project_id,
            cluster_id,
            backend_id,
            db_name,
            config):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/adminusers' \
            .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(config))
        return resp

    def get_sgw_links(
            self,
            tenant_id,
            project_id,
            cluster_id,
            backend_id,
            db_name):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/connect' \
            .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    def get_sgw_info(self, tenant_id, project_id, cluster_id, backend_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}' .format(
            self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    def get_sgw_certificate(
            self,
            tenant_id,
            project_id,
            cluster_id,
            backend_id,
            db_name):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/publiccert' \
            .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    def get_node_metrics(
            self,
            tenant_id,
            project_id,
            cluster_id,
            metrics,
            step,
            start,
            end):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/metrics/{}/query_range' \
            .format(self.internal_url, tenant_id, project_id, cluster_id, metrics)
        payload = {'step': step, 'start': start, 'end': end}
        resp = self.do_internal_request(url, method="GET", params=payload)
        return resp

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.create_project
    def create_project(self, tenant_id, name):
        project_details = {"name": name, "tenantId": tenant_id}

        url = '{}/v2/organizations/{}/projects'.format(self.internal_url, tenant_id)
        capella_api_response = self.do_internal_request(url, method="POST",
                                                        params=json.dumps(project_details))
        return capella_api_response
    """

    """
    Method Deprecated.
    New Method - ClusterOperationsAPIs.delete_project
    def delete_project(self, tenant_id, project_id):
        url = '{}/v2/organizations/{}/projects/{}'.format(self.internal_url, tenant_id,
                                                          project_id)
        capella_api_response = self.do_internal_request(url, method="DELETE",
                                                        params='')
        return capella_api_response
    """

    def turn_off_cluster(self, tenant_id, project_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/off' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="POST", params='')
        return resp

    def turn_on_cluster(self, tenant_id, project_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/on' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        payload = "{\"turnOnAppService\":true}"
        resp = self.do_internal_request(
            url, method="POST", params=json.dumps(payload))
        return resp
