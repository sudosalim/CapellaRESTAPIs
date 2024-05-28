# -*- coding: utf-8 -*-
# Generic/Built-in
import base64
import logging

import json

from ..common.CapellaAPI_v4 import OrganizationOperationsAPIs


class ColumnarAPIs(OrganizationOperationsAPIs):

    def __init__(self, url, secret, access, bearer_token):
        super(ColumnarAPIs, self).__init__(url, secret, access, bearer_token)
        self.columnar_ops_API_log = logging.getLogger(__name__)
        self.analytics_clusters_endpoint = self.project_endpoint + "/{}/analyticsClusters"

    def turn_analytics_cluster_off(
            self,
            organizationId,
            projectId,
            instanceId,
            headers=None,
            **kwargs):
        """
        Switches the columnar instance state to OFF.

        Args:
            organizationId: The tenantID in which the instance is present. (UUID)
            projectId: The ID of the Project in which the instance is present. (UUID)
            instanceId: The ID of the Instance. (UUID)
            headers: Headers to be sent with the API call. (dict)
            **kwargs: Do not use this under normal circumstances. This is only to test negative scenarios. (dict)

        Returns:
            Success : Status Code ONLY
            Error : message, hint, code, HttpStatusCode
        """
        self.columnar_ops_API_log.info(
            "Turning off the columnar instance {}, inside project {}, inside "
            "tenant {}".format(instanceId, projectId, organizationId))

        if kwargs:
            params = kwargs
        else:
            params = None

        resp = (self.capella_api_del("{}/{}".format(
            self.analytics_clusters_endpoint.format(
                organizationId, projectId), instanceId), params, headers))
        return resp

    def turn_analytics_cluster_on(
            self,
            organizationId,
            projectId,
            instanceId,
            headers=None,
            **kwargs):
        """
        Switches the columnar instance state to ON.

        Args:
            organizationId: The tenantID in which the instance is present. (UUID)
            projectId: The ID of the Project in which the instance is present. (UUID)
            instanceId: The ID of the Instance. (UUID)
            headers: Headers to be sent with the API call. (dict)
            **kwargs: Do not use this under normal circumstances. This is only to test negative scenarios. (dict)

        Returns:
            Success : Status Code ONLY
            Error : message, hint, code, HttpStatusCode
        """
        self.columnar_ops_API_log.info(
            "Turning on the columnar instance {}, inside project {}, inside "
            "tenant {}".format(instanceId, projectId, organizationId))

        if kwargs:
            params = kwargs
        else:
            params = None

        resp = (self.capella_api_post("{}/{}".format(
            self.analytics_clusters_endpoint.format(
                organizationId, projectId), instanceId), params, headers))
        return resp

    def create_analytics_cluster(
            self,
            organizationId,
            projectId,
            name,
            cloudProvider,
            region,
            nodes,
            description="",
            headers=None,
            **kwargs):
        """
        Creates a columnar instance inside a Project

        Args:
            organizationId: The tenantID in which the instance is present. (UUID)
            projectId: The ID of the Project in which the instance is present. (UUID)
            name: Name of the instance. (str)
            cloudProvider: Keyword. (str)
            region: keyword, based on cloudProvider. (str)
            nodes: Number of nodes to be allotted to the instance. (int)
            description: Description of the columnar instance. (str)
            headers: Headers to be sent with the API call. (dict)
            **kwargs: Do not use this under normal circumstances. This is only to test negative scenarios. (dict)

        Returns:
            Success : Status Code ONLY
            Error : message, hint, code, HttpStatusCode
        """
        self.columnar_ops_API_log.info(
            "Creating an columnar instance inside project {}, inside tenant "
            "{}.".format(projectId, organizationId))

        params = {
            "name": name,
            "cloudProvider": cloudProvider,
            "region": region,
            "nodes": nodes
        }
        if description is not None:
            params["description"] = description
        for k, v in kwargs:
            params[k] = v

        resp = self.capella_api_post(
            self.analytics_clusters_endpoint.format(organizationId, projectId),
            params, headers)
        return resp

    def delete_analytics_cluster(
            self,
            organizationId,
            projectId,
            instanceId,
            headers=None,
            **kwargs):
        """
        Deletes the instance by the given ID.

        Args:
            organizationId: The tenantID in which the instance is present. (UUID)
            projectId: The ID of the Project in which the instance is present. (UUID)
            instanceId: The ID of the Instance. (UUID)
            headers: Headers to be sent with the API call. (dict)
            **kwargs: Do not use this under normal circumstances. This is only to test negative scenarios. (dict)

        Returns:
            Success : Status Code ONLY
            Error : message, hint, code, HttpStatusCode
        """
        self.columnar_ops_API_log.info(
            "Deleting the columnar instance {}, inside project {}, inside "
            "tenant {}.".format(instanceId, projectId, organizationId))

        if kwargs:
            params = kwargs
        else:
            params = None

        resp = self.capella_api_del("{}/{}".format(
            self.analytics_clusters_endpoint.format(organizationId, projectId),
            instanceId), params, headers)
        return resp

    def fetch_analytics_cluster_info(
            self,
            organizationId,
            projectId,
            instanceId,
            headers=None,
            **kwargs):
        """
        Fetches configuration / info of the instance given its ID.

        Args:
            organizationId: The tenantID in which the instance is present. (UUID)
            projectId: The ID of the Project in which the instance is present. (UUID)
            instanceId: The ID of the Instance. (UUID)
            headers: Headers to be sent with the API call. (dict)
            **kwargs: Do not use this under normal circumstances. This is only to test negative scenarios. (dict)

        Returns:
            Success : Status Code, dict
            Error : message, hint, code, HttpStatusCode
        """
        self.columnar_ops_API_log.info(
            "Fetching the info for columnar instance {}, inside project {}, "
            "inside tenant {}.".format(instanceId, projectId, organizationId))

        if kwargs:
            params = kwargs
        else:
            params = None

        resp = self.capella_api_get("{}/{}".format(
            self.analytics_clusters_endpoint.format(organizationId, projectId),
            instanceId), params, headers)
        return resp

    def list_organization_level_analytics_clusters(
            self,
            organizationId,
            page=None,
            perPage=None,
            sortBy=None,
            sortDirection=None,
            headers=None,
            **kwargs):
        """
        Lists all the columnar instances inside the specified tenant.

        Args:
            organizationId: The tenantID in which the instances are present. (UUID)
            headers: Headers to be sent with the API call. (dict)
            **kwargs: Do not use this under normal circumstances. This is only to test negative scenarios. (dict)
            page: Sets what page you would like to view. (int)
            perPage: Sets how many results you would like to have on each page. (int)
            sortBy: Sets order of how you would like to sort results and also the key you would like to order by ([string])
                Example: sortBy=name
            sortDirection: The order on which the items will be sorted. (str)
                Accepted Values - asc / desc

        Returns:
            Success : Status Code, (list)
            Error : message, hint, code, HttpStatusCode
        """
        self.columnar_ops_API_log.info(
            "Listing all columnar instances inside tenant {}"
            .format(organizationId))

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
            self.organization_endpoint + "/{}/analyticsClusters"
            .format(organizationId), params, headers)
        return resp

    def list_project_level_analytics_clusters(
            self,
            organizationId,
            projectId,
            page=None,
            perPage=None,
            sortBy=None,
            sortDirection=None,
            headers=None,
            **kwargs):
        """
        Lists all the columnar instances inside a specified project under a tenant
        Args:
            organizationId: The tenantID in which the project is present. (UUID)
            projectId: The ID of the Project in which the instances are present. (UUID)
            page: Sets what page you would like to view. (int)
            perPage: Sets how many results you would like to have on each page. (int)
            sortBy: Sets order of how you would like to sort results and also the key you would like to order by ([string])
                Example: sortBy=name
            sortDirection: The order on which the items will be sorted. (str)
                Accepted Values - asc / desc
            headers: Headers to be sent with the API call. (dict)
            **kwargs: Do not use this under normal circumstances. This is only to test negative scenarios. (dict)

        Returns:
            Success : Status Code, (list)
            Error : message, hint, code, HttpStatusCode
        """
        self.columnar_ops_API_log.info(
            "Listing all columnar instances inside project {}, "
            "inside tenant {}.".format(projectId, organizationId))

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
            self.analytics_clusters_endpoint.format(organizationId, projectId),
            params, headers)
        return resp

    def update_analytics_cluster(
            self,
            organizationId,
            projectId,
            instanceId,
            name,
            nodes,
            description="",
            headers=None,
            **kwargs):
        """
        Updates the specifications of a columnar instance based on Payload Params.

        Args:
            organizationId: The tenantID in which the instance is present. (UUID)
            projectId: The ID of the Project in which the instance is present. (UUID)
            instanceId: The ID of the Instance. (UUID)
            name: New name of the instance
            nodes: New number of nodes to be allotted to the instance. (int)
            description: New description of the columnar instance. (str)
            headers: Headers to be sent with the API call. (dict)
            **kwargs: Do not use this under normal circumstances. This is only to test negative scenarios. (dict)

        Returns:
            Success : Status Code
            Error : message, hint, code, HttpStatusCode
        """
        self.columnar_ops_API_log.info(
            "Updating the columnar instance {}, inside project {}, inside "
            "tenant {}".format(instanceId, projectId, organizationId))

        params = {
            "name": name,
            "nodes": nodes,
        }
        if description is not None:
            params["description"] = description
        for k, v in kwargs.items():
            params[k] = v

        resp = self.capella_api_put("{}/{}".format(
            self.analytics_clusters_endpoint.format(organizationId, projectId),
            instanceId), params, headers)
        return resp
