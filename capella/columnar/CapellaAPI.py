import json

from ..common.CapellaAPI import CommonCapellaAPI


class CapellaAPI(CommonCapellaAPI):
    """
        Class to support Capella Columnar endpoints and operations.

        Parameters:
            url (str): The base URL of the Columnar API.
            secret (str): The secret key for authentication.
            access (str): The access key for authentication.
            user (str): The username for authentication.
            pwd (str): The password for authentication.
            TOKEN_FOR_INTERNAL_SUPPORT (str, optional): An optional token
            for internal support.
    """

    def get_columnar_services(self):
        """
            Retrieve a list of all Columnar services.
        """
        url = "{}/internal/support/serverlessanalytics/service".format(
            self.internal_url)
        resp = self.do_internal_request(url, "GET")
        return resp

    def get_specific_columnar_service(self, serviceId):
        """
            Retrieve a specific Columnar service.

            Parameters:
                serviceId : ID of the service whose service needs to be retrieved
        """
        url = '{}/internal/support/serverlessanalytics/service/{}'.format(
            self.internal_url, serviceId)
        resp = self.do_internal_request(url, "GET")
        return resp

    def create_columnar_instance(self, tenant_id, project_id, name,
                                 description, provider, region, nodes,
                                 instance_types, support_package, availability_zone,
                                 **kwargs):
        """
            Create a new Columnar instance within a specified project.

            Parameters:
                tenant_id (str): The ID of the tenant under which the instance will be created.
                project_id (str): The ID of the project where the instance will be added.
                name (str): The name of the Columnar instance.
                description (str): A description for the Columnar instance.
                provider (str): The provider or cloud platform for the instance (e.g., AWS, Azure).
                region (str): The region or location where the instance will be deployed.
                nodes (int): The number of nodes to allocate for the instance.
                instance_types (dict): Instance type to deploy the columnar instance with.
                support_package (dict): The support package selected for columnar instance.
                availability_zone (string): single vs multiple availability zones.
                **kwargs: Additional keyword arguments to pass to the API request.
        """
        url = "{}/v2/organizations/{}/projects/{}/instance".format(
            self.internal_url, tenant_id, project_id)
        body = {
            "name": name,
            "description": description,
            "provider": provider,
            "region": region,
            "nodes": nodes,
            "instanceTypes": instance_types,
            "supportPackage": support_package,
            "availabilityZone": availability_zone
        }
        for key, value in kwargs.items():
            body[key] = value

        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(body))
        return resp

    def get_columnar_instances(self, tenant_id, project_id, page=1,
                               perPage=100):
        """
            Retrieve a list of Columnar instances within a specified project.

            Parameters:
                tenant_id (str): The ID of the tenant associated with the project.
                project_id (str): The ID of the project for which instances will be retrieved.
                page (int): The page number to retrieve. Default is 1.
                perPage (int): Number of results to return per page. Default is 100.

        """
        url = "{}/v2/organizations/{}/projects/{}/instance?page={}&perPage=" \
              "{}".format(
            self.internal_url, tenant_id, project_id, page, perPage)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def get_specific_columnar_instance(self, tenant_id, project_id, instance_id):
        """
            Retrieve information about a specific Columnar instance within a project.

            Parameters:
                tenant_id (str): The ID of the tenant associated with the project.
                project_id (str): The ID of the project where the instance is located.
                instance_id (str): The ID of the Columnar instance to retrieve information for.
        """
        url = "{}/v2/organizations/{}/projects/{}/instance/{}".format(
            self.internal_url, tenant_id, project_id, instance_id)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def delete_columnar_instance(self, tenant_id, project_id, instance_id):
        """
            Delete a specific Columnar instance within a project.

            Parameters:
                tenant_id (str): The ID of the tenant associated with the project.
                project_id (str): The ID of the project where the instance is located.
                instance_id (str): The ID of the Columnar instance to delete.
        """
        url = "{}/v2/organizations/{}/projects/{}/instance/{}".format(
            self.internal_url, tenant_id, project_id, instance_id)
        resp = self.do_internal_request(url, method="DELETE")
        return resp

    def update_columnar_instance(self, tenant_id, project_id, instance_id, name, description, nodes, **kwargs):
        """
            Modify a specific Columnar instance within a project.

            Parameters:
                tenant_id (str): The ID of the tenant associated with the project.
                project_id (str): The ID of the project where the instance is located.
                instance_id (str): The ID of the Columnar instance to delete.
                name (str): The name of the Columnar instance.
                description (str): A description for the Columnar instance.
                nodes (int): The number of nodes to allocate for the instance.
                **kwargs: Additional keyword arguments to pass to the API request.
        """
        url = "{}/v2/organizations/{}/projects/{}/instance/{}".format(
            self.internal_url, tenant_id, project_id, instance_id)
        body = {
            "name": name,
            "description": description,
            "nodes": nodes
        }
        for key, value in kwargs.items():
            body[key] = value

        resp = self.do_internal_request(url, method="PATCH", params=json.dumps(body))
        return resp

    def create_instance_api_keys(self, tenant_id, project_id, instance_id,
                                 payload):

        url = "{}/v2/organizations/{}/projects/{}/instance/{}/apikeys".format(
            self.internal_url, tenant_id, project_id, instance_id)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(payload))
        return resp

    def create_columnar_role(self, tenant_id, project_id, instance_id,
                             payload):
        url = "{}/v2/organizations/{}/projects/{}/instance/{}/roles".format(
            self.internal_url, tenant_id, project_id, instance_id)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(payload))
        return resp

    def delete_columnar_role(self, tenant_id, project_id, instance_id,
                             role_id):
        url = "{}/v2/organizations/{}/projects/{}/instance/{}/roles/{}".format(
            self.internal_url, tenant_id, project_id, instance_id, role_id)
        resp = self.do_internal_request(
            url, method="DELETE")
        return resp

    def create_api_keys(self, tenant_id, project_id, instance_id):
        """
            Create a Columnar apikey

            Parameters:
                tenant_id (str): The ID of the tenant associated with the project.
                project_id (str): The ID of the project where the instance is located.
                instance_id (str): The ID of the Columnar instance to create keys for.
        """
        url = "{}/v2/organizations/{}/projects/{}/instance/{}/apikeys".format(
            self.internal_url, tenant_id, project_id, instance_id)
        resp = self.do_internal_request(url, method="POST")
        return resp

    def get_api_keys(self, tenant_id, project_id, instance_id, page=1,
                     perPage=100):
        """
            Create a Columnar apikey

            Parameters:
                tenant_id (str): The ID of the tenant associated with the project.
                project_id (str): The ID of the project where the instance is located.
                instance_id (str): The ID of the Columnar instance to get keys for.
        """
        url = "{}/v2/organizations/{}/projects/{}/instance/{}/apikeys?" \
              "page={}&perPage={}".format(
              self.internal_url, tenant_id, project_id, instance_id,
              page, perPage)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def delete_api_keys(self, tenant_id, project_id, instance_id, api_key):
        """
            Revoke a Columnar apikey
            Parameters:
                tenant_id (str): The ID of the tenant associated with the project.
                project_id (str): The ID of the project where the instance is located.
                instance_id (str): The ID of the Columnar instance to create keys for.
                api_key (str): api key id.
        """
        url = "{}/v2/organizations/{}/projects/{}/instance/{}/apikeys/{}".format(
            self.internal_url, tenant_id, project_id, instance_id, api_key)
        resp = self.do_internal_request(
            url, method="DELETE")
        return resp

    def allow_ip(self, tenant_id, project_id, instance_id, cidr, comment="", **kwargs):
        """
            Add a CIDR to the columnar CIDR allowed list.
            Parameters:
                tenant_id (str): The ID of the tenant associated with the project.
                project_id (str): The ID of the project where the instance is located.
                instance_id (str): The ID of the Columnar instance to create keys for.
                cidr (str): The CIDR that is to be added to the allowed CIDR list
                comment (str): Comment for the CIDR that is to be added
                **kwargs: Additional keyword arguments to pass to the API request.

        """
        url = "{}/v2/organizations/{}/projects/{}/instance/{}/allowlists".format(
            self.internal_url, tenant_id, project_id, instance_id)
        body = {"cidr": cidr, "comment": comment}
        for key, value in kwargs.items():
            body[key] = value

        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(body))
        return resp

    def turn_off_instance(self, tenant_id, project_id, instance_id):
        """
            Turn off a columnar instance
            Parameters:
                tenant_id (str): The ID of the tenant associated with the project.
                project_id (str): The ID of the project where the instance is located.
                instance_id (str): The ID of the Columnar instance to create keys for.
        """
        url = '{}/v2/organizations/{}/projects/{}/instance/{}/off' \
            .format(self.internal_url, tenant_id, project_id, instance_id)
        resp = self.do_internal_request(url, method="POST", params='')
        return resp

    def turn_on_instance(self, tenant_id, project_id, instance_id):
        """
            Turn on a columnar instance
            Parameters:
                tenant_id (str): The ID of the tenant associated with the project.
                project_id (str): The ID of the project where the instance is located.
                instance_id (str): The ID of the Columnar instance to create keys for.
        """
        url = '{}/v2/organizations/{}/projects/{}/instance/{}/on' \
            .format(self.internal_url, tenant_id, project_id, instance_id)
        resp = self.do_internal_request(url, method="POST", params='')
        return resp
