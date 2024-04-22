# -*- coding: utf-8 -*-
# Generic/Built-in
import base64
import json

from ..common.CapellaAPI import CommonCapellaAPI


class CapellaAPI(CommonCapellaAPI):

    def set_logging_level(self, level):
        self._log.setLevel(level)

    # Cluster methods
    def get_clusters(self, params=None):
        capella_api_response = self.capella_api_get('/v3/clusters', params)
        return (capella_api_response)

    def get_cluster_info(self, cluster_id):
        capella_api_response = self.capella_api_get('/v3/clusters/' + cluster_id)

        return (capella_api_response)

    def get_cluster_status(self, cluster_id):
        capella_api_response = self.capella_api_get('/v3/clusters/' + cluster_id + '/status')

        return (capella_api_response)

    def create_cluster(self, cluster_configuration):
        capella_api_response = self.capella_api_post('/v3/clusters', cluster_configuration)

        return (capella_api_response)

    def update_cluster_servers(self, cluster_id, new_cluster_server_configuration):
        capella_api_response = self.capella_api_put('/v3/clusters' + '/' + cluster_id + '/servers',
                                                    new_cluster_server_configuration)

        return (capella_api_response)

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

    def delete_cluster(self, cluster_id):
        capella_api_response = self.capella_api_del('/v3/clusters' + '/' + cluster_id)
        return (capella_api_response)

    def get_cluster_users(self, cluster_id):
        capella_api_response = self.capella_api_get('/v3/clusters' + '/' + cluster_id +
                                                    '/users')
        return (capella_api_response)

    def delete_cluster_user(self, cluster_id, cluster_user):
        capella_api_response = self.capella_api_del('/v3/clusters' + '/' + cluster_id +
                                                    '/users/'+ cluster_user)
        return (capella_api_response)

    # Cluster certificate
    def get_cluster_certificate(self, cluster_id):
        capella_api_response = self.capella_api_get('/v3/clusters' + '/' + cluster_id +
                                                    '/certificate')
        return (capella_api_response)

    # Cluster buckets
    def get_cluster_buckets(self, cluster_id):
        capella_api_response = self.capella_api_get('/v2/clusters' + '/' + cluster_id +
                                                    '/buckets')
        return (capella_api_response)

    def create_cluster_bucket(self, cluster_id, bucket_configuration):
        capella_api_response = self.capella_api_post('/v2/clusters' + '/' + cluster_id +
                                                     '/buckets', bucket_configuration)
        return (capella_api_response)

    def update_cluster_bucket(self, cluster_id, bucket_id, new_bucket_configuration):
        capella_api_response = self.capella_api_put('/v2/clusters' + '/' + cluster_id +
                                                    '/buckets/' + bucket_id , new_bucket_configuration)
        return (capella_api_response)

    def delete_cluster_bucket(self, cluster_id, bucket_configuration):
        capella_api_response = self.capella_api_del('/v2/clusters' + '/' + cluster_id +
                                                    '/buckets', bucket_configuration)
        return (capella_api_response)

    # Cluster Allow lists
    def get_cluster_allowlist(self, cluster_id):
        capella_api_response = self.capella_api_get('/v2/clusters' + '/' + cluster_id +
                                                    '/allowlist')
        return (capella_api_response)

    def delete_cluster_allowlist(self, cluster_id, allowlist_configuration):
        capella_api_response = self.capella_api_del('/v2/clusters' + '/' + cluster_id +
                                                    '/allowlist', allowlist_configuration)
        return (capella_api_response)

    def create_cluster_allowlist(self, cluster_id, allowlist_configuration):
        capella_api_response = self.capella_api_post('/v2/clusters' + '/' + cluster_id +
                                                     '/allowlist', allowlist_configuration)
        return (capella_api_response)

    def update_cluster_allowlist(self, cluster_id, new_allowlist_configuration):
        capella_api_response = self.capella_api_put('/v2/clusters' + '/' + cluster_id +
                                                    '/allowlist', new_allowlist_configuration)
        return (capella_api_response)

    # Cluster user
    def create_cluster_user(self, cluster_id, cluster_user_configuration):
        capella_api_response = self.capella_api_post('/v3/clusters' + '/' + cluster_id +
                                                     '/users', cluster_user_configuration)
        return (capella_api_response)

    # Capella Users
    def get_users(self):
        capella_api_response = self.capella_api_get('/v2/users?perPage=' + str(self.perPage))
        return (capella_api_response)

    def create_bucket(self, tenant_id, project_id, cluster_id,
                      bucket_params):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        url = '{}/buckets'.format(url)
        default = {"name": "default", "bucketConflictResolution": "seqno",
                   "memoryAllocationInMb": 100, "flush": False, "replicas": 0,
                   "durabilityLevel": "none", "timeToLive": None}
        default.update(bucket_params)
        resp = self.do_internal_request(url, method="POST",
                                    params=json.dumps(default))
        return resp

    def get_buckets(self, tenant_id, project_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        url = '{}/buckets'.format(url)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    def flush_bucket(self, tenant_id, project_id, cluster_id, bucket_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        url = url + "/buckets/" + bucket_id + "/flush"
        resp = self.do_internal_request(url, method="POST")
        return resp

    def delete_bucket(self, tenant_id, project_id, cluster_id,
                      bucket_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        url = '{}/buckets/{}'.format(url, bucket_id)
        resp = self.do_internal_request(url, method="DELETE")
        return resp

    def update_bucket_settings(self, tenant_id, project_id, cluster_id,
                               bucket_id, bucket_params):
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/buckets/{}" \
            .format(self.internal_url, tenant_id, project_id,
                    cluster_id, bucket_id)
        resp = self.do_internal_request(url, method="PUT", params=json.dumps(bucket_params))
        return resp

    def get_cluster_specs(self, tenant_id, project_id, cluster_id):
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/specs" \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    def jobs(self, project_id, tenant_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        url = '{}/jobs'.format(url)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    def get_cluster_internal(self, tenant_id, project_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(self.internal_url, tenant_id, project_id, cluster_id)

        resp = self.do_internal_request(url, method="GET",
                                    params='')
        return resp

    def get_nodes(self, tenant_id, project_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        url = '{}/nodes'.format(url)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    def get_db_users(self, tenant_id, project_id, cluster_id,
                     page=1, limit=100):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
              .format(self.internal_url, tenant_id, project_id, cluster_id)
        url = url + '/users?page=%s&perPage=%s' % (page, limit)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def delete_db_user(self, tenant_id, project_id, cluster_id, user_id):
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/users/{}" \
            .format(self.internal_url, tenant_id, project_id, cluster_id,
                    user_id)
        resp = self.do_internal_request(url, method="DELETE",
                                    params='')
        return resp

    def create_db_user(self, tenant_id, project_id, cluster_id,
                       user, pwd):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        body = {"name": user, "password": pwd,
                "permissions": {"data_reader": {}, "data_writer": {}}}
        url = '{}/users'.format(url)
        resp = self.do_internal_request(url, method="POST",
                                    params=json.dumps(body))
        return resp

    def allow_my_ip(self, tenant_id, project_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
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

    def add_allowed_ips(self, tenant_id, project_id, cluster_id, ips):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}'\
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

    def load_sample_bucket(self, tenant_id, project_id, cluster_id,
                           bucket_name):
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/buckets/samples"\
              .format(self.internal_url, tenant_id, project_id, cluster_id)
        param = {'name': bucket_name}
        resp = self.do_internal_request(url, method="POST",
                                    params=json.dumps(param))
        return resp

    def upgrade_dp_agent(self, cluster_id, version_hash):
        url = "{}/internal/support/clusters/{}/agent-versions/activate"\
              .format(self.internal_url, cluster_id)
        param = {'hash': version_hash}
        resp = self._urllib_request(url, method="POST",
                                    params=json.dumps(param),
                                    headers=self.cbc_api_request_headers)
        return resp

    def create_cluster_CPUI(self, tenant_id, config):
        '''
        #Sample Config
        {
            "server": "7.2.0",
            "timezone": "PT",
            "description": "Amazing Cloud",
            "singleAZ": False,
            "specs":
            [
                {
                    "count": 3,
                    "services":
                    [
                        "kv",
                        "n1ql"
                    ],
                    "compute": "n2-standard-8",
                    "disk":
                    {
                        "type": "pd-ssd",
                        "sizeInGb": 500
                    },
                    "provider": "hostedGCP",
                    "diskAutoScaling":
                    {
                        "enabled": True
                    }
                },
                {
                    "count": 3,
                    "services":
                    [
                        "index"
                    ],
                    "compute": "n2-standard-8",
                    "disk":
                    {
                        "type": "pd-ssd",
                        "sizeInGb": 500
                    },
                    "provider": "hostedGCP",
                    "diskAutoScaling":
                    {
                        "enabled": True
                    }
                }
            ],
            "provider": "hostedGCP",
            "name": "riteshagarwal007_gcp_C1",
            "cidr": NONE,
            "region": "us-east1",
            "plan": "Enterprise",
            "projectId": ""
        }
        '''
        url = '{}/v2/organizations/{}/clusters'.format(
            self.internal_url, tenant_id)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(config))
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

    def upgrade_cluster(self, tenant_id, project_id, cluster_id, config):
        '''
        Sample Config
        {
            "token": "",
            "image": "couchbase-cloud-server-7.1.4-3639-v1.0.17",
            "server": "7.1.4",
            "releaseID": "1.0.17"
        }
        '''
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/version'.format(
            self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(config))
        return resp

    def get_deployment_options(self, tenant_id, provider='aws'):
        """
        Get deployment options, including a suggested CIDR for deploying a
        cluster.

        Example use:

        ```
        resp = client.get_deployment_options(tenant_id, provider='aws')
        suggestedCidr = resp.json().get('suggestedCidr')
        ```
        """
        url = '{}/v2/organizations/{}/clusters/deployment-options?provider={}' \
              .format(self.internal_url, tenant_id, provider)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def create_eventing_function(self, cluster_id, name, body, function_scope=None):
        url = '{}/v2/databases/{}/proxy/_p/event/api/v1/functions/{}'.format(self.internal_url, cluster_id, name)

        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])

        resp = self.do_internal_request(url, method="POST",
                                    params=json.dumps(body))
        return resp

    def __set_eventing_function_settings(self, cluster_id, name, body, function_scope=None):
        url = '{}/v2/databases/{}/proxy/_p/event/api/v1/functions/{}/settings'.format(self.internal_url, cluster_id, name)

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
        return self.__set_eventing_function_settings(cluster_id, name, body, function_scope)

    def resume_eventing_function(self, cluster_id, name, function_scope=None):
        body = {
            "processing_status": True,
            "deployment_status": True,
        }
        return self.__set_eventing_function_settings(cluster_id, name, body, function_scope)

    def deploy_eventing_function(self, cluster_id, name, function_scope=None):
        body = {
            "deployment_status": True,
            "processing_status": True,
        }
        return self.__set_eventing_function_settings(cluster_id, name, body, function_scope)

    def undeploy_eventing_function(self, cluster_id, name, function_scope=None):
        body = {
            "deployment_status": False,
            "processing_status": False
        }
        return self.__set_eventing_function_settings(cluster_id, name, body, function_scope)

    def get_composite_eventing_status(self, cluster_id):
        url = '{}/v2/databases/{}/proxy/_p/event/api/v1/status'.format(self.internal_url, cluster_id)

        resp = self.do_internal_request(url, method="GET")
        return resp

    def get_all_eventing_stats(self, cluster_id, seqs_processed=False):
        url = '{}/v2/databases/{}/proxy/_p/event/api/v1/stats'.format(self.internal_url, cluster_id)

        if seqs_processed:
            url += "?type=full"

        resp = self.do_internal_request(url, method="GET")
        return resp

    def delete_eventing_function(self, cluster_id, name, function_scope=None):
        url = '{}/v2/databases/{}/proxy/_p/event/deleteAppTempStore/?name={}'.format(self.internal_url, cluster_id, name)

        if function_scope is not None:
            url += "&bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        resp = self.do_internal_request(url, method="GET")
        return resp

    def create_private_network(self, tenant_id, project_id, cluster_id,
                               private_network_params):
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/virtualnetworks"\
              .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(private_network_params))
        return resp

    def get_private_network(self, tenant_id, project_id, cluster_id,
                            private_network_id):
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/virtualnetworks/{}"\
              .format(self.internal_url, tenant_id, project_id, cluster_id, private_network_id)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def update_specs(self, tenant_id, project_id, cluster_id, specs):
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/specs"\
                .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="POST",
                                    params=json.dumps(specs))
        return resp

    def restore_from_backup(self, tenant_id, project_id, cluster_id, bucket_name):
        """
        method used to restore from the backup
        :param tenant_id:
        :param project_id:
        :param cluster_id:
        :param bucket_name:
        :return: response object
        """
        payload = {"sourceClusterId": cluster_id,
                   "targetClusterId": cluster_id,
                   "options": {"services": ["data", "query", "index", "search"], "filterKeys": "", "filterValues": "",
                               "mapData": "", "includeData": "", "excludeData": "", "autoCreateBuckets": True,
                               "autoRemoveCollections": True, "forceUpdates": True}}
        bucket_id = self.get_backups_bucket_id(tenant_id=tenant_id,
                                               project_id=project_id,
                                               cluster_id=cluster_id,
                                               bucket_name=bucket_name)
        url = r"{}/v2/organizations/{}/projects/{}/clusters/{}/buckets/{}/restore" \
            .format(self.internal_url, tenant_id, project_id, cluster_id, bucket_id)
        resp = self.do_internal_request(url, method="POST", params=json.dumps(payload))
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

    def get_backups_bucket_id(self, tenant_id, project_id, cluster_id, bucket_name):
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

    def set_backup_ami(self, tenant_id, project_id, cluster_id, backup_ami):
        """
        method to override backup client ami
        :param tenant_id:
        :param project_id:
        :param cluster_id:
        :param backup_ami:
        :return: response object
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/backup-image".format(
            self.internal_url,
            tenant_id,
            project_id,
        )
        payload = {"image": backup_ami}
        resp = self.do_internal_request(url, method="POST", params=json.dumps(payload))
        return resp

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
        resp = self.do_internal_request(url, method="POST", params=json.dumps(payload))
        return resp

    def list_all_bucket_backups(self, tenant_id, project_id, cluster_id, bucket_id):
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

    def generate_export_link(self, tenant_id, project_id, cluster_id, export_id):
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

    def remove_user(self, tenant_id, user_id):
        """
        Remove a user from the tenant
        """
        url = "{}/v2/organizations/{}/users/{}".format(self.internal_url, tenant_id, user_id)
        resp = self.do_internal_request(url, method="DELETE")
        return resp

    def create_xdcr_replication(self, tenant_id, project_id, cluster_id, payload):
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
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/xdcr"\
              .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(payload))
        return resp

    def list_cluster_replications(self, tenant_id, project_id, cluster_id):
        """
        Get all XDCR replications for a cluster
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/xdcr"\
              .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def get_replication(self, tenant_id, project_id, cluster_id, replication_id):
        """
        Get a specific XDCR replication for a cluster
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/xdcr/{}"\
              .format(self.internal_url, tenant_id, project_id, cluster_id, replication_id)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def delete_replication(self, tenant_id, project_id, cluster_id, replication_id):
        """
        Delete an XDCR replication
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/xdcr/{}"\
              .format(self.internal_url, tenant_id, project_id, cluster_id, replication_id)
        resp = self.do_internal_request(url, method="DELETE")
        return resp

    def pause_replication(self, tenant_id, project_id, cluster_id, replication_id):
        """
        Pause an XDCR replication
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/xdcr/{}/pause"\
              .format(self.internal_url, tenant_id, project_id, cluster_id, replication_id)
        resp = self.do_internal_request(url, method="POST")
        return resp

    def start_replication(self, tenant_id, project_id, cluster_id, replication_id):
        """
        Start an XDCR replication
        """
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/xdcr/{}/start"\
              .format(self.internal_url, tenant_id, project_id, cluster_id, replication_id)
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
        url = '{}/v2/organizations/{}/backends'.format(self.internal_url, tenant_id)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(config))
        return resp

    def get_sgw_backend(self, tenant_id, project_id, cluster_id, backend_id):
        """
        Get details about a SyncGateway backend for a cluster
        """
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def delete_sgw_backend(self, tenant_id, project_id, cluster_id, backend_id):
        """
        Delete a SyncGateway backend
        """
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self.do_internal_request(url, method="DELETE")
        return resp

    def create_sgw_database(self, tenant_id, project_id, cluster_id, backend_id, config):
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
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases?page=1&perPage=250' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def resume_sgw_database(self, tenant_id, project_id, cluster_id, backend_id, db_name):
        "Resume the sgw database (app endpoint)"
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/online' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="POST")
        return resp

    def pause_sgw_database(self, tenant_id, project_id, cluster_id, backend_id, db_name):
        "Resume the sgw database (app endpoint)"
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/offline' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="POST")
        return resp

    def allow_my_ip_sgw(self, tenant_id, project_id, cluster_id, backend_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/allowip'\
            .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self._urllib_request("https://ifconfig.me", method="GET")
        if resp.status_code != 200:
            raise Exception("Fetch public IP failed!")
        body = {"cidr": "{}/32".format(resp.content.decode()), "comment": ""}
        resp = self.do_internal_request(url, method="POST",
                                    params=json.dumps(body))
        return resp

    def add_allowed_ip_sgw(self, tenant_id, project_id, cluster_id, backend_id, ip):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/allowip'\
            .format(self.internal_url, tenant_id, project_id, backend_id, cluster_id)
        body = {"cidr": "{}/32".format(ip), "comment": ""}
        resp = self.do_internal_request(url, method="POST",
                                    params=json.dumps(body))
        return resp

    def update_sync_function_sgw(self, tenant_id, project_id, cluster_id, backend_id, db_name, config):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/sync' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(config))
        return resp

    def add_app_role_sgw(self, tenant_id, project_id, cluster_id, backend_id, db_name, config):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/roles' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(config))
        return resp

    def add_user_sgw(self, tenant_id, project_id, cluster_id, backend_id, db_name, config):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/users' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(config))
        return resp

    def add_admin_user_sgw(self, tenant_id, project_id, cluster_id, backend_id, db_name, config):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/adminusers' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(config))
        return resp

    def get_sgw_links(self, tenant_id, project_id, cluster_id, backend_id, db_name):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/connect' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    def get_sgw_info(self, tenant_id, project_id, cluster_id, backend_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    def get_sgw_certificate(self, tenant_id, project_id, cluster_id, backend_id, db_name):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/publiccert' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    def get_sgw_logstreaming_config(self, tenant_id, project_id, cluster_id, backend_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/logstreaming' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self.do_internal_request(url, method="GET")
        return resp

    def enable_sgw_logstreaming(self, tenant_id, project_id, cluster_id, backend_id):
        "Enable log streaming for the sgw"
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/logstreaming/enable' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self.do_internal_request(url, method="POST")
        return resp

    def disable_sgw_logstreaming(self, tenant_id, project_id, cluster_id, backend_id):
        "Disable log streaming for the sgw"
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/logstreaming/disable' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self.do_internal_request(url, method="POST")
        return resp

    def create_or_override_log_streaming_config(self, tenant_id, project_id, cluster_id, backend_id, config):
        "Create or override log streaming config"
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/logstreaming/config' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self.do_internal_request(url, method="PUT",
                                        params=json.dumps(config))
        return resp

    def delete_log_streaming_config(self, tenant_id, project_id, cluster_id, backend_id):
        "Delete log streaming config"
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/logstreaming/config' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self.do_internal_request(url, method="DELETE")
        return resp

    def get_sgw_logstreaming_collector_options(self, tenant_id, project_id, cluster_id, backend_id):
        "Get log streaming bit collector configuration"
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/logstreaming/collector-options' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    def get_sgw_logstreaming_collector_option_selected(self, tenant_id, project_id, cluster_id, backend_id):
        "Get log streaming bit collector option selected"
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/logstreaming/collector-option-selected' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    def get_logging_options(self, tenant_id, project_id, cluster_id, backend_id):
        "Get allowed logging options and selections"
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/logging-options' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    def get_logging_config(self, tenant_id, project_id, cluster_id, backend_id, db_name):
        "Get current logging options for the app endpoint"
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/logging' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    def update_logging_config(self, tenant_id, project_id, cluster_id, backend_id, db_name, config):
        "Update current logging options for the app endpoint"
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/{}/logging' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, backend_id, db_name)
        resp = self.do_internal_request(url, method="POST",
                                        params=json.dumps(config))
        return resp

    def get_node_metrics(self, tenant_id, project_id, cluster_id, metrics, step, start, end):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/metrics/{}/query_range' \
              .format(self.internal_url, tenant_id, project_id, cluster_id, metrics)
        payload = {'step':step, 'start':start, 'end':end}
        resp = self.do_internal_request(url, method="GET", params=payload)
        return resp

    def create_project(self, tenant_id, name):
        project_details = {"name": name, "tenantId": tenant_id}

        url = '{}/v2/organizations/{}/projects'.format(self.internal_url, tenant_id)
        capella_api_response = self.do_internal_request(url, method="POST",
                                                        params=json.dumps(project_details))
        return capella_api_response

    def delete_project(self, tenant_id, project_id):
        url = '{}/v2/organizations/{}/projects/{}'.format(self.internal_url, tenant_id,
                                                          project_id)
        capella_api_response = self.do_internal_request(url, method="DELETE",
                                                        params='')
        return capella_api_response

    def turn_off_cluster(self, tenant_id, project_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/off' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="POST", params='')
        return resp

    def turn_on_cluster(self, tenant_id, project_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/on' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        payload = {"turnOnAppService" : True}
        resp = self.do_internal_request(url, method="POST", params=json.dumps(payload))
        return resp

    def enable_private_endpoint(self, tenant_id, project_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/privateendpoint'\
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="POST", params='')
        return resp

    def get_private_endpoint_status(self, tenant_id, project_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/privateendpoint' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    def delete_private_endpoint(self, tenant_id, project_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/privateendpoint' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="DELETE", params='')
        return resp

    def get_gcp_private_endpoint_connection_link(self, tenant_id, project_id, cluster_id, body):
        """
            body =
            {
                "vpcId": "{customers-VPC}",
                "subnetIds": "{customers-subnet}"
            }
        """
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/privateendpoint/linkcommand' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="POST", params=json.dumps(body))
        return resp

    def accept_gcp_private_endpoint_connection(self, tenant_id, project_id, cluster_id, body):
        """
            body =
            {
                "endpointId": "{customers-GCP-Project-Id}"
            }
        """
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/privateendpoint/connection' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="POST", params=json.dumps(body))
        return resp

    def list_private_endpoint_connections(self, tenant_id, project_id, cluster_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/privateendpoint/connection' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="GET", params='')
        return resp

    def reject_private_endpoint_connection(self, tenant_id, project_id, cluster_id,
                                              gcp_project_id):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/privateendpoint/connection/{}' \
            .format(self.internal_url, tenant_id, project_id, cluster_id, gcp_project_id)
        resp = self.do_internal_request(url, method="DELETE", params='')
        return resp

    def update_cluster_sepcs(self, tenant_id, project_id, cluster_id, specs):
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/specs' \
            .format(self.internal_url, tenant_id, project_id, cluster_id)
        resp = self.do_internal_request(url, method="POST", params=json.dumps(specs))
        return resp

    def start_online_storage_migration(
        self,
        organization_id: str,
        project_id: str,
        cluster_id: str,
        buckets,
        headers={},
    ):
        # FIXME Not for merging
        url = f"{self.internal_url}/v4/organizations/{organization_id}/projects/{project_id}/clusters/{cluster_id}/bucketStorageMigration"
        if isinstance(buckets, str):
            buckets = [buckets]

        params = {"buckets": buckets}
        resp = self.do_internal_request(
            url, method="PUT", params=params, headers=headers
        )

        return resp