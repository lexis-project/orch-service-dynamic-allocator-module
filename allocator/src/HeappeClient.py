import json
import datetime
import math
import requests

DEFAULT_TIMEOUT = 6


def datetime_to_json(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()
    return None


class HeappeClient:
    def __init__(self, url, logger):
        self.logger = logger
        self.base_url = url
        self.info_dict = self.init_info(self.base_url)
        if not self.info_dict:
            return None
        self.project_info_dict = None
        self.session_code = None

    def init_info(self, url):
        if url is None:
            return False
        self.logger.doLog("Initializing HeappeClient on URL: " + url)
        try:
            r = requests.get(
                url + "/heappe/ClusterInformation/ListAvailableClusters",
                timeout=DEFAULT_TIMEOUT,
            )
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            self.logger.doLog(str(errh))
            return False
        except requests.exceptions.ConnectionError as errc:
            self.logger.doLog(str(errc))
            return False
        except requests.exceptions.Timeout as errt:
            self.logger.doLog(str(errt))
            return False
        except requests.exceptions.RequestException as err:
            self.logger.doLog(str(err))
            return False
        info_dict = {}
        if r.status_code == 200:
            try:
                for cluster in r.json():
                    info_dict[cluster["Name"]] = cluster
                    info_dict[cluster["Name"]][
                        "timer"
                    ] = datetime.datetime.now() - datetime.timedelta(seconds=1)
                self.logger.doLog("Successully gathered static cluster info.")
            except BaseException:
                self.logger.doLog(
                    "ERROR: invalid response (not json readable) from %s" % (url)
                )
                return False
        else:
            self.logger.doLog(
                "Initial request failed for other reasons than requests exception"
            )
            return False
        return info_dict

    def set_center(self, center):
        self.name = center

    def clusters(self):
        return self.info_dict.keys()

    def av_clusters(self):
        if self.project_info_dict is not None:
            return self.project_info_dict.keys()

    def auth(self, user, password):
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json-patch+json",
        }
        data = (
            '{"Credentials":{"Password":"' + password + '","Username":"' + user + '"}}'
        )
        try:
            r = requests.post(
                self.base_url
                + "/heappe/UserAndLimitationManagement/AuthenticateUserPassword",
                headers=headers,
                data=data,
            )
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            self.logger.doLog(str(errh))
            return False
        except requests.exceptions.ConnectionError as errc:
            self.logger.doLog(str(errc))
            return False
        except requests.exceptions.Timeout as errt:
            self.logger.doLog(str(errt))
            return False
        except requests.exceptions.RequestException as err:
            self.logger.doLog(str(err))
            return False
        if r.status_code == 200:
            self.session_code = r.json()
            self.logger.doLog("=== Successfully authenticated ===")
            return True
        else:
            self.logger.doLog("Auth failed")
            return False

    def auth_openid(self, user, token):
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
        }
        data = (
            '{"credentials":{"Username":"'
            + user
            + '","OpenIdAccessToken":"'
            + token
            + '"}}'
        )
        try:
            r = requests.post(
                self.base_url
                + "/heappe/UserAndLimitationManagement/AuthenticateUserOpenId",
                headers=headers,
                data=data,
            )
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            self.logger.doLog(str(errh))
            return False
        except requests.exceptions.ConnectionError as errc:
            self.logger.doLog(str(errc))
            return False
        except requests.exceptions.Timeout as errt:
            self.logger.doLog(str(errt))
            return False
        except requests.exceptions.RequestException as err:
            self.logger.doLog(str(err))
            return False
        if r.status_code == 200:
            self.session_code = r.json()
            self.logger.doLog("=== Successfully authenticated with OpenId ===")
            return True
        else:
            self.logger.doLog("Auth failed")
            return False

    def get_queue_static_info(self, cluster, queue):
        queue_list = self.info_dict[cluster]["NodeTypes"]
        for item in queue_list:
            if item["Name"] == queue:
                return item

    def get_queue_info(self, qid):
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json-patch+json",
        }
        data = (
            '{"ClusterNodeId":'
            + str(qid)
            + ',"SessionCode":"'
            + self.session_code
            + '"}'
        )
        try:
            r = requests.post(
                self.base_url + "/heappe/ClusterInformation/CurrentClusterNodeUsage",
                headers=headers,
                data=data,
                timeout=DEFAULT_TIMEOUT,
            )
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            self.logger.doLog(str(errh))
            return False
        except requests.exceptions.ConnectionError as errc:
            self.logger.doLog(str(errc))
            return False
        except requests.exceptions.Timeout as errt:
            self.logger.doLog(str(errt))
            return False
        except requests.exceptions.RequestException as err:
            self.logger.doLog(str(err))
            return False
        if r.status_code == 200:
            data = json.loads(r.content)
            return data
        else:
            self.logger.doLog("Failed request for queue info for other reasons")
            return False

    def check_template_queues(self, cluster, template_name):
        ret = []
        for queue in self.project_info_dict[cluster]["NodeTypes"]:
            for template in queue["CommandTemplates"]:
                if template_name == template["Name"]:
                    ret.append(
                        {
                            "Name": queue["Name"],
                            "Id": queue["Id"],
                            "template_id": template["Id"],
                        }
                    )
        return ret

    def get_av_cluster_id(self, cluster):
        if cluster not in self.project_info_dict.keys():
            return -1
        else:
            return self.project_info_dict[cluster]["Id"]

    def update_cluster(self, cluster, user, token, resubmit, passauth=False):
        temp = self.init_info(self.base_url)
        if not temp:
            self.logger.doLog(
                "WARNING: cannot contact service Heappe Instance: " + str(self.base_url)
            )
            return False
        else:
            check = False
            if temp.keys() != self.info_dict.keys():
                check = True
            else:
                for cluster_it in temp:
                    if (
                        temp[cluster_it]["NodeTypes"]
                        != self.info_dict[cluster_it]["NodeTypes"]
                    ):
                        check = True
                        break
            if check:
                self.info_dict = temp
        if cluster not in self.info_dict:
            return False
        self.logger.doLog("Updating status for cluster: " + cluster)
        if self.info_dict[cluster]["timer"] <= datetime.datetime.now() or resubmit:
            if passauth:
                self.auth(user, token)
            else:
                self.auth_openid(user, token)
            if self.session_code is None:
                self.logger.doLog("ERROR during HEAppE auth")
                return False
            queues_status = {}
            for queue in self.info_dict[cluster]["NodeTypes"]:
                self.logger.doLog("Updating status for queue: " + str(queue["Id"]))
                stat = self.get_queue_info(queue["Id"])
                if stat:
                    queues_status[queue["Name"]] = stat
                else:
                    self.logger.doLog(
                        "Failed update for cluster: "
                        + cluster
                        + ", queue: "
                        + str(queue["Id"])
                    )
                    continue
                self.logger.doLog("Successfully updated queue load status info.")
            self.info_dict[cluster]["QueueStatus"] = queues_status
            self.info_dict[cluster][
                "timer"
            ] = datetime.datetime.now() + datetime.timedelta(minutes=5)
        self.session_code = None
        return self.info_dict[cluster]["QueueStatus"].keys()

    def get_heappe(self, heappe_url, token):
        self.project_info_dict = self.init_info(heappe_url)
        if not self.project_info_dict:
            return False
        return True

    def compute_occupation(self, cluster, queue_name, max_cores):
        info = self.get_queue_static_info(cluster, queue_name)
        nodes_req = math.ceil(max_cores / info["CoresPerNode"]) / info["NumberOfNodes"]
        used_nodes = self.info_dict[cluster]["QueueStatus"][queue_name][
            "NumberOfUsedNodes"
        ]
        occupation = used_nodes / info["NumberOfNodes"]
        return nodes_req, occupation
