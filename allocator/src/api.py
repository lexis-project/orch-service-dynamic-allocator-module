# import modules and libraries
from flask_cors import CORS as cors
from flask_oidc import OpenIDConnect as otk
from flask_restful import reqparse
from flask_restful import inputs
from flask import jsonify
from flask import Response
from flask import request
from datetime import date
from marshmallow import ValidationError, EXCLUDE
from json import loads, dumps
from ReqSchema import HPCSchema, CloudSchema
import clusters
import lxmlog
import datetime
import json
import subprocess as cli
import requests
import os

api_script_path = os.path.dirname(os.path.realpath(__file__))
db_dump_path = os.path.join(api_script_path, "..", "dbs")

# provide a class with object managing the routes and api


class APIRest:
    # constructur of the class
    def __init__(self, service, logger, lxc):
        self.service = service
        self.lxc = lxc
        self.logger = logger
        self.verbosity = 2
        self.db_name_1 = lxc.lxm_conf["lxm_db1"]
        self.db_name_2 = lxc.lxm_conf["lxm_db2"]
        self.db_name_3 = lxc.lxm_conf["lxm_db3"]
        self.db_active1 = True
        self.db_active2 = True
        self.db_active3 = True
        self.idb_c1 = lxc.idb_c1
        self.idb_c2 = lxc.idb_c2
        self.idb_c3 = lxc.idb_c3
        self.platform = clusters.Clusters(logger, lxc, self)
        self.idx1 = 0
        self.idx2 = 0
        self.idx3 = 0
        self.keycloak_URL = lxc.lxm_conf["keycloak_URL"]
        self.KC_REALM = lxc.lxm_conf["KC_REALM"]
        self.KC_CLID = lxc.lxm_conf["KC_CLID"]
        self.KC_SECRET = lxc.lxm_conf["KC_SECRET"]

    # auth methods
    def auth(self, token):
        ret = {"status": None, "jmsg": None}
        token_list = token.split()
        if token_list[0] != "Bearer":
            ret["status"] = 400
            ret["jmsg"] = "Invalid token"
            return ret
        else:
            token = token_list[1]
        try:
            data = {"token": token}
            r = requests.post(
                self.keycloak_URL
                + "/auth/realms/"
                + self.KC_REALM
                + "/protocol/openid-connect/token/introspect",
                data=data,
                auth=(self.KC_CLID, self.KC_SECRET),
                verify=False,
            )
            r.raise_for_status()
            ret["status"] = r.status_code
            if r.status_code == 200:
                msg = r.json()
                if msg["active"]:
                    ret["status"] = 200
                    ret["jmsg"] = "active"
                    return ret
                else:
                    ret["status"] = 400
                    ret["jmsg"] = "not active"
                    return ret
            else:
                ret["status"] = 400
                ret["jmsg"] = "Could not authenticate"
                return ret
        except requests.exceptions.HTTPError as errh:
            ret["status"] = 400
            ret["jmsg"] = str(errh)
            return ret
        except requests.exceptions.ConnectionError as errc:
            ret["status"] = 400
            ret["jmsg"] = str(errc)
            return ret
        except requests.exceptions.Timeout as errt:
            ret["status"] = 400
            ret["jmsg"] = str(errt)
            return ret
        except requests.exceptions.RequestException as err:
            ret["status"] = 400
            ret["jmsg"] = str(err)
            return ret

    # exchange auth token

    def exchange_token(self, token):
        ret = {"access_token": None, "refresh_token": None}
        try:
            data = {
                "scope": "offline_access",
                "client_id": self.KC_CLID,
                "client_secret": self.KC_SECRET,
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "subject_token": token,
                "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
                "requested_token_type": "urn:ietf:params:oauth:token-type:refresh_token",
            }
            r = requests.post(
                self.keycloak_URL
                + "/auth/realms/"
                + self.KC_REALM
                + "/protocol/openid-connect/token",
                data=data,
                verify=False,
            )
            r.raise_for_status()
            if r.status_code == 200:
                msg = r.json()
                if "access_token" in msg.keys() and "refresh_token" in msg.keys():
                    ret["access_token"] = msg["access_token"]
                    ret["refresh_token"] = msg["refresh_token"]
            return ret
        except requests.exceptions.HTTPError as errh:
            self.logger.doLog("Error in exchange token req:" + str(errh))
            return ret
        except requests.exceptions.ConnectionError as errc:
            self.logger.doLog("Error in exchange token req:" + str(errc))
            return ret
        except requests.exceptions.Timeout as errt:
            self.logger.doLog("Error in exchange token req:" + str(errt))
            return ret
        except requests.exceptions.RequestException as err:
            self.logger.doLog("Error in exchange token req:" + str(err))
            return ret

    # refresh auth token

    def refresh_token(self, token):
        ret = {"access_token": None, "refresh_token": None}
        try:
            data = {"refresh_token": token, "grant_type": "refresh_token"}
            r = requests.post(
                self.keycloak_URL
                + "/auth/realms/"
                + self.KC_REALM
                + "/protocol/openid-connect/token",
                data=data,
                auth=(self.KC_CLID, self.KC_SECRET),
                verify=False,
            )
            r.raise_for_status()
            if r.status_code == 200:
                msg = r.json()
                if "access_token" in msg.keys() and "refresh_token" in msg.keys():
                    ret["access_token"] = msg["access_token"]
                    ret["refresh_token"] = msg["refresh_token"]
            return ret
        except requests.exceptions.HTTPError as errh:
            self.logger.doLog("Error in exchange token req:" + str(errh))
            return ret
        except requests.exceptions.ConnectionError as errc:
            self.logger.doLog("Error in exchange token req:" + str(errc))
            return ret
        except requests.exceptions.Timeout as errt:
            self.logger.doLog("Error in exchange token req:" + str(errt))
            return ret
        except requests.exceptions.RequestException as err:
            self.logger.doLog("Error in exchange token req:" + str(err))
            return ret

    # check if the databases are active: it sets the object variables db_active_<n> to true or false depending if the corresponding database
    # is active or not (see db_clients idb_c<n>)

    def testDbActive(self):
        # test the first database
        db_exist = False
        dbs = self.idb_c1.get_list_database()
        for entry in dbs:
            if entry["name"] == self.db_name_1:
                db_exist = True
                break
        if db_exist:
            self.db_active1 = True
        else:
            self.db_active1 = False
        # test the second database
        db_exist = False
        dbs = self.idb_c2.get_list_database()
        for entry in dbs:
            if entry["name"] == self.db_name_2:
                db_exist = True
                break
        if db_exist:
            self.db_active2 = True
        else:
            self.db_active2 = False
        # test the third database
        db_exist = False
        dbs = self.idb_c3.get_list_database()
        for entry in dbs:
            if entry["name"] == self.db_name_3:
                db_exist = True
                break
        if db_exist:
            self.db_active3 = True
        else:
            self.db_active3 = False
        return 0

    # get the programmed maintenance periods for a given cluster machine

    def get_programmed_maintenance(self, cluster):
        validFlag = True
        state = 200
        jmsg = {}
        validFlag = self.check_cluster_name(cluster)
        if not validFlag:
            jmsg["message"] = "cluster '%s' is not valid" % (cluster)
            jmsg["status"] = "err"
            self.logger.doLog(
                "served '/maintenance/dates/%s' [err: cluster %s is not valid]"
                % (cluster, cluster)
            )
            state = 400
        else:
            # check if the heappe client has been set up or not -- in the latter case, it just use internal
            # in memory database for getting information (actually at the
            # moment return None)
            if not self.platform.heappe:
                tmp = self.platform.get_maintenance(cluster)
                if tmp is None:
                    jmsg["message"] = "testing mode -- maintenance date is still empty"
                    jmsg["status"] = "ok"
                    state = 200
                else:
                    # this is kept for compatibility with thee previous version
                    # of the BLU
                    dates_iso = []
                    for item in tmp:
                        dates_iso.append(
                            (item[0].isoformat(), item[1].isoformat()))
                    jmsg["message"] = dates_iso
                    jmsg["status"] = "ok"
                    state = 200
            # in this case, we are gettting information from the influxDB
            # client 3
            else:
                self.testDbActive()
                if self.db_active3:
                    # call the external executable script to get the list of
                    # entries
                    ret = cli.run(["./get_maintenance.sh", cluster],
                                  stdout=False, check=True)
                    self.logger.doLog(
                        "running external script for querying the database [ok]"
                    )
                    with open("../dbs/lxm_maintenance_date.txt") as fp:
                        dump = fp.readlines()
                    dump = [entry.strip() for entry in dump]
                    jmsg = {}
                    jptv = []
                    id_dump = 0
                    for line in dump:
                        jpt = {}
                        fields = line.split()
                        jpt["id_entry"] = id_dump
                        jpt["timestamp"] = fields[0]
                        jpt["cluster_name"] = fields[1]
                        jpt["start_maintenance"] = fields[4]
                        jpt["end_maintenance"] = fields[2]
                        id_dump += 1
                        jptv.append(jpt)
                    # eventually remove the temporary file
                    ret = cli.run(
                        ["rm", "../dbs/lxm_maintenance_date.txt"],
                        stdout=False, check=True)
                    jmsg = {"message": jptv}
                    jmsg["status"] = "ok"
                    state = 200
                    self.logger.doLog(
                        "served '/maintenance/dates/' (cluster: %s) [ok]" %
                        (cluster))
                else:
                    jmsg = {
                        "message": "the maintenance database is not available -- request failed"}
                    jmsg["status"] = "err"
                    state = 400
                    self.logger.doLog(
                        "served '/maintenance/dates/' (cluster: %s) [err: maintenance database is not available]" %
                        (cluster))
        return (jmsg, state)

    # get the speed performance when transferring files from a source cluster to a destination cluster,
    # also by specifying the size per file to transfer; if the file size is zero, then all the speeds
    # associated to the pair source-destination are listed

    def get_speed_perf(self, src, dst, size):
        validFlag = True
        state = 200
        jmsg = {}
        center_exist_s = True
        center_exist_d = True
        if src not in self.platform.center_list or src == dst:
            center_exist_s = False
        if dst not in self.platform.center_list:
            center_exist_d = False
        # if they exist then execute the query
        if (center_exist_s) and (center_exist_d):
            self.testDbActive()
            if self.db_active1:
                # call the external executable script to get the list of
                # entries
                ret = cli.run(["./get_speed_perf.sh", src, dst],
                              stdout=False, check=True)
                self.logger.doLog(
                    "running external script for querying the database [ok]"
                )
                with open("../dbs/lxm_speed_perf.txt") as fp:
                    dump = fp.readlines()
                dump = [entry.strip() for entry in dump]
                jmsg = {}
                jptv = []
                id_dump = 0
                dis = 999999999999999999
                for line in dump:
                    jpt = {}
                    fields = line.split()
                    jpt["id_entry"] = id_dump
                    jpt["timestamp"] = fields[0]
                    jpt["center_src"] = fields[2]
                    jpt["center_dst"] = fields[1]
                    jpt["performance"] = fields[4]
                    id_dump += 1
                    value = jpt["performance"].split("_")
                    if float(size) > 0:
                        if dis > abs(float(size) - float(value[0])):
                            dis = abs(float(size) - float(value[0]))
                            jptv = []
                        if dis == abs(float(size) - float(value[0])):
                            jpt["size"] = value[0]
                            jpt["performance"] = float(value[1])
                            jptv.append(jpt)
                    else:
                        jpt["size"] = value[0]
                        jpt["performance"] = float(value[1])
                        jptv.append(jpt)
                # eventually remove the temporary file
                ret = cli.run(["rm", "../dbs/lxm_speed_perf.txt"],
                              stdout=False, check=True)
                jmsg = {"message": jptv}
                jmsg["status"] = "ok"
                state = 200
                if float(size) <= 0:
                    self.logger.doLog(
                        "served '/list/netperf' (src: %s, dst: %s) [ok]" %
                        (src, dst))
                else:
                    self.logger.doLog(
                        "served '/load/netperf' (src: %s, dst: %s, size: %s) [ok]" %
                        (src, dst, size))
            else:
                jmsg = {
                    "message": "the netperf is not available -- request failed"}
                jmsg["status"] = "err"
                state = 400
                if float(size) <= 0:
                    self.logger.doLog(
                        "served '/list/netperf' (src: %s, dst: %s) [err: netperf database is not available]" %
                        (src, dst))
                else:
                    self.logger.doLog(
                        "served '/load/netperf' (src: %s, dst: %s, size: %s) [err: netperf database is not available]" %
                        (src, dst, size))
        else:
            jmsg = {"message": "one or both of the centers do not exist"}
            jmsg["status"] = "err"
            state = 400
        return (jmsg, state)

    # delete a given entry in the database containing the transfer speeds, by specifying the source-destination
    # clusters pair and the size_per_file

    def delete_speed_perf(self, src, dst, size):
        ret = self.get_speed_perf(src, dst, size)
        if ret[1] == 200:
            jmsg = {}
            found = False
            for item in ret[0]["message"]:
                if size != item["size"]:
                    continue
                else:
                    found = True
                    timestamp = int(item["timestamp"])
                    delete_ret = self.idb_c1.query(
                        'DELETE FROM "networkEvaluation" WHERE time = %d' %
                        (timestamp))
            if found:
                jmsg["message"] = (
                    "netperf item removed for dest center '%s', source center '%s' and size '%s'" %
                    (str(dst), str(src), str(size)))
            else:
                jmsg["message"] = (
                    "no previous netperf items for dest center '%s', source center '%s' and size '%s'" %
                    (str(dst), str(src), str(size)))
            self.logger.doLog("served '/netperf/remove/' [ok]")
            jmsg["status"] = "ok"
        else:
            jmsg = ret[0]
        return (jmsg, ret[1])

    # insert the evaluation in the influxdb (db2):
    def write_evaluation(self, job_id, job_list):
        ret = True
        jmm = {}
        start = True
        r = ""
        if self.db_active2:
            for queue in job_list["val"]:
                if not start:
                    r += ","
                if job_list["params"]["type"] == "cloud" or (
                    job_list["params"]["type"] == "both"
                    and "cluster_id" not in queue["dest"].keys()
                ):
                    m = str(queue["dest"]["location"]) + "_cloud"
                    jmm[m] = queue["mean"]
                else:
                    m = (
                        str(queue["dest"]["location"])
                        + "_"
                        + str(queue["dest"]["cluster_id"])
                    )
                    jmm[m] = queue["mean"]
                r += "%s:%.5f" % (m, jmm[m])
                start = False
            db_body = [
                {
                    "measurement": "machineEvaluation",
                    "tags": {
                        "job_id": job_id,
                        "job_type": job_list["params"]["type"],
                        "id": self.idx2,
                    },
                    "fields": {"rank": r},
                }
            ]
            self.idx2 += 1
            if self.verbosity > 1:
                self.logger.doLog("ranking: %s" % (r))
            ret = self.idb_c2.write_points(db_body)
            if ret:
                self.logger.doLog(
                    "ended evaluation, result wrote in DB '/evaluate/machines' [ok]"
                )
            else:
                self.logger.doLog(
                    "ended evaluation '/evaluate/machines' [war: evaluation failed for some of the machines]"
                )
        else:
            self.logger.doLog(
                "ended evaluation '/evaluate/machines' [err: database '%s' is not active or has been deleted]" %
                (self.db_name_2))
            return False
        return True

    # stop the server execution quietly
    def shutdown_server(self):
        shutdown = request.environ.get("werkzeug.server.shutdown")
        if shutdown is None:
            self.logger.doLog(
                "unable to shutdown the server -- not running with 'Werkzeug Server'"
            )
            raise RuntimeError("Not running with the Werkzeug Server")
        shutdown()

    # ---------------------------------------------
    # Clusters and Cloud partitions' names
    # ---------------------------------------------
    # <heappeclient>.clusters() => get the list of cluster names
    # <heappeclient>.name => get the name of the center
    # <openstackclient>.name => get the name of the cloud partition
    # id_resource_hpc = <center_name>_<cluster_name>
    # id_resource_cloud = cloud_<cloud_partition_name>

    def check_cluster_name(self, id_resource):
        if id_resource in self.platform.clusters_list:
            return True
        return False

    # checks if the passed dates are in a valid format
    def check_datetime_format(dt_str):
        strptime_format = "%Y-%m-%d"
        try:
            datetime.datetime.strptime(str(dt_str), strptime_format)
        except BaseException:
            return False
        return True

    # create the routes

    def manageRoutes(self):

        # manage server response after requests are served
        @self.service.after_request
        def after_request(response):
            response.headers.add(
                "Access-Control-Allow-Origin",
                "http://localhost:8081")
            response.headers.add(
                "Access-Control-Allow-Headers", "Content-Type,Authorization"
            )
            response.headers.add(
                "Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS"
            )
            response.headers.add("Access-Control-Allow-Credentials", "true")
            return response

        # base root static route
        @self.service.route("/")
        def index():
            self.logger.doLog("serving static route '/'")
            http_msg = "<h1> service is running </h1>"
            return http_msg

        # quietly shutdown the server
        @self.service.route("/service/shutdown", methods=["GET"])
        def service_shutdown():
            parser = reqparse.RequestParser()
            parser.add_argument(
                "Authorization", type=str, required=True, location="headers"
            )
            args = parser.parse_args()
            auth_res = self.auth(args["Authorization"])
            if auth_res["status"] != 200 or auth_res["jmsg"] == "not active":
                self.logger.doLog(
                    "served '/service/shutdown' [auth err: -- status (%d), msg (%s)]" %
                    (auth_res["status"], auth_res["jmsg"]))
                return (jsonify(auth_res["jmsg"]), auth_res["status"])
            self.shutdown_server()
            self.logger.doLog("quietly shutdowning the service ...")
            jmsg = {}
            jmsg["message"] = "the service is going to shutdown"
            jmsg["status"] = "shutdown"
            return jsonify(jmsg)

        # show the available endpoints
        @self.service.route("/getendpoints", methods=["GET"])
        def getendpoints():
            jmsg = {}
            entry = {}
            entry["method"] = "POST"
            entry["input"] = "src=<string>, dst=<string>"
            jmsg["/store/netperf/<STRING:measure>"] = entry
            entry = {}
            entry["method"] = "GET"
            entry["input"] = "--"
            jmsg["/alive"] = entry
            entry = {}
            entry["method"] = "GET"
            entry["input"] = "src=<string>, dst=<string>, size=<string>"
            jmsg["/load/netperf"] = entry
            entry = {}
            entry["method"] = "GET"
            entry["input"] = "src=<string>, dst=<string>"
            jmsg["/list/netperf"] = entry
            entry = {}
            entry["method"] = "DELETE"
            entry["input"] = "--"
            jmsg["/removedb/<STRING:database>"] = entry
            entry = {}
            entry["method"] = "GET"
            entry["input"] = "--"
            jmsg["/getendpoints"] = entry
            entry = {}
            entry["method"] = "PUT"
            entry["input"] = "date=<string>"
            jmsg["/maintenance/<STRING:cluster>"] = entry
            entry = {}
            entry["method"] = "DELETE"
            entry["input"] = "date=<string>"
            jmsg["/maintenance/remove/<STRING:cluster>"] = entry
            entry = {}
            entry["method"] = "POST"
            entry["input"] = "job_id=<integer>, type={hpc, cloud}"
            jmsg["/evaluate/machines"] = entry
            entry = {}
            entry["method"] = "GET"
            entry["input"] = "number=<integer>, type={hpc, cloud}"
            jmsg["/get/machines/<INT:job_id>"] = entry
            entry = {}
            entry["method"] = "DELETE"
            entry["input"] = "--"
            jmsg["/evaluate/machines/remove/<INT:job_id>"] = entry
            entry = {}
            entry["method"] = "GET"
            entry["input"] = "--"
            jmsg["/maintenance/dates/<string:cluster>"] = entry
            entry = {}
            entry["method"] = "GET"
            entry["input"] = "no_points=<integer>, type={hpc, cloud}"
            jmsg["/job/rank/<INT:job_id>"] = entry
            entry = {}
            entry["method"] = "GET"
            entry["input"] = "--"
            jmsg["/service/shutdown"] = entry
            entry = {}
            entry["method"] = "GET"
            entry["input"] = "--"
            jmsg["/dumpdb/<STRING:database>"] = entry
            entry = {}
            entry["method"] = "GET"
            entry["input"] = "--"
            jmsg["/restoredb/<STRING:database>"] = entry
            entry = {}
            entry["method"] = "GET"
            entry["input"] = "--"
            jmsg["/maintenance/clusters"] = entry
            self.logger.doLog("served '/getendpoints' [ok]")
            return jsonify(jmsg)

        # alternative way of showing endpoints
        # show the available endpoints
        @self.service.route("/help", methods=["GET"])
        def help():
            msg = "POST   :  /store/netperf/<STRING:measure>           : src=<string>, dst=<string>\n"
            msg += "GET    :  /alive                                    : --\n"
            msg += "GET    :  /load/netperf                             : src=<string>, dst=<string>, size=<string>\n"
            msg += "GET    :  /list/netperf                             : src=<string>, dst=<string>\n"
            msg += "DELETE :  /removedb/<STRING:database>               : --\n"
            msg += "GET    :  /getendpoints                             : --\n"
            msg += "GET    :  /maintenance/dates/<STRING:cluster>       : --\n"
            msg += "GET    :  /maintenance/clusters                     : --\n"
            msg += "PUT    :  /maintenance/<STRING:cluster>             : date=<string> (i.e., yyyymmddTHH:MM/yyyymmddTHH:MM) => start_maintenance/end_maintenance\n"
            msg += "DELETE :  /maintenance/remove/<STRING:cluster>      : timestamp=<string> (i.e., numeric unique ID as reeturned by GET /maintenance/dates/) \n"
            msg += "POST   :  /evaluate/machines                        : job_id=<integer>, type={hpc, cloud}\n"
            msg += "GET    :  /get/machines/<INT:job_id>                : number=<integer>, type={hpc, cloud}\n"
            msg += "DELETE :  /evaluate/machines/remove/<INT:job_id>    : --\n"
            msg += "GET    :  /service/shutdown                         : --\n"
            msg += "GET    :  /job/rank/<INT:job_id>                    : no_points=<integer> type={hpc, cloud}\n"
            msg += "GET    :  /dumpdb/<string:database>                 : --\n"
            msg += "GET    :  /restoredb/<string:database>              : --\n"
            self.logger.doLog("served '/help' [ok]")
            return msg

        # ---------------------------------------------
        # Alberto's endpoints -- InfluxDB API
        # ---------------------------------------------
        # provide an alive message
        @self.service.route("/alive", methods=["GET"])
        def test_alive():
            state = 200
            jmsg = {}
            jmsg["message"] = "service is alive"
            jmsg["status"] = "ok"
            return (jsonify(jmsg), state)

        # insert a json stream-element in the database:
        # the peformance of the networking measured on a given cluster site
        @self.service.route("/store/netperf/<string:measure>",
                            methods=["POST"])
        def store_netperf(measure):
            # get  the new measure from the request as input arguments
            parser = reqparse.RequestParser()
            parser.add_argument("src", type=str, required=True)
            parser.add_argument("dst", type=str, required=True)
            parser.add_argument("size", type=str, required=True)
            parser.add_argument(
                "Authorization", type=str, required=True, location="headers"
            )
            args = parser.parse_args()
            auth_res = self.auth(args["Authorization"])
            if auth_res["status"] != 200 or auth_res["jmsg"] == "not active":
                self.logger.doLog(
                    "served '/store/netperf/<string:measure>' [auth err: -- status (%d), msg (%s)]" %
                    (auth_res["status"], auth_res["jmsg"]))
                return (jsonify(auth_res["jmsg"]), auth_res["status"])
            state = 200
            jmsg = {}
            center_exist_s = True
            center_exist_d = True
            if (
                args["src"] not in self.platform.center_list
                or args["src"] == args["dst"]
            ):
                center_exist_s = False
            if args["dst"] not in self.platform.center_list:
                center_exist_d = False
            # check if the database is active
            self.testDbActive()
            # if the cluster is present in the checked platforms it goes to add
            # the stream in the influx database
            if (center_exist_s) and (center_exist_d) and (self.db_active1):
                temp = self.delete_speed_perf(
                    args["src"], args["dst"], args["size"])
                if temp[1] != 200:
                    self.logger.doLog(
                        "error when deleting old entry '/store/netperf/' (%s -> %s) [err]" %
                        (args["src"], args["dst"]))
                    return (jsonify(temp[0]), temp[1])
                jmsg["message"] = "performance of the network for '%s -> %s' added" % (
                    args["src"], args["dst"], )
                jmsg["status"] = ""
                measure = args["size"] + "_" + measure
                db_body = [
                    {
                        "measurement": "networkEvaluation",
                        "tags": {
                            "center_src": args["src"],
                            "center_dst": args["dst"],
                            "id": self.idx1,
                        },
                        "fields": {"performance": measure},
                    }
                ]
                self.idx1 += 1
                ret = self.idb_c1.write_points(db_body)
                if ret:
                    jmsg["status"] = "ok"
                    self.logger.doLog(
                        "served '/store/netperf/' (%s -> %s) [ok]"
                        % (args["src"], args["dst"])
                    )
                else:
                    jmsg["status"] = "err"
                    self.logger.doLog(
                        "served '/store/netperf/' (%s -> %s) [err: operation failed]" %
                        (args["src"], args["dst"]))
                    state = 400
            else:
                # manage the error
                if not self.db_active1:
                    jmsg[
                        "message"
                    ] = "target stream database is not acitve or has been deleted"
                    jmsg["status"] = "war"
                    self.logger.doLog(
                        "served '/store/netperf/' (%s -> %s) [war: target stream-db is not active or has been deleted]" %
                        (args["src"], args["dst"]))
                    self.logger.doLog(
                        "target stream database is not acitve or has been deleted"
                    )
                else:
                    jmsg["message"] = (
                        "source cluster '%s' and/or destination cluster '%s' do not exist" %
                        (args["src"], args["dst"]))
                    jmsg["status"] = "err"
                    self.logger.doLog(
                        "served '/store/netperf/' (%s -> %s) [err: cluster(s) not exist]" %
                        (args["src"], args["dst"]))
                    state = 400
            return (jsonify(jmsg), state)

        # listing all the pairs size_per_file and transfer_speed for a given
        # source and destination locations and for a given size_per_file

        @self.service.route("/load/netperf", methods=["GET"])
        def load_netperf():
            parser = reqparse.RequestParser()
            parser.add_argument("src", type=str, required=True)
            parser.add_argument("dst", type=str, required=True)
            parser.add_argument("size", type=str, required=True)
            parser.add_argument(
                "Authorization", type=str, required=True, location="headers"
            )
            args = parser.parse_args()
            auth_res = self.auth(args["Authorization"])
            if auth_res["status"] != 200 or auth_res["jmsg"] == "not active":
                self.logger.doLog(
                    "served '/load/netperf' [auth err: -- status (%d), msg (%s)]" %
                    (auth_res["status"], auth_res["jmsg"]))
                return (jsonify(auth_res["jmsg"]), auth_res["status"])
            ret = self.get_speed_perf(args["src"], args["dst"], args["size"])
            return (jsonify(ret[0]), ret[1])

        @self.service.route("/delete/netperf", methods=["DELETE"])
        def delete_netperf():
            parser = reqparse.RequestParser()
            parser.add_argument("src", type=str, required=True)
            parser.add_argument("dst", type=str, required=True)
            parser.add_argument("size", type=str, required=True)
            parser.add_argument(
                "Authorization", type=str, required=True, location="headers"
            )
            args = parser.parse_args()
            auth_res = self.auth(args["Authorization"])
            if auth_res["status"] != 200 or auth_res["jmsg"] == "not active":
                self.logger.doLog(
                    "served '/delete/netperf' [auth err: -- status (%d), msg (%s)]" %
                    (auth_res["status"], auth_res["jmsg"]))
                return (jsonify(auth_res["jmsg"]), auth_res["status"])
            ret = self.delete_speed_perf(
                args["src"], args["dst"], args["size"])
            return (jsonify(ret[0]), ret[1])

        # listing all the pairs size_per_file and transfer_speed for a given
        # source and destination locations
        @self.service.route("/list/netperf", methods=["GET"])
        def list_netperf():
            parser = reqparse.RequestParser()
            parser.add_argument("src", type=str, required=True)
            parser.add_argument("dst", type=str, required=True)
            parser.add_argument(
                "Authorization", type=str, required=True, location="headers"
            )
            args = parser.parse_args()
            auth_res = self.auth(args["Authorization"])
            if auth_res["status"] != 200 or auth_res["jmsg"] == "not active":
                self.logger.doLog(
                    "served '/list/netperf' [auth err: -- status (%d), msg (%s)]" %
                    (auth_res["status"], auth_res["jmsg"]))
                return (jsonify(auth_res["jmsg"]), auth_res["status"])
            ret = self.get_speed_perf(args["src"], args["dst"], "0")
            return (jsonify(ret[0]), ret[1])

        # delete the database

        @self.service.route("/removedb/<string:database>", methods=["DELETE"])
        def removedb(database):
            parser = reqparse.RequestParser()
            parser.add_argument(
                "Authorization", type=str, required=True, location="headers"
            )
            args = parser.parse_args()
            auth_res = self.auth(args["Authorization"])
            if auth_res["status"] != 200 or auth_res["jmsg"] == "not active":
                self.logger.doLog(
                    "served '/removedb/<string:database>' [auth err: -- status (%d), msg (%s)]" %
                    (auth_res["status"], auth_res["jmsg"]))
                return (jsonify(auth_res["jmsg"]), auth_res["status"])
            state = 200
            jmsg = {}
            if database == self.db_name_1:
                self.idb_c1.drop_database(self.db_name_1)
                jmsg["message"] = "database '%s' has been removed correctly" % (
                    database)
                jmsg["status"] = "ok"
                self.logger.doLog("served '/removedb/%s' [ok]" % (database))
            elif database == self.db_name_2:
                self.idb_c2.drop_database(self.db_name_2)
                jmsg["message"] = "database '%s' has been removed correctly" % (
                    database)
                jmsg["status"] = "ok"
                self.logger.doLog("served '/removedb/%s' [ok]" % (database))
            elif database == self.db_name_3:
                self.idb_c3.drop_database(self.db_name_3)
                jmsg["message"] = "database '%s' has been removed correctly" % (
                    database)
                jmsg["status"] = "ok"
                self.logger.doLog("served '/removedb/%s' [ok]" % (database))
            else:
                jmsg["message"] = "database '%s' does not exist" % (database)
                jmsg["status"] = "err"
                self.logger.doLog(
                    "served '/removedb/%s' [err: database does not exist]" %
                    (database))
                state = 400
            return (jsonify(jmsg), state)

        # create a dump of the database

        @self.service.route("/dumpdb/<string:database>", methods=["GET"])
        def dumpdb(database):
            parser = reqparse.RequestParser()
            parser.add_argument(
                "Authorization", type=str, required=True, location="headers"
            )
            args = parser.parse_args()
            auth_res = self.auth(args["Authorization"])
            if auth_res["status"] != 200 or auth_res["jmsg"] == "not active":
                self.logger.doLog(
                    "served '/dumpdb/<string:database>' [auth err: -- status (%d), msg (%s)]" %
                    (auth_res["status"], auth_res["jmsg"]))
                return (jsonify(auth_res["jmsg"]), auth_res["status"])
            state = 200
            jmsg = {}
            if database == self.db_name_1:
                # influxd backup -portable -database $1
                # /home/di39mal/devel/flask/lexis_db/databases/_$1_dump
                target = db_dump_path + "/_" + self.db_name_1 + "_dump"
                ret = cli.run(
                    [
                        "influxd",
                        "backup",
                        "-portable",
                        "-database",
                        self.db_name_1,
                        target,
                    ],
                    check=True
                )
                if ret.returncode == 0:
                    jmsg["message"] = "database %s has been correctly dumped" % (
                        database)
                    jmsg["status"] = "ok"
                    self.logger.doLog(
                        "served '/dumpdb/%s' [ok: exit code is %d]"
                        % (database, ret.returncode)
                    )
                else:
                    jmsg["message"] = "database %s has not correctly dumped" % (
                        database)
                    jmsg["status"] = "err"
                    state = 400
                    self.logger.doLog(
                        "served '/dumpdb/%s' [err: exit code is %d]"
                        % (database, ret.returncode)
                    )
            elif database == self.db_name_2:
                # influxd backup -portable -database $1
                # /home/di39mal/devel/flask/lexis_db/databases/_$1_dump
                target = db_dump_path + "/_" + self.db_name_2 + "_dump"
                ret = cli.run(
                    [
                        "influxd",
                        "backup",
                        "-portable",
                        "-database",
                        self.db_name_2,
                        target,
                    ],
                    check=True
                )
                if ret.returncode == 0:
                    jmsg["message"] = "database %s has been correctly dumped" % (
                        database)
                    jmsg["status"] = "ok"
                    self.logger.doLog(
                        "served '/dumpdb/%s' [ok: exit code is %d]"
                        % (database, ret.returncode)
                    )
                else:
                    jmsg["message"] = "database %s has not correctly dumped" % (
                        database)
                    jmsg["status"] = "err"
                    state = 400
                    self.logger.doLog(
                        "served '/dumpdb/%s' [err: exit code is %d]"
                        % (database, ret.returncode)
                    )
            elif database == self.db_name_3:
                # influxd backup -portable -database $1
                # /home/di39mal/devel/flask/lexis_db/databases/_$1_dump
                target = db_dump_path + "/_" + self.db_name_3 + "_dump"
                ret = cli.run(
                    [
                        "influxd",
                        "backup",
                        "-portable",
                        "-database",
                        self.db_name_3,
                        target,
                    ],
                    check=True
                )
                if ret.returncode == 0:
                    jmsg["message"] = "database %s has been correctly dumped" % (
                        database)
                    jmsg["status"] = "ok"
                    self.logger.doLog(
                        "served '/dumpdb/%s' [ok: exit code is %d]"
                        % (database, ret.returncode)
                    )
                else:
                    jmsg["message"] = "database %s has not correctly dumped" % (
                        database)
                    jmsg["status"] = "err"
                    state = 400
                    self.logger.doLog(
                        "served '/dumpdb/%s' [err: exit code is %d]"
                        % (database, ret.returncode)
                    )
            else:
                jmsg["message"] = "database %s does not exist" % (database)
                jmsg["status"] = "err"
                self.logger.doLog(
                    "served '/removedb/%s' [err: database does not exist]" %
                    (database))
                state = 400
            return (jsonify(jmsg), state)

        # restore a specifcic dump of the database

        @self.service.route("/restoredb/<string:database>", methods=["GET"])
        def restoredb(database):
            parser = reqparse.RequestParser()
            parser.add_argument(
                "Authorization", type=str, required=True, location="headers"
            )
            args = parser.parse_args()
            auth_res = self.auth(args["Authorization"])
            if auth_res["status"] != 200 or auth_res["jmsg"] == "not active":
                self.logger.doLog(
                    "served '/restoredb/<string:database>' [auth err: -- status (%d), msg (%s)]" %
                    (auth_res["status"], auth_res["jmsg"]))
                return (jsonify(auth_res["jmsg"]), auth_res["status"])
            state = 200
            jmsg = {}
            # restore procedure: (step 1) drop down the current database; (step
            # 2) restore the database from the file
            if database == self.db_name_1:
                target = db_dump_path + "/_" + self.db_name_1 + "_dump"
                self.idb_c1.drop_database(self.db_name_1)
                ret = cli.run(
                    ["influxd", "restore", "-db", self.db_name_1, "-portable", target],
                    check=True
                )
                if ret.returncode == 0:
                    jmsg["message"] = "database %s has been correctly restored" % (
                        database)
                    jmsg["status"] = "ok"
                    self.logger.doLog(
                        "served '/restoredb/%s' [ok: exit code is %d]"
                        % (database, ret.returncode)
                    )
                else:
                    jmsg["message"] = "database %s has not correctly restored" % (
                        database)
                    jmsg["status"] = "err"
                    state = 400
                    self.logger.doLog(
                        "served '/restoredb/%s' [err: exit code is %d]"
                        % (database, ret.returncode)
                    )
            elif database == self.db_name_2:
                # influxd backup -portable -database $1
                # /home/di39mal/devel/flask/lexis_db/databases/_$1_dump
                target = db_dump_path + "/_" + self.db_name_2 + "_dump"
                self.idb_c2.drop_database(self.db_name_2)
                ret = cli.run(
                    ["influxd", "restore", "-db", self.db_name_2, "-portable", target],
                    check=True
                )
                if ret.returncode == 0:
                    jmsg["message"] = "database %s has been correctly restored" % (
                        database)
                    jmsg["status"] = "ok"
                    self.logger.doLog(
                        "served '/restoredb/%s' [ok: exit code is %d]"
                        % (database, ret.returncode)
                    )
                else:
                    jmsg["message"] = "database %s has not correctly restored" % (
                        database)
                    jmsg["status"] = "err"
                    state = 400
                    self.logger.doLog(
                        "served '/restoredb/%s' [err: exit code is %d]"
                        % (database, ret.returncode)
                    )
            elif database == self.db_name_3:
                target = db_dump_path + "/_" + self.db_name_3 + "_dump"
                self.idb_c3.drop_database(self.db_name_3)
                ret = cli.run(
                    ["influxd", "restore", "-db", self.db_name_3, "-portable", target],
                    check=True
                )
                if ret.returncode == 0:
                    jmsg["message"] = "database %s has been correctly restored" % (
                        database)
                    jmsg["status"] = "ok"
                    self.logger.doLog(
                        "served '/restoredb/%s' [ok: exit code is %d]"
                        % (database, ret.returncode)
                    )
                else:
                    jmsg["message"] = "database %s has not correctly restored" % (
                        database)
                    jmsg["status"] = "err"
                    state = 400
                    self.logger.doLog(
                        "served '/restoredb/%s' [err: exit code is %d]"
                        % (database, ret.returncode)
                    )
            else:
                jmsg["message"] = "database %s does not exist" % (database)
                jmsg["status"] = "err"
                self.logger.doLog(
                    "served '/removedb/%s' [err: database does not exist]" %
                    (database))
                state = 400
            return (jsonify(jmsg), state)

        # ---------------------------------------------
        # Giacomo's endpoints -- Allocator API
        # ---------------------------------------------
        # maintenance date format: yyyy-mm-dd/yyyy-mm-dd
        # get the maintenance dates for a given machine:
        # the machine identifier is <center_name>_<cluster_name>

        @self.service.route("/maintenance/dates/<string:cluster>",
                            methods=["GET"])
        def get_programmed_maintenance_api(cluster):
            parser = reqparse.RequestParser()
            parser.add_argument(
                "Authorization", type=str, required=True, location="headers"
            )
            args = parser.parse_args()
            auth_res = self.auth(args["Authorization"])
            if auth_res["status"] != 200 or auth_res["jmsg"] == "not active":
                self.logger.doLog(
                    "served '/maintenance/dates/<string:cluster>' [auth err: -- status (%d), msg (%s)]" %
                    (auth_res["status"], auth_res["jmsg"]))
                return (jsonify(auth_res["jmsg"]), auth_res["status"])
            ret = self.get_programmed_maintenance(cluster)
            return (jsonify(ret[0]), ret[1])

        @self.service.route("/maintenance/clusters", methods=["GET"])
        def get_maintenance_clusters():
            parser = reqparse.RequestParser()
            parser.add_argument(
                "Authorization", type=str, required=True, location="headers"
            )
            args = parser.parse_args()
            auth_res = self.auth(args["Authorization"])
            if auth_res["status"] != 200 or auth_res["jmsg"] == "not active":
                self.logger.doLog(
                    "served '/maintenance/clusters' [auth err: -- status (%d), msg (%s)]" %
                    (auth_res["status"], auth_res["jmsg"]))
                return (jsonify(auth_res["jmsg"]), auth_res["status"])
            ret = {"message": "no cluster attached!", "status": "war"}
            if len(self.platform.clusters_list) != 0:
                ret["status"] = "ok"
                ret["message"] = self.platform.clusters_list
                self.logger.doLog("served '/maintenance/clusters' [ok]")
            else:
                self.logger.doLog(
                    "served '/maintenance/clusters' no clusters available! [war]"
                )
            return (jsonify(ret), 200)

        # insert maintenance for a machine
        @self.service.route("/maintenance/<string:cluster>", methods=["PUT"])
        def add_programmed_maintenance(cluster):
            state = 200
            parser = reqparse.RequestParser()
            parser.add_argument(
                "date",
                type=inputs.iso8601interval,
                help="Invalid format: use ISO8601 intervals",
                required=True,
            )
            parser.add_argument(
                "Authorization", type=str, required=True, location="headers"
            )
            args = parser.parse_args()
            auth_res = self.auth(args["Authorization"])
            if auth_res["status"] != 200 or auth_res["jmsg"] == "not active":
                self.logger.doLog(
                    "served '/maintenance/<string:cluster>' [auth err: -- status (%d), msg (%s)]" %
                    (auth_res["status"], auth_res["jmsg"]))
                return (jsonify(auth_res["jmsg"]), auth_res["status"])
            dates = args["date"]
            jmsg = {}
            try:
                start_date = dates[0].strftime("%Y%m%d(%H:%M)")
                end_date = dates[1].strftime("%Y%m%d(%H:%M)")
            except BaseException:
                jmsg["message"] = "wrong dates format. %Y%m%d(%H:%M) is needed"
                jmsg["status"] = "err"
                self.logger.doLog(
                    "served '/maintenance/%s' [err: wrong date format]" %
                    (cluster))
                state = 400
                return (jsonify(jmsg), state)
            if start_date > end_date:
                jmsg["message"] = "maintenance start date is after end date!"
                jmsg["status"] = "err"
                self.logger.doLog(
                    "served '/maintenance/%s' [err: start date after end date]"
                    % (cluster)
                )
                state = 400
                return (jsonify(jmsg), state)
            validFlag = True
            # check if the cluster actually exist
            validFlag = self.check_cluster_name(cluster)
            if not validFlag:
                # cluster does not exist -- return an error
                jmsg["message"] = "cluster '%s' does not exist" % (cluster)
                jmsg["status"] = "err"
                self.logger.doLog(
                    "served '/maintenance/%s' [err: cluster does not exist]" %
                    (cluster))
                state = 400
                return (jsonify(jmsg), state)
            else:
                # cluster exist, so try to insert the date in the maintenance
                # database
                self.testDbActive()
                if self.db_active3:
                    db_body = [
                        {
                            "measurement": "systemMaintenance",
                            "tags": {"cluster": cluster, "id": self.idx3},
                            "fields": {
                                "start_maintenance": start_date,
                                "end_maintenance": end_date,
                            },
                        }
                    ]
                    self.idx3 += 1
                    ret = self.idb_c3.write_points(db_body)
                    if ret:
                        jmsg["status"] = "ok"
                        self.logger.doLog(
                            "served '/maintenance/%s' (start_maintenance: %s / end_maintenance: %s) [ok]" %
                            (cluster, start_date, end_date))
                    else:
                        jmsg["status"] = "err"
                        self.logger.doLog(
                            "served '/maintenance/%s' (start_maintenance: %s / end_maintenance: %s) [failed]" %
                            (cluster, start_date, end_date))
                        state = 400
                else:
                    jmsg["message"] = "Maintenance DB not found"
                    jmsg["status"] = "err"
                    self.logger.doLog(
                        "served '/maintenance/%s' [err: maintenance DB does not exist]" %
                        (cluster))
                    state = 400
                return (jsonify(jmsg), state)

        # remove the maintenance for a given machine --- TBD
        @self.service.route("/maintenance/remove/<string:cluster>",
                            methods=["DELETE"])
        def delete_programmed_maintenance(cluster):
            state = 200
            parser = reqparse.RequestParser()
            parser.add_argument("timestamp", type=str, required=True)
            parser.add_argument(
                "Authorization", type=str, required=True, location="headers"
            )
            args = parser.parse_args()
            auth_res = self.auth(args["Authorization"])
            if auth_res["status"] != 200 or auth_res["jmsg"] == "not active":
                self.logger.doLog(
                    "served '/maintenance/remove/<string:cluster>' [auth err: -- status (%d), msg (%s)]" %
                    (auth_res["status"], auth_res["jmsg"]))
                return (jsonify(auth_res["jmsg"]), auth_res["status"])
            timestamp = int(args["timestamp"])
            jmsg = {}
            self.testDbActive()
            validFlag = True
            validFlag = self.check_cluster_name(cluster)
            # check if the database is online and the cluster name exists
            if (self.db_active3) and (validFlag):
                ret = self.idb_c3.query(
                    'DELETE FROM "systemMaintenance" WHERE time = %d' %
                    (timestamp))
                jmsg["message"] = "maintenance date removed for cluster '%s'" % (
                    str(cluster))
                self.logger.doLog(
                    "served '/maintenance/remove/%s' [ok]" %
                    (cluster))
                jmsg["status"] = "ok"
            else:
                if validFlag:
                    jmsg[
                        "message"
                    ] = "cluster '%s' is not present in the allocator list" % (
                        str(cluster)
                    )
                    jmsg["status"] = "war"
                    self.logger.doLog(
                        "served '/maintenance/remove/%s' [war: cluster is not valid]" %
                        (cluster))
                else:
                    jmsg["message"] = "the database is not available"
                    jmsg["status"] = "err"
                    self.logger.doLog(
                        "served '/maintenance/remove/%s' [err: database is not available]" %
                        (cluster))
            return (jsonify(jmsg), state)

        # evaluate the available machines
        @self.service.route("/evaluate/machines", methods=["POST"])
        def request_site_benchmark():
            state = 200
            jmsg = {}
            parser = reqparse.RequestParser()
            parser.add_argument("type", type=str, required=True)
            parser.add_argument(
                "Authorization", type=str, required=True, location="headers"
            )
            args = parser.parse_args()
            auth_res = self.auth(args["Authorization"])
            if auth_res["status"] != 200 or auth_res["jmsg"] == "not active":
                self.logger.doLog(
                    "served '/evaluate/machines' [auth err: -- status (%d), msg (%s)]" %
                    (auth_res["status"], auth_res["jmsg"]))
                return (jsonify(auth_res["jmsg"]), auth_res["status"])
            token = self.exchange_token(args["Authorization"].split()[1])
            if token["access_token"] is None or token["refresh_token"] is None:
                jmsg["message"] = "error when exchanging tokens"
                jmsg["status"] = "err"
                self.logger.doLog(
                    "served '/evaluate/machines' [err: error when exchanging tokens ]"
                )
                state = 500
                return (jsonify(jmsg), state)
            if (args["type"] != "hpc") and (args["type"] != "cloud"):
                jmsg["message"] = "wrong input parameter(s)"
                jmsg["status"] = "err"
                self.logger.doLog(
                    "served '/evaluate/machines' [err: wrong input parameter(s)"
                )
                state = 400
                return (jsonify(jmsg), state)
            elif args["type"] == "cloud":
                schema = CloudSchema()
            else:
                schema = HPCSchema()
            if not request.is_json:
                jmsg["message"] = "wrong body type (not json)"
                jmsg["status"] = "err"
                self.logger.doLog(
                    "served '/evaluate/machines' [err: wrong body type (not json)]"
                )
                state = 400
                return (jsonify(jmsg), state)
            request.get_json()
            request_data = request.json
            try:
                result = schema.load(request_data, unknown=EXCLUDE)
            except ValidationError as err:
                jmsg["message"] = "wrong input parameter(s)"
                jmsg["status"] = "err"
                return jsonify(err.messages, 400)
            data_now_json_str = dumps(result)
            a_dict = loads(data_now_json_str)
            a_dict["type"] = args["type"]
            uid = self.platform.new_job_req()
            jmsg["uid"] = uid
            a_dict["job_id"] = uid
            self.platform.evaluate(a_dict, token)
            jmsg["message"] = "evaluation ongoing for all the machines"
            jmsg["status"] = "ok"
            self.logger.doLog(
                "served '/evaluate/machines' [ok] -- type (%s), job_id (%s)]"
                % (args["type"], a_dict["job_id"])
            )
            if not self.db_active2:
                jmsg[
                    "message"
                ] = "the database ('%s') is not active or has been deleted" % (
                    self.db_name_2
                )
                jmsg["status"] = "err"
                self.logger.doLog(
                    "served '/evaluate/machines' [err: database '%s' is not active or has been deleted]" %
                    (self.db_name_2))
                state = 400
            return (jsonify(jmsg), state)

        # retrieve the best 'number' machines where to run the job

        @self.service.route("/job/rank/<int:job_id>", methods=["GET"])
        def show_site_rank(job_id):
            state = 200
            parser = reqparse.RequestParser()
            parser.add_argument("type", type=str)
            parser.add_argument("no_points", type=int)
            parser.add_argument(
                "Authorization", type=str, required=True, location="headers"
            )
            args = parser.parse_args()
            jmsg = {}
            jpt = {}
            auth_res = self.auth(args["Authorization"])
            if auth_res["status"] != 200 or auth_res["jmsg"] == "not active":
                self.logger.doLog(
                    "served '/job/rank/<int:job_id>' [auth err: -- status (%d), msg (%s)]" %
                    (auth_res["status"], auth_res["jmsg"]))
                return (jsonify(auth_res["jmsg"]), auth_res["status"])
            if not self.db_active2:
                jmsg["message"] = "error using the machineEvaluation database"
                jmsg["status"] = "err"
                state = 400
                return (jsonify(jmsg), state)
            query = "SELECT * FROM machineEvaluation"
            rs2 = self.idb_c2.query(query)
            points = list(
                rs2.get_points(
                    measurement="machineEvaluation",
                    tags={"job_id": str(job_id), "job_type": args["type"]},
                )
            )
            barrier = args["no_points"]
            if barrier > len(points):
                barrier = len(points)
            for i in range(0, barrier):
                jpt[
                    i
                ] = "time (%s): job_id (%s) with type '%s' got evaluation <%s>\n" % (
                    points[i]["time"],
                    points[i]["job_id"],
                    points[i]["job_type"],
                    points[i]["rank"],
                )
                i += 1
            jmsg["rank"] = jpt
            jmsg["message"] = "served '/get/rank/%d' [ok]" % (job_id)
            jmsg["status"] = "ok"
            return (jsonify(jmsg), state)

        # retrieve the best 'number' machines where to run the job

        @self.service.route("/get/machines/<job_id>", methods=["GET"])
        def request_site_list(job_id):
            state = 200
            jmsg = {}
            parser = reqparse.RequestParser()
            parser.add_argument(
                "Authorization", type=str, required=True, location="headers"
            )
            args = parser.parse_args()
            auth_res = self.auth(args["Authorization"])
            if auth_res["status"] != 200 or auth_res["jmsg"] == "not active":
                self.logger.doLog(
                    "served '/get/machines/<job_id>' [auth err: -- status (%d), msg (%s)]" %
                    (auth_res["status"], auth_res["jmsg"]))
                return (jsonify(auth_res["jmsg"]), auth_res["status"])
            if not self.platform.job_id_exists(job_id):
                jmsg["message"] = []
                jmsg[
                    "status"
                ] = "err. job ID is not valid -- call evaluation endpoint first"
                self.logger.doLog(
                    "served '/get/machines/%s' [err: %s is not a valid ID for a job]" %
                    (job_id, job_id))
                state = 400
            else:
                if self.platform.get_job_info(job_id)["status"] != "done":
                    jmsg["message"] = []
                    jmsg["status"] = (
                        self.platform.get_job_info(job_id)["status"]
                        + ". "
                        + self.platform.get_job_info(job_id)["msg"]
                    )
                else:
                    best_machines = self.platform.get_best_machines(job_id)
                    if len(best_machines) == 0:
                        jmsg["message"] = []
                        jmsg[
                            "status"
                        ] = "No available/compatible location. Try again later."
                    else:
                        jmsg["message"] = best_machines
                        jmsg["status"] = "ok"
                    self.logger.doLog(
                        "served '/get/machines/%s' [ok]" %
                        (job_id))
            return (jsonify(jmsg), state)

        # remove the request for machine evaluation (by job  ID)

        @self.service.route(
            "/evaluate/machines/remove/<int:job_id>", methods=["DELETE"]
        )
        def remove_job_benchmark(job_id):
            state = 200
            jmsg = {}
            parser = reqparse.RequestParser()
            parser.add_argument(
                "Authorization", type=str, required=True, location="headers"
            )
            args = parser.parse_args()
            auth_res = self.auth(args["Authorization"])
            if auth_res["status"] != 200 or auth_res["jmsg"] == "not active":
                self.logger.doLog(
                    "served '/evaluate/machines' [auth err: -- status (%d), msg (%s)]" %
                    (auth_res["status"], auth_res["jmsg"]))
                return (jsonify(auth_res["jmsg"]), auth_res["status"])
            if not self.platform.job_id_exists(job_id):
                jmsg["message"] = "job ID is not valid"
                jmsg["status"] = "err"
                self.logger.doLog(
                    "served '/evaluate/machines/remove/%d [err: %d is not a valid Id for a job]'" %
                    (job_id, job_id))
                state = 400
            else:
                self.platform.remove_job(job_id)
                jmsg["message"] = "job (ID = %d) correctly removed" % (
                    int(job_id))
                jmsg["status"] = "ok"
                self.logger.doLog(
                    "served '/evaluate/machines/remove/%d [ok]'" % (job_id)
                )
            return (jsonify(jmsg), state)
