# import  libraries and modules
# provide a class storing the configuration of the back-end engine
class LXMconfig:
    # constructor of the class
    def __init__(self):
        self.idb_c1 = None
        self.idb_c2 = None
        self.idb_c3 = None
        self.lxm_conf = dict()
        self.program_name = "lx_allocator"
        self.lxm_conf["header_request_id"] = 0
        self.lxm_conf["header_forwarded_for"] = 1
        self.lxm_conf["service_ip"] = "0.0.0.0"
        self.lxm_conf["service_port"] = 9000
        self.lxm_conf["influx_server"] = "0.0.0.0"
        self.lxm_conf["influx_port"] = 8086
        self.lxm_conf["debug"] = 1
        self.lxm_conf["hard_exit"] = True
        self.lxm_conf["hard_startup"] = True
        self.lxm_conf["lxm_db1"] = "lxm_ddi_performance"
        self.lxm_conf["lxm_db2"] = "lxm_allocation"
        self.lxm_conf["lxm_db3"] = "lxm_maintenance"
        self.lxm_conf["cleanup_maintenance"] = 1
        self.lxm_conf["backend_URL"] = None
        self.lxm_conf["keycloak_URL"] = None
        self.lxm_conf["KC_REALM"] = None
        self.lxm_conf["KC_CLID"] = None
        self.lxm_conf["KC_SECRET"] = None
        self.lxm_conf["heappe_middleware_available"] = 0
        self.lxm_conf["openstack_available"] = 0
        self.lxm_conf["hpc_centers"] = None
        self.lxm_conf["heappe_service_URLs"] = None
        self.lxm_conf["transfer_sizes"] = ""
        self.lxm_conf["transfer_speeds"] = ""

    #  define configuration routines
    def getConfiguration(self, conf_path):
        config = conf_path
        conf = open(config, "r")
        go = True
        while go:
            line = conf.readline()
            if line == "":
                go = False
            else:
                # the character '#' is used to put a line comment in the
                # configuration file
                if (line[0] == "#") or (line[0] == "\n") or (line[0] == "\t"):
                    continue
                fields = line.split("=")
                param = str(fields[0])
                value = str(fields[1])
                param = param.strip("\n ")
                value = value.strip("\n ")
                # parse the file and create the configuration dictionary
                if param == "influx_server":
                    self.lxm_conf["influx_server"] = value
                elif param == "influx_port":
                    self.lxm_conf["influx_port"] = int(value)
                elif param == "debug":
                    self.lxm_conf["debug"] = int(value)
                elif param == "service_ip":
                    self.lxm_conf["service_ip"] = value
                elif param == "service_port":
                    self.lxm_conf["service_port"] = int(value)
                elif param == "influx_db1":
                    self.lxm_conf["lxm_db1"] = value
                elif param == "influx_db2":
                    self.lxm_conf["lxm_db2"] = value
                elif param == "influx_db3":
                    self.lxm_conf["lxm_db3"] = value
                elif param == "cleanup_maintenance":
                    self.lxm_conf["cleanup_maintenance"] = int(value)
                elif param == "header_request_id":
                    self.lxm_conf["header_request_id"] = int(value)
                elif param == "header_forwarded_for":
                    self.lxm_conf["header_forwarded_for"] = int(value)
                elif param == "hard_exit":
                    if int(value) == 1:
                        self.lxm_conf["hard_exit"] = True
                    else:
                        self.lxm_conf["hard_exit"] = False
                elif param == "hard_startup":
                    if int(value) == 1:
                        self.lxm_conf["hard_startup"] = True
                    else:
                        self.lxm_conf["hard_startup"] = False
                elif param == "hpc_centers":
                    self.lxm_conf["hpc_centers"] = value
                elif param == "transfer_sizes":
                    self.lxm_conf["transfer_sizes"] = value
                elif param == "transfer_speeds":
                    self.lxm_conf["transfer_speeds"] = value
                elif param == "heappe_middleware_available":
                    self.lxm_conf["heappe_middleware_available"] = int(value)
                elif (
                    param == "heappe_service_URLs"
                    and self.lxm_conf["heappe_middleware_available"] == 1
                ):
                    self.lxm_conf["heappe_service_URLs"] = value
                elif param == "openstack_available":
                    self.lxm_conf["openstack_available"] = int(value)
                elif param == "backend_URL":
                    self.lxm_conf["backend_URL"] = value
                elif param == "keycloak_URL":
                    self.lxm_conf["keycloak_URL"] = value
                elif param == "KC_REALM":
                    self.lxm_conf["KC_REALM"] = value
                elif param == "KC_CLID":
                    self.lxm_conf["KC_CLID"] = value
                elif param == "KC_SECRET":
                    self.lxm_conf["KC_SECRET"] = value
                else:
                    print(" error - unrecognized option (%s)" % (param))
        conf.close()
        return 0

    # print out the current configuration
    def postConfiguration(self):
        if self.lxm_conf["debug"] == 1:
            print("  --------------------------------------------------------")
            print("  Backend Service Configuration:")
            print("  --------------------------------------------------------")
            if self.lxm_conf["hard_exit"]:
                print("  hard-exit mode    : True")
            else:
                print("  hard-exit mode    : False")
            if self.lxm_conf["hard_startup"]:
                print("  hard-startup mode : True")
            else:
                print("  hard-startup mode : False")
            print("  debug mode        : %d" % (self.lxm_conf["debug"]))
            print("  service address   : %s " % (self.lxm_conf["service_ip"]))
            print("  service port      : %-5d " % (self.lxm_conf["service_port"]))
            print("  influxDB address  : %s " % (self.lxm_conf["influx_server"]))
            print("  influxDB port     : %-5d " % (self.lxm_conf["influx_port"]))
            print("  db_client_1       : %s " % (self.lxm_conf["lxm_db1"]))
            print("  db_client_2       : %s " % (self.lxm_conf["lxm_db2"]))
            print("  db_client_3       : %s " % (self.lxm_conf["lxm_db3"]))
            print("  --------------------------------------------------------")
        # executing some action before serving the routes (initialization)
        if self.lxm_conf["debug"] == 1:
            print("  (dbg) the webapp is more verbose to support debugging")
        return 0
