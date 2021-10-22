# import modules and libraries
import time
import json
import time
from datetime import timedelta
from flask import Flask as flask
from api import APIRest as apirest
from influxdb import InfluxDBClient
from config import LXMconfig as lxconf
from lxmlog import LXMlog as lxmlog
import os

# global variables
api_script_path = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(api_script_path, '..', 'config', 'lxm.conf')
log_path = os.path.join(api_script_path, '..', 'logs', 'lxm.log')

# define main function


def main():
    try:
        # get the configuration object and initialize it with deaful values
        lxc = lxconf()
        # get the configuration
        lxc.getConfiguration(config_path)
        # if debug is enable then print out the current configuration
        lxc.postConfiguration()
        # get a logger object
        logger = lxmlog()
        # open the logger file
        logger.openLog(log_path)
        # start logging messages
        logger.doLog("lxmonitoring logger is running")
        # get a influx DB client
        lxc.idb_c1 = InfluxDBClient(
            host=lxc.lxm_conf["influx_server"],
            port=lxc.lxm_conf["influx_port"])
        lxc.idb_c2 = InfluxDBClient(
            host=lxc.lxm_conf["influx_server"],
            port=lxc.lxm_conf["influx_port"])
        lxc.idb_c3 = InfluxDBClient(
            host=lxc.lxm_conf["influx_server"],
            port=lxc.lxm_conf["influx_port"])
        #idbc = InfluxDBClient(host=lxm_conf["influx_server"], port=lxm_conf["influx_port"], database=lxm_conf["lxm_db"])
        # if hard startup is enable then the local influx databases are dropped (forced action)
        # before starting the application
        if(lxc.lxm_conf["hard_startup"]):
            logger.doLog("hard-startup mode is active")
            logger.doLog(
                "database '%s' is dropped before starting application" %
                (lxc.lxm_conf["lxm_db1"]))
            lxc.idb_c1.drop_database(lxc.lxm_conf["lxm_db1"])
            logger.doLog(
                "database '%s' is dropped before starting application" %
                (lxc.lxm_conf["lxm_db2"]))
            lxc.idb_c2.drop_database(lxc.lxm_conf["lxm_db2"])
            logger.doLog(
                "database '%s' is dropped before starting application" %
                (lxc.lxm_conf["lxm_db3"]))
            lxc.idb_c3.drop_database(lxc.lxm_conf["lxm_db3"])
        # log the hard exit action (it will be ignored if application is
        # running in background and it will be killed)
        if (lxc.lxm_conf["hard_exit"]):
            logger.doLog(
                "hard-exit mode is active (the DB will be dropped when closing)")
        logger.doLog("getting an influxDB client instance")
        #
        # get the first handle of the influx db through the client 1
        dbs = None
        dbs = lxc.idb_c1.get_list_database()
        i = 0
        # check if the database is already created
        db_exist = False
        for entry in dbs:
            if (entry['name'] == lxc.lxm_conf["lxm_db1"]):
                logger.doLog(
                    "database ('%s') has already been created" %
                    (lxc.lxm_conf["lxm_db1"]))
                db_exist = True
                break
            if (lxc.lxm_conf["debug"] == 1):
                print("  [%-2d]: %s" % (i, str(entry['name'])))
            i += 1
        # if it does not exist  it goes to create it
        if (db_exist != True):
            logger.doLog(
                "creating the database ('%s')" %
                (lxc.lxm_conf["lxm_db1"]))
            lxc.idb_c1.create_database(lxc.lxm_conf["lxm_db1"])
            # check if the creation is gone well
            dbs = lxc.idb_c1.get_list_database()
            db_exist = False
            for entry in dbs:
                if (entry['name'] == lxc.lxm_conf["lxm_db1"]):
                    logger.doLog("db created correctly")
                    db_exist = True
                    break
        if (db_exist == False):
            logger.doLog("error during database creation")
            print("  (err): unabale to create the database -- abort")
            return (1)
        # get the second hanlde of the influx db through the client 2
        dbs = None
        dbs = lxc.idb_c2.get_list_database()
        i = 0
        # check if the database is already created
        db_exist = False
        for entry in dbs:
            if (entry['name'] == lxc.lxm_conf["lxm_db2"]):
                logger.doLog(
                    "database ('%s') has already been created" %
                    (lxc.lxm_conf["lxm_db2"]))
                db_exist = True
                break
            if (lxc.lxm_conf["debug"] == 1):
                print("  [%-2d]: %s" % (i, str(entry['name'])))
            i += 1
        # if it does not exist  it goes to create it
        if (db_exist != True):
            logger.doLog(
                "creating the database ('%s')" %
                (lxc.lxm_conf["lxm_db2"]))
            lxc.idb_c2.create_database(lxc.lxm_conf["lxm_db2"])
            # check if the creation is gone well
            dbs = lxc.idb_c2.get_list_database()
            db_exist = False
            for entry in dbs:
                if (entry['name'] == lxc.lxm_conf["lxm_db2"]):
                    logger.doLog("db created correctly")
                    db_exist = True
                    break
        if (db_exist == False):
            logger.doLog("error during database creation")
            print("  (err): unabale to create the database -- abort")
            return (1)
        # get the third hanlde of the influx db through the client 3
        dbs = None
        dbs = lxc.idb_c3.get_list_database()
        i = 0
        # check if the database is already created
        db_exist = False
        for entry in dbs:
            if (entry['name'] == lxc.lxm_conf["lxm_db3"]):
                logger.doLog(
                    "database ('%s') has already been created" %
                    (lxc.lxm_conf["lxm_db3"]))
                db_exist = True
                break
            if (lxc.lxm_conf["debug"] == 1):
                print("  [%-2d]: %s" % (i, str(entry['name'])))
            i += 1
        # if it does not exist  it goes to create it
        if (db_exist != True):
            logger.doLog(
                "creating the database ('%s')" %
                (lxc.lxm_conf["lxm_db3"]))
            lxc.idb_c3.create_database(lxc.lxm_conf["lxm_db3"])
            # check if the creation is gone well
            dbs = lxc.idb_c3.get_list_database()
            db_exist = False
            for entry in dbs:
                if (entry['name'] == lxc.lxm_conf["lxm_db3"]):
                    logger.doLog("db created correctly")
                    db_exist = True
                    break
        if (db_exist == False):
            logger.doLog("error during database creation")
            print("  (err): unabale to create the database -- abort")
            return (1)
        #
        # switching on the new database
        logger.doLog("switching on the new influx database(s)")
        lxc.idb_c1.switch_database(lxc.lxm_conf["lxm_db1"])
        lxc.idb_c2.switch_database(lxc.lxm_conf["lxm_db2"])
        lxc.idb_c3.switch_database(lxc.lxm_conf["lxm_db3"])
        # run the backend
        service = flask(__name__)
        api = apirest(service, logger, lxc)
        api.manageRoutes()
        logger.doLog("launching backend service engine")
        logger.forceFlush()
        service.run(
            host=lxc.lxm_conf["service_ip"],
            port=lxc.lxm_conf["service_port"])
        # perform some action after the application completed
        lxc.idb_c1.close()
        lxc.idb_c2.close()
        lxc.idb_c3.close()
        logger.doLog("closing the database (%s)" % (lxc.lxm_conf["lxm_db1"]))
        logger.forceFlush()
        logger.doLog("closing the database (%s)" % (lxc.lxm_conf["lxm_db2"]))
        logger.forceFlush()
        logger.doLog("closing the database (%s)" % (lxc.lxm_conf["lxm_db3"]))
        logger.forceFlush()
        if (lxc.lxm_conf["hard_exit"]):
            logger.doLog(
                "removing the streaming database (%s)" %
                (lxc.lxm_conf["lxm_db1"]))
            logger.doLog(
                "removing the streaming database (%s)" %
                (lxc.lxm_conf["lxm_db2"]))
            logger.doLog(
                "removing the streaming database (%s)" %
                (lxc.lxm_conf["lxm_db3"]))
            lxc.idb_c1.drop_database(lxc.lxm_conf["lxm_db1"])
            lxc.idb_c2.drop_database(lxc.lxm_conf["lxm_db2"])
            lxc.idb_c3.drop_database(lxc.lxm_conf["lxm_db3"])
        logger.closeLog()

    except Exception as exc:
        print(" (err): error while starting the backend:", end="")
        print(" ", str(exc))
        return (1)


# running the web backend server
if __name__ == "__main__":
    main()
