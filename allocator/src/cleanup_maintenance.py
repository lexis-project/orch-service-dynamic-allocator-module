# inport libraries and modules
from influxdb import InfluxDBClient
from config import LXMconfig as lxconf
import datetime
import os
import subprocess as cli


# constants
api_script_path = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(api_script_path, "..", "config", "lxm.conf")


# main function
def main():
    # get the configuration
    lxc = lxconf()
    lxc.getConfiguration(config_path)
    # get a valid influxdb client
    idbc = InfluxDBClient(
        host=lxc.lxm_conf["influx_server"], port=lxc.lxm_conf["influx_port"]
    )
    # check if the database is available
    dbs = idbc.get_list_database()
    i = 0
    # check if the database is available created
    db_exist = False
    for entry in dbs:
        if entry["name"] == lxc.lxm_conf["lxm_db3"]:
            db_exist = True
            break
        i += 1
    # if it does not exist then exit
    if not db_exist:
        return 0
    # otherwise switch to the database
    else:
        idbc.switch_database(lxc.lxm_conf["lxm_db3"])
    # check if the cleanup has been enabled
    if lxc.lxm_conf["cleanup_maintenance"] == 1:
        # call the external executable script to get the list of entries
        ret = cli.run(["./get_maintenance.sh", "ALL"], stdout=False)
        with open("../dbs/lxm_cleanup_date.txt") as fp:
            dump = fp.readlines()
        dump = [entry.strip() for entry in dump]
        for line in dump:
            # fields = line.strip()
            fields = line.split()
            # check if the end_maintenance is before current date:
            # fields[0] => is the unique timestamp as provided by influxdb
            # fields[2] => is the end_maintenance date string
            today = datetime.datetime.today()
            endm = datetime.datetime.strptime(fields[2], "%Y%m%d(%H:%M)")
            if endm < today:
                # remove the entry in the influxdb table
                idbc.query(
                    'DELETE FROM "systemMaintenance" WHERE time = %d' % (int(fields[0]))
                )
        # eventually remove the temporary file
        ret = cli.run(["rm", "../dbs/lxm_cleanup_date.txt"], stdout=False)


# running the web backend server
if __name__ == "__main__":
    main()
