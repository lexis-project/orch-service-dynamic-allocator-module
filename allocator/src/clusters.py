# libraries and modules
import json
import requests
import lxmlog
import datetime
import uuid
import re
import math
from HeappeClient import HeappeClient
from OpenStackClient import OpenStackClient, compare_image_names

# define a class storing information on the available sites (supercomputing centers)
class Clusters(object):
    # hasing of the input object to speedup its search on a dictionary
    __hash__ = object.__hash__

    # class constructor
    def __init__(self, logger, lxc, api):
        self.logger = logger
        self.api = api
        self.job_list = dict()
        self.default_transfer_speeds = dict()
        self.heappe = lxc.lxm_conf["heappe_middleware_available"]
        self.openstack = lxc.lxm_conf["openstack_available"]
        self.backend_URL = lxc.lxm_conf["backend_URL"]
        self.transfer_sizes = lxc.lxm_conf["transfer_sizes"].split(',')
        self.transfer_speeds = lxc.lxm_conf["transfer_speeds"].split(',')
        tsizes_list_len = len(self.transfer_sizes)
        tspeeds_list_len = len(self.transfer_speeds)
        if (tsizes_list_len != tspeeds_list_len):
            self.logger.doLog("WARNING: the numbers of items in the 'transfer_sizes' and 'transfer_speeds' lists are not equal")
            if (tsizes_list_len < tspeeds_list_len):
                list_len = tsizes_list_len
            else:
                list_len = tspeeds_list_len
        else:
            list_len = tspeeds_list_len
        ids = range(list_len)
        for i in ids:
            self.default_transfer_speeds[self.transfer_sizes[i]] = self.transfer_speeds[i]
        self.center_list = lxc.lxm_conf["hpc_centers"].split(',')
        self.heappe_URLs = lxc.lxm_conf["heappe_service_URLs"].split(',')
        if (len(self.center_list) != len(self.heappe_URLs)):
            self.logger.doLog("WARNING: the numbers of items in the 'hpc_centers' and 'heappe_URLs' lists are not equal")
        self.clusters_info = {}
        for idx,center in enumerate(self.center_list):
            self.clusters_info[center] = {}
            if (self.heappe == True):
                if idx >= len(self.heappe_URLs):
                    self.clusters_info[center]['hpc'] = HeappeClient("", self.logger)
                else:
                    self.clusters_info[center]['hpc'] = HeappeClient(self.heappe_URLs[idx], self.logger)
                if self.clusters_info[center]['hpc'].info_dict != False:
                    self.clusters_info[center]['hpc'].set_center(center)
                    self.logger.doLog("Got Heappe instance for %s" %(center))
                else:
                    self.logger.doLog("Could not connect to Heappe instance for %s" %(center))
                    del self.clusters_info[center]['hpc']
            if (self.openstack == True):
                self.clusters_info[center]['cloud'] = OpenStackClient(center, self.logger)
        self.update_clusters_info()
        if len(self.clusters_list) == 0:
            self.logger.doLog("WARNING: no cluster attached!")

    # class methods
    # add new job list element
    def new_job_req(self):
        uid = str(uuid.uuid4())
        self.job_list[uid] = dict()
        self.job_list[uid]['status'] = "ongoing"
        self.job_list[uid]['msg'] = ""
        return uid

    #update cluster name
    def update_clusters_info(self):
        self.clusters_list = []
        for center in self.center_list:
                for resource in self.clusters_info[center]:
                    if resource == 'hpc':
                        for machine in self.clusters_info[center][resource].clusters():
                            self.clusters_list.append(center + '_' + machine)
                    elif resource == 'cloud':
                        self.clusters_list.append(center + '_cloud')

    def get_maintenance_dates(self, center, machine, now):
        dates = self.api.get_programmed_maintenance(center + "_" + machine)
        if dates[1] != 200:
            False
        else:
            dates = dates[0]['message']
        ret = []
        if isinstance(dates, list):
            for date in dates:
                end = datetime.datetime.strptime(date['end_maintenance'], "%Y%m%d(%H:%M)")
                if now > end:
                    start = datetime.datetime.strptime(date['start_maintenance'], "%Y%m%d(%H:%M)")
                    ret.append((start, end))
        return ret

    # get the list of scheduled jobs
    def get_job_list(self):
        return self.job_list

    # check token validation and eventually refresh
    def check_refresh_token(self, token):
        auth_res = self.api.auth("Bearer " + token['access_token'])
        if auth_res['status'] != 200:
            return False
        elif auth_res['jmsg'] == "not active":
            new_token = self.api.refresh_token(token['access_token'])
            if new_token['access_token'] is None or new_token['refresh_token'] is None:
                return False
            else:
                token['access_token'] = new_token['access_token']
                token['refresh_token'] = new_token['refresh_token']
        return True
        

    def get_av_speed(self, sizeXfile, dest_zone, src_zone, default = False):
        check = True
        if default:
            check = False
        else:
            resp = self.api.get_speed_perf(src_zone, dest_zone, sizeXfile)
            if resp[1] != 200 or len(resp[0]['message']) == 0:
                check = False
        if not check:
            if not default:
                self.logger.doLog("WARNING: no netperf info on DB for source %s and destination %s, using default values" %(src_zone, dest_zone))
            dif = 999999999999999
            perf = 0
            for item in self.default_transfer_speeds.keys():
                if dif > abs(float(item) - sizeXfile):
                    perf = float(self.default_transfer_speeds[item])
                    dif = abs(float(item) - sizeXfile)
        else:
            perf = resp[0]['message'][0]['performance']
        return perf

    # check the data origins site with best 
    def get_best_origin(self, sizeXfile, dest_zone, origin_zones, default = False):
        origin_speed = 0
        origin = ""
        for zone in origin_zones:
            zone_name = zone.replace("_iRODS", "")
            if zone_name == dest_zone:
                return 0, zone_name
            speed = self.get_av_speed(sizeXfile, dest_zone, zone_name, default = default)
            if speed == 0:
                return False, False
            if speed > origin_speed:
                origin = zone_name
                origin_speed = speed
        return origin_speed, origin

    # compute data transf mark
    def data_transf(self, data_params, centerName, default = False):
        tot_transf_time = 0
        origins = []
        for data in data_params:
            sizeXfile = data['size']/data['numberOfFiles']
            if default:
                speed = self.get_av_speed(sizeXfile, "", "", default = default)
                orig = ""
            else:
                speed, orig = self.get_best_origin(sizeXfile, centerName, data['locations'], default = default)
            if orig == False:
                return False, False
            origins.append(orig+ "_iRODS")
            if speed > 0:
                tot_transf_time += data['size']/speed
        return tot_transf_time, origins

    # weighted criteria mean for HPC clusters
    def HPC_weighted_criteria_mean(self, job_args, center, heappe_endpoint, hpc_project, token):
        if self.check_refresh_token(token) == False:
            return False
        if center.get_heappe(heappe_endpoint, token) == False:
            return False
        for cluster in center.av_clusters():
            av_queues = center.check_template_queues(cluster, job_args['taskName'])
            if len(av_queues) == 0:
                continue
            if self.check_refresh_token(token) == False:
                return False
            if "resubmit" not in job_args.keys():
                job_args['resubmit'] = False
            queue_status = center.update_cluster(cluster, "testuser", token['access_token'], job_args['resubmit'])
            if queue_status == False:
                return False
            if len(queue_status) == 0:
                continue
            res = dict()
            res['dest'] = dict()
            res['dest']['location'] = center.name
            res['dest']['cluster_id'] = center.get_av_cluster_id(cluster)
            res['dest']['HEAppE_URL'] = heappe_endpoint
            res['dest']['project'] = hpc_project
            res['dest']['tasks'] = []
            task = dict()
            task['name'] = job_args['taskName']
            if res['dest']['cluster_id'] < 0:
                continue
            total_max_time, origins = self.data_transf(job_args['storage_inputs'], center.name)
            if origins == False:
                return False
            else:
                data_tranf_score = 1 - (total_max_time / (total_max_time + job_args['max_walltime']))
            check = True
            for maintenance in self.get_maintenance_dates(center.name, cluster, datetime.datetime.now()):
                if datetime.datetime.now() <= maintenance[1] and datetime.datetime.now() >= maintenance[0]:
                    check = False
                    break
                elif datetime.datetime.now() + datetime.timedelta(seconds=total_max_time + job_args['max_walltime']) >= maintenance[0] and datetime.datetime.now() <= maintenance[0]:
                    check = False
                    break
            if not check:
                continue
            res['dest']['storage_inputs'] = origins
            for queue in av_queues:
                if queue['Name'] not in queue_status:
                    continue
                task['cluster_node_type_id'] = queue['Id']
                task['command_template_id'] = queue['template_id']
                res['dest']['tasks'].append(task)
                #computing the mean for the queue
                nodes_req_perc, occupation = center.compute_occupation(cluster, queue['Name'], job_args['max_cores'])
                values = []
                values.append(1 - occupation)
                values.append(1 - nodes_req_perc)
                if len(job_args['storage_inputs']) != 0:
                    values.append(data_tranf_score)
                self.logger.doLog(values)
                res['mean'] = 0
                for value in values:
                    if value >= 0:
                        res['mean'] += value
                    else:
                        self.logger.doLog("score < 0. Values = ")
                        self.logger.doLog(values)
                        continue
                res['mean'] /= len(values)
                if res['mean'] <= 0:
                    continue
                self.job_list[job_args['job_id']]['val'].append(res)
            #End of the loop over queue types
        return True


    # weighted criteria mean for Cloud clusters
    def Cloud_weighted_criteria_mean(self, job_args, center, openstack_endpoint, openstack_netip, heappe_endpoint, cloud_project, token):
        res = dict()
        res['dest'] = dict()
        res['dest']['location'] = center.name
        res['dest']['NetworkIP'] = openstack_netip
        res['dest']['OpenStack_URL'] = openstack_endpoint
        res['dest']['HEAppE_URL'] = heappe_endpoint
        res['dest']['project'] = cloud_project
        if self.check_refresh_token(token) == False:
            return False
        info_dict = center.auth_and_update(openstack_endpoint, "test_user", token['access_token'], heappe_url=heappe_endpoint)
        if info_dict == False:
            self.logger.doLog("Openstack auth failed/unreachable for center %s" %(center.name))
            return False
        check = True
        for maintenance in self.get_maintenance_dates(center.name, "cloud", datetime.datetime.now()):
            if datetime.datetime.now() <= maintenance[1] and datetime.datetime.now() >= maintenance[0]:
                check = False
                break
        if not check:
            return False
        if info_dict['network']['floatingip']['limit'] - info_dict['network']['floatingip']['used'] <= 0 and info_dict['network']['floatingip']['limit'] >= 0:
            self.logger.doLog("Floating IP quota reached!")
            return False
        # Check image
        image_available = False
        for image, image_features in info_dict['images'].items():
            if compare_image_names(job_args['os_version'], image):
                image_available = True
                res['dest']['image_id'] = image_features['id']
                break
        if image_available == False:
            self.logger.doLog("OS image version not available.")
            return False
        # Match flavour
        selected_flavour = None
        selected_features = [100000000,10000]
        temp_selected = selected_features
        for flavour, flavour_features in info_dict['flavours'].items():
            if flavour_features['ram'] >= job_args['mem'] and flavour_features['vcpus'] >= job_args['vCPU']:
                if flavour_features['ram'] < temp_selected[0]:
                    selected_flavour = flavour
                    temp_selected = [flavour_features['ram'], flavour_features['vcpus']]
                elif flavour_features['ram'] == temp_selected[0]:
                    if flavour_features['vcpus'] < temp_selected[1]:
                        selected_flavour = flavour
                        temp_selected = [flavour_features['ram'], flavour_features['vcpus']]
                else:
                    continue
        if selected_flavour == None:
            self.logger.doLog("No Flavour matches requirements")
            return False

        flavour_ram = temp_selected[0]
        flavour_vcpus = temp_selected[1]
        # Define resources metrics
        free_ram = 10000000 # 10TB, max_ram = -1 means unlimited
        denominator = free_ram
        if info_dict['compute']['maxTotalRAMSize'] > 0:
            free_ram = info_dict['compute']['maxTotalRAMSize'] - info_dict['compute']['totalRAMUsed']
            denominator = info_dict['compute']['maxTotalRAMSize']
        memory_metric = (free_ram - job_args['inst']*flavour_ram) / denominator
        free_cores = 10000
        denominator = free_cores
        if info_dict['compute']['maxTotalCores'] > 0:
            free_cores = info_dict['compute']['maxTotalCores'] - info_dict['compute']['totalCoresUsed']
            denominator = info_dict['compute']['maxTotalCores']
        cpu_metric = (free_cores - job_args['inst']*flavour_vcpus) / denominator
        free_instances = 1000
        denominator = free_instances
        if info_dict['compute']['maxTotalInstances'] > 0:
            free_instances = info_dict['compute']['maxTotalInstances'] - info_dict['compute']['totalInstancesUsed']
            denominator = info_dict['compute']['maxTotalInstances']
        instances_metric = (free_instances - job_args['inst']) / denominator
        total_max_time, origins = self.data_transf(job_args['storage_inputs'], center.name)
        data_tranf_score = 1
        if origins == False:
                return False
        elif len(job_args['storage_inputs']) != 0:
            time_param, not_used = self.data_transf(job_args['storage_inputs'], center.name, default = True)
            data_tranf_score = math.exp(-(total_max_time / (2 * time_param)))
        res['dest']['storage_inputs'] = origins
        values = []
        values.append( memory_metric )
        values.append( cpu_metric )
        values.append( instances_metric )
        if len(job_args['storage_inputs']) != 0:
            values.append( data_tranf_score )
        res['real_mean'] = 0
        for value in values:
            if value < 0:
                self.logger.doLog("Not enough Openstack resources to allocate task.")
                self.logger.doLog("score < 0. Values = ")
                self.logger.doLog(values)
                return True
            res['real_mean'] += value
        res['real_mean'] /= len(values)
        res['mean'] = res['real_mean']
        if len(job_args['storage_inputs']) != 0:
            res['mean'] = data_tranf_score
        res['dest']['flavour'] = selected_flavour
        self.job_list[job_args['job_id']]['val'].append(res)
        return True

    def backend_av_resources(self, projectID, token):
        if self.check_refresh_token(token) == False:
            return False
        headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + str(token['access_token'])
        }
        r = requests.get(self.backend_URL+'/hpc/resource', headers=headers)
        if (r.status_code == 200):
            ret = (x for x in r.json() if x["AssociatedLEXISProject"] == projectID)
            return ret
        else:
            self.logger.doLog('Available resource for project request failed ')
            return False

	# evaluate all the available machines based on the weighted criteria mean
    def evaluate(self, args, token):
        self.job_list[args['job_id']]['params'] = args
        self.job_list[args['job_id']]['val'] = []
        av_resources = self.backend_av_resources(args['project'], token)
        if av_resources == False:
            self.job_list[args['job_id']]['status'] = "err"
            self.job_list[args['job_id']]['msg'] = "auth_backend"
            self.logger.doLog("Cannot authenticate in LEXIS backend or cannot refresh not active token")
            return (0)
        check = False
        if args['type'] == "both" or args['type'] == "cloud":
            items = (x for x in av_resources if x["ResourceType"] == "CLOUD")
            for item in items:
                for center in self.clusters_info:
                    if "cloud" not in self.clusters_info[center].keys() or (center != item["HPCProvider"] and center != item["HPCProvider"].lower()):
                        continue
                    else:
                        if self.Cloud_weighted_criteria_mean(args, self.clusters_info[center]['cloud'], item['OpenStackEndpoint'], item['CloudNetworkName'], item['HEAppEEndpoint'], item['AssociatedHPCProject'], token) == True:
                            check = True
                        break
        if args['type'] == "both" or args['type'] == "hpc":
            items = (x for x in av_resources if x["ResourceType"] == "HPC")
            for item in items:
                for center in self.clusters_info:
                    if "hpc" not in self.clusters_info[center].keys() or (center != item["HPCProvider"] and center != item["HPCProvider"].lower()):
                        continue
                    else:
                        if self.HPC_weighted_criteria_mean(args, self.clusters_info[center]['hpc'], item['HEAppEEndpoint'], item['AssociatedHPCProject'], token) == True:
                            check = True
                        break
        if check == True:
            self.job_list[args['job_id']]['status'] = "done"
            self.job_list[args['job_id']]['msg'] = "evaluation successfully ended."
        else:
            self.job_list[args['job_id']]['status'] = "err"
            self.job_list[args['job_id']]['msg'] = "evaluation failed."
        if self.api.write_evaluation(args['job_id']) == False:
            self.job_list[args['job_id']]['msg'] = self.job_list[args['job_id']]['msg'] + " WAR: could not write the result in the DB. DB not available."
        return (0)


    # delete a job previously inserted in the system
    def remove_job(self, job_id):
        del self.job_list[job_id]
        return (0)


    # get the best machine(s) where to run a given job (job_id)
    def get_best_machines(self, job_id):
        results = []
        temp = sorted(self.job_list[job_id]['val'], key = lambda x: x['mean'], reverse=True)
        for i in temp:
            if (len(results) >= self.job_list[job_id]['params']['number']):
                break
            elif (i['mean'] > 0):
                results.append(i['dest'])
        return (results)
