import requests
import json
import sys
import datetime
import math
from lxmlog import LXMlog as lxmlog 

default_timeout = 6

class HeappeClient():

    def __init__(self, url, logger):
        self.logger = logger        
        self.base_url = url
        self.info_dict = self.init_info(self.base_url)
        if self.info_dict == False:
            return None
        self.project_info_dict = None
        self.session_code = None
        
    def get_system_status(self):
        self.logger.doLog('Reporting basic system status')
        try:
            r = requests.get(url+'/status', timeout=default_timeout)
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
        if (r.status_code == 200):
            self.logger.doLog('System status fetched')
            return info_dict
        else:
            self.logger.doLog('System status failed')
            return False


    def auth(self, user, password):
        headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json-patch+json',
        }
        data = '{"Credentials":{"Password":"'+password+'","Username":"'+user+'"}}'
        try:
            r = requests.post(self.base_url+'/heappe/UserAndLimitationManagement/AuthenticateUserPassword', headers=headers, data=data)
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
        if (r.status_code == 200):
            self.session_code = r.json()
            self.logger.doLog('Status correctly obtained')
            return True
        else:
            self.logger.doLog('Auth failed')
            return False
    