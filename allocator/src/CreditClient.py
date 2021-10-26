import requests

default_timeout = 6


class CreditClient:
    def __init__(self, url, logger):
        self.logger = logger
        self.base_url = url
        self.project_info_dict = None
        self.session_code = None

    def get_system_status(self, lexis_id=None):
        self.logger.doLog("Reporting basic system status")
        endpoint = "/status"
        if lexis_id is not None:
            enpoint = endpoint + "/" + str(lexis_id)
        try:
            r = requests.get(self.base_url + endpoint, timeout=default_timeout)
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
            self.logger.doLog("System status fetched")
            return r.json()
        self.logger.doLog("System status failed")
        return False

    def get_available(self, lexis_id):
        self.logger.doLog("Credit status of the account with the provided id")
        endpoint = "/account/available" + "/" + str(lexis_id)
        try:
            r = requests.get(self.base_url + endpoint, timeout=default_timeout)
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
            self.logger.doLog("Available credits fetched")
            return r.json()
        self.logger.doLog("Get available credits failed failed")
        return False
