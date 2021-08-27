from keystoneauth1.identity import v3
from keystoneauth1 import session
from keystoneclient.v3 import client
from novaclient import client as nova_client
from cinderclient import client as cinder_client
from glanceclient import client as glance_client
from neutronclient.v2_0 import client as neutron_client
import json
import requests
import re
from lxmlog import LXMlog as lxmlog 

def compare_image_names(job_img, openstack_img):
    job_tokens = re.split('_|-|,| ', job_img.lower() )
    os_tokens = re.split('_|-|,| ', openstack_img.lower() )
    for t in job_tokens:
        if t not in os_tokens:
            return False
    return True

class OpenStackClient():

    def __init__(self, center, logger):
        self.auth_url = None
        self.name = center
        self.nova = None
        self.cinder = None
        self.glance = None
        self.neutron = None
        self.session = None
        self.logger = logger
        self.info_dict = {}
    
    def set_center(self, name):
        self.name = name

    def set_auth_url(self, auth_url):
        self.auth_url = auth_url
    
    def auth_pass(self, user_id, password, project_id):
        auth = v3.Password(auth_url=self.auth_url,
                    user_id=user_id,
                    password=password,
                    project_id=project_id)

        self.session = session.Session(auth=auth)
        return self.session

    def auth_heappe(self, heappe_base_url, user, token):
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }
        data = '{"credentials":{"Username":"'+user+'","OpenIdAccessToken":"'+token+'"}}'
        try:
            r = requests.post(heappe_base_url+'/heappe/UserAndLimitationManagement/AuthenticateUserOpenStack', headers=headers, data=data)
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
            auth_data = json.loads(r.content)
            self.logger.doLog('=== Successfully authenticated in Heappe for OpenStack ===')
        else:
            self.logger.doLog('Auth failed')
            return False       
        
        application_credential = v3.ApplicationCredentialMethod(
            application_credential_secret=auth_data['ApplicationCredentialsSecret'],
            application_credential_id=auth_data['ApplicationCredentialsId']
            )
        auth = v3.Auth(auth_url=self.auth_url,
               auth_methods=[application_credential]
              )
        self.session = session.Session(auth=auth)
        if self.session is None:
            self.logger.doLog('OpenStack auth failed using OpenId Token')
            return False
        return True

    def init_clients(self):
        self.nova = nova_client.Client(2.1, session=self.session)
        self.cinder = cinder_client.Client(3, session=self.session)
        self.glance = glance_client.Client('2', session=self.session)
        self.neutron = neutron_client.Client(session=self.session)


    def get_nova_client(self):
        if self.nova == None:
            self.nova = nova_client.Client(2.1, session=self.session)
        return self.nova

    def get_cinder_client(self):
        if self.cinder == None:
            self.cinder = cinder_client.Client(3, session=self.session)
        return self.cinder
    
    def get_glance_client(self):
        if self.glance == None:
            self.glance = glance_client.Client('2', session=self.session)
        return self.glance

    def get_neutron_client(self):
        if self.neutron == None:
            self.neutron = neutron_client.Client(session=self.session)
        return self.neutron

    def get_compute_limits(self):
        if self.nova == None:
            self.nova = nova_client.Client(2.1, session=self.session)
        compute_limits = self.nova.limits.get().absolute
        self.compute_limits_dict = {}
        for item in compute_limits:
            self.compute_limits_dict[item.name] = item.value
        return self.compute_limits_dict
    
    def get_storage_limits(self):
        if self.cinder == None:
            self.cinder = cinder_client.Client(3, session=self.session)
        cinder_limits = self.cinder.limits.get().absolute
        self.cinder_limits_dict = {}
        for item in cinder_limits:
            self.cinder_limits_dict[item.name] = item.value
        return self.cinder_limits_dict

    def get_instances(self):
        if self.nova == None:
            self.nova = nova_client.Client(2.1, session=self.session)
        return self.nova.servers.list()
    
    def get_flavours(self):
        if self.nova == None:
            self.nova = nova_client.Client(2.1, session=self.session)
        fl_list = self.nova.flavors.list(detailed=True)
        self.flavours = {}
        for fl in fl_list:
            self.flavours[fl.name] = self.nova.flavors.get(fl.id).to_dict()
        return self.flavours

    def get_images(self):
        if self.glance == None:
            self.glance = glance_client.Client('2', session=self.session)
        self.images = {}
        for image in self.glance.images.list():
            self.images[image.name] = image
        return self.images

    def get_net_quotas(self):
        if self.neutron == None:
            self.neutron = neutron_client.Client(session=self.session)
        tenant_id = self.neutron.get_quotas_tenant()['tenant']['tenant_id']
        self.quotas = self.neutron.show_quota_details(tenant_id)
        return self.quotas['quota']

    def update(self):
        self.init_clients()
        self.info_dict['compute'] = self.get_compute_limits()
        self.info_dict['storage'] = self.get_storage_limits()
        self.info_dict['flavours'] = self.get_flavours()
        self.info_dict['images'] = self.get_images()
        self.info_dict['network'] = self.get_net_quotas()
        self.session = None
        return self.info_dict

    def auth_and_update(self, openstack_url, user, token, heappe_url=None, password=None, project_id=None):
        if openstack_url is None:
            return False
        else:
            openstack_url = openstack_url + ":5000"
            self.set_auth_url(openstack_url)
        if heappe_url != None:
            if not self.auth_heappe(heappe_url, user, token):
                self.logger.doLog('Openstack openid token authentication failed')
                self.session = None
                return False
        else:
            if self.auth_pass(user, password, project_id) == None:
                self.logger.doLog('Openstack password authentication failed')
                self.session = None
                return False
        return self.update()
