import unittest
import sys
import threading
from mocks import E2eContext
from azure.cli.command_modules.ml.service.realtime import realtime_service_list


class RealtimeE2eTests(unittest.TestCase):
    local_context = E2eContext()
    local_context.local_mode = True
    local_context.az_account_name = 'stcobmesosstor'
    local_context.az_account_key = 'qqlajVsnlLW1GZ0zSTYR5T/uONvILUoogQPQBQREQHbW61cDwr4gycNNjYLQ6MuXQAEJq5Q05v2PKGBXzLqTsQ=='
    local_context.acr_home = 'stcobmesosacr.azurecr.io'
    local_context.acr_user = 'stcobmesosacr'
    local_context.acr_pw = '+=wNR+h/+BG++/=mi6=7T5C6=HFE/Q/P'

    kube_context = E2eContext()
    kube_context.env_is_k8s = True
    kube_context.az_account_name = 'stcobwink8s2stor'
    kube_context.az_account_key = 'w53gYgs9Ydd6SPoMqLjQ60Ou29dhBts5UMUVIQuPca8q8+6vxgF2kVRhqFTXIP+ejcYRid6M6FyeGMlQRXnKmg=='
    kube_context.acr_home = 'stcobwink8s2acr.azurecr.io'
    kube_context.acr_user = 'stcobwink8s2acr'
    kube_context.acr_pw = '+=a=x=+=/i=Ix=mQL1Ar=mcDzsO/=ml/'
    kube_context.app_insights_account_name = 'stcobwink8s2app_ins'
    kube_context.app_insights_account_key = 'e7f8cf3c-7ce0-428a-96d5-aeed56b92ad8'

    mesos_context = E2eContext()
    mesos_context.env_is_k8s = False
    mesos_context.az_account_name = 'stcobmesosstor'
    mesos_context.az_account_key = 'qqlajVsnlLW1GZ0zSTYR5T/uONvILUoogQPQBQREQHbW61cDwr4gycNNjYLQ6MuXQAEJq5Q05v2PKGBXzLqTsQ=='
    mesos_context.acr_home = 'stcobmesosacr.azurecr.io'
    mesos_context.acr_user = 'stcobmesosacr'
    mesos_context.acr_pw = '+=wNR+h/+BG++/=mi6=7T5C6=HFE/Q/P'
    mesos_context.app_insights_account_name = 'stcobmesosapp_ins'
    mesos_context.app_insights_account_key = 'dce4b677-19bd-4dc6-9edd-bdbb87804c33'
    mesos_context.acs_master_url = 'stcobmesosacsmaster.eastus.cloudapp.azure.com'
    mesos_context.acs_agent_url = 'stcobmesosacsagent.eastus.cloudapp.azure.com'

    def test_list_local(self):
        c = E2eContext()
        c.local_mode = True
        realtime_service_list(context=c)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertTrue(output.startswith('+------'))
        self.assertTrue(output.endswith('-----+'))

    def test_list_mesos(self):
        realtime_service_list(context=self.mesos_context)

        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertEqual(output, '')

    def test_list_kubernetes(self):
        realtime_service_list(context=self.kube_context)
        if not hasattr(sys.stdout, "getvalue"):
            self.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.assertTrue(output.startswith('+------'))
        self.assertTrue(output.endswith('-----+'))


if __name__ == '__main__':
    assert not hasattr(sys.stdout, "getvalue")
    unittest.main(module=__name__, buffer=True, exit=False)