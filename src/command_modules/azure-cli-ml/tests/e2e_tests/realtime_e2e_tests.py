import unittest
import sys
import threading
from mocks import E2eContext
from azure.cli.command_modules.ml.service.realtime import realtime_service_list


class UserScenario(object):
    def __init__(self, context):
        self.context = context
        self.verify = unittest.TestCase("__init__")

    def get_test_name(self):
        return 'test_{}_{}'.format(self.__class__.__name__.lower().replace('scenario', ''),
                                   self.context.name)

    def test_scenario(self):
        raise NotImplementedError


class ListScenario(UserScenario):
    def test_scenario(self):
        realtime_service_list(context=self.context)
        if not hasattr(sys.stdout, "getvalue"):
            self.verify.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.verify.assertTrue(output.startswith('+------'))
        self.verify.assertTrue(output.endswith('-----+'))


class TestManager(unittest.TestCase):
    @classmethod
    def setupClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

local_context = E2eContext('local')
local_context.local_mode = True
local_context.az_account_name = 'o16ntestk8sstor'
local_context.az_account_key = 'w8KA6w4EHBqMZzN5JIKFY3kvsqo2Br6fs1GkdCYRqoU7ZGqCGsoWZJDUcjAJkFDZzcQIY4T6ExxrI4LLWBhXAg=='
local_context.acr_home = 'o16ntestk8sacr.azurecr.io'
local_context.acr_user = 'o16ntestk8sacr'
local_context.acr_pw = 'rd/W+7SZXZmW63+UTesibShbH+l9bGhh'

kube_context = E2eContext('k8s')
kube_context.env_is_k8s = True
kube_context.az_account_name = 'o16ntestk8sstor'
kube_context.az_account_key = 'w8KA6w4EHBqMZzN5JIKFY3kvsqo2Br6fs1GkdCYRqoU7ZGqCGsoWZJDUcjAJkFDZzcQIY4T6ExxrI4LLWBhXAg=='
kube_context.acr_home = 'o16ntestk8sacr.azurecr.io'
kube_context.acr_user = 'o16ntestk8sacr'
kube_context.acr_pw = 'rd/W+7SZXZmW63+UTesibShbH+l9bGhh'
kube_context.app_insights_account_name = 'o16ntestk8sapp_ins'
kube_context.app_insights_account_key = 'ca90361e-751a-4cdf-874a-bbeb82f6b9f0'

mesos_context = E2eContext('mesos')
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

contexts = [
    local_context,
    kube_context,
    mesos_context
]

scenarios = [
    ListScenario,
]
#
# class RealtimeE2eTests(unittest.TestCase):
#     local_context = E2eContext('local')
#     local_context.local_mode = True
#     local_context.az_account_name = 'stcobmesosstor'
#     local_context.az_account_key = 'qqlajVsnlLW1GZ0zSTYR5T/uONvILUoogQPQBQREQHbW61cDwr4gycNNjYLQ6MuXQAEJq5Q05v2PKGBXzLqTsQ=='
#     local_context.acr_home = 'stcobmesosacr.azurecr.io'
#     local_context.acr_user = 'stcobmesosacr'
#     local_context.acr_pw = '+=wNR+h/+BG++/=mi6=7T5C6=HFE/Q/P'
#
#     kube_context = E2eContext('k8s')
#     kube_context.env_is_k8s = True
#     kube_context.az_account_name = 'stcobwink8s2stor'
#     kube_context.az_account_key = 'w53gYgs9Ydd6SPoMqLjQ60Ou29dhBts5UMUVIQuPca8q8+6vxgF2kVRhqFTXIP+ejcYRid6M6FyeGMlQRXnKmg=='
#     kube_context.acr_home = 'stcobwink8s2acr.azurecr.io'
#     kube_context.acr_user = 'stcobwink8s2acr'
#     kube_context.acr_pw = '+=a=x=+=/i=Ix=mQL1Ar=mcDzsO/=ml/'
#     kube_context.app_insights_account_name = 'stcobwink8s2app_ins'
#     kube_context.app_insights_account_key = 'e7f8cf3c-7ce0-428a-96d5-aeed56b92ad8'
#
#     mesos_context = E2eContext('mesos')
#     mesos_context.env_is_k8s = False
#     mesos_context.az_account_name = 'stcobmesosstor'
#     mesos_context.az_account_key = 'qqlajVsnlLW1GZ0zSTYR5T/uONvILUoogQPQBQREQHbW61cDwr4gycNNjYLQ6MuXQAEJq5Q05v2PKGBXzLqTsQ=='
#     mesos_context.acr_home = 'stcobmesosacr.azurecr.io'
#     mesos_context.acr_user = 'stcobmesosacr'
#     mesos_context.acr_pw = '+=wNR+h/+BG++/=mi6=7T5C6=HFE/Q/P'
#     mesos_context.app_insights_account_name = 'stcobmesosapp_ins'
#     mesos_context.app_insights_account_key = 'dce4b677-19bd-4dc6-9edd-bdbb87804c33'
#     mesos_context.acs_master_url = 'stcobmesosacsmaster.eastus.cloudapp.azure.com'
#     mesos_context.acs_agent_url = 'stcobmesosacsagent.eastus.cloudapp.azure.com'
#
#     contexts = [
#         local_context,
#         kube_context,
#         mesos_context
#     ]
#
#     scenarios = [
#         ListScenario,
#     ]
#
#     def test_list_local(self):
#         realtime_service_list(context=self.local_context)
#         if not hasattr(sys.stdout, "getvalue"):
#             self.fail("need to run in buffered mode")
#         output = sys.stdout.getvalue().strip()
#         self.assertTrue(output.startswith('+------'))
#         self.assertTrue(output.endswith('-----+'))
#
#     def test_list_mesos(self):
#         realtime_service_list(context=self.mesos_context)
#
#         if not hasattr(sys.stdout, "getvalue"):
#             self.fail("need to run in buffered mode")
#         output = sys.stdout.getvalue().strip()
#         self.assertTrue(output.startswith('+------'))
#         self.assertTrue(output.endswith('-----+'))
#
#     def test_list_kubernetes(self):
#         realtime_service_list(context=self.kube_context)
#         if not hasattr(sys.stdout, "getvalue"):
#             self.fail("need to run in buffered mode")
#         output = sys.stdout.getvalue().strip()
#         self.assertTrue(output.startswith('+------'))
#         self.assertTrue(output.endswith('-----+'))


if __name__ == '__main__':
    assert not hasattr(sys.stdout, "getvalue")
    for context in contexts:
        for scenario in scenarios:
            test = scenario(context)
            print('Setting {} to {}'.format(test.get_test_name(), test.test_scenario))
            setattr(TestManager, test.get_test_name(), test.test_scenario)

    unittest.main(buffer=True, exit=False)
