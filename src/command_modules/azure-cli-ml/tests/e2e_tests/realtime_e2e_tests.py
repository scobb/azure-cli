import unittest
import sys
from mocks import E2eContext
from azure.cli.command_modules.ml.service.realtime import realtime_service_list
from azure.cli.command_modules.ml.service.realtime import realtime_service_view


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


class ViewScenario(UserScenario):
    def test_scenario(self):
        realtime_service_view('basic', context=self.context)
        if not hasattr(sys.stdout, "getvalue"):
            self.verify.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        lines = output.split('\n')
        self.verify.assertTrue(lines[0].startswith('+--------+'))
        self.verify.assertTrue(lines[1].startswith('| NAME   |'))
        self.verify.assertTrue(lines[2].startswith('|--------+'))
        print(lines[3])
        print('| basic  | {}/basic'.format(self.context.acr_home))
        self.verify.assertTrue(lines[3].startswith('| basic  | {}/basic'.format(self.context.acr_home)))
        self.verify.assertTrue(lines[4].startswith('+--------+'))
        self.verify.assertEqual(lines[5], 'Usage:')
        self.verify.assertTrue(lines[6].startswith('  az ml  : az ml service run realtime -n basic '))
        self.verify.assertTrue(lines[7].startswith('  curl : curl -X POST -H "Content-Type:application/json"'))


class ViewNonExistentScenario(UserScenario):
    def test_scenario(self):
        realtime_service_view('nonexistent_service', context=self.context)
        if not hasattr(sys.stdout, "getvalue"):
            self.verify.fail("need to run in buffered mode")
        output = sys.stdout.getvalue().strip()
        self.verify.assertEqual(output, 'No service running with name nonexistent_service on your ACS cluster')


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
    # local_context,
    kube_context,
    mesos_context
]

scenarios = [
    ListScenario,
    ViewScenario,
    ViewNonExistentScenario
]



if __name__ == '__main__':
    assert not hasattr(sys.stdout, "getvalue")
    for context in contexts:
        for scenario in scenarios:
            test = scenario(context)
            print('Setting {} to {}'.format(test.get_test_name(), test.test_scenario))
            setattr(TestManager, test.get_test_name(), test.test_scenario)

    unittest.main(buffer=True, exit=False)
