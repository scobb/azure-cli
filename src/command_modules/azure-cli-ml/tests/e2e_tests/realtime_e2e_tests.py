import unittest
import sys
from mocks import E2eContext
from azure.cli.command_modules.ml.service.realtime import realtime_service_list


class RealtimeE2eTests(unittest.TestCase):
    kube_context = E2eContext()
    kube_context.env_is_k8s = True
    kube_context.az_account_name = 'stcobwink8s2stor'
    kube_context.az_account_key = 'w53gYgs9Ydd6SPoMqLjQ60Ou29dhBts5UMUVIQuPca8q8+6vxgF2kVRhqFTXIP+ejcYRid6M6FyeGMlQRXnKmg=='
    kube_context.acr_home = 'stcobwink8s2acr.azurecr.io'
    kube_context.acr_user = 'stcobwink8s2acr'
    kube_context.acr_pw = '+=a=x=+=/i=Ix=mQL1Ar=mcDzsO/=ml/'
    kube_context.app_insights_account_name = 'scwink8sacrapp_ins'
    kube_context.app_insights_account_key = '913c6712-3cf5-4a71-8f28-6019b51b6af1'


    def test_list_local(self):
        c = E2eContext()
        c.local_mode = True
        realtime_service_list(context=c)

    def test_list_mesos(self):
        c = E2eContext()
        c.local_mode = False
        realtime_service_list(context=c)
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