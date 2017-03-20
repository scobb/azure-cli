import requests
from ._util import TableResponse
from ._util import cli_context
from ._batchutilities import batch_env_is_valid
from ._batchutilities import batch_get_url
from ._batchutilities import BATCH_ALL_WS_FMT
from ._batchutilities import get_success_and_resp_str
from ._batchutilities import batch_list_service_header_to_fn_dict

from ._batchutilities import BATCH_SINGLE_WS_FMT
from ._util import MultiTableResponse
from ._batchutilities import batch_view_service_header_to_fn_dict
from ._batchutilities import batch_view_service_usage_header_to_fn_dict
from ._batchutilities import batch_get_parameter_str


def batch_service_list(context=cli_context):
    """
    Processing for listing existing batch services
    :param context: CommandLineInterfaceContext object
    :return: None
    """
    if not batch_env_is_valid(context):
        return
    url = batch_get_url(context, BATCH_ALL_WS_FMT)
    try:
        resp = context.http_call('get', url, auth=(context.hdi_user, context.hdi_pw))
        print(get_success_and_resp_str(context, resp, response_obj=TableResponse(
            batch_list_service_header_to_fn_dict))[1])
    except requests.ConnectionError:
        print("Error connecting to {}. Please confirm SparkBatch app is healthy.".format(url))
        return


def batch_service_view(service_name, verb, context=cli_context):
    """
    Processing for viewing an existing batch service
    :param context: CommandLineInterfaceContext object
    :param args: list of str arguments
    :return: None
    """
    if not batch_env_is_valid(context):
        return

    url = batch_get_url(context, BATCH_SINGLE_WS_FMT, service_name)
    try:
        resp = context.http_call('get', url, auth=(context.hdi_user, context.hdi_pw))

        success, response = get_success_and_resp_str(context, resp, response_obj=MultiTableResponse(
            [batch_view_service_header_to_fn_dict, batch_view_service_usage_header_to_fn_dict]), verbose=verb)
        print(response)
        if success:
            param_str = ' '.join([batch_get_parameter_str(p) for
                                  p in sorted(resp.json()['Parameters'],
                                              key=lambda x: '_' if 'Value' in x
                                              else x['Direction'])])
            usage = 'Usage: aml service run batch -n {} {} [-w] [-j <job_id>] [-v]'.format(service_name,
                                                                                           param_str)
            print(usage)

    except requests.ConnectionError:
        print("Error connecting to {}. Please confirm SparkBatch app is healthy.".format(url))
        return
