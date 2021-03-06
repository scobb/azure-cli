# coding=utf-8
# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------
# coding: utf-8
# pylint: skip-file
from msrest.serialization import Model


class SubscriptionNotification(Model):
    """SubscriptionNotification.

    :param registration_date:
    :type registration_date: str
    :param state: Possible values include: 'NotDefined', 'Registered',
     'Unregistered', 'Warned', 'Suspended', 'Deleted'
    :type state: str or :class:`SubscriptionNotificationState
     <azure.mgmt.devtestlabs.models.SubscriptionNotificationState>`
    :param properties:
    :type properties: :class:`SubscriptionNotificationProperties
     <azure.mgmt.devtestlabs.models.SubscriptionNotificationProperties>`
    """

    _attribute_map = {
        'registration_date': {'key': 'registrationDate', 'type': 'str'},
        'state': {'key': 'state', 'type': 'str'},
        'properties': {'key': 'properties', 'type': 'SubscriptionNotificationProperties'},
    }

    def __init__(self, registration_date=None, state=None, properties=None):
        self.registration_date = registration_date
        self.state = state
        self.properties = properties
