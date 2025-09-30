from bridge.tests.util import BridgeTestBase
from django.utils import timezone
from django.core.serializers.json import DjangoJSONEncoder
from swap.connections.constants import SHOPIFY, SUCCESS, VARIANT, ERROR, INVENTORY
from swap.connections.models import ConnectedApp
import json
import datetime

from swap.transactions.tests.flow_tests import TransactionCreatorMixin
from swap.transactions.status_codes import QUERY_CODES

class BridgeShopifyConnectionTransactionTests(
        BridgeTestBase,
        TransactionCreatorMixin
    ):

    def test_shopify_connection_events(self):
        now = timezone.now()

        shopify_ca_query = ConnectedApp.objects.filter(name=SHOPIFY)
        if shopify_ca_query.exists():
            ca = shopify_ca_query.first()
        else:
            ca = self.make(
                'connections.ConnectedApp',
                name=SHOPIFY,
            )

        uac = self.make(
            'connections.UserAppConnection',
            user=self.make('contacts.User'),
            retailer=self.retailer1,
            app=ca
        )

        for i in range(10):
            cr = self.make(
                'connections.ConnectionResource',
                connection=uac,
                status=SUCCESS,
                brand='Adidas',
                resource=VARIANT,
                parsed_data={ 'some_data': i, 'is_in_results': True },
                source_ids=['12345', '22345', '32345'],
                sku=f'{i}23456789',
                # name
                # inventory
                # wholesale_cost
            )
            cr.modified = now-datetime.timedelta(minutes=i + 1)
            cr.save(update_modified=False)

        # Filter these out

        # Outside time range
        oor_cr = self.make(
            'connections.ConnectionResource',
            connection=uac,
            status=SUCCESS,
            brand='Adidas',
            resource=VARIANT,
            parsed_data={ 'some_data': 10, 'is_in_results': False },
            source_ids=['12345', '22345', '32345'],
            sku=f'{i}23456789',
        )
        oor_cr.modified = now-datetime.timedelta(minutes=16)
        oor_cr.save(update_modified=False)

        # Error status
        es_cr = self.make(
            'connections.ConnectionResource',
            connection=uac,
            status=ERROR,
            brand='Adidas',
            resource=VARIANT,
            parsed_data={ 'some_data': 11, 'is_in_results': False },
            source_ids=['12345', '22345', '32345'],
            sku=f'{i}23456789',
        )
        es_cr.modified = now-datetime.timedelta(minutes=1)
        es_cr.save(update_modified=False)

        # Inventory
        i_cr = self.make(
            'connections.ConnectionResource',
            connection=uac,
            status=SUCCESS,
            brand='Adidas',
            resource=INVENTORY,
            parsed_data={ 'some_data': 12, 'is_in_results': False },
            source_ids=['12345', '22345', '32345'],
            sku=f'{i}23456789',
        )
        i_cr.modified = now-datetime.timedelta(minutes=1)
        i_cr.save(update_modified=False)

        start = (now-datetime.timedelta(minutes=15)).isoformat().split('.')[0]
        end = now.isoformat().split('.')[0]

        resp = self.get(
            f'/bridge/shopifyconnectionevents/?start={start}&end={end}'
        )
        self.assertEqual(resp.data['count'], 10)
        for cr in resp.data['results']:
            self.assertTrue(cr['parsed_data']['is_in_results'])
        
        # print (json.dumps(resp.data, indent=4, cls=DjangoJSONEncoder))

"""
{
    "count": 10,
    "next": null,
    "previous": null,
    "results": [
        {
            "modified": "2023-07-07T16:05:51.890546Z",
            "parsed_data": {
                ...shopify product: https://jsonhero.io/j/vm7t0CNShzG3
            },
            "sku": "023456789",
            "seller_label": "pretailer",
            "seller_id": "12345",
            "seller_lat_lon": [
                40.7484,
                -73.9967
            ],
            "seller_postal_code": "10001"
        },
        ..... more results
    ]
}
"""
