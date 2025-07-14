import traceback
from typing import Iterable, Optional
from copy import deepcopy
from datetime import datetime, timedelta
from django.utils import timezone
from pydash.arrays import flatten
import sentry_sdk

from celery import group
from django.apps import apps
from django.db.models import Count, Q, OuterRef, Exists
from django.conf import settings
from django.core.cache import cache

from pyrate_limiter import BucketFullException
from rest_framework.exceptions import ValidationError

from logging import getLogger

logger = getLogger(__name__)

RELOAD_OVERLOAD_PREVENTER_KEY = 'shopify_update_for_connection_{connection_id}'

@app.task
def sync_all_collections_periodically():
    def get_apps(): return ShopifyAppConnection.objects.filter(
        is_active=True,
        app_data__has_keys=['collection_id', 'api_secret_key', 'private_key']
    )
    total = get_apps().count()
    page_size = 20
    for offset in range(0, total, page_size):
        group(
            retrieve_shopify_products.signature(
                kwargs={
                    'connection_id': _app.id
                },
                queue='collections_queue',
            )
            for _app in get_apps().order_by('id')[offset:offset + page_size]
        ).apply_async(
            queue='collections_queue',
        )


# TODO: Move this to a more reusable space
# https://shopify.dev/api/admin-rest#rate_limits
api_task = app.task(
    bind=True,
    max_retries=500,
    default_retry_delay=1,
    autoretry_for=(BucketFullException,),
    retry_backoff=True,
    retry_backoff_max=500,
    retry_jitter=True,
)


def sync_products_from_collection(
    connection_id: int,
    data: dict,
    topic: str,
    collection_id: int,
    received: float
):
    connection = ShopifyAppConnection.objects.get(id=connection_id)
    if connection.app_data['collection_id'] != collection_id:
        return err({
            'error': 'collection_id does not match app_data.collection_id',
            'data': data,
            'collection_id': collection_id,
            'connection_id': connection_id
        })
    if topic != 'collections/deleted':
        retrieve_shopify_products.apply_async(
            kwargs=dict(connection_id=connection_id, limit=100),
            queue='collections_queue',
        )
        return ok()


@app.task
def remove_missing_products_from_mr(connection_id: int):
    ShopifyImport = apps.get_model('connections.ConnectionProductBatch')
    datasets = ShopifyImport.objects.filter(
        connection_id=connection_id
    ).values_list(
        'imported_data',
        flat=True
    )
    ids = [
        variant['id']
        for dataset in datasets
        for product in dataset['products']
        for variant in product['variants']
    ]
    return remove_missing_products_from_mr_by_variant_id(connection_id, ids)


def remove_missing_products_from_mr_by_variant_id(
    connection_id: int,
    ids: Iterable[int],
    exclude=True
):
    ListingUnitModel = apps.get_model('listings.ListingUnit')
    ListingModel = apps.get_model('listings.Listing')

    ConnectionResource = apps.get_model('connections.ConnectionResource')

    ShopifyAppConnectionModel = apps.get_model(
        'connections.ShopifyAppConnection')

    connection = ShopifyAppConnectionModel.objects.filter(
        id=connection_id,
    ).first()

    missing_units = ListingUnitModel.objects.select_related(
        'listing__productvariation__product'
    ).filter(
        listing__productvariation__product__app_id=connection_id,
        listing__provider__id=connection.retailer.id,
    )

    # Delete all unsuccessful connection resources for these
    if not exclude:
        ConnectionResource.objects.filter(
            resource=VARIANT,
            source_ids__overlap=map(str, ids),
            connection__id=connection_id
        ).exclude(status=SUCCESS).update(status=REMOVED)
    else:
        ConnectionResource.objects.filter(
            resource=VARIANT,
            connection__id=connection_id,
        ).exclude(
            status=SUCCESS,
            source_ids__overlap=map(str, ids),
        ).update(status=REMOVED)

    # NOTE: we only want to exclude if a complete set of variant ids has been
    # provided. Else we assume that the ids provided should be removed.
    if exclude:
        missing_units = missing_units.exclude(source_id__in=ids)
    else:
        missing_units = missing_units.filter(source_id__in=ids)

    listings_updated = ListingModel.objects.filter(
        id__in=missing_units.values_list('listing_id', flat=True)
    )
    for listing in listings_updated:
        listing.is_active = False
        listing.save()
    return ok(
        dict(
            listings_updated=listings_updated,
        )
    ).serialize()


def get_offset_calculator(
    bucket_size: int,
    recovery_time: int,
    time_per_request: float
):
    return lambda index: (
        # calculate when the bucket it item should be collected
        time_per_request * (index % bucket_size) + (
            # offset by the time per bucket prior
            (index // bucket_size) * bucket_size * time_per_request
        )
    ) + (
        # offset by recovery time per prior bucket
        (index // bucket_size) * recovery_time
    )


@api_task
@check_breaker
@check_rate_limit
def retrieve_shopify_products(self, connection_id, path=None, limit=50, expected=None, page: Optional[int] = None, queue_name=None):
    queue_name = queue_name or 'collections_queue'
    ConnectedAppModel = apps.get_model('connections.ShopifyAppConnection')
    ConnectionResource = apps.get_model('connections.ConnectionResource')
    ShopifyImport = apps.get_model('connections.ConnectionProductBatch')
    connection = ConnectedAppModel.objects.filter(
        id=connection_id,
        is_active=True,
        retailer__is_active=True
    ).first()

    if not connection:
        return 'Invalid connection id'

    expected = expected if expected else limit

    api = API(**connection.credentials)
    if not path:
        # Only run this on first execution of 'retrieve_shopify_products', path should only be None on first run.
        ShopifyImport.objects.filter(connection=connection).delete()
        path = f'/admin/api/2022-04/products.json'

    if not limit:
        response = api.shopify.get(
            path,
            parser=Products,
            include_headers=True,
            raise_for_status=False
        )
    else:
        response = api.shopify.get(
            path,
            parser=Products,
            include_headers=True,
            params={
                'limit': limit,
                'collection_id': connection.app_data['collection_id'],
                'status': 'active'
            },
            raise_for_status=False
        )

    handle_shopify_rate_limit(self, response, connection_id)

    # Remove headers before saving to import log, headers are incompatible as JSONField data.
    data = deepcopy(response.json)
    del data['headers']

    ShopifyImport.objects.create(imported_data=data, connection=connection)

    time_to_process = 0.5  # seen that average processing time in celery is ~0.3s
    bleed_rate = 0.5  # shopify's allowed frequency i.e. inverse of 2req/s
    calc_offset = get_offset_calculator(
        bucket_size=40,
        recovery_time=30,
        time_per_request=bleed_rate + time_to_process
    )

    # To loop over all returned products do the following:
    page = page if page else 0
    # NOTE: We add a buffer to allow time for the pages to load for the
    #       retailer.
    # NOTE: that a collection of 2k products shouldn't take more than 2min
    collection_load_buffer = 180
    page_offset = (page * calc_offset(50)) + collection_load_buffer
    tasks = []
    now = datetime.utcnow()
    parser = response.parser
    for idx, product in enumerate(parser):
        try:
            # NOTE: The following equation will spread the API calls in such a
            #       way that the first attempt at the API call will be at the
            #       optimal time per retailer. The only way the subsequent calls
            #       would hit the rate limit would be if this or other tasks per
            #       the retailer were to be hit at the same time.
            # NOTE: The only way to get around the above issue of concurrent
            #       tasks would be to add a queue per retailer and to debounce
            #       task calls.
            offset = calc_offset(idx) + page_offset
            tasks.append(
                retrieve_related_product_variants_data.signature(
                    kwargs=dict(
                        connection_id=connection_id,
                        product_data=product.json
                    ),
                    immutable=True,
                    eta=now + timedelta(seconds=offset),
                    queue=queue_name,
                ) | update_shopify_store_report.si(connection_id)
            )
        except ParserError as exc:
            ConnectionResource.objects.create(
                connection=connection,
                resource=PRODUCT,
                parsed_data=product.json,
                status=ERROR,
                errors={'parser_error': str(exc)}
            )

    # NOTE: we want to first get all of the pages and then get each product item
    has_next_page = parser.next_url and parser.count == expected
    if has_next_page:
        retrieve_shopify_products.apply_async(
            kwargs=dict(
                connection_id=connection_id,
                path=response.parser.next_url,
                limit=None,
                expected=expected,
                page=page + 1,
                queue_name=queue_name
            ),
            eta=now + timedelta(seconds=calc_offset(page)),
            queue=queue_name,
        )
    else:
        # NOTE: Now that we have all of the collections data let's remove
        #       products from MR that are no longer on shopify.
        remove_missing_products_from_mr.apply_async(
            kwargs=dict(connection_id=connection_id),
            queue=queue_name,
        )

        # we should only get one of these for the LAST page of each
        # shopify collection, signifying that all pages have been queued
        RetailerEventCreator.spawn_retailer_event(
            connection.retailer,
            connection,
            RetailerEventCreator.SHOPIFY_INVENTORY_QUEUED_FOR_UPDATE
        )

    if tasks:
        group(tasks).apply_async(queue=queue_name)

    return parser.next_url


@api_task
@check_breaker
@check_rate_limit
def retrieve_related_product_variants_data(self, connection_id, product_data):
    ShopifyAppConnection = apps.get_model('connections.ShopifyAppConnection')
    ConnectionResource = apps.get_model('connections.ConnectionResource')

    connection = ShopifyAppConnection.objects.get(id=connection_id)
    api = API(**connection.credentials)

    # Get list of shopify variant inventory item id's, these should never be a null value
    inventory_ids = ",".join([str(variant['inventory_item_id'])
                             for variant in product_data['variants']])
    source_ids = [variant['id'] for variant in product_data['variants']]

    # Get cost from each inventory item and add it to the variant unit_price
    response = api.shopify.get(
        f'/admin/api/2022-04/inventory_items.json?ids={inventory_ids}',
        parser=InventoryItems,
        raise_for_status=False
    )

    handle_shopify_rate_limit(self, response, connection_id)

    product_data = update_products_with_inventory_data(
        product_data,
        response.parser.inventory_items
    )

    del response.json['headers']

    inventory_resource = ConnectionResource.objects.filter(
        # https://docs.djangoproject.com/en/3.2/ref/contrib/postgres/fields/#overlap
        source_ids__overlap=source_ids,
        resource=INVENTORY,
        connection=connection
    ).first()

    if not inventory_resource:
        ConnectionResource.objects.create(
            connection=connection,
            resource=INVENTORY,
            name=product_data['title'],
            sku=None,
            brand=product_data['vendor'],
            parsed_data=response.json,
            status=SUCCESS
        )
    else:
        inventory_resource.parsed_data = response.json
        inventory_resource.save()

    parser = Product(product_data)

    for variant in parser.variants:
        variant_ids = [variant.id for variant in variant.listing_units]

        resource = ConnectionResource.objects.filter(
            # https://docs.djangoproject.com/en/3.2/ref/contrib/postgres/fields/#overlap
            source_ids__overlap=variant_ids,
            resource=VARIANT,
            connection=connection
        ).first()
        original_id = None
        if not resource:
            resource = ConnectionResource(
                connection=connection,
                resource=VARIANT
            )
        else:
            original_id = resource.id

        resource.sku = variant.sku
        resource.source_ids = variant_ids
        resource.name = variant.name
        resource.brand = variant.collection.brand
        resource.wholesale_cost = variant.wholesale_price
        resource.parsed_data = parse_color_variant_from_raw_json(variant.json)
        resource.inventory = variant.inventory

        if len(parser.errors):
            resource.status = ERROR
            resource.errors = {
                'exception': 'Unsupported product',
                'error_report_reasons': parser.errors,
            }
            resource.save()
            continue

        product_listing, modification = load_product_listing(
            variant, connection,
            original_id
        )

        save_product_listing_and_resource(
            resource, product_listing, modification)

    return product_data


@app.task
def modify_shopify_product_variant(original_id, modification_id):
    ConnectionResource = apps.get_model('connections.ConnectionResource')
    ResourceModification = apps.get_model('connections.ShopifyResourceModification')
    resource = ConnectionResource.objects.get(id=original_id)

    modification = ResourceModification.objects.get(
        id=modification_id,
        resource=resource
    )

    parser = Product(resource.parsed_data['product'])

    for variant in parser.variants:
        product_listing, _ = load_product_listing(
            variant,
            resource.connection,
            resource.id
        )
        save_product_listing_and_resource(
            resource, product_listing, modification)


def load_product_listing(variant_data, connection, original_id=None):
    ConnectionResource = apps.get_model('connections.ConnectionResource')
    ResourceModification = apps.get_model('connections.ShopifyResourceModification')

    original_resource = ConnectionResource.objects.filter(
        id=original_id).first()
    modification = ResourceModification.objects.filter(
        resource=original_resource).first()

    if modification:
        variant_data.override_data(modification.submitted_data)

    color_variant = ProductColorVariantSerializer(
        variant_data, context={"app": connection})

    product_listing = ProductBrandListingSerializer(
        color_variant.data['listing'],
        data=color_variant.data,
        context=dict(
            app=connection,
            retailer=connection.retailer,
            validate_active_listing=True
        )
    )

    return product_listing, modification


@app.task
def update_catalog_metadata(resource_modification_id):
    ResourceModificationModel = apps.get_model(
        'connections.ShopifyResourceModification'
    )
    resource_modification = ResourceModificationModel.objects.get(
        id=resource_modification_id
    )
    connection = resource_modification.connection
    connection_resource = resource_modification.resource

    parser = Product(connection_resource.parsed_data['product'])

    total_catalog_metadata = {}
    product = None
    for variant in parser.variants:
        variant.override_data(resource_modification.submitted_data)

        color_variant = ProductColorVariantSerializer(
            variant,
            context={"app": connection}
        )

        listing = color_variant.data['listing']

        if listing:
            product_variation = listing.productvariation
            product = product_variation.product
            new_catalog_metadata = get_updated_catalog_metadata(
                color_variant.data,
                product,
                product_variation
            )
            total_catalog_metadata.update(new_catalog_metadata)

    if product:
        product.catalog_metadata = total_catalog_metadata
        product.save()


@api_task
def get_product_shopify_collection(self, product_data, account_id, product_id):
    ShopifyAppConnection = apps.get_model('connections.ShopifyAppConnection')
    connection = ShopifyAppConnection.objects.filter(
        app_data__contains={'account_id': account_id},
        is_active=True,
        retailer__is_active=True
    ).first()
    if not connection:
        return 'No ShopifyAppConnection found for account_id'

    # NOTE: we can't use the decorator here because the task signature is
    #       different than the other use cases.
    _check_breaker(self, connection.id)
    _check_rate_limit(self, connection.id)
    collection_id = connection.app_data.get('collection_id')
    api = API(**connection.credentials)

    # Get shopify store custom collections and filter by collection id of the product
    response = api.shopify.get(f'/admin/api/2022-04/collects.json?product_id={product_id}&collection_id={collection_id}',
                               raise_for_status=False, parser=Collects)

    handle_shopify_rate_limit(self, response, connection.id)

    if response.parser.exists:
        retrieve_related_product_variants_data.apply_async(
            kwargs=dict(
                connection_id=connection.id,
                product_data=product_data
            ),
            link=update_shopify_store_report.si(connection.id),
            queue='collections_queue',
        )
        return True

    # NOTE: This should be a separate task and should really be in a chain
    # Get shopify store smart collections and filter by collection id of the product
    response = api.shopify.get(f'/admin/api/2022-04/smart_collections.json?product_id={product_id}',
                               raise_for_status=False, parser=SmartCollections)

    handle_shopify_rate_limit(self, response, connection.id)

    for collection in response.parser:
        if collection.id == collection_id:
            retrieve_related_product_variants_data.apply_async(
                kwargs=dict(
                    connection_id=connection.id,
                    product_data=product_data
                ),
                link=update_shopify_store_report.si(connection.id),
                queue='collections_queue',
            )
            return True

    # NOTE: If we can't find the product in the collection that we care about
    #       we should remove it if it exists in MR.
    source_ids = [variant['id']
                  for variant in product_data.get('variants', [])]
    if source_ids:
        result = remove_missing_products_from_mr_by_variant_id(
            connection.id,
            source_ids,
            exclude=False
        )
        logger.info(
            'Tried to remove missing products',
            extra=dict(result=result)
        )
    else:
        logger.warn(
            'Unexpected product_data structure',
            extra=dict(product_data=product_data)
        )
    return 'Product not associated with Retailer tracked collection'


@app.task
def deactivate_product_listing(account_id, product_id):
    ShopifyAppConnectionModel = apps.get_model(
        'connections.ShopifyAppConnection')
    ConnectionResourceModel = apps.get_model('connections.ConnectionResource')
    ListingUnitModel = apps.get_model('listings.ListingUnit')
    ListingModel = apps.get_model('listings.Listing')

    connection = ShopifyAppConnectionModel.objects.filter(
        app_data__contains={'account_id': account_id},
    ).first()

    if not connection:
        logger.warn('No ShopifyAppConnection found for account_id')
        return

    connection_resources = ConnectionResourceModel.objects.filter(
        resource=VARIANT,
        parsed_data__product__id=int(product_id),
        connection=connection,
    )

    connection_resources.update(status=REMOVED)

    all_source_ids = [
        item for sub_list in connection_resources.values_list('source_ids', flat=True)
        for item in sub_list
    ]

    listings = ListingModel.objects.filter(
        id__in=ListingUnitModel.objects.filter(
            source_id__in=all_source_ids
        ).values_list('listing__id', flat=True)
    )

    for listing in listings:
        listing.is_active = False
        listing.save()


def configure_webhooks(connection_id):
    clean_up = (
        get_webhooks.s(connection_id) |
        remove_legacy_webhooks.s(connection_id=connection_id)
    )
    return (
        clean_up |
        group(
            create_webhook.si(connection_id, webhook_name)
            for webhook_name in webhooks.keys()
        )
    ).apply_async()


@api_task
@rate_limited
def get_webhooks(self, connection_id):
    connection = ShopifyAppConnection.objects.get(id=connection_id)
    return api.get_webhooks(connection).serialize()


@app.task
def remove_legacy_webhooks(webhooks_result, connection_id):
    data = webhooks_result['data']
    if not webhooks_result['success']:
        return err({'error': 'upstream error', 'data': data}).serialize()
    existing = data['webhooks']
    topics = set([webhook.topic for webhook in webhooks.values()])
    new_address = get_address('v1')
    ids = [
        wh['id']
        for wh in existing
        if wh['topic'] in topics and wh['address'] != new_address
    ]
    group(
        delete_webhook.si(connection_id, webhook_id)
        for webhook_id in ids
    ).apply_async()
    return ok({'ids': ids}).serialize()


@api_task
@rate_limited
def delete_webhook(self, connection_id, webhook_id):
    connection = ShopifyAppConnection.objects.get(id=connection_id)
    return api.delete_webhook(connection, webhook_id).serialize()


@api_task
@rate_limited
def create_webhook(self, connection_id, webhook_name):
    webhook = webhooks[webhook_name]
    instance = webhook.create(connection_id)
    return ok({'connection_webhook_id': instance.id}).serialize()


@app.task
def update_shopify_store_report(connection_id):
    ShopifyAppConnection = apps.get_model('connections.ShopifyAppConnection')
    ConnectionResource = apps.get_model('connections.ConnectionResource')
    ListingUnit = apps.get_model('listings.ListingUnit')
    Listing = apps.get_model('listings.Listing')
    connection = ShopifyAppConnection.objects.get(id=connection_id)

    source_ids = set()
    products = set()

    variants = ConnectionResource.objects.filter(
        connection=connection, resource=VARIANT)

    # by getting all of the counts at the same time there shouldn't be any race
    # conditions that cause weird results e.g. negative values
    stats = variants.aggregate(
        error=Count('pk', filter=Q(status=ERROR)),
        pending=Count('pk', filter=Q(status=PENDING)),
        success=Count('pk', filter=Q(status=SUCCESS)),
        removed=Count('pk', filter=Q(status=REMOVED)),
        ignored=Count('pk', filter=Q(status=IGNORED)),
        zero_quantity=Count(
            'pk',
            filter=Q(status=PENDING, inventory=0)
        ),
        less_than_min_price=Count(
            'pk',
            filter=Q(
                status=PENDING,
                inventory__gt=0,
                wholesale_cost__gte=1,
                wholesale_cost__lt=SHOPIFY_MIN_WHOLESALE_COST,
            )
        )
    )

    pending = stats['pending']
    success = stats['success']
    removed = stats['removed']
    error = stats['error']
    ignored = stats['ignored']
    zero_quantity = stats['zero_quantity']
    less_than_min_price = stats['less_than_min_price']

    workable_wares = success + pending
    verification_progress = round(
        success / workable_wares, 4) * 100 if workable_wares else 0

    for resource in variants.filter(status__in=[PENDING, SUCCESS]):
        products.add(resource.parsed_data['product']['id'])
        for variant in resource.parsed_data['size_variants']:
            source_ids.add(variant['id'])

    units = ListingUnit.objects.filter(
        source_id__in=list(source_ids),
        listing__provider=connection.retailer,
        listing__is_active=True,
        listing__sold_out=False
    )

    reporting_data = dict(
        products=len(products),
        listings=Listing.objects.filter(
            listingunit__in=units
        ).distinct().count(),
        variants=workable_wares,
        listing_units=units.count(),
        errors=error,
        pending=pending,
        removed=removed,  # products that were in the collection but then removed
        ignored=ignored,
        zero_quantity=zero_quantity,
        pending_with_inventory=pending - zero_quantity - less_than_min_price,
        verification_progress=verification_progress,
        less_than_min_price=less_than_min_price,
    )

    ShopifyAppConnection.objects.filter(
        id=connection_id).update(reporting_data=reporting_data)

    return connection_id


def create_new_pending_variants_slack_message(day, pending_variants):

    emoji = ':kermit:' if settings.ENVIRONMENT == 'production' else ':nope:'

    env_str = '' if settings.ENVIRONMENT == 'production' else f' ({settings.ENVIRONMENT})'

    message = f'{emoji} Products added {day.strftime("%a %b %-d")}{env_str}\n'

    if not len(pending_variants):
        message += 'None'
        return message

    for pending_variant in pending_variants:
        message += (
            f'{pending_variant["connection__retailer__id"]} - '
            f'{pending_variant["connection__retailer__store_name"]}: '
            f'{pending_variant["count"]} added '
            f'{day.strftime("%m/%d/%Y")}\n'
        )

    return message


@app.task
def slack_daily_new_active_pending_variants():
    ConnectionResource = apps.get_model('connections.ConnectionResource')

    day = timezone.now() - timedelta(days=1)

    connection_resources = ConnectionResource.objects.filter(
        resource=VARIANT,
        status=PENDING,
        inventory__gt=0,
        created__gte=day,
    ).filter(
        SHOPIFY_VALID_WHOLESALE_QUERY
    ).values(
        'connection__retailer__id',
        'connection__retailer__store_name',
    ).annotate(
        count=Count('pk')
    ).order_by('-count')

    message = create_new_pending_variants_slack_message(
        day,
        connection_resources
    )

    send_slack_message_to_channel.apply_async(
        kwargs=dict(
            channel_id=SHOPIFY_DAILYADDS,
            message=message
        ),
        queue='fast_queue'
    )

    return connection_resources


@app.task(queue='collections_queue')
def find_potential_inactive_shopify_connections():
    ShopifyAppConnectionModel = apps.get_model(
        'connections.ShopifyAppConnection')
    ConnectionResourceModel = apps.get_model('connections.ConnectionResource')
    potention_inactive_connections = ShopifyAppConnectionModel.objects.filter(
        ~Exists(
            ConnectionResourceModel.objects.filter(
                pk=OuterRef('pk'),
                created__gte=timezone.now() - timedelta(days=2)
            )
        ),
        is_active=True,
        retailer__is_active=True,
    )

    for pic in potention_inactive_connections:
        test_potential_inactive_shopify_connections.apply_async(
            kwargs={"connection_id": pic.id},
        )


@app.task(queue='collections_queue')
def test_potential_inactive_shopify_connections(connection_id):
    ShopifyAppConnection = apps.get_model('connections.ShopifyAppConnection')
    potential_inactive_connection = ShopifyAppConnection.objects.get(
        id=connection_id
    )
    connection_errors = potential_inactive_connection.test_is_connection_active()
    if len(connection_errors):
        connection_error_str = '\n\t'.join(connection_errors)

        emoji = ':mild_panic:' if settings.ENVIRONMENT == 'production' else ':nope:'
        message = (
            f'{emoji} '
            f'Retailer connection error\n'
            f'Retailer ID: {potential_inactive_connection.retailer.id}\n'
            f'Retailer Store Name: {potential_inactive_connection.retailer.store_name}\n'
            f"Errors: \n\t{connection_error_str}"
        )
        send_slack_message_to_channel.apply_async(
            kwargs={
                "channel_id": SHOPIFY_DAILYADDS,
                "message": message,
            },
            queue='fast_queue'
        )


@app.task
def continuously_reload_shopify_connections():
    logger.info("continuously_reload_shopify_connections")

    # filter ShopifyAppConnections who haven't been updated in at least 24 hours
    ShopifyAppConnection = apps.get_model('connections.ShopifyAppConnection')

    # here we want the least recently updated so we can reload it
    lru_connection = ShopifyAppConnection.objects.filter(
        is_active=True,
        retailer__is_active=True,
        app__name='Shopify',
        last_updated__lte=timezone.now() - timedelta(hours=24)
    ).order_by(
        'last_updated', 'id').first() 

    if lru_connection:
        lru_connection.last_updated = timezone.now()
        lru_connection.save()
        logger.info(f"reloading ShopifyAppConnection {lru_connection.id}")
        retrieve_shopify_products.apply_async(
            kwargs=dict(
                connection_id=lru_connection.id,
                limit=100,
                queue_name='shopify_reloads',
            ),
            queue='shopify_reloads',
        )

@app.task
def reprocess_resource_modifications(user_id, resource_mod_refs):
    ListingUnit = apps.get_model('listings.ListingUnit')
    ResourceModification = apps.get_model('connections.RTRResourceModification')
    num_exceptions = 0
    
    for rm in ResourceModification.objects.filter(id__in=resource_mod_refs):
        if not ListingUnit.objects.filter(source_id__in=rm.resource.source_ids).exists():
            try:
                modify_shopify_product_variant(original_id=rm.resource.id, modification_id=rm.id)
            except:
                num_exceptions += 1

    slack_this(user_id, f'Processed {len(resource_mod_refs)} resource modifications. FAILURES: {num_exceptions}')
