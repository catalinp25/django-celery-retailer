import re
from copy import deepcopy
from django.utils import timezone
from django.apps import apps
from decimal import Decimal

from swap.distribution.constants import SIZE_DELIMITER
from swap.connections.api.parser import (
    Iterable,
    Parser,
    Field,
    StringField,
    DateTimeField,
    IntegerField,
    ParserField,
    DecimalField,
    BaseVariant,
    parse_brand
)

COLOR = 'Color'
COLORS = 'Colors'
COLOUR = 'Colour'
COLOURS = 'Colours'
COLOR_OPTIONS = [COLOR, COLORS, COLOUR, COLOURS]
SIZE = 'Size'
SIZES = 'Sizes'
SHOE_SIZE = 'Shoe Size'
SIZE_OPTIONS = [x.lower() for x in [SIZE, SIZES, SHOE_SIZE]]
MATERIAL = 'Material'
WIDTH = 'Width'
DEFAULT_COLOR = 'ONE COLOR'
DEFAULT_SIZE = 'ONE SIZE'

from swap.listings.constants import CONDITION_NEW


class Option(Parser):

    def __str__(self):
        return f"{self.type}: ({','.join(self.values)})"

    id = Field()
    type = Field()
    values = Field()
    product_id = Field()


class Collect(Parser):
    collection_id = StringField()
    product_id = StringField()


class Collects(Parser):
    """
    Collects are meant for managing the relationship between products and custom collections.
    For every product in a custom collection there is a collect that tracks the ID of both the product and the custom
    collection.
    """
    collects = ParserField('collects', child=Collect, many=True)
    exists = Field(parser=lambda x: len(x['collects']) > 0)


class SmartCollection(Parser):
    """
    A smart collection is a grouping of products defined by rules that are set by the merchant.
    Shopify automatically changes the contents of a smart collection based on the rules.
    """
    id = StringField()
    title = StringField()


class SmartCollections(Iterable, Parser):
    iterable = 'collections'

    """Shopify array of smart collections."""
    collections = ParserField(
        'smart_collections', child=SmartCollection, many=True)


def parse_variant_option(data, position):
    for option in data['options']:
        if option['position'] == position:
            return Option({
                'type': option['name'],
                'values': option['values'],
                'product_id': option['product_id'],
                'id': option['id']
            })
    return Option({})


class Collection(Parser):
    brand = StringField('vendor')
    name = "Shopify Upload"
    year = timezone.now().year


class InventoryItem(Parser):
    id = StringField()
    wholesale_cost = DecimalField('cost')


class InventoryItems(Parser):
    inventory_items = ParserField(
        'inventory_items', child=InventoryItem, many=True)


def get_variant_sku(data):
    for variant in data['size_variants']:
        if variant['sku']:
            return variant['sku']


def get_variant_image(data):
    if data.get('image'):
        return data['image']

    variant_ids = [variant['id'] for variant in data['size_variants']]
    for image in data['product']['images']:
        for _id in image['variant_ids']:
            if _id in variant_ids:
                return image['src']

    # If there is only color variant for this product AND there are no variant images, return the main product image
    if data['num_of_colors'] == 1:
        return data['product']['images'][0]['src']


def get_variant_images(data):
    # TODO: add logic to place the variant image at the beginning of the list
    if data.get('images'):
        return data['images']

    images = []

    for image in data['product']['images']:
        images.append(image['src'])
    return images


def get_variant_wholesale_cost(data):
    for variant in data['size_variants']:
        if variant['wholesale_cost'] is not None:
            return variant['wholesale_cost']


def get_variant_retail_price(data):
    for variant in data['size_variants']:
        if variant['price'] is not None:
            return variant['price']


def get_variant_quantity(data):
    qty = 0
    for variant in data['size_variants']:
        if variant['inventory_quantity'] is not None:
            qty += variant['inventory_quantity']
    return qty


def get_category(data, position):
    tags = data['tags'].split(',')
    categories = None
    for tag in tags:
        if tag.count('/') == 2:
            categories = tag.split('/')
    return categories[position].lstrip().rstrip()


def parse_color_variant_from_raw_json(data):
    # Overwrite product.variants with size variants
    # Size variants are already
    raw = deepcopy(data)
    raw['product']['variants'] = raw['size_variants']

    return raw


class ListingUnit(Parser):
    id = Field()
    size_label = Field('size')
    quantity = Field('inventory_quantity')
    condition = CONDITION_NEW


def split_tags(data):
    tags = data.split(',')

    # Strip whitespace from each tag
    for x, tag in enumerate(tags):
        tags[x] = tag.lstrip().rstrip()

    return tags


class ProductVariant(BaseVariant):
    """
    ProductVariant parser, variants are unique per color.

    Fields:
        id: Shopify product ID
        name: Product name, derived from shopify product title.
        top_category: Top category parsed from product tags, must match max retail categories.
        mid_category: Mid category parsed from product tags, must match max retail categories.
        base_category: Base category parsed from product tags, must match max retail categories.
        description: Product description.
        tags: Product tags.
        sku: Product variant sku. Variants can only list one SKU, if more than one sku is detected value will be null.
            The same sku can be listed across multiple variants, as long as they are an exact match.
        image: Product variant photo that matches the variant color. If product has only one variant, the primary root
            product image can be used. At least one product variant image must be returned or variant cannot be listed.
        images: Product variant photos that match the color variant. If product has only one variant, the primary root
            product images can be used. At least one product variant image must be returned or variant cannot be listed.
        sizes: Dictionary of product sizes for this product variant color. Each size returns its inventory quantity.
        color: Color for this product variant, variant can only be associated to one color.
        wholesale_cost: Wholesale cost for product variant. Cost is derived from Shopify inventory items. Cost for at
            least one variant must be available from Shopify cost or item cannot be listed.
        retail_price: Retail price in retailers shopify store for this product variant.
        quantity: Number of available units for this product variant.
        listing_type: Constant value "sell".
        collection: Collection for this product.
    """

    def __str__(self):
        return f'<ProductVariant: ({self.color})>'

    def __repr__(self):
        return self.__str__()

    id = Field('product.id')
    name = StringField('product.title')
    top_category = StringField('product.top_category')
    mid_category = StringField('product.mid_category')
    base_category = StringField('product.base_category')
    description = StringField(parser=lambda x: x['product']['body_html'])
    tags = Field(parser=lambda x: split_tags(x['product']['tags']))
    sku = StringField(parser=get_variant_sku)
    image = StringField(parser=get_variant_image)
    images = StringField(parser=get_variant_images)
    sizes = Field()
    color = StringField()
    wholesale_price = DecimalField(parser=get_variant_wholesale_cost)
    retail_price = DecimalField(parser=get_variant_retail_price)
    quantity = IntegerField(parser=get_variant_quantity)
    listing_units = ParserField('size_variants', child=ListingUnit, many=True)
    inventory = IntegerField(parser=lambda x: sum(
        [count for _, count in x['sizes'].items()]))
    listing_type = "sell"
    collection = ParserField('product', child=Collection)
    country_of_origin = StringField()
    composition = StringField()
    normalized_color = StringField()
    pattern = StringField()
    material = StringField()

class Image(Parser):
    url = StringField('src')
    width = IntegerField()
    height = IntegerField()
    position = IntegerField()
    variant_ids = Field()


def get_next_url(data):
    link = data['headers']['Link']
    if 'rel="previous"' in link:
        url = link.split(',')[1].split('>; ')[0].split('<')[1]
        path = url.split('://')[1].split('.myshopify.com')[1]
        return path
    else:
        url = link.split('>; ')[0].split('<')[1]
        path = url.split('://')[1].split('.myshopify.com')[1]
        return path


def is_color_option(option):
    return (
        option['name'].lower() == COLOR.lower()
        or option['name'].lower() == COLOUR.lower()
    )


def is_size_option(option):
    return option['name'].lower() in SIZE_OPTIONS


def is_material_option(option):
    return option['name'].lower() == MATERIAL.lower()


def is_width_option(option):
    return option['name'].lower() == WIDTH.lower()


def is_default_title_option(option):
    return (
        option['name'] == 'Title'
        and len(option['values']) == 1
        and option['values'][0] == 'Default Title'
    )


def all_variants_have_the_same_other_option(data):
    color, sizes, material, other = get_positions(data['options'])

    for option in [material, other]:
        if option is not None:
            # compiles a unique set of all the "other" options across all variants.
            # if they are all the same option it is irrelevant and we can ignore it.

            other_options = set(variant[option] for variant in data['variants'])
            if len(other_options) > 1:  # there is more than "other" option used
                return False
    return True


def get_positions(options):
    color_key = None
    size_keys = []
    material_key = None
    width_key = None
    other_option_key = None

    for option in options:
        if is_color_option(option):
            color_key = f"option{option['position']}"
        elif is_size_option(option):
            size_keys.append(f"option{option['position']}")
        elif is_material_option(option):
            material_key = f"option{option['position']}"
        elif is_width_option(option):
            width_key = f"option{option['position']}"
        elif option is not None:
            other_option_key = f"option{option['position']}"

    if width_key is not None:
        size_keys.append(width_key)

    return color_key, size_keys, material_key, other_option_key


def parse_variants(data):
    shopify_variants = data.get('variants')
    options = data.get('options')

    colors = dict()
    color_key, size_keys, material_key, other_key = get_positions(options)
    for variant in shopify_variants:
        colors[variant.get(color_key)] = {
            'sizes': {},
            'size_variants': [],
            'product': data,
            'num_of_colors': 0,
            'sku': data.get('handle'),
            'brand': parse_brand(data)
        }

    for variant in shopify_variants:

        # for each key in size_keys, get the value of that key from the variant and if it is not None, add it to the list
        # concatenate all size values with spaces in between
        # if they are all None or if size_keys is an empty list, then use 'O/S' as the size value
        size_value = SIZE_DELIMITER.join(
            [str(variant.get(k)) for k in size_keys if variant.get(k) is not None]) or 'O/S'

        variant['color'] = variant.get(color_key)
        variant['size'] = size_value
        colors[variant.get(color_key)]['color'] = variant.get(color_key)
        colors[variant.get(color_key)]['sizes'][size_value] = variant.get(
            'inventory_quantity')
        colors[variant.get(color_key)]['size_variants'].append(variant)

    color_count = len(colors.values())
    for color, value in colors.items():
        value['num_of_colors'] = color_count

    return [ProductVariant(color) for color in colors.values()]


def parse_product_errors(data):
    errors = []

    # Validate options
    options = data.get('options', [])
    total_option_count = 0
    color_option_count = 0
    size_option_count = 0
    width_option_count = 0
    other_option_count = 0
    for option in options:
        # If a product has no variants at all, shopify adds a 'Title' option
        # we are counting this as a one size product so we ignore
        if is_default_title_option(option):
            continue
        elif is_color_option(option):
            color_option_count += 1
        elif is_size_option(option):
            size_option_count += 1
        elif is_width_option(option):
            width_option_count += 1
        else:
            other_option_count += 1

        total_option_count += 1

    if not (
        # allow all the following cases:
        total_option_count == 0 # only one single variant with no ummm... variants
        or (color_option_count == 1 and total_option_count == 1) # only color
        or (size_option_count == 1 and total_option_count == 1) # only size
        or (size_option_count == 1 and color_option_count == 1 and total_option_count == 2) #size and color
        or (size_option_count == 1 and width_option_count == 1 and total_option_count == 2) #size and width
        or (size_option_count == 1 and color_option_count == 1 and width_option_count == 1 and total_option_count == 3) #size, color, width
        or (
            (size_option_count == 1 or color_option_count == 1) 
            and other_option_count == 1 
            and all_variants_have_the_same_other_option(data)
        ) # size or color and ONE other single option value shared by all variants
    ):
        errors.append('Unsupported product variant options')

    return errors


class Product(Parser):
    """Shopify Product API Parser."""

    def __str__(self):
        return f'shopify product: <{self.name}>'

    id = IntegerField()
    name = StringField('title')
    description = StringField(parser=lambda x: x['body_html'])
    brand = StringField(parser=parse_brand)
    sku = StringField('handle')
    top_category = StringField(parser=lambda x: get_category(x, 0))
    mid_category = StringField(parser=lambda x: get_category(x, 1))
    base_category = StringField(parser=lambda x: get_category(x, 2))
    tags = Field(parser=lambda x: split_tags(x['tags']))
    created_at = DateTimeField()
    updated_at = DateTimeField()
    status = StringField()
    variants = Field(parser=lambda data: parse_variants(data))
    option1 = Field(parser=lambda data: parse_variant_option(data, 1))
    option2 = Field(parser=lambda data: parse_variant_option(data, 2))
    option3 = Field(parser=lambda data: parse_variant_option(data, 3))
    images = ParserField(child=Image, many=True)
    image = ParserField(child=Image)
    errors = Field(parser=parse_product_errors)


class Products(Iterable, Parser):
    iterable = 'products'

    """Shopify array of products parser."""
    products = ParserField(child=Product, many=True)
    count = Field(parser=lambda data: len(data['products']))
    next_url = Field(parser=get_next_url)


def update_products_with_inventory_data(product_data, inventory_data):
    inventory_data_keyed_by_id = {
        inventory_item.id: inventory_item for inventory_item in inventory_data
    }
    color_key, _sk, _mk, _ok = get_positions(product_data['options'])

    # red: $123.00
    map_color_variant_to_wholesale_cost = {}

    # Fills in size variant costs from matching color variants
    # Sets wholesale_cost the non-zero minimum
    for variant in product_data['variants']:
        color_value = variant.get(color_key)
        variant_inventory_id = variant.get('inventory_item_id')
        variant_inventory_data = inventory_data_keyed_by_id.get(
            variant_inventory_id
        )

        possible_costs = []

        if variant_inventory_data and variant_inventory_data.wholesale_cost:
            possible_costs.append(
                Decimal(variant_inventory_data.wholesale_cost)
            )

        if map_color_variant_to_wholesale_cost.get(color_value):
            possible_costs.append(
                map_color_variant_to_wholesale_cost.get(color_value)
            )

        map_color_variant_to_wholesale_cost[color_value] = (
            min(possible_costs) if len(possible_costs) else 0
        )

    for variant in product_data['variants']:
        color_value = variant.get(color_key)
        variant['wholesale_cost'] = map_color_variant_to_wholesale_cost.get(
            color_value
        )

    return product_data
