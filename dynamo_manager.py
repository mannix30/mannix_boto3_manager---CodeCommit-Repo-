import logging
import random
import uuid
import operator as op
from datetime import datetime
from decimal import Decimal
from pathlib import Path, PosixPath
from boto3.dynamodb.conditions import Key, Attr

import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s',)
log = logging.getLogger()

# Create a DynamoDB Table
def create_table(table_name, pk, pkdef):
    ddb = boto3.resource('dynamodb')
    table = ddb.create_table(
        TableName=table_name,
        KeySchema=pk,
        AttributeDefinitions=pkdef,
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5,
        }
    )
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    return table

# Use an existing DynamoDB Table        
def get_table(table_name):
    ddb = boto3.resource('dynamodb')
    return ddb.Table(table_name)
    
# Create an Item
def create_product(category, sku, **item):
    table = get_table('products')
    keys = {
        'category': category,
        'sku': sku,
    }
    item.update(keys)
    table.put_item(Item=item)
    return table.get_item(Key=keys)['Item']
    
# Update an Item
def update_product(category, sku, **item):
    table = get_table('products')
    keys = {
        'category': category,
        'sku': sku,
    }
    expr = ', '.join([f'{k}=:{k}' for k in item.keys()])
    vals = {f':{k}': v for k, v in item.items()}
    table.update_item(
        Key=keys,
        UpdateExpression=f'SET {expr}',
        ExpressionAttributeValues=vals,
    )
    return table.get_item(Key=keys)['Item']

# Delete an Item
def delete_product(category, sku):
    table = get_table('products')
    keys = {
        'category': category,
        'sku': sku,
    }
    res = table.delete_item(Key=keys)
    if res.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
        return True
    else:
        log.error(f'There was an error when deleting the product: {res}')
        return False
        
# Create an Item (Batch)
def create_items(table_name, items, keys=None):
    table = get_table(table_name)
    params = {
        'overwrite_by_pkeys': keys
    } if keys else {}
    with table.batch_writer(**params) as batch:
        for item in items:
            batch.put_item(Item=item)
    return True

# Search items (Batch)
def query_products(key_expr, filter_expr=None):
    # Query requires that you provide the key filters
    table = get_table('products')
    params = {
        'KeyConditionExpression': key_expr,
    }
    if filter_expr:
        params['FilterExpression'] = filter_expr
    res = table.query(**params)
    return res['Items']
    
# Scan products (Batch)
def scan_products(filter_expr):
# Scan does not require a key filter. It will go through
# all items in your table and return all matching items.
# Use with caution!
    table = get_table('products')
    params = {
        'FilterExpression': filter_expr,
    }
    res = table.scan(**params)
    return res['Items']
    
# Delete a table
def delete_dynamo_table(table_name):
    table = get_table(table_name)
    table.delete()
    table.wait_until_not_exists()
    return True
    
def main(args_)
    if hasattr(args_, 'func')
        if args_.func.__name__ == 'create_table'
            args_.func(args_.name)
        elif args_.func.__name__ == 'get_table'

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    # Subparser for Adhoc Processing
    sp = parser.add_subparsers( title = 'Adhoc Commands')
    
    #_______________Create table subcommand_______________
    sp_create_table = sp.add_parser('create_table', help = 'Create a DynamoDB Table',)
    sp_create_table.add_argument('name', help = 'Name of table to be created',)
    sp_create_table.add_argument('key_schema', help = 'KeySchema of table to be created',)
    sp_create_table.add_argument('attribute_definition', help = 'Attribute Definition of table to be created',)

    #_______________Use an existing table subcommand_______________
    sp_get_table = sp.add_parser('get_table', help = 'Use an existing DynamoDB Table',)
    sp_get_table.add_argument('name', help = 'Name of table to be used',)
    
    #_______________Create an Item subcommand_______________
    sp_create_product = sp.add_parser('create_product', help = 'Create an Item',)
    sp_create_product.add_argument('catergory', help = 'Category for the item to be created',)
    sp_create_product.add_argument('sku', help = 'SKU of item to be created')
    '''for **item'''
    #sp_create_product.add_argument('item')
    
    #_______________Update an Item subcommand_______________
    sp_update_product = sp.add_parser('update_product', help = 'Update an Item',)
    sp_update_product.add_argument('catergory', help = 'Category for the item to be updated')
    sp_update_product.add_argument('sku', help = 'SKU of item to be updated')
    '''for **item'''
    #sp_create_product.add_argument('item')
    
    #_______________Delete an Item subcommand_______________
    sp_delete_product = sp.add_parser('delete_product', help = 'Delete an Item',)
    sp_delete_product.add_argument('catergory', help = 'Category for the item to be deleted')
    sp_delete_product.add_argument('sku', help = 'SKU of item to be deleted')
    
    #_______________Delete a table______________
    sp_delete_table = sp.add_parser('delete_table', help = 'Delete a Table')
    sp_delete_table.add_argument('table_name', help = 'Name of table to be deleted',)
    
    # Subparser for Batch Processing
    spb = parser.add_subparsers( title = 'Batch processing commands')
    
    #_______________Create Items_______________
    spb_create_items = spb.add_parser('create_items', help = 'Create DynamoDB items',)
    spb_create_items.add_argument('table_name', help = 'Table for the items to be created',)
    spb_create_items.add_argument('items', help = 'Items to be created',)
    spb_create_items.add_argument('keys', help = 'Item keys')
    
    #_______________Search Items_______________
    # Subparser for search items
    spbs = parser.add_subparsers( title = 'Search item commands')
    #---------------Query items---------------
    spbs_search_items = spbs.add_parser('query_products', help = 'Search items with key filter',)
    spbs_search_items.add_argument('key_condition_expression', help = 'Key Condition Expression',)
    spbs_search_items.add_argument('filter_expression', help = 'Expression filter',)
    #---------------Scan Products---------------
    spbs_scan_products = spbs.add_parser('scan_products', help = 'Search items without a key filter',)
    spbs_scan_products.add_argument('filter_expression', help = 'Expression filter',)
    
    args_ = parser.parse_args()
    main(args_)