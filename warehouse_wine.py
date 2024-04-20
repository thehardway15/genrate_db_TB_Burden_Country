import csv
from collections import namedtuple
from tqdm import tqdm


def load_csv(file_name):
    results = []
    with open(file_name, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            results.append(row)
    return results

data = load_csv('Warehouse_and_Retail_Sales.csv')

# group month in quater
quater_1 = [1, 2, 3]
quater_2 = [4, 5, 6]
quater_3 = [7, 8, 9]
quater_4 = [10, 11, 12]

# record in csv
Record = namedtuple('Record', ['year', 'month', 'supplier', 'item_code', 'item_description', 'item_type', 'retail_sales', 'retail_transfers', 'warehouse_sales'])

records = []

for row in data:
    records.append(Record(row['YEAR'], row['MONTH'], row['SUPPLIER'], row['ITEM CODE'], row['ITEM DESCRIPTION'], row['ITEM TYPE'], row['RETAIL SALES'], row['RETAIL TRANSFERS'], row['WAREHOUSE SALES']))

print(records[0])


# Generate insert string from dict
def generate_insert_statements(data, table_name, batch_size=1000):
    insert_template = "INSERT INTO {} ({}) VALUES {};\n"
    columns = ', '.join(data[0].keys())

    insert_statements = []
    values = []

    for row in data:
        formatted_values = ', '.join("'{}'".format(val.replace("'", "''")) if isinstance(val, str) else str(val) if val is not None else 'NULL' for val in row.values())
        values.append(formatted_values)

        if len(values) == batch_size:
            values_str = ', '.join(['({})'.format(val) for val in values])
            insert_statements.append(insert_template.format(table_name, columns, values_str))
            values = []

    # Add any remaining data if not a perfect multiple of batch_size
    if values:
        values_str = ', '.join(['({})'.format(val) for val in values])
        insert_statements.append(insert_template.format(table_name, columns, values_str))

    return insert_statements

# period
years = []

for row in records:
    if row.year not in years:
        years.append(row.year)

print(years)
year_quater = []
start = 0
for i, year in enumerate(years):
    start += 1
    year_quater.append({'period_id': start, 'year': int(year), 'quarter': 1})
    year_quater.append({'period_id': start + 1, 'year': int(year), 'quarter': 2})
    year_quater.append({'period_id': start + 2, 'year': int(year), 'quarter': 3})
    year_quater.append({'period_id': start + 3, 'year': int(year), 'quarter': 4})
    start += 3

# insert_statements = generate_insert_statements(year_quater, 'Period')
# for insert_statement in insert_statements:
#     print(insert_statement)


# item type
item_type_list = []

for row in tqdm(records):
    if row.item_type not in item_type_list:
        item_type_list.append(row.item_type)

item_type_record = []
start = 0
for i, item_type in enumerate(item_type_list):
    start += 1
    item_type_record.append({'item_type_id': start, 'item_type': item_type})

# insert_statements = generate_insert_statements(item_type_record, 'Item_type')
# for insert_statement in insert_statements:
#     print(insert_statement)

# supplier
supplier_list = []

for row in tqdm(records):
    if row.supplier not in supplier_list:
        supplier_list.append(row.supplier)

supplier_record = []
start = 0
for i, supplier in enumerate(supplier_list):
    start += 1
    supplier_record.append({'supplier_id': start, 'supplier_name': supplier})

# insert_statements = generate_insert_statements(supplier_record, 'Supplier')
# for insert_statement in insert_statements:
#     print(insert_statement)

# item
# item record (item_code, item_description, item_type_id)
item_list = []
item_exists = {} # cache to check if item exists

for row in tqdm(records):
    ie = item_exists.get(row.item_code, False)

    # add item if not exists
    if not ie:
        item_exists[row.item_code] = True
    else:
        continue

    item_type_id = filter(lambda x: x['item_type'] == row.item_type, item_type_record) # find foreign key from item type
    item_type_id = next(item_type_id)['item_type_id']
    item_list.append((row.item_code, row.item_description, item_type_id))

# convert item list to dict with db field
item_record = []
start = 0
for i, item in enumerate(item_list):
    start += 1
    item_exists[item[0]] = start
    item_record.append({'item_id': start, 'item_code': item[0], 'item_description': item[1], 'item_type_id': item[2]})

# insert_statements = generate_insert_statements(item_record, 'Item')
# for insert_statement in insert_statements:
#     print(insert_statement)

# warehouse sales

warehouse_record = []
start = 1
warehouse_exists = {} # cache to check if warehouse exists

for row in tqdm(records):

    # select quater by month
    month_number = int(row.month)
    if month_number in [1, 2, 3]:
        quater = 1
    elif month_number in [4, 5, 6]:
        quater = 2
    elif month_number in [7, 8, 9]:
        quater = 3
    else:
        quater = 4

    # find period fk
    period_id = filter(lambda x: x['year'] == int(row.year) and x['quarter'] == quater, year_quater)
    period_id = next(period_id)['period_id']

    # find suplier fk
    supplier_id = filter(lambda x: x['supplier_name'] == row.supplier, supplier_record)
    supplier_id = next(supplier_id)['supplier_id']

    # item_id = filter(lambda x: x['item_code'] == row.item_code, item_record)
    # item_id = next(item_id)['item_id']

    # get item id from item cache
    item_id = item_exists.get(row.item_code, None)

    if item_id is None:
        raise Exception('item not found')

    # make key for cache
    key = f"{row.year}-{quater}-{row.item_code}"
    w_index = None
    if key in warehouse_exists:
        w_index = warehouse_exists[key]

    # w_record = list(filter(lambda x: x[1]['period_id'] == period_id and x[1]['supplier_id'] == supplier_id and x[1]['item_id'] == item_id, enumerate(warehouse_record)))

    if w_index is not None:
        # update existing record
        warehouse_record[w_index]['retails_sales'] += float(row.retail_sales or 0)
        warehouse_record[w_index]['retail_transfers'] += float(row.retail_transfers or 0)
        warehouse_record[w_index]['warehouse_sales'] += float(row.warehouse_sales or 0)
        warehouse_record[w_index]['record_count'] += 1
        warehouse_record[w_index]['retails_sales_min'] = min(warehouse_record[w_index]['retails_sales_min'], float(row.retail_sales or 0))
        warehouse_record[w_index]['retail_transfers_min'] = min(warehouse_record[w_index]['retail_transfers_min'], float(row.retail_transfers or 0))
        warehouse_record[w_index]['warehouse_sales_min'] = min(warehouse_record[w_index]['warehouse_sales_min'], float(row.warehouse_sales or 0))
        warehouse_record[w_index]['retails_sales_max'] = max(warehouse_record[w_index]['retails_sales_max'], float(row.retail_sales or 0))
        warehouse_record[w_index]['retail_transfers_max'] = max(warehouse_record[w_index]['retail_transfers_max'], float(row.retail_transfers or 0))
        warehouse_record[w_index]['warehouse_sales_max'] = max(warehouse_record[w_index]['warehouse_sales_max'], float(row.warehouse_sales or 0))
        warehouse_record[w_index]['retails_sales_avg'] = warehouse_record[w_index]['retails_sales'] / warehouse_record[w_index]['record_count']
        warehouse_record[w_index]['retail_transfers_avg'] = warehouse_record[w_index]['retail_transfers'] / warehouse_record[w_index]['record_count']
        warehouse_record[w_index]['warehouse_sales_avg'] = warehouse_record[w_index]['warehouse_sales'] / warehouse_record[w_index]['record_count']
    else:
        # insert new record
        warehouse_record.append({
            'sales_id': start,
            'period_id': period_id,
            'supplier_id': supplier_id,
            'item_id': item_id,
            'retails_sales': float(row.retail_sales or 0),
            'retail_transfers': float(row.retail_transfers or 0),
            'warehouse_sales': float(row.warehouse_sales or 0),
            'retails_sales_min': float(row.retail_sales or 0),
            'retail_transfers_min': float(row.retail_transfers or 0),
            'warehouse_sales_min': float(row.warehouse_sales or 0),
            'retails_sales_max': float(row.retail_sales or 0),
            'retail_transfers_max': float(row.retail_transfers or 0),
            'warehouse_sales_max': float(row.warehouse_sales or 0),
            'retails_sales_avg': float(row.retail_sales or 0),
            'retail_transfers_avg': float(row.retail_transfers or 0),
            'warehouse_sales_avg': float(row.warehouse_sales or 0),
            'record_count': 1
        })
        warehouse_exists[key] = len(warehouse_record)-1
        start += 1 # pk for warehouse record

print(len(warehouse_record))

# write warehouse sql
with open('warehouse.sql', 'w') as warehouse_sql:
    warehouse_sql.write('DELETE FROM Warehouse_sales')
    warehouse_sql.write('\n')
    warehouse_sql.write('DELETE FROM Item')
    warehouse_sql.write('\n')
    warehouse_sql.write('DELETE FROM Supplier')
    warehouse_sql.write('\n')
    warehouse_sql.write('DELETE FROM Period')
    warehouse_sql.write('\n')
    warehouse_sql.write('DELETE FROM Item_type')

    warehouse_sql.write('\n')
    warehouse_sql.write('\n')
    warehouse_sql.write('\n')
    warehouse_sql.write('\n')

    insert_statements = generate_insert_statements(year_quater, 'Period')
    for insert_statement in insert_statements:
        warehouse_sql.write(insert_statement + '\n')

    warehouse_sql.write('\n')
    warehouse_sql.write('\n')
    warehouse_sql.write('\n')
    warehouse_sql.write('\n')

    insert_statements = generate_insert_statements(item_type_record, 'Item_type')
    for insert_statement in insert_statements:
        warehouse_sql.write(insert_statement + '\n')

    warehouse_sql.write('\n')
    warehouse_sql.write('\n')
    warehouse_sql.write('\n')
    warehouse_sql.write('\n')

    insert_statements = generate_insert_statements(supplier_record, 'Supplier')
    for insert_statement in insert_statements:
        warehouse_sql.write(insert_statement + '\n')

    warehouse_sql.write('\n')
    warehouse_sql.write('\n')
    warehouse_sql.write('\n')
    warehouse_sql.write('\n')

    insert_statements = generate_insert_statements(item_record, 'Item')
    for insert_statement in insert_statements:
        warehouse_sql.write(insert_statement + '\n')

    warehouse_sql.write('\n')
    warehouse_sql.write('\n')
    warehouse_sql.write('\n')
    warehouse_sql.write('\n')

    insert_statements = generate_insert_statements(warehouse_record, 'Warehouse_sales')
    for insert_statement in insert_statements:
        warehouse_sql.write(insert_statement + '\n')

    warehouse_sql.write('\n')
    warehouse_sql.write('\n')
    warehouse_sql.write('\n')
    warehouse_sql.write('\n')