# Script to load CSV ane generate SQL for it

import csv
import sqlite3
from collections import namedtuple

def load_csv(file_name):
    results = []
    with open(file_name, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            results.append(row)
    return results


data = load_csv('TB_Burden_Country.csv')

#labels = list(data[0].keys())
#print(labels)

# label to database column
labels_mapper = {
    'Country or territory name': 'country_name',
    'ISO 2-character country/territory code': 'country_iso_code',
    'ISO 3-character country/territory code': 'country_iso3_code',
    'ISO numeric country/territory code': 'country_iso_numeric',
    'Region': 'region',
    'Year': 'record_year',
    'Estimated total population number': 'country_population',
    'Estimated prevalence of TB (all forms) per 100 000 population': 'prevalence',
    'Estimated prevalence of TB (all forms) per 100 000 population, low bound': 'prevalence_lb',
    'Estimated prevalence of TB (all forms) per 100 000 population, high bound': 'prevalence_ub',
    'Estimated prevalence of TB (all forms)': 'prevalence_all_forms',
    'Estimated prevalence of TB (all forms), low bound': 'prevalence_all_forms_lb',
    'Estimated prevalence of TB (all forms), high bound': 'prevalence_all_forms_ub',
    'Method to derive prevalence estimates': 'prevalence_estimation_method',
    'Estimated mortality of TB cases (all forms, excluding HIV) per 100 000 population': 'mortality_excluding_HIV',
    'Estimated mortality of TB cases (all forms, excluding HIV), per 100 000 population, low bound': 'mortality_excluding_HIV_lb',
    'Estimated mortality of TB cases (all forms, excluding HIV), per 100 000 population, high bound': 'mortality_excluding_HIV_ub',
    'Estimated number of deaths from TB (all forms, excluding HIV)': 'deaths_excluding_HIV',
    'Estimated number of deaths from TB (all forms, excluding HIV), low bound': 'deaths_excluding_HIV_lb',
    'Estimated number of deaths from TB (all forms, excluding HIV), high bound': 'deaths_excluding_HIV_ub',
    'Estimated mortality of TB cases who are HIV-positive, per 100 000 population': 'mortality_HIV_positive',
    'Estimated mortality of TB cases who are HIV-positive, per 100 000 population, low bound': 'mortality_HIV_positive_lb',
    'Estimated mortality of TB cases who are HIV-positive, per 100 000 population, high bound': 'mortality_HIV_positive_ub',
    'Estimated number of deaths from TB in people who are HIV-positive': 'deaths_HIV_positive',
    'Estimated number of deaths from TB in people who are HIV-positive, low bound': 'deaths_HIV_positive_lb',
    'Estimated number of deaths from TB in people who are HIV-positive, high bound': 'deaths_HIV_positive_ub',
    'Method to derive mortality estimates': 'mortality_estimation_method',
    'Estimated incidence (all forms) per 100 000 population': 'incidence',
    'Estimated incidence (all forms) per 100 000 population, low bound': 'incidence_lb',
    'Estimated incidence (all forms) per 100 000 population, high bound': 'incidence_ub',
    'Estimated number of incident cases (all forms)': 'incident_cases',
    'Estimated number of incident cases (all forms), low bound': 'incident_cases_lb',
    'Estimated number of incident cases (all forms), high bound': 'incident_cases_ub',
    'Method to derive incidence estimates': 'incidence_estimation_method',
    'Estimated HIV in incident TB (percent)': 'HIV_in_incident_TB_percent',
    'Estimated HIV in incident TB (percent), low bound': 'HIV_in_incident_TB_percent_lb',
    'Estimated HIV in incident TB (percent), high bound': 'HIV_in_incident_TB_percent_ub',
    'Estimated incidence of TB cases who are HIV-positive per 100 000 population': 'incidence_HIV_positive',
    'Estimated incidence of TB cases who are HIV-positive per 100 000 population, low bound': 'incidence_HIV_positive_lb',
    'Estimated incidence of TB cases who are HIV-positive per 100 000 population, high bound': 'incidence_HIV_positive_ub',
    'Estimated incidence of TB cases who are HIV-positive': 'incidence_HIV_positive_all_forms',
    'Estimated incidence of TB cases who are HIV-positive, low bound': 'incidence_HIV_positive_all_forms_lb',
    'Estimated incidence of TB cases who are HIV-positive, high bound': 'incidence_HIV_positive_all_forms_ub',
    'Method to derive TBHIV estimates': 'TBHIV_estimation_method',
    'Case detection rate (all forms), percent': 'case_detection_rate_percent',
    'Case detection rate (all forms), percent, low bound': 'case_detection_rate_percent_lb',
    'Case detection rate (all forms), percent, high bound': 'case_detection_rate_percent_ub'
}

data_with_correct_label = []
# change keys
for row in data:
    new_row = {}
    for key, value in row.items():
        if key in labels_mapper:
            new_row[labels_mapper[key]] = value
    data_with_correct_label.append(new_row)


# for row in data_with_correct_label[0:50]:
#     print(row)

# country table
countries_table = []
country_row = namedtuple('country_row', 'country_name country_iso_code country_iso3_code country_iso_numeric id')
country_pk = 1
for row in data_with_correct_label:
    if len(list(filter(lambda x: x['country_name'] == row['country_name'], countries_table))) > 0:
        continue
    country = country_row(
        country_name=row['country_name'],
        country_iso_code=row['country_iso_code'],
        country_iso3_code=row['country_iso3_code'],
        country_iso_numeric=row['country_iso_numeric'],
        id=country_pk
    )
    country_pk += 1
    countries_table.append(country._asdict())

# relation country to data
for row in data_with_correct_label:
    country_pk = filter(lambda x: x['country_name'] == row['country_name'], countries_table)
    country_pk = next(country_pk)['id']
    row['country_id'] = country_pk
    # remove unwanted columns
    del row['country_name']
    del row['country_iso_code']
    del row['country_iso3_code']
    del row['country_iso_numeric']

# region table
regions_table = []
region_row = namedtuple('region_row', 'region id')
region_pk = 1
for row in data_with_correct_label:
    if len(list(filter(lambda x: x['region'] == row['region'], regions_table))) > 0:
        continue
    region = region_row(
        region=row['region'],
        id=region_pk
    )
    region_pk += 1
    regions_table.append(region._asdict())

# relation region to data
for row in data_with_correct_label:
    region_pk = filter(lambda x: x['region'] == row['region'], regions_table)
    region_pk = next(region_pk)['id']
    row['region_id'] = region_pk
    # remove unwanted columns
    del row['region']

# estimate method table
estimate_method_table = []
estimate_method_row = namedtuple('estimate_method_row', 'estimate_method id')
estimate_method_pk = 1
for row in data_with_correct_label:
    if len(list(filter(lambda x: x['estimate_method'] == row['prevalence_estimation_method'], estimate_method_table))) == 0:
        estimate_method = estimate_method_row(
            estimate_method=row['prevalence_estimation_method'],
            id=estimate_method_pk
        )
        estimate_method_pk += 1
        estimate_method_table.append(estimate_method._asdict())

    if len(list(filter(lambda x: x['estimate_method'] == row['mortality_estimation_method'], estimate_method_table))) == 0:
        estimate_method = estimate_method_row(
            estimate_method=row['mortality_estimation_method'],
            id=estimate_method_pk
        )
        estimate_method_pk += 1
        estimate_method_table.append(estimate_method._asdict())

    if len(list(filter(lambda x: x['estimate_method'] == row['incidence_estimation_method'], estimate_method_table))) == 0:
        estimate_method = estimate_method_row(
            estimate_method=row['incidence_estimation_method'],
            id=estimate_method_pk
        )
        estimate_method_pk += 1
        estimate_method_table.append(estimate_method._asdict())

    if len(list(filter(lambda x: x['estimate_method'] == row['TBHIV_estimation_method'], estimate_method_table))) == 0:
        estimate_method = estimate_method_row(
            estimate_method=row['TBHIV_estimation_method'],
            id=estimate_method_pk
        )
        estimate_method_pk += 1
        estimate_method_table.append(estimate_method._asdict())

# relation estimate method to data
for row in data_with_correct_label:
    estimate_method_pk = filter(lambda x: x['estimate_method'] == row['prevalence_estimation_method'], estimate_method_table)
    estimate_method_pk = next(estimate_method_pk)['id']
    row['prevalence_estimation_method_id'] = estimate_method_pk
    estimate_method_pk = filter(lambda x: x['estimate_method'] == row['mortality_estimation_method'], estimate_method_table)
    estimate_method_pk = next(estimate_method_pk)['id']
    row['mortality_estimation_method_id'] = estimate_method_pk
    estimate_method_pk = filter(lambda x: x['estimate_method'] == row['incidence_estimation_method'], estimate_method_table)
    estimate_method_pk = next(estimate_method_pk)['id']
    row['incidence_estimation_method_id'] = estimate_method_pk
    estimate_method_pk = filter(lambda x: x['estimate_method'] == row['TBHIV_estimation_method'], estimate_method_table)
    estimate_method_pk = next(estimate_method_pk)['id']
    row['TBHIV_estimation_method_id'] = estimate_method_pk

    # remove unwanted columns
    del row['prevalence_estimation_method']
    del row['mortality_estimation_method']
    del row['incidence_estimation_method']
    del row['TBHIV_estimation_method']

print('estimate_method_table', len(estimate_method_table))
print('countries_table', len(countries_table))
print('regions_table', len(regions_table))
print('data_with_correct_label', len(data_with_correct_label))


# number in string to int
for row in data_with_correct_label:
    for key, value in row.items():
        if isinstance(row[key], str) and value.isnumeric():
            row[key] = int(value)
        if isinstance(row[key], str) and value.replace('.', '', 1).isdigit():
            row[key] = float(value)
        if value == '':
            row[key] = None

for row in countries_table:
    for key, value in row.items():
        if isinstance(value, str) and value.isnumeric():
            row[key] = int(value)

#for row in data_with_correct_label[0:50]:
#    print(row)

#print(data_with_correct_label[0])
# for key, value in data_with_correct_label[0].items():
#     print(key, value)

# keys in record
# 'year',
# 'population',
# 'prevalence',
# 'prevalence_lb',
# 'prevalence_ub',
# 'prevalence_all_forms',
# 'prevalence_all_forms_lb',
# 'prevalence_all_forms_ub',
# 'mortality_excluding_HIV',
# 'mortality_excluding_HIV_lb',
# 'mortality_excluding_HIV_ub',
# 'deaths_excluding_HIV',
# 'deaths_excluding_HIV_lb',
# 'deaths_excluding_HIV_ub',
# 'mortality_HIV_positive',
# 'mortality_HIV_positive_lb',
# 'mortality_HIV_positive_ub',
# 'deaths_HIV_positive',
# 'deaths_HIV_positive_lb',
# 'deaths_HIV_positive_ub',
# 'incidence',
# 'incidence_lb',
# 'incidence_ub',
# 'incident_cases',
# 'incident_cases_lb',
# 'incident_cases_ub',
# 'HIV_in_incident_TB_percent',
# 'HIV_in_incident_TB_percent_lb',
# 'HIV_in_incident_TB_percent_ub',
# 'incidence_HIV_positive',
# 'incidence_HIV_positive_lb',
# 'incidence_HIV_positive_ub',
# 'incidence_HIV_positive_all_forms',
# 'incidence_HIV_positive_all_forms_lb',
# 'incidence_HIV_positive_all_forms_ub',
# 'case_detection_rate_percent',
# 'case_detection_rate_percent_lb',
# 'case_detection_rate_percent_ub',
# 'country_pk',
# 'region_pk',
# 'prevalence_estimation_method_pk',
# 'mortality_estimation_method_pk',
# 'incidence_estimation_method_pk',
# 'TBHIV_estimation_method_pk'])

## Generate SQL

### Create table

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


#### Country
sql = 'CREATE TABLE country (\n'
sql += '  id int IDENTITY(1,1) PRIMARY KEY, \n'
sql += '  country_name VARCHAR(255), \n'
sql += '  country_iso_code VARCHAR(2), \n'
sql += '  country_iso3_code VARCHAR(3), \n'
sql += '  country_iso_numeric int \n'
sql += ');'
creste_country_sql = sql

#### Region
sql = 'CREATE TABLE region (\n'
sql += '  id int IDENTITY(1,1) PRIMARY KEY, \n'
sql += '  region VARCHAR(255) \n'
sql += ');'
creste_region_sql = sql

#### Estimate method
sql = 'CREATE TABLE estimate_method (\n'
sql += '  id int IDENTITY(1,1) PRIMARY KEY, \n'
sql += '  estimate_method VARCHAR(255) \n'
sql += ');'
creste_estimate_method_sql = sql

#### Record
sql = 'CREATE TABLE record (\n'
sql += '  id int IDENTITY(1,1) PRIMARY KEY, \n'
sql += '  country_id int FOREIGN KEY (country_id) REFERENCES country(id), \n'
sql += '  region_id int FOREIGN KEY (region_id) REFERENCES region(id), \n'
sql += '  prevalence_estimation_method_id int FOREIGN KEY (prevalence_estimation_method_id) REFERENCES estimate_method(id), \n'
sql += '  mortality_estimation_method_id int FOREIGN KEY (mortality_estimation_method_id) REFERENCES estimate_method(id), \n'
sql += '  incidence_estimation_method_id int FOREIGN KEY (incidence_estimation_method_id) REFERENCES estimate_method(id), \n'
sql += '  TBHIV_estimation_method_id int FOREIGN KEY (TBHIV_estimation_method_id) REFERENCES estimate_method(id), \n'
sql += '  record_year int, \n'
sql += '  country_population int, \n'
sql += '  prevalence int, \n'
sql += '  prevalence_lb int, \n'
sql += '  prevalence_ub int, \n'
sql += '  prevalence_all_forms int, \n'
sql += '  prevalence_all_forms_lb int, \n'
sql += '  prevalence_all_forms_ub int, \n'
sql += '  mortality_excluding_HIV int, \n'
sql += '  mortality_excluding_HIV_lb int, \n'
sql += '  mortality_excluding_HIV_ub int, \n'
sql += '  deaths_excluding_HIV int, \n'
sql += '  deaths_excluding_HIV_lb int, \n'
sql += '  deaths_excluding_HIV_ub int, \n'
sql += '  mortality_HIV_positive FLOAT, \n'
sql += '  mortality_HIV_positive_lb FLOAT, \n'
sql += '  mortality_HIV_positive_ub FLOAT, \n'
sql += '  deaths_HIV_positive FLOAT, \n'
sql += '  deaths_HIV_positive_lb FLOAT, \n'
sql += '  deaths_HIV_positive_ub FLOAT, \n'
sql += '  incidence int, \n'
sql += '  incidence_lb int, \n'
sql += '  incidence_ub int, \n'
sql += '  incident_cases int, \n'
sql += '  incident_cases_lb int, \n'
sql += '  incident_cases_ub int, \n'
sql += '  HIV_in_incident_TB_percent FLOAT, \n'
sql += '  HIV_in_incident_TB_percent_lb FLOAT, \n'
sql += '  HIV_in_incident_TB_percent_ub FLOAT, \n'
sql += '  incidence_HIV_positive FLOAT, \n'
sql += '  incidence_HIV_positive_lb FLOAT, \n'
sql += '  incidence_HIV_positive_ub FLOAT, \n'
sql += '  incidence_HIV_positive_all_forms FLOAT, \n'
sql += '  incidence_HIV_positive_all_forms_lb FLOAT, \n'
sql += '  incidence_HIV_positive_all_forms_ub FLOAT, \n'
sql += '  case_detection_rate_percent float, \n'
sql += '  case_detection_rate_percent_lb float, \n'
sql += '  case_detection_rate_percent_ub float \n'
sql += ');'
creste_record_sql = sql

### Insert data in bach 1000 row

# country
insert_statements = generate_insert_statements(countries_table, 'country', batch_size=1000)
insert_country_sql = insert_statements

# region
insert_statements = generate_insert_statements(regions_table, 'region', batch_size=1000)
insert_region_sql = insert_statements

# estimate_method
insert_statements = generate_insert_statements(estimate_method_table, 'estimate_method', batch_size=1000)
insert_estimate_method_sql = insert_statements

# record
insert_statements = generate_insert_statements(data_with_correct_label, 'record', batch_size=1000)
insert_record_sql = insert_statements

# Save to single SQL file

with open('schema.sql', 'w') as f:
    # DROP IF EXISTS

    f.write('DROP TABLE IF EXISTS record;\n')
    f.write('DROP TABLE IF EXISTS country;\n')
    f.write('DROP TABLE IF EXISTS region;\n')
    f.write('DROP TABLE IF EXISTS estimate_method;\n')
    f.write('\n')
    f.write('\n')

    f.write(creste_country_sql)
    f.write('\n')
    f.write('\n')
    f.write(creste_region_sql)
    f.write('\n')
    f.write('\n')
    f.write(creste_estimate_method_sql)
    f.write('\n')
    f.write('\n')
    f.write(creste_record_sql)
    f.write('\n')
    f.write('\n')

    for insert_statement in insert_country_sql:
        f.write(insert_statement)
        f.write('\n')
        f.write('\n')

    for insert_statement in insert_region_sql:
        f.write(insert_statement)
        f.write('\n')
        f.write('\n')

    for insert_statement in insert_estimate_method_sql:
        f.write(insert_statement)
        f.write('\n')
        f.write('\n')

    for insert_statement in insert_record_sql:
        f.write(insert_statement)
        f.write('\n')
        f.write('\n')
