from dataflows import Flow,dump_to_path, printer, ResourceWrapper, PackageWrapper, load,unpivot, delete_fields


def set_format_and_name_population(package: PackageWrapper):

    package.pkg.descriptor['name'] = 'population'
    package.pkg.descriptor['title'] = 'London population'

    package.pkg.descriptor['licenses'] = [{
        "name": "OGL",
        "path": 'http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/',
        "title": 'Open Government Licence'
    }]


    package.pkg.descriptor['resources'][0]['path'] = 'data/london-population-history.csv'
    package.pkg.descriptor['resources'][0]['name'] = 'london-population-history'

    package.pkg.descriptor['resources'][1]['path'] = 'data/london-population-projection.csv'
    package.pkg.descriptor['resources'][1]['name'] = 'london-population-projection'

    yield package.pkg
    res_iter = iter(package)
    first: ResourceWrapper = next(res_iter)
    second: ResourceWrapper = next(res_iter)
    yield first.it
    yield second.it
    yield from package


def filter_population(rows):
    for row in rows:
        if row['Code'] == 'H':
            yield row


def filter_population_projection(rows):
    for row in rows:
        #print (row['district'])
        if row['age'] != '-1':
            if row['district'] == 'London' and row['age'] == 'All Persons':
                yield row
        else:
            yield row



def add_gss_column_to_schema(package):
    package.pkg.descriptor['resources'][0]['schema']['fields'].append(dict(
        name='gss_code',
        type='any'
    ))
    yield package.pkg
    yield from package

def add_district_column_to_schema(package):
    package.pkg.descriptor['resources'][0]['schema']['fields'].append(dict(
        name='district',
        type='any'
    ))

    yield package.pkg
    yield from package

def add_component_column_to_schema(package):
    package.pkg.descriptor['resources'][0]['schema']['fields'].append(dict(
        name='component',
        type='any'
    ))

    yield package.pkg
    yield from package

def add_sex_column_to_schema(package):
    package.pkg.descriptor['resources'][0]['schema']['fields'].append(dict(
        name='sex',
        type='any'
    ))

    yield package.pkg
    yield from package

def add_age_column_to_schema(package):
    package.pkg.descriptor['resources'][0]['schema']['fields'].append(dict(
        name='age',
        type='any'
    ))

    yield package.pkg
    yield from package

def add_gss(row):
    row['gss_code'] = '-1'


def add_district(row):
    row['district'] = '-1'

def add_component(row):
    row['component'] = '-1'

def add_sex(row):
    row['sex'] = '-1'

def add_age(row):
    row['age'] = '-1'

def check_none(rows):
    for row in rows:
        if row['Value'] is not None:
            yield row

def check_repetition(rows):
    myDict = {}
    for row in rows:

        year = row['Year']
        year = year.replace('-','')
        if year in myDict:
            pass
        else:
            myDict[year] = row['Value']
            yield row


unpivot_fields = [{'name': 'PERSONS ALL AGES ([0-9]{4})', 'keys': {'Year': r'\1' '-01-01'}},
                  {'name': '([0-9]{4,5})', 'keys': {'Year':r'\1' '-01-01' }}
                  ]

extra_keys = [
    {'name': 'Year', 'type': 'any'}
]
extra_value = {'name': 'Value', 'type': 'any'}

Flow(
    load('https://data.london.gov.uk/download/office-national-statistics-ons-population-estimates-borough/20dc1341-e74a-4e20-b1ff-a01c45e9fa10/ons-mye-population-totals.xls',
        format="xls",
        headers=[1, 2],
        fill_merged_cells=True,
        sheet=2,
        engine='openpyxl'
    ),
    filter_population,
    delete_fields(['Code','New Code', 'Area name', 'Inner or Outer London', 'Area (hectares)', 'LAND AREA (Sq Km)']),
    add_gss_column_to_schema,
    add_district_column_to_schema,
    add_component_column_to_schema,
    add_sex_column_to_schema,
    add_age_column_to_schema,
    add_gss,
    add_district,
    add_component,
    add_sex,
    add_age,
    load(
        'https://data.london.gov.uk/download/projections/b9daabe6-6dd3-4082-adcf-a6b458ef4945/central_trend_2016_base.xlsx',
        format="xlsx",
        # headers=[1, 2],
        fill_merged_cells=True,
        sheet=1,
        engine='openpyxl'),
    filter_population_projection,
    delete_fields(['gss_code','district', 'component', 'sex', 'age']),
    unpivot(unpivot_fields, extra_keys, extra_value),
    check_none,
    check_repetition,
    set_format_and_name_population,
    dump_to_path(),
    printer()
).process()
