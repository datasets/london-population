import datetime

from dataflows import Flow, load, dump_to_path, PackageWrapper, ResourceWrapper,printer, unpivot, delete_fields

def set_format_and_name(package: PackageWrapper):
    package.pkg.descriptor['title'] = 'London population'
    package.pkg.descriptor['name'] = 'london-population'
    # Change path and name for the resource:
    package.pkg.descriptor['resources'][0]['path'] = 'data/population.csv'
    package.pkg.descriptor['resources'][0]['name'] = 'london-population'


    yield package.pkg
    res_iter = iter(package)
    first: ResourceWrapper = next(res_iter)
    yield first.it
    yield from package


def filter_population(rows):
    for row in rows:
        if row['Code'] is 'H':
            yield row


# TODO: fix unpivoting with regex.. so we can unpivot all historical data
unpivot_fields = [
    {'name': 'PERSONS ALL AGES 2000', 'keys': {'Year': 2000}},
    {'name': 'PERSONS ALL AGES 2001', 'keys': {'Year': 2001}},
    {'name': 'PERSONS ALL AGES 2002', 'keys': {'Year': 2002}},
    {'name': 'PERSONS ALL AGES 2003', 'keys': {'Year': 2003}},
    {'name': 'PERSONS ALL AGES 2004', 'keys': {'Year': 2004}},
    {'name': 'PERSONS ALL AGES 2005', 'keys': {'Year': 2005}},
    {'name': 'PERSONS ALL AGES 2006', 'keys': {'Year': 2006}},
    {'name': 'PERSONS ALL AGES 2007', 'keys': {'Year': 2007}},
    {'name': 'PERSONS ALL AGES 2008', 'keys': {'Year': 2008}},
    {'name': '2009', 'keys': {'Year': 2009}},
    {'name': '2010', 'keys': {'Year': 2010}},
    {'name': '2011', 'keys': {'Year': 2011}},
    {'name': '2012', 'keys': {'Year': 2012}},
    {'name': '2013', 'keys': {'Year': 2013}},
    {'name': '2014', 'keys': {'Year': 2014}},
    {'name': '2015', 'keys': {'Year': 2015}},
]
extra_keys = [
    {'name': 'Year', 'type': 'year'}
]
extra_value = {'name': 'Value', 'type': 'integer'}


def london_population(link):
    Flow(
        load(link,
             format="xls",
             headers=[1, 2],
             fill_merged_cells=True,
             sheet=2),
        filter_population,
        delete_fields(['Code', 'New Code', 'Area name', 'Inner or Outer London', 'Area (hectares)', 'LAND AREA (Sq Km)']),
        set_format_and_name,
        unpivot(unpivot_fields, extra_keys, extra_value),
        dump_to_path(),
        printer(num_rows=1)

    ).process()


london_population('https://data.london.gov.uk/download/office-national-statistics-ons-population-estimates-borough/20dc1341-e74a-4e20-b1ff-a01c45e9fa10/ons-mye-population-totals.xls')
