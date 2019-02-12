import datetime

from dataflows import Flow, load, dump_to_path, PackageWrapper, ResourceWrapper,printer, unpivot, delete_fields

def set_format_and_name(package: PackageWrapper):
    package.pkg.descriptor['title'] = 'London population'
    package.pkg.descriptor['name'] = 'population'
    # Change path and name for the resource:
    package.pkg.descriptor['resources'][0]['path'] = 'data/population.csv'
    package.pkg.descriptor['resources'][0]['name'] = 'population'

    yield package.pkg
    res_iter = iter(package)
    first: ResourceWrapper = next(res_iter)
    yield first.it
    yield from package


def filter_population(rows):
    for row in rows:
        if row['Code'] is 'H':
            yield row



unpivot_fields = [{'name': 'PERSONS ALL AGES ([0-9]{4})', 'keys': {'Year': r'\1' '-01-01'}},
                  {'name': '([0-9]{4,5})', 'keys': {'Year':r'\1' '-01-01' }}
                  ]

extra_keys = [
    {'name': 'Year', 'type': 'any'}
]
extra_value = {'name': 'Value', 'type': 'any'}


def london_population(link, ):
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