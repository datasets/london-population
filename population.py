import datetime

from dataflows import Flow, load, dump_to_path, PackageWrapper, ResourceWrapper,printer

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

def london_population(link):
    Flow(
        load(link,
             format="xls",
             headers=[1, 2],
             fill_merged_cells=True,
             sheet=2),
        filter_population,
        set_format_and_name,
        dump_to_path(),
        printer(num_rows=1)

    ).process()


london_population('https://data.london.gov.uk/download/office-national-statistics-ons-population-estimates-borough/20dc1341-e74a-4e20-b1ff-a01c45e9fa10/ons-mye-population-totals.xls')
