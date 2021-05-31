"""
Counts total number of documents.
"""


def do_query(archives, config_file=None, logger=None, context=None):
    """
    Iterate through archives and return all titles grouped by year.
    """
    # [archive, archive, ...]
    documents = archives.flatMap(
        lambda archive: [(document.year, document) for document in list(archive)])
    info = documents.map(lambda d: (d[0], d[1].title)) \
        .groupByKey() \
        .map(lambda d: (d[0], list(d[1]))) \
        .collect()
    return info
