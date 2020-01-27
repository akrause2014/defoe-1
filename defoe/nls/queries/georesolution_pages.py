""" 
Identify the locations per page and geo-resolve them.
"""

from defoe import query_utils
from defoe.nls.query_utils import clean_page_as_string, georesolve_page
from pyspark.sql import Row, SparkSession, SQLContext

import yaml, os

def do_query(archives, config_file=None, logger=None, context=None):
    """
    Ingest NLS pages, applies scpaCy NLP pipeline for identifying the possible locations of each page. And applies the edinburgh geoparser for getting the latituted and longitude of each of them.
    Before applying the geoparser, two clean steps are applied - long-S and hyphen words. 
    
    Example:

    ("Descriptive account of the principal towns in Scotland: to accompany Wood's town atlas", '1828', 1828, 'Edinburgh', '/home/tdm/datasets/nls-data-gazetteersOfScotland/97350713', 'alto/97350911.34.xml', 'page', 'Page17', 376, 'book', 'nls', 'CONTENTS. Page. Aberdeen, 1 Annan, 19 Arbroath, 23 Ayr, .--SO Banff, 39 Berwick, 4S Brechin, 55 Crieff, 61 Cupar Fife, • 65 Dalkeith, 70 Dingwall, 76 DunbartorT, • 79 Dundee, 83 Dumfries, <• 91 Dunfermline, 99 Dunkeid, « 105 Edinburgh, -. . 1 1 1 Elgin, . . . ]29 Forfar, -135 Forres, 139 Glasgow, . 117', {}), ("Descriptive account of the principal towns in Scotland: to accompany Wood's town atlas", '1828', 1828, 'Edinburgh', '/home/tdm/datasets/nls-data-gazetteersOfScotland/97350713', 'alto/97350923.34.xml', 'page', 'Page18', 376, 'book', 'nls', 'Xll Greenock, 171 Haddington, 181 Hamilton, 185 Hawick, 191 Inverary, 199 Inverness, . * •> 203 Irvine, * 211 Jedburgh, * * 215 Kelso, 221 Kilmarnock, • 227 Kirkcaldy 233 Kinross, * * 241 Lanark, * 247 Leith, 253 Linlithgow, «• * 265 Montrose, 271 Nairn, 277 Paisley, 281 Peebles, 291 Perth, * 297 Portobello, 309 Rothesay, * 313 Selkirk, > , 319 St Andrews, 323 Stirling, -^331 Stonehaven, * 339 Stornowav, ... Si-5', {('Hamilton', '1'): ('55.77731433348086', '-4.067392672500774'), ('Inverary', '2'): ('56.2333333', '-5.0666667'), ('Inverness', '3'): ('57.47871409771949', '-4.212450527351024'), ('Lanark', '4'): ('55.67483195471274', '-3.775417694605498')}),



    :param archives: RDD of defoe.nls.archive.Archive
    :type archives: pyspark.rdd.PipelinedRDD
    :param config_file: query configuration file
    :type config_file: str or unicode
    :param logger: logger (unused)
    :type logger: py4j.java_gateway.JavaObject
    :return: "0"
    :rtype: string
    """
    with open(config_file, "r") as f:
        config = yaml.load(f)
    
    lang_model = config["lang_model"]
    text_unit = "page"
    # [(tittle, edition, year, place, archive filename, num pages, 
    # type of archive, model, document)]
    documents = archives.flatMap(
        lambda archive: [(document.title, document.edition, document.year, \
                          document.place, document.archive.filename, document.num_pages, \
                           document.document_type, document.model, document) for document in list(archive)])
    
    # [(tittle, edition, year, place, archive filename, page filename, text_unit, text_unit_id, 
    #   num_text_unit, type of archive, type of disribution, model, clean_page)]
    
    pages_clean = documents.flatMap(
        lambda year_document: [(year_document[0], year_document[1], year_document[2],\
                               year_document[3], year_document[4], page.code, text_unit, page.page_id, \
                               year_document[5], year_document[6], year_document[7], \
                               clean_page_as_string(page)) for page in year_document[8]])

    # [(tittle, edition, year, place, archive filename, page filename, text_unit, text_unit_id, 
    #   num_text_unit, type of archive, type of disribution, model, clean_page, georesolution_loc)]
    spacy_docs = pages_clean.flatMap(
        lambda clean_page: [(clean_page[0], clean_page[1], clean_page[2],\
                               clean_page[3], clean_page[4], clean_page[5], clean_page[6], clean_page[7], \
                               clean_page[8], clean_page[9], clean_page[10], clean_page[11],\
                               query_utils.spacy_nlp(clean_page[11], lang_model))])

    matching_docs = spacy_docs.map(
        lambda spacy_doc:
        (spacy_doc[0],
         {"edition": spacy_doc[1],
          "year": spacy_doc[2], 
          "place": spacy_doc[3],
          "archive_filename": spacy_doc[4],
          "page_filename": spacy_doc[5],
          "text_unit": spacy_doc[6],
          "text_unit id": spacy_doc[7],
          "num_text_unit": spacy_doc[8],
          "type_distribution": spacy_doc[9],
          "model": spacy_doc[10],
          "clean_text": spacy_doc[11],
          "display_doc": query_utils.display_spacy(spacy_doc[12]),
          "lang_model": lang_model,
          "georesolution_page": georesolve_page(spacy_doc[12])}))

    # [(title, {"edition": edition, ...}), ...]
    # =>
    # [(title, [{"edition": edition, ...], {...}), ...)]
    result = matching_docs \
        .groupByKey() \
        .map(lambda date_context:
             (date_context[0], list(date_context[1]))) \
        .collect()
    return result
