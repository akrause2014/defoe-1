"""
Get concordance (also called details) of articles in which we have keywords.
Filter those by target words and group results by year. 
This query is the recommended to use when there are target words. 
"""

from operator import add
import os
import sys
import yaml

from defoe import query_utils
from defoe.papers.query_utils import preprocess_clean_article, clean_article_as_string
from defoe.papers.query_utils import get_sentences_list_matches, get_articles_list_matches


def do_query(issues, config_file=None, logger=None, context=None):
    """
    Select the articles text along with metadata by using a list of 
    keywords or keysentences and groups by year.

    config_file must be the path to a lexicon file with a list of the keywords 
    to search for, one per line.

    Also the config_file can indicate the preprocess treatment, along with the defoe
    path, and the type of operating system. We can also configure how many target words 
    we want to use, and in which position the lexicon words starts. 

    Returns result of form:

        {
          <YEAR>:
          [
            [- article_id: 
             - authors:
             - filename:
             - issue_id:
             - page_ids:
             - text:
             - term
             - title ]
            ...
          ],
          <YEAR>:
          ...
        }


    :param issues: RDD of defoe.papers.issue.Issue
    :type archives: pyspark.rdd.PipelinedRDD
    :param config_file: query configuration file
    :type config_file: str or unicode
    :param logger: logger (unused)
    :type logger: py4j.java_gateway.JavaObject
    :return: number of occurrences of keywords grouped by year
    :rtype: dict
    """
    with open(config_file, "r") as f:
        config = yaml.load(f)

    os_type = "sys-i386-64"
    if sys.platform == 'linux':
        os_type = "sys-i386-64"
    elif sys.platform == 'darwin':
        os_type= "sys-i386-snow-leopard"

    if "defoe_path" in config :
        defoe_path= config["defoe_path"]
    else:
        defoe_path = "./"

    preprocess_type = query_utils.extract_preprocess_word_type(config)
    print(f'preprocessing: {preprocess_type}')

    unproc_keywords = config['keywords']
    keywords = []
    for k in unproc_keywords:
        keywords.append(' '.join(
            [query_utils.preprocess_word(word, preprocess_type) for word in k.split()])
        )
    print(f'keywords: {keywords}')


    # [(year, article_string), ...]
    clean_articles = issues.flatMap(
        lambda issue: [(issue.date.year, issue, article, clean_article_as_string(
            article, defoe_path, os_type)) for article in issue.articles])


    # [(year, preprocess_article_string), ...]
    t_articles = clean_articles.flatMap(
        lambda cl_article: [(cl_article[0], cl_article[1], cl_article[2],
                                    preprocess_clean_article(cl_article[3], preprocess_type))])

    # [(year, clean_article_string)
    filter_articles = t_articles.filter(
        lambda year_article: any(k in year_article[3] for k in keywords))

    # [(year, [keysentence, keysentence]), ...]
    # Note: get_articles_list_matches ---> articles count
    # Note: get_sentences_list_matches ---> word_count

    matching_articles = filter_articles.map(
        lambda year_article: (year_article[0], year_article[1], year_article[2], get_articles_list_matches(year_article[3], keywords)))

 #   matching_sentences = matching_articles.flatMap(
 #       lambda year_sentence: [(year_sentence[0], year_sentence[1], year_sentence[2], sentence)\
 #                               for sentence in year_sentence[3]])

    matching_data = matching_articles.map(
        lambda sentence_data:
        (sentence_data[0],
        {"title": sentence_data[2].title_string,
         "article_id:": sentence_data[2].article_id,
         "authors:": sentence_data[2].authors_string,
         "page_ids": list(sentence_data[2].page_ids),
         "section": sentence_data[2].ct,
         "term": sentence_data[3],
         "original text": sentence_data[2].words_string,
         "issue_id": sentence_data[1].newspaper_id,
         "filename": sentence_data[1].filename}))

    result = matching_data \
        .groupByKey() \
        .map(lambda date_context:
             (date_context[0], list(date_context[1]))) \
        .collect()
    return result
