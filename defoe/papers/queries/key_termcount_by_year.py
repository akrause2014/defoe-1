"""
Count number of times that keywords appear
Group results by year
"""

from operator import add

from defoe import query_utils
from defoe.papers.query_utils import clean_article_as_string, get_sentences_list_matches, preprocess_clean_article

import yaml, os, sys

def do_query(issues, config_file=None, logger=None, context=None):
    print('Loading config')
    with open(config_file, "r") as f:
        config = yaml.load(f)
    print(f'config: {config}')

    if sys.platform == "linux":
        os_type = "sys-i386-64"
    else:
        os_type= "sys-i386-snow-leopard"
    print(f'platform: {sys.platform}')

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

    clean_articles = issues.flatMap(
        lambda issue: [(issue.date.year, clean_article_as_string(
            article, defoe_path, os_type)) for article in issue.articles])

    preprocessed_articles = clean_articles.map(
        lambda cl_article: (cl_article[0], preprocess_clean_article(cl_article[1], preprocess_type)))

    # [(year, article_string)
    filter_articles = preprocessed_articles.filter(
        lambda year_article: any(
            k in year_article[1] for k in keywords))

    # [(year, [keysentence, keysentence]), ...]
    # Note: get_articles_list_matches ---> articles count
    # Note: get_sentences_list_matches ---> word_count
    matching_articles = filter_articles.map(
        lambda year_article: (year_article[0],
                               get_sentences_list_matches(
                                  year_article[1],
                                  keywords)))

    # [[(year, keysentence), 1) ((year, keysentence), 1) ] ...]
    matching_sentences = matching_articles.flatMap(
        lambda year_sentence: [((year_sentence[0], sentence), 1)
                               for sentence in year_sentence[1]])


    # [((year, keysentence), num_keysentences), ...]
    # =>
    # [(year, (keysentence, num_keysentences)), ...]
    # =>
    # [(year, [keysentence, num_keysentences]), ...]
    result = matching_sentences\
        .reduceByKey(add)\
        .map(lambda yearsentence_count:
             (yearsentence_count[0][0],
              (yearsentence_count[0][1], yearsentence_count[1]))) \
        .groupByKey() \
        .map(lambda a: (a[0], [{x[0]: x[1]} for x in a[1]])) \
        .collect()
    return result

