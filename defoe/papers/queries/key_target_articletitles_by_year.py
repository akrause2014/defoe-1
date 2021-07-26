"""
Get titles of the articles that contain target words and one or more keywords.
"""

import re
import sys
import yaml

from defoe import query_utils
from defoe.papers.query_utils import preprocess_clean_article, clean_article_as_string
from defoe.papers.query_utils import get_sentences_list_matches, get_articles_list_matches


def find_matches(text, keywords):
    match = []
    for key in keywords:
        pattern = re.compile(r'\b%s\b'%key)
        if re.search(pattern, text):
            match.append(key)
    return sorted(match)

def do_query(issues, config_file=None, logger=None, context=None):

    print('Loading config')
    with open(config_file, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
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

    unproc_targetwords = config['targetwords']
    targetwords = []
    for t in unproc_targetwords:
        targetwords.append(' '.join(
            [query_utils.preprocess_word(word, preprocess_type) for word in t.split()])
        )
    print(f'targetwords: {targetwords}')

    # [(year, article_string), ...]
    clean_articles = issues.flatMap(
        lambda issue: [(issue.date.year, issue, article, clean_article_as_string(
            article, defoe_path, os_type)) for article in issue.articles])


    # [(year, preprocess_article_string), ...]
    preprocessed_articles = clean_articles.flatMap(
        lambda cl_article: [(cl_article[0], cl_article[1], cl_article[2],
                                    preprocess_clean_article(cl_article[3], preprocess_type))])

    # [(year, clean_article_string)
    filter_articles = preprocessed_articles.filter(
        lambda year_article: any(t in year_article[3] for t in targetwords))

    # [(year, [keysentence, keysentence]), ...]
    matching_articles = filter_articles.map(
        lambda year_article: (
            year_article[0], 
            year_article[1], 
            year_article[2], 
            find_matches(year_article[3], keywords)
        ))

    matching_data = matching_articles.map(
        lambda sentence_data:
        (sentence_data[0],
        {"title": sentence_data[2].title_string,
         "article_id": sentence_data[2].article_id,
         "page_ids": list(sentence_data[2].page_ids),
         "section": sentence_data[2].ct,
         "keywords": sentence_data[3],
         "targets": targetwords,
         "issue_id": sentence_data[1].newspaper_id,
         "filename": sentence_data[1].filename}))

    result = matching_data \
        .groupByKey() \
        .map(lambda date_context:
             (date_context[0], list(date_context[1]))) \
        .collect()
    return result
