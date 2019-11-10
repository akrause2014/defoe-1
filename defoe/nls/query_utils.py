"""
Query-related utility functions.
"""

from defoe import query_utils
from defoe.query_utils import PreprocessWordType
from nltk.corpus import words
import re
# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def get_page_matches(document,
                     keywords,
                     preprocess_type=PreprocessWordType.NORMALIZE):
    """
    Get pages within a document that include one or more keywords.
    For each page that includes a specific keyword, add a tuple of
    form:

        (<YEAR>, <DOCUMENT>, <PAGE>, <KEYWORD>)

    If a keyword occurs more than once on a page, there will be only
    one tuple for the page for that keyword.
    If more than one keyword occurs on a page, there will be one tuple
    per keyword.

    :param document: document
    :type document: defoe.nls.document.Document
    :param keywords: keywords
    :type keywords: list(str or unicode:
    :param preprocess_type: how words should be preprocessed
    (normalize, normalize and stem, normalize and lemmatize, none)
    :type preprocess_type: defoe.query_utils.PreprocessWordType
    :return: list of tuples
    :rtype: list(tuple)
    """
    matches = []
    for keyword in keywords:
        for page in document:
            match = None
            for word in page.words:
                preprocessed_word = query_utils.preprocess_word(
                    word, preprocess_type)
                if preprocessed_word == keyword:
                    match = (document.year, document, page, keyword)
                    break
            if match:
                matches.append(match)
                continue  # move to next page
    return matches


def get_document_keywords(document,
                          keywords,
                          preprocess_type=PreprocessWordType.NORMALIZE):
    """
    Gets list of keywords occuring within an document.

    :param article: Article
    :type article: defoe.papers.article.Article
    :param keywords: keywords
    :type keywords: list(str or unicode)
    :param preprocess_type: how words should be preprocessed
    (normalize, normalize and stem, normalize and lemmatize, none)
    :type preprocess_type: defoe.query_utils.PreprocessWordType
    :return: sorted list of keywords that occur within article
    :rtype: list(str or unicode)
    """
    matches = set()
    for page in document:
        for word in page.words:
            preprocessed_word = query_utils.preprocess_word(word,
                                                            preprocess_type)
            if preprocessed_word in keywords:
                matches.add(preprocessed_word)
    return sorted(list(matches))


def document_contains_word(document,
                           keyword,
                           preprocess_type=PreprocessWordType.NORMALIZE):
    """
    Checks if a keyword occurs within an article.

    :param article: Article
    :type article: defoe.papers.article.Article
    :param keywords: keyword
    :type keywords: str or unicode
    :param preprocess_type: how words should be preprocessed
    (normalize, normalize and stem, normalize and lemmatize, none)
    :type preprocess_type: defoe.query_utils.PreprocessWordType
    :return: True if the article contains the word, false otherwise
    :rtype: bool
    """
    for page in document:
        for word in page.words:
            preprocessed_word = query_utils.preprocess_word(word,
                                                            preprocess_type)
            if keyword == preprocessed_word:
                return True
    return False


def calculate_words_within_dictionary(page, 
                   preprocess_type=PreprocessWordType.NORMALIZE):
    """
    Calculates the % of page words within a dictionary and also returns the page quality (pc)
    Page words are normalized. 
    :param page: Page
    :type page: defoe.nls.page.Page
    :param preprocess_type: how words should be preprocessed
    (normalize, normalize and stem, normalize and lemmatize, none)
    :return: matches
    :rtype: list(str or unicode)
    """
    dictionary = words.words()
    counter= 0
    total_words= 0
    for word in page.words:
         preprocessed_word = query_utils.preprocess_word(word, preprocess_type)
         if preprocessed_word!="":
            total_words += 1
            if  preprocessed_word in dictionary:
               counter +=  1
    try:
       calculate_pc = str(counter*100/total_words)
    except:
       calculate_pc = "0" 
    return calculate_pc

def calculate_words_confidence_average(page):
    """
    Calculates the average of "words confidence (wc)"  within a page.
    Page words are normalized. 
    :param page: Page
    :type page: defoe.nls.page.Page
    :param preprocess_type: how words should be preprocessed
    (normalize, normalize and stem, normalize and lemmatize, none)
    :return: matches
    :rtype: list(str or unicode)
    """
    dictionary = words.words()
    counter= 0
    total_wc= 0
    for wc in page.wc:
               total_wc += float(wc)
    try:
       calculate_wc = str(total_wc/len(page.wc))
    except:
       calculate_wc = "0" 
    return calculate_wc

def get_page_as_string(page,
                          preprocess_type=PreprocessWordType.LEMMATIZE):
    """
    Return a page as a single string.

    :param page: Page
    :type page: defoe.nls.Page
    :param preprocess_type: how words should be preprocessed
    (normalize, normalize and stem, normalize and lemmatize, none)
    :type preprocess_type: defoe.query_utils.PreprocessWordType
    :return: page words as a string
    :rtype: string or unicode
    """
    page_string = ''
    for word in page.words:
        preprocessed_word = query_utils.preprocess_word(word,
                                                         preprocess_type)
        if page_string == '':
            page_string = preprocessed_word
        else:
            page_string += (' ' + preprocessed_word)
    return page_string


def get_sentences_list_matches(text, keysentence):
    """
    Check which key-sentences from occurs within a string
    and return the list of matches.

    :param text: text
    :type text: str or unicode
    :type: list(str or uniocde)
    :return: Set of sentences
    :rtype: set(str or unicode)
    """
    match = []
    text_list= text.split()
    for sentence in keysentence:
        if len(sentence.split()) > 1:
            if sentence in text:
                count = text.count(sentence)
                for i in range(0, count):
                    match.append(sentence)
        else:
            pattern = re.compile(r'^%s$'%sentence)
            for word in text_list:
                if re.search(pattern, word):
                    match.append(sentence)
    return sorted(match)

def toCSVLine(data):

  return ','.join(str(d) for d in data)
