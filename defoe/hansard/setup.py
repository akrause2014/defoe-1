"""
Given a filename create a defoe.papers.issue.Issue.
"""

from defoe.hansard.debates import Hansard


def filename_to_object(filename):
    """
    Given a filename create a defoe.hansard.Hansard. If an error
    arises during its creation this is caught and returned as a
    string.

    :param filename: filename
    :type filename: str or unicode
    :return: tuple of form (Hansard, None) or (filename, error message),
    if there was an error creating Hansard
    :rtype: tuple(defoe.hansard. | str or unicode, str or unicode)
    """
    try:
        result = (Hansard(filename), None)
    except Exception as exception:
        result = (filename, str(exception))
    return result

def object_as_dict(issue):
    for article in issue:
        if article.words:
            yield {"id": article.article_id, "text": " ".join(article.words).replace('"', ",")}
