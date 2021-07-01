"""
Given a filename create a defoe.books.archive.Archive.
"""

from defoe.phdtheses.dissertation import Dissertation


def filename_to_object(filename):
    """
    Given a filename create a defoe.pdf.document.PDFDocument.  If an error
    arises during its creation this is caught and returned as a
    string.

    :param filename: filename
    :type filename: str or unicode
    :return: tuple of form (Archive, None) or (filename, error message),
    if there was an error creating Archive
    :rtype: tuple(defoe.pdf.document.PDFDocument | str or unicode, str or unicode)
    """
    try:
        result = (Dissertation(filename), None)
    except Exception as exception:
        result = (filename, str(exception))
    return result

def object_as_dict(dissertation):
    yield {'id': dissertation.handle, 'text': " ".join(dissertation.words).replace('"', ",")}
