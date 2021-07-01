from io import StringIO

from defoe.spark_utils import open_stream

from pdfminer.layout import LAParams
from pdfminer.high_level import extract_text, extract_text_to_fp

class PDFDocument():

    def __init__(self, filename):
        self.filename = filename
        stream = open_stream(filename)
        output_string = StringIO()
        extract_text_to_fp(stream, output_string, laparams=LAParams())
        self.raw_text = output_string

    def words(self):
        return self.raw_text.getvalue().split()
