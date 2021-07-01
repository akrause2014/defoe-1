import json
import os

from defoe.pdf.document import PDFDocument

class Dissertation():
    def __init__(self, dirname) -> None:
        self.path = dirname
        self.documents = set()
        self.metadata = {}
        for filename in os.listdir(dirname):
            fullpath = os.path.join(dirname, filename)
            if filename.endswith('.pdf'):
                self.documents.add(PDFDocument(fullpath))
            elif filename.endswith('.json'):
                with open(fullpath) as f:
                    self.metadata[filename[:-5]] = json.load(f)

        try:
            self.handle = self.metadata['ERA']['handle']
        except:
            self.handle = dirname
    
    @property
    def words(self):
        all_words = []
        for pdf in self.documents:
            all_words += pdf.words()
        return all_words