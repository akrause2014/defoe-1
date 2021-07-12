from collections import defaultdict, namedtuple
from lxml import etree

from defoe.spark_utils import open_stream

Paragraph = namedtuple('Paragraph', ['id', 'text'])
Speaker = namedtuple('Speaker', ['id', 'name'])

class Heading():
    def __init__(self, id, title):
        self._id = id
        self.title = title
        self.speeches = []
        self.subheadings = []
    def add_speech(self, speech):
        self.speeches.append(speech)

class Speech():
    def __init__(self, id, speaker) -> None:
        self._id = id
        self.speaker = speaker
        self.paragraphs = []
    def add_paragraph(self, pid, text):
        self.paragraphs.append(Paragraph(pid, text))
    @property
    def text(self):
        if self.paragraphs:
            result = ''
            for p in self.paragraphs:
                result += p.text
            return result
        else:
            return None
    def __repr__(self) -> str:
        result = ''
        if self.speaker:
            result = f"Speaker: {self.speaker}\n"
        for p in self.paragraphs:
            result += str(p)
        return result

def get_text(element):
    if element.text is not None:
        all_text = element.text
    else:
        all_text = ''
    for child in element:
        if child.text is not None:
            all_text += child.text
        if child.tail is not None:
            all_text += child.tail
    if element.tail is not None:
        all_text += element.tail
    return all_text

def parse_tree(tree):
    r = tree.xpath('/publicwhip/*')
    headings = []
    major_heading = None
    current_heading = None
    for rs in r:
        if rs.tag == 'oral-heading':
            headings.append(Heading(rs.get('id'), get_text(rs)))
        elif rs.tag == 'major-heading':
            major_heading = Heading(rs.get('id'), get_text(rs))
            current_heading = major_heading
            headings.append(major_heading)
        elif rs.tag == 'minor-heading':
            current_heading = Heading(rs.get('id'), get_text(rs))
            major_heading.subheadings.append(current_heading)
        elif rs.tag == 'speech':
            if rs.get('nospeaker') is None:
                speaker = Speaker(rs.get('person_id'), rs.get('speakername'))
            else:
                speaker = None
            speech = Speech(rs.get('id'), speaker) 
            current_heading.add_speech(speech)      
            for p in rs.xpath('p'):
                speech.add_paragraph(p.get('pid'), get_text(p))
    return headings

class Hansard():

    def __init__(self, filename) -> None:
        self.filename = filename
        stream = open_stream(self.filename)
        tree = etree.parse(stream)
        self.headings = parse_tree(tree)


def get_headings(headings):
    for heading in headings:
        if heading.subheadings:
            yield from heading.subheadings
        else:
            yield heading

# if __name__ == '__main__':
#     tree = etree.parse('/Users/akrause/EIDF/TDM/data/debates/debates2021-07-01a.xml')
#     headings = parse_tree(tree)
#     for heading in get_headings(headings):
#         print(heading.title)
