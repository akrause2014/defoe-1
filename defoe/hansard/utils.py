import re

from defoe import query_utils
from defoe.query_utils import PreprocessWordType

def find_matches(text, keywords):
    match = []
    for key in keywords:
        pattern = re.compile(r'\b%s\b'%key)
        if re.search(pattern, text):
            match.append(key)
    return sorted(match)

def get_headings(hansard):
    for heading in hansard.headings:
        if heading.subheadings:
            yield from heading.subheadings
        else:
            yield heading

def clean_text(heading, preprocess_type=PreprocessWordType.LEMMATIZE):
    words = []
    for speech in heading.speeches:
        st = speech.text
        if st is not None:
            words += st.split()
    all_text = ''
    for word in words:
        preprocessed_word = query_utils.preprocess_word(word, preprocess_type)
        all_text += (' ' + preprocessed_word)
    return all_text



# if __name__=='__main__':
#     matches = find_matches('The petitioners therefore request that the House of Commons urge the Government to bring forward measures that will ensure that Orchards Academy is rebuilt.',
#         ['House of Commons', 'Govern', 'therefore request', 'notincluded'])
#     print(matches)
