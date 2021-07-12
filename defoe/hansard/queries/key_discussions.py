'''
Find debates that contain keywords.
'''

import yaml

from defoe import query_utils
from defoe.hansard.utils import find_matches, get_headings, clean_text

def do_query(hansards, config_file=None, logger=None, context=None):
    
    with open(config_file, "r") as f:
        config = yaml.load(f)

    preprocess_type = query_utils.extract_preprocess_word_type(config)
    print(f'preprocessing: {preprocess_type}')

    unproc_keywords = config['keywords']
    keywords = []
    for k in unproc_keywords:
        keywords.append(' '.join(
            [query_utils.preprocess_word(word, preprocess_type) for word in k.split()])
        )
    print(f'keywords: {keywords}')

    # [(year, discussion_string), ...]
    headings = hansards.flatMap(
        lambda hansard: [(hansard, h) for h in get_headings(hansard)])

    # [(discussion, clean_text), ...]
    discussion_text = headings.map(
        lambda hansard_discussion: (
            hansard_discussion[0], 
            hansard_discussion[1], 
            clean_text(hansard_discussion[1], preprocess_type)
        )
    )

    # [(discussion, clean_text)
    filter_discussions = discussion_text.filter(
        lambda disc: any(k in disc[2] for k in keywords))

    # [(discussion, clean_text)
    matching_discussions = filter_discussions.map(
        lambda disc: (disc[0], disc[1], find_matches(disc[2], keywords)))


    matching_data = matching_discussions.map(
        lambda discussion:
        (discussion[1]._id,
            {
                "title": discussion[1].title,
                "id": discussion[1]._id,
                "filename": discussion[0].filename,
                "term": list(discussion[2]),
            }
        )
    )

    result = matching_data.collect()
    return result
