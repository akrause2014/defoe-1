'''
Find debates that contain keywords.
'''

import yaml

from defoe import query_utils
from defoe.hansard.utils import find_matches, get_headings, clean_text
from defoe.papers.query_utils import within_distance

def do_query(hansards, config_file=None, logger=None, context=None):
    
    with open(config_file, "r") as f:
        config = yaml.load(f)

    preprocess_type = query_utils.extract_preprocess_word_type(config)
    print(f'preprocessing: {preprocess_type}')

    distance = int(config['distance'])

    unproc_keywords = config['keywords']
    keywords = []
    for k in unproc_keywords:
        keywords.append(' '.join(
            [query_utils.preprocess_word(word, preprocess_type) for word in k.split()])
        )
    print(f'keywords: {keywords}')

    unproc_controlwords = config['controlwords']
    controlwords = []
    for c in unproc_controlwords:
        controlwords.append(' '.join(
            [query_utils.preprocess_word(word, preprocess_type) for word in c.split()])
        )
    print(f'controlwords: {controlwords}')

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
    matching_discussions = filter_discussions.flatMap(
        lambda disc: [
            (disc[0], disc[1], w) for w in within_distance(keywords, controlwords, disc[2], distance)
        ])

    matching_data = matching_discussions.flatMap(
        lambda discussion:
        [(
            discussion[1]._id,
            {
                "title": discussion[1].title,
                "heading_id": discussion[1]._id,
                "speech_id": speech._id,
                "speaker": ((speech.speaker.id, speech.speaker.name) if speech.speaker is not None else ''),
                "text": speech.text,
                "filename": discussion[0].filename,
                "keywords": list(discussion[2][1]),
                "controls": list(discussion[2][0]),
            }
         )
         for speech in discussion[1].speeches
        ])

    result = matching_data \
        .groupByKey() \
        .map(lambda speech:
             (speech[0], list(speech[1]))) \
        .collect()
    return result
