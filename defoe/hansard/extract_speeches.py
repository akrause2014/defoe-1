from collections import defaultdict
import yaml

from defoe.hansard.debates import Hansard, get_headings


def extract_discussions(results_file):
    with open(results_file) as f:
        results = yaml.load(f, Loader=yaml.FullLoader)
    files = defaultdict(dict)
    for d_id, discussion in results.items():
        files[discussion['filename']][discussion['id']] = discussion
    return files

def generate_text(files):
    for filename, discussions in files.items():
        hansard = Hansard(filename)
        for heading in get_headings(hansard.headings):
            if heading._id in discussions:
                discussion = discussions[heading._id]
                d = dict(discussion)
                d['speeches'] = []
                for speech in heading.speeches:
                    s = {
                        'id': speech._id,
                        'text': speech.text,
                    }
                    if speech.speaker is not None:
                        s['speakername'] = speech.speaker.name
                        if speech.speaker.id is not None:
                            s['speaker_id'] = speech.speaker.id
                    d['speeches'].append(s)
                yield heading._id, d

if __name__ == '__main__':
    import sys

    results_file = sys.argv[1]

    files = extract_discussions(results_file)
    debates_text = {}
    for d, disc in generate_text(files):
        debates_text[d] = disc

    try:
        output_file = sys.argv[2]
        with open(output_file, 'w') as f:
            yaml.dump(debates_text, f)
    except:
        print(debates_text)