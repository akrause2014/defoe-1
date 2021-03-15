#!/usr/bin/env bash

conda install --yes lxml nltk pep8 pylint pytest PyYAML regex requests pathlib
conda install -c conda-forge spacy
python -m spacy download en
python -m spacy download en_core_web_lg
bash scripts/download_ntlk_corpus.sh

