# "Defoe" - analysis of historical books and newspapers data

This repository contains code to analyse historical books and newspapers datasets using Apache Spark - .

---

This repository was forked from [the previous defoe repository](https://github.com/defoe-code/), in March 2021, for adding new funcionalities to defoe:
 - [detecting and aligning similar passages in text](https://github.com/dasmiq/passim/) - March to July 2021. 
 - additional queries, similar to [CDCS Text and Data Mining Lab](https://www.cdcs.ed.ac.uk/cdcs-text-mining-lab-call-projects?utm_campaign=2619665_CDCS%20Digest%20W%2FC%2016%20March%202020&utm_medium=email&utm_source=College%20of%20Arts%2C%20Humanities%20%26%20Social%20Sciences%2C%20The%20University%20of%20Edinburgh&dm_t=0,0,0,0,0) - March to July 2021. 

Both projects have used defoe as the backend to perform several text mining queries across several digital textual historical documents (see bellow).

To learn more about the outcomes of *CDCS Text and Data Mining Lab* using defoe, we recommend to check the [defoe CDCS_Text_Mining_Lab](https://github.com/defoe-code/CDCS_Text_Mining_Lab) repository. 

The latest work on defoe is supported by DDI Beacon.

Developers: 
* Rosa Filgueira from November 2019
* Amy Krause (EPCC) from March 2021
* Anna Roubickova (EPCC) from March 2021 

---
## Supported datasets

Defoe supports already several datasets. In order to query a daset, defoe needs a list of files and/or directories that conform the dataset. Many of those files (used so far), can be found under the [others](./others) directory. Those files would need to be modifed, in order to update them with the corresponding paths. 
  

### British Library Books

This dataset consists of ~1TB of digitised versions of ~68,000 books from the 16th to the 19th centuries. The books have been scanned into a collection of XML documents. Each book has one XML document one per page plus one XML document for metadata about the book as a whole. The XML documents for each book are held within a compressed, ZIP, file. Each ZIP file holds the XML documents for a single book (the exception is 1880-1889's 000000037_0_1-42pgs__944211_dat.zip which wholds the XML documents for 2 books). These ZIP files occupy ~224GB.

This dataset is available under an open, public domain, licence. See [Datasets for content mining](https://www.bl.uk/collection-guides/datasets-for-content-mining) and [BL Labs Flickr Data: Book data and tag history (Dec 2013 - Dec 2014)](https://figshare.com/articles/BL_Labs_Flickr_Data/1269249). For links to the data itself, see [Digitised Books largely from the 19th Century](https://data.bl.uk/digbks/). The data is provided by [Gale](https://www.gale.com), a division of [CENGAGE](https://www.cengage.com/).

### British Library Newspapers

This dataset consists of ~1TB of digitised versions of newspapers from the 18th to the early 20th century. Each newspaper has an associated folder of XML documents where each XML document corresponds to a single issue of the newspaper. Each XML document conforms to a British Library-specific XML schema.

This dataset is available, under licence, from [Gale](https://www.gale.com), a division of [CENGAGE](https://www.cengage.com/). The dataset is in 5 parts e.g. [Part I: 1800-1900](https://www.gale.com/uk/c/british-library-newspapers-part-i). For links to all 5 parts, see [British Library Newspapers](https://www.gale.com/uk/s?query=british+library+newspapers). 

### Times Digital Archive

The code can also handle the [Times Digital Archive](https://www.gale.com/uk/c/the-times-digital-archive) (TDA). 

This dataset is available, under licence, from [Gale](https://www.gale.com), a division of [CENGAGE](https://www.cengage.com/).

The code was used with papers from 1785-2009.

### Find My Past Newspapers

This dataset is available, under licence, from [Find My Past](https://www.findmypast.co.uk/). To run queries with this dataset we can chose either to use:
* ALTO model: for running queries at page level. These are the same queries for the BL books.
* FMP model: for running queries at article level.  

### Papers Past New Zealand and Pacific newspapers

[Papers Past](http://paperspast.natlib.govt.nz/) provide digitised [New Zealand and Pacific newspapers](http://paperspast.natlib.govt.nz/newspapers) from the 19th and 20th centuries.

Data can be accessed via API calls which return search results in the form of XML documents. Each XML document holds one or more articles.

This dataset is available, under licence, from [Papers Past](http://paperspast.natlib.govt.nz).


### National Library of Scotland (NLS) digital collections 

[National Library of Scotland](https://data.nls.uk/data/digitised-collections/) provide several digitised collections, such as:
- [Encyclopaedia Britanica](https://data.nls.uk/data/digitised-collections/encyclopaedia-britannica/) from the 18th and 20th centuries.
- [ChapBooks](https://data.nls.uk/data/digitised-collections/chapbooks-printed-in-scotland/)
- [Scottish Gazetteers](https://data.nls.uk/data/digitised-collections/gazetteers-of-scotland/)

**Note, that ALL collections offered by NLS use the same XML and METS format. Therefore, we can use the defoe NLS model to query any of those collections.**

See [copyrights restrictions](https://www.nls.uk/copyright)



---

# Get started

Set up (local):

* [Set up local environment](./docs/setup-local.md)

Set up (Urika):

* [Set up Urika environment](./docs/setup-urika.md)
* [Import data into Urika](./docs/import-data-urika.md)
* [Import British Library Books and Newspapers data into Urika](./docs/import-data-urika-ati-se-humanities-uoe.md) (Alan Turing Institute-Scottish Enterprise Data Engineering Program University of Edinburgh project members only)

Set up (Cirrus - HPC Cluster):
* [Installing defoe, Spark, and transferring data](https://github.com/defoe-code/CDCS_Text_Mining_Lab/blob/master/README.md)

Set up (VM):

* [Set up a VM, with defoe, Spark, Hadoop, Edinburgh Geoparser](./docs/setup-VM.md)

Run queries:

* [Specify data to query](./docs/specify-data-to-query.md)
* [Specify Azure data to query](./docs/specify-data-to-query-azure.md)
* [Run individual queries](./docs/run-queries.md)
* [Run multiple queries at once - just one ingestion](./docs/run-list-of-queries.md)
* [Extracting, Transforming and Saving RDD objects to HDFS as a dataframe](./docs/nls_demo_examples/nls_demo_individual_queries.md#writing-and-reading-data-fromto-hdfs)
* [Loading dataframe from HDFS and performing a query](./docs/nls_demo_examples/nls_demo_individual_queries.md#writing-and-reading-data-fromto-hdfs)
* [Extracting, Transforming and Saving RDD objects to PostgreSQL database](./docs/nls_demo_examples/nls_demo_individual_queries.md#writing-and-reading-data-fromto-postgresql-database)
* [Loading dataframe from PostgreSQL database and performing a query](./docs/nls_demo_examples/nls_demo_individual_queries.md#writing-and-reading-data-fromto-postgresql-database)
* [Extracting, Transforming and Saving RDD objects to ElastiSearch](./docs/nls_demo_examples/nls_demo_individual_queries.md#writing-and-reading-data-tofrom-elasticsearch-es)
* [Loading dataframe from ElasticSearch and performing a query](./docs/nls_demo_examples/nls_demo_individual_queries.md#writing-and-reading-data-tofrom-elasticsearch-es)

Available queries:

* [ALTO documents](./docs/alto/index.md) (British Library Books and Find My Past Newspapers (at page level))
* [British Library Newspapers](./docs/papers/index.md) (these can also be run on the Times Digital Archive)
* [FMP newspapers](./docs/fmp/index.md) (Find My Past Newspapers datasets at article level)
* [Papers Past New Zealand and Pacific newspapers](./docs/nzpp/index.md)
* [Generic XML document queries](./docs/generic_xml/index.md) (these can be run on arbitrary XML documents)
* [NLS queries](./docs/nls/index.md) (these can be run on the Encyclopaedia Britannica, Scottish Gazetteers or ChapBooks datasets)
* [HDFS queries](./docs/hdfs/index.md) (running queries against HDFS files - for interoperability across models)
* [ES queries](./docs/es/index.md) (running queries against ES - for interoperability across models)
* [PostgreSQL queries](/docs/psql/index.md) (running queries against PostgreSQL database - for interoperability across models)
* [NLSArticles query](./docs/nlsArticles/index.md) (just for extracting automatically articles from the Encyclopaedia Britannica dataset)


Developers:

* [Run unit tests](./docs/tests.md)
* [Design and implementation notes](./docs/design-implementation.md)

---

## Origins and acknowledgements

### British Library Books analysis code

The code to analyse the British Library Books dataset has its origins in the first phase of '[Enabling Complex Analysis of Large Scale Digital Collections](http://figshare.com/articles/Enabling_Complex_Analysis_of_Large_Scale_Digital_Collections/1319482)', a project funded by the [Jisc Research Data Spring](https://www.jisc.ac.uk/rd/projects/research-data-spring) in 2015.

The project team included: Melissa Terras (UCL), James Baker (British Library), David Beavan (UCL), James Hetherington (UCL), Martin Zaltz Austwick (UCL), Oliver Duke-Williams (UCL), Will Finley (University of Sheffield), Helen O'Neill (UCL), Anne Welsh (UCL).

The code originated from the the GitHub repository [UCL-dataspring/cluster-code](https://github.com/UCL-dataspring/cluster-code):

* Branch: [sparkrods](https://github.com/UCL-dataspring/cluster-code/commits/sparkrods).
* Commit: [08d8bfd0a6cf37f7e4408a9475b38d6747c0cfeb](https://github.com/UCL-dataspring/cluster-code/commit/08d8bfd0a6cf37f7e4408a9475b38d6747c0cfeb) (10 November 2016).
* Developers: James Hetherington (UCL), James Baker (BL)

### Times Digital Archive and British Library Newspapers analysis code

The code to analyse the Times Digital Archive and British Library Newspapers dataset has its origins in code developed by [UCL](https://www.ucl.ac.uk/) to analyse the Times Digital Archive. This work took place from 2016-2018.

The project team included: James Hetherington (UCL), Raquel Alegre (UCL), Roma Klapaukh (UCL).

The code originated from the the GitHub repository [UCL/i_newspaper_rods](https://github.com/UCL/i_newspaper_rods):

* Branch: [master](https://github.com/UCL/i_newspaper_rods/tree/master).
* Commit: [ffe58042b7c4655274aa6b99fbdd6f6b0304f7ff](https://github.com/UCL/i_newspaper_rods/commit/ffe58042b7c4655274aa6b99fbdd6f6b0304f7ff) (22 June 2018)
* Developers: James Hetherington (UCL), Raquel Alegre (UCL), Roma Klapaukh (UCL).

### Analysing humanities data using Cray Urika-GX

Both the above codes were updated and extended by [EPCC](https://www.epcc.ed.ac.uk) as part of the [Research Engineering Group](https://www.turing.ac.uk/research/research-engineering) of the [The Alan Turing Institute](https://www.turing.ac.uk). The work focused on running both codes on the [Alan Turing Institute Cray Urika-GX Service](https://ati-rescomp-service-docs.readthedocs.io/en/latest/cray/introduction.html) and analysing British Library Books, British Library Newspapers and Papers Past New Zealand and Pacific newspapers datasets.

This work was done in conjunction with Melissa Terras, College of Arts, Humanities and Social Sciences (CAHSS), The University of Edinburgh. The work was funded by Scottish Enterprise as part of the Alan Turing Institute-Scottish Enterprise Data Engineering Program. This work runs from 2018 to 2019 and is ongoing at present, using this repository.

The project team includes: Rosa Filgueira (EPCC), Mike Jackson (EPCC), Anna Roubickova (EPCC).

The code originated from the the GitHub repositories:

* [alan-turing-institute/cluster-code](https://github.com/alan-turing-institute/cluster-code)
  - Branch: [epcc-sparkrods](https://github.com/alan-turing-institute/cluster-code/tree/epcc-sparkrods)
  - Commit: [00561bff61030fdff131a20fe45ede97897c4743](https://github.com/alan-turing-institute/cluster-code/commit/00561bff61030fdff131a20fe45ede97897c4743) (21 December 2018)
* [alan-turing-institute/i_newspaper_rods](https://github.com/alan-turing-institute/i_newspaper_rods)
  - Branch: [epcc-master](https://github.com/alan-turing-institute/i_newspaper_rods/tree/epcc-master)
  - Commit: [b9c89764f97987ff1600a35cc3d3bc7bb68da79f](https://github.com/alan-turing-institute/i_newspaper_rods/commit/b9c89764f97987ff1600a35cc3d3bc7bb68da79f) (28 January 2019).
* [alan-turing-institute/i_newspaper_rods](https://github.com/alan-turing-institute/i_newspaper_rods)
  - Branch: [other-archives](https://github.com/alan-turing-institute/i_newspaper_rods/tree/other-archives)
  - Commit: [43748ccd3839b71347660f4375e9a18c45648118](https://github.com/alan-turing-institute/i_newspaper_rods/commit/43748ccd3839b71347660f4375e9a18c45648118) (13 February 2019).
* Developers: Rosa Filgueira (EPCC), Mike Jackson (EPCC), Anna Roubickova (EPCC).

### Living With Machines

The code to analyse the Find My Past Newspapers dataset and to support blobs on Azure was developed by David Beavan (The Alan Turing Institute) as part of [Living With Machines](https://www.turing.ac.uk/research/research-projects/living-machines) funded by UKRI's [Strategic Priorities Fund](https://www.ukri.org/funding/funding-opportunities/) and led by the [Arts and Humanities Research Council](https://ahrc.ukri.org/) (AHRC). Living With Machines runs from 2018-2023 and is ongoing at present using this repository.

The development team includes: David Beavan (Alan Turing Institute), Rosa Filgueira (EPCC), Mike Jackson (EPCC).

The code originated from the the GitHub repositories:

* [DavidBeavan/cluster-code](https://github.com/DavidBeavan/cluster-code)
  - Branch: [epcc-sparkrods](https://github.com/DavidBeavan/cluster-code/tree/epcc-sparkrods)
  - Commit: [8e37fdaa0a57e164aecbdadaa4981b5b225a3932](https://github.com/DavidBeavan/cluster-code/commit/8e37fdaa0a57e164aecbdadaa4981b5b225a3932) (15 January 2019)
* [DavidBeavan/cluster-code](https://github.com/DavidBeavan/cluster-code)
  - Branch: [azure-sparkrods](https://github.com/DavidBeavan/cluster-code/tree/azure-sparkrods)
  - Commit: [8110fb498631edcc5b385029cf5a45dd91d216fc](https://github.com/DavidBeavan/cluster-code/commit/8110fb498631edcc5b385029cf5a45dd91d216fc) (23 November 2018)
* Developer: David Beavan (Alan Turing Institute)

---

## Name

The code is called "defoe" after [Daniel Defoe](https://en.wikipedia.org/wiki/Daniel_Defoe), writer, journalist and pamphleteer of the 17-18 century.

---

## Copyright and licence

Copyright (c) 2015-2019 University College London

Copyright (c) 2018-2019 The University of Edinburgh

All code is available for use and reuse under a [MIT Licence](http://opensource.org/licenses/MIT). See [LICENSE](./LICENSE).

### Third-party data

**`defoe/test/books/fixtures/000000037_0_1-42pgs__944211_dat_modified.zip`**

A modified copy of the file `000000037_0_1-42pgs__944211_dat.zip` from [OCR text derived from digitised books published 1880 - 1889 in ALTO XML](https://data.bl.uk/digbks/db11.html) (doi: 10.21250/db11) which is licenced under [CC0 1.0 Public Domain](https://creativecommons.org/licenses/by/4.0/).

The modifications are as follows:

000000037_metadata.xml:

```
-               <MODS:placeTerm type="text">Manchester</MODS:placeTerm>
=>
+               <MODS:placeTerm type="text">Manchester [1823]</MODS:placeTerm>
```

000000218_metadata.xml:

```
-               <MODS:placeTerm type="text">London</MODS:placeTerm>
+               <MODS:placeTerm type="text">London [1823]</MODS:placeTerm>
```

**`defoe/test/alto/fixtures/000000037_000005.xml`**

A copy of the file `ALTO/000000037_000005.xml` from the above file.

**`defoe/test/papers/fixtures/1912_11_10.xml`**

A copy of the file [newsrods/test/fixtures/2000_04_24.xml](https://github.com/UCL/i_newspaper_rods/blob/master/newsrods/test/fixtures/2000_04_24.xml) from from [ucl/i_newspaper_rods](https://github.com/ucl/i_newspaper_rods). The file has been renamed, most of its content removed, and its data replaced by dummy data.
