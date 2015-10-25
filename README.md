# datasets2sqlite

These are some scripts to convert some large datasets from their native format to SQLite.
These scripts are designed to have minimal dependencies so that they may be copied and
run independently of each other.

The scripts individually provide usage help if executed with insufficient parameters
and can read the compressed version of data.

## Datasets

 - Amazon Reviews
   - Source: [http://jmcauley.ucsd.edu/data/amazon/](http://jmcauley.ucsd.edu/data/amazon/)
   - `python json2sqlite.py --gzip aggressive_dedup.json.gz amazon.sqlite reviews` 
   - `python amazon_metadata2sqlite.py --gzip metadata.json.gz amazon.sqlite`

 - Wikipedia Metadata
   - Source: [https://snap.stanford.edu/data/wiki-meta.html](https://snap.stanford.edu/data/wiki-meta.html) (NOT the complete wikipedia history)
   - `python wikimeta2sqlite.py --bz2 enwiki-20080103.main.bz2 wikipedia_2008.sqlite main`
   - `python wikimeta2sqlite.py --bz2 enwiki-20080103.users.bz2 wikipedia_2008.sqlite users`
   - etc.

 - Memetracker data
   - Source: [https://snap.stanford.edu/data/memetracker9.html](https://snap.stanford.edu/data/memetracker9.html) 
   - `python meme2sqlite.py --gzip quotes_2008-08.txt.gz memetracker2.sqlite meme`
   - `python meme2sqlite.py --gzip quotes_2008-09.txt.gz memetracker2.sqlite meme`
   - `python meme2sqlite.py --gzip quotes_2008-10.txt.gz memetracker2.sqlite meme`
   - etc.

 - Reddit data
   - Source: [https://archive.org/details/2015_reddit_comments_corpus](https://archive.org/details/2015_reddit_comments_corpus)
   - From 2015-04, the comments contain 1 extra field: `removal_reason`. Hence, the headers need to be explicitly supplied.
   - `python json2sqlite.py --bz2 RC_2015-01.bz2 --headers reddit_headers.txt reddit.sqlite comments`
   - `python json2sqlite.py --bz2 RC_2015-02.bz2 --headers reddit_headers.txt reddit.sqlite comments`
   - `python json2sqlite.py --bz2 RC_2015-03.bz2 --headers reddit_headers.txt reddit.sqlite comments`
   - etc.

 - StackExchange data
   - Source: [https://archive.org/details/stackexchange](https://archive.org/details/stackexchange)
   - Extract the `Badge.xml`, `Comments.xml`, `PostLinks.xml`, etc. in the current folder.
   - `python so2sqlite.py`
   - To import this data into Postgres, see [musically-ut/stackexchange-dump-to-postgres](https://github.com/musically-ut/stackexchange-dump-to-postgres)

## Acknowledgements

I use code from [rgrp/csv2sqlite](https://github.com/rgrp/csv2sqlite) for guessing types.
The code for converting StackExchange dataset is taken (with minor changes) from [testlnord/sedumpy](https://github.com/testlnord/sedumpy).
