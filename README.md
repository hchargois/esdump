# esdump

**esdump** is a _simple_ and _efficient_ CLI tool to dump (retrieve) the documents contained in an Elasticsearch index via scrolling.

It outputs to standard output, in JSON lines (a.k.a. JSONL or NDJSON) format.

It works with Elasticsearch versions 7.x and 8.x.

# Features

- Correctly clears the scroll contexts before exiting, to free Elasticsearch resources
- Automatically uses slicing for best performance
- You can specify which document fields to output
- Options to scroll in a random order and only a given number of docs, to easily get samples
- You can specify a query in full Elasticsearch format or in the `query_string` (a.k.a. Lucene) syntax
- Adaptive throttling
- Works with security (username/password & HTTPS with custom certificate)
- Option to turn HTTP gzip compression off (uses less CPU if network is not a bottleneck)
- ... and more!

# Install

Requires Go >= 1.18

    go install github.com/hchargois/esdump@latest

# Examples

Usage is detailed in sections below, but here are a few simple examples:

    # Dump an index to a file using default settings
    esdump http://localhost myindex > out.jsonl

    # Dump multiple indexes using multi-target notation
    esdump http://localhost myindex1,myindex2*

    # Select the document fields to dump
    esdump http://localhost myindex --fields id,date,description

    # Specify a query in "query_string" format
    esdump http://localhost myindex --query "rabbit OR bunny"

    # Specify a search query on standard input
    echo '{"query": {"term": {"animal": "rabbit"}}}' | esdump http://localhost myindex

    # Dump a random sample of 1000 documents
    esdump http://localhost myindex --random --count 1000

    # Access an Elasticsearch server secured with TLS (with a custom cert) and username/password
    esdump https://user:pass@localhost myindex --verify=cacert.pem

# Usage

    esdump base-url index-target [flags]

    Arguments:

      base-url      The base URL of the Elasticsearch server (e.g. http://localhost)
                    If the port is not specified, 9200 is assumed
      index-target  The name of the index you want to dump. Multi-target syntax is
                    also supported (e.g. myindex1,myindex2 or myindex*)
    
    Flags:
    
      -f, --fields string             comma-separated list of fields to include in the output, or if starting with ^ to exclude
      -q, --query string              filter the documents with a "query_string" query
      -t, --throttle float32          delay factor for adaptive throttling, set 0 to disable throttling (default 4)
      -n, --count uint                output that many documents maximum (default unlimited)
      -s, --scroll-size int           number of hits per scroll request (default 1000)
      -m, --metadata                  include hit metadata (_index, _id, _source...), if not set only outputs the contents of _source
      -M, --metadata-only             only include hit metadata (_index, _id...), no _source
      -r, --random                    dump the documents in a random order
      -z, --no-compression            disable HTTP gzip compression
          --verify string             certificate file to verify the server's certificate, or "no" to skip all TLS verification
          --slices int                max number of slices per index (default 10)
          --scroll-timeout duration   scroll timeout (default 1m0s)
          --http-timeout duration     HTTP client timeout (default 1m0s)

# How to...

## Filter the documents to dump

There are two options to selectively dump a subset of the index documents.

The simple one is using the `-q` flag with a "query string" query, described here https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html

The more advanced one is supplying a full Elasticsearch query to standard input. The format to use in this case is the format that is suitable for the `/_search` Elasticsearch endpoint, i.e. a JSON object containing a `"query"` key. You can also use this to set a custom sort order.

## Choose what to dump

By default, esdump dumps only the documents, i.e. the contents of the `"_source"` in the Elasticsearch hits:

    {"title": "lorem ipsum", "price": 1.23}
    {"title": "dolor sit amet", "price": 4.56}
    {"title": "consectetur adipiscing elit", "price": 7.89}

With the `-m`/`--metadata` flag, the documents are wrapped with their metadata:

    {"_index": "items", "_id": "1", "_source": {"title": "lorem ipsum", "price": 1.23}}
    {"_index": "items", "_id": "2", "_source": {"title": "dolor sit amet", "price": 4.56}}
    {"_index": "items", "_id": "3", "_source": {"title": "consectetur adipiscing elit", "price": 7.89}}

With the `-M`/`--metadata-only` flag, only the metadata is output, which is the fastest way to retrieve only the `_id`:

    {"_index": "items", "_id": "1"}
    {"_index": "items", "_id": "2"}
    {"_index": "items", "_id": "3"}

The `-f`/`--fields` can be used to specify a set of fields to include or exclude:

* `-f a,b,c` will only output the fields a, b and c
* `-f ^a,b,c` will output all the fields except a, b and c

## Adjust the load on the server with adaptive throttling

esdump uses a very simple but effective throttling algorithm that automatically adapts to the capabilities and current load of the Elasticsearch cluster.

Instead of using the usual token bucket with a fixed rate, or a fixed delay between requests, you can set a _relative_ throttle factor, which depends on the time taken by the last request.

For example, if you set a throttle factor of 10 (`-t10`), and the first scroll request takes 10 ms, then esdump will sleep for 100 ms before sending the next scroll request. If the next request takes only 5 ms, esdump will then sleep for 50 ms. This ensures that the scroll requests take a constant proportion of the cluster's load, even if it becomes more or less loaded for other reasons.

By default, a throttle factor of 4 is used.

To completely disable throttling, set a 0 throttle factor (`-t0`).

## Go fast

* Disable throttling with `-t0`
* Select only the required fields with `-f`...
* ... or if you only need the document `_id`s, use `-M` to only retrieve the metadata
* If the network is fast, try disabling the gzip compression with `-z` (automatically done if the server is on a loopback address)
* Increase the maximum number of slices with `--slices`; but this will only have an effect if your index has at least as many shards
* Do not use random scrolling (no `-r`); do not specify a custom `sort` order in a query supplied on stdin

## Work with a secured Elasticsearch cluster

If the cluster uses TLS, make sure to use the `HTTPS` scheme in the URL:

    https://localhost

By default, your host's CA bundle is used to verify the server certificate. To specify a custom trusted certificate, use `--verify`:

    esdump https://localhost myindex --verify cert.pem

... or turn off certificate validation with `--verify no`

To use a username and password, simply include them in the URL:

    https://username:password@localhost

# Benchmark

Against what seems to be the most popular alternative, [elasticsearch-dump/elasticsearch-dump](https://github.com/elasticsearch-dump/elasticsearch-dump) (7k+ stars!)

Dumping an index that has 2 million documents of around 500 bytes each, in 10 shards, on a single node (a desktop computer) on localhost.

| command | time | speed |
| ------- | ---- | ----- |
| `elasticdump --input=http://localhost:9200/testindex1 --output=out.json` | 5 hours 33 min | 100 docs/s |
| `esdump http://localhost testindex1 > out.json` (default, 4x throttling) | 7.28 s | 274 774 docs/s |
| `esdump http://localhost testindex1 -t0 > out.json` (no throttling) | 1.67 s | 1 199 154 docs/s |

No, there is no error. `esdump` really is _ten thousand times_ faster than `elasticdump`.
The (main) reason is that `elasticdump` has an insane throttling of 5 requests every 5 seconds.
And the worst part of it? It is hardcoded, so it can't be configured, and it's also undocumented.
Yep. It's that bad.

# Alternatives

You may ask, _surely_ there must already exist such a tool, right? Well, I searched for a few hours and couldn't find something that "just worked". So I had to make my own...

* elasticsearch-dump/elasticsearch-dump: apart from the throttling issue described above, it also doesn't use slicing, and doesn't clear scroll contexts...
* [miku/esdump](https://github.com/miku/esdump): does not produce JSONL, cannot specify an Elasticsearch query (only a Lucene one), no slicing...
* [wubin1989/esdump](https://github.com/wubin1989/esdump): seems that it cannot actually dump but only reindex into another Elasticsearch index?
* [shinexia/elasticdump](https://github.com/shinexia/elasticdump): cannot dump to stdout, doesn't use slicing...

There are also a few that are simply too old and don't work with recent (>=7.x) Elasticsearch versions:

* [wricardo/esdump](https://github.com/wricardo/esdump)
* [berglh/escroll](https://github.com/berglh/escroll)
