Code to see if there are instances where a uniqueKey value appears in
more than one shard.  Uses the latest 9.x SolrJ and Http2SolrClient.

Usage:

./shard_duplicate_finder -s [core URL for shard1] -s [core URL for shard2] ...

Each url would be of this format:

http(s)://server:port/solr/corename

You'll need one URL for each shard.  If SolrCloud replication has not
encountered any problems, then you can pick any replica.  A different
program would be required to detect discrepancies between replicas
within a single shard.

Accepts -D options for system properties just like Java would, if for
instance you need to provide a keystore with CA certificates.

A -v option can be provided to also log debug level messages.


Will output usage info if the -s option is not provided or the -h
option is used.
