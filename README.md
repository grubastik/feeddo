# feeddo

## General description
This project implements parsing of Heaureka feeds and storing them into Kafka.

## Topics which should be covered
- feed could be downloaded or loaded from file
- multiple feeds can be processed one by one
- models added for feed xml and for kafka json object
- price is parsed and all supported formats converted to decimal and error generated if price not supported
- kafka topics populated (one topic contains all items and second only with price per click set)
- metrics endpoint for prometeus and simple metrics should be created
- docker file and docker compose file
- convert cli to service with periodic processing provided urls

## Usage
Feed references shoul be provided as a command line args:
`feeddo --feedUrl file:///feeds/some.xml --feedUrl http://some.host.org/src/someFeed.xml --kafkaUrl kafka.org`

Short options also could be used
`feeddo -f file:///feeds/some.xml -f http://some.host.org/src/someFeed.xml -k kafka.org`

## Tests
Tests could be run with a command
`go test ./...`

## Benchmark
Benchmarks can be run with a command
`go test cmd/feeddo -bench=.`
Results for different commits could be found in file [benchmark_results.md](benchmark_results.md)
Note: before running benchmarks gzipped files should be unzipped with the following command
`gunzip cmd/feeddo/testdata/*.gz`

## Prometeus metrics
Metrics exposed on port 2112. Available metrics per feed:
- feed_[HOST_WITH_DOTS_REPLACED_BY_UNDERSCORES] 1 meens started and 0 meens finished
- total_[HOST_WITH_DOTS_REPLACED_BY_UNDERSCORES] total number of items processed
- succeeded_[HOST_WITH_DOTS_REPLACED_BY_UNDERSCORES] total number of items which processed successfully
- failed_[HOST_WITH_DOTS_REPLACED_BY_UNDERSCORES] total number of items which processed with error
