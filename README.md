# rsort
A tool for sorting large texts based on external mergesort.

## External Mergesort
* parse texts by giving OPTION
* split files in N size (-s OPTION) and sort (internal mergesort)
* merge M files with K chunks (winner tree)

## Usage
**Install rsort**
```
make
```
**Run rsort**
* Using Makefile
```bash
#edit make run in Makefile
make run
```
* Command
```bash
./rsort [OPTION]... [FILE]...
```

## Command
| Command | Description | Example |
| ---             | ---    | ---       |
| **-rb** | obtain record begin by specific tag | -rb @Gais_REC: |
| **-d** | split giving FILE by delimiters and sort | -d |
| **-k{num}** | sort by nth ocurrence of specific key column | -k2 @T: |
| **-kr** | sort by last occurrence of specific key column | -kr ' ' -n |
| **-n** | sort by numerical comparison | -n |
| **-r** | output result in reverse order | -r |
| **-chunk** | number of external chunks using in merge | -chunk 4 |
| **-s** | limit file size (byte) | -s 200000000 |
| **-parallel** | number of threads needed | -parallel 4 |
| **-o** | output in specific file | -o result.rec |
| **-u** | remove duplicate records | -u result.rec |
| **--help** | show rsort information | --help |

## Benchmark
This experiment using 4 threads, 8 chunks and limit 500 MB for each split texts.

**Environment**
* CPU - 2.3 GHz 2 Core Intel Core i5
* MEM - 8 GB 2133 MHz LPDDR3

**17G YouTube data**

| Command | Total Records | Time | Max Memory Usage |
| ---             | ---   | ---       | ---       |
| ./rsort -chunk 8 [FILE] | 577214559 records | 30 - 33 min | 1.1 - 1.2 GB |
| ./rsort -chunk 8 -rb @url: [FILE] | 79117015 records | 9 - 11 min | 500 - 600 MB |

**16G ETtoday data ( cat 2G data to 16G )**

| Command | Total Records | Time | Max Memory Usage |
| ---             | ---   | ---       | ---       |
| ./rsort -chunk 8 [FILE] | 159999952 records | 9 - 11 min | 700 - 800 MB |
| ./rsort -chunk 8 -rb @Gais_REC: [FILE] | 6638304 records | 4 - 5 min | 500 - 600 MB |