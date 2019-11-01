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