#ifndef _MERGESORT_H_
#define _MERGESORT_H_

#include <stdbool.h>

typedef struct SortFormat {
    char *recordBegin;
    char *keyBegin;
} SortFormat;

//***************************************************************************
// Sort Command
// @Description : 
// case -rb             : record begin
// case -d              : split by delimiter
// case -k              : sort by specific key column
// case -n              : numerical comparison
// case -r              : reverse order
// case --chunk         : external chunk number
// case -s              : file size limit (byte)
//***************************************************************************

typedef struct SortArgs {
    char *beginTag;
    char *endTag;
    char *keyTag;
    bool numeric;
    bool reverse;
    int chunk;
    size_t maxFileSize;
} SortArgs;

extern int *mergeSort(SortFormat **, int **, int, SortArgs *);

#endif