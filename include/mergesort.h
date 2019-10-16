#ifndef _MERGESORT_H_
#define _MERGESORT_H_

#include <stdbool.h>

typedef struct SortData {
    char *record;
} SortData;

typedef struct SortConfig {
    char *beginTag;
    char *keyTag;
    char *output;
    int keyPos;
    bool isCutByDelim;
    bool numeric;
    bool reverse;
    int chunk;
    int maxFileSize;
    int thread;
} SortConfig;

extern int *mergeSort(SortData **, int **, int, SortConfig *);

#endif