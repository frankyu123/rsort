#ifndef _MERGESORT_H_
#define _MERGESORT_H_

#include <stdbool.h>

typedef struct RecordList {
    char *record;
} RecordList;

typedef struct SortConfig {
    char *keyTag;
    int keyPos;
    bool numeric;
    bool reverse;
} SortConfig;

extern SortConfig *initSortConfig();
extern int *mergeSort(RecordList **, int **, int, int, SortConfig *);

#endif