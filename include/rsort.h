#ifndef _MAIN_H_
#define _MAIN_H_

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <mergesort.h>
#include <winner_tree.h>

typedef struct RsortConfig {
    char *output;
    char *input;
    char *beginTag;
    bool isCutByDelim;
    bool isUniquify;
    int chunk;
    int maxFileSize;
    int thread;
    SortConfig *sort;
} RsortConfig;

void usage();
RsortConfig *initRsortConfig(int, char **);
void splitKFile(RecordList **, int, RsortConfig *);

#endif