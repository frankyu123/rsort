#ifndef _WINNE_TREE_H_
#define _WINNE_TREE_H_

#include <stdbool.h>

#include <mergesort.h>

typedef struct WinnerTree {
    int *nodeList;
    SortData *nodeValue;
} WinnerTree;

extern void mergeKFile(int, SortConfig *);

#endif