#ifndef _WINNE_TREE_H_
#define _WINNE_TREE_H_

#include <stdbool.h>
#include <mergesort.h>

typedef struct WinnerTreeNode {
    char *record;
} WinnerTreeNode;

typedef struct WinnerTree {
    int *nodeList;
    WinnerTreeNode *nodeValue;
} WinnerTree;

extern void mergeKFile(int, int, int, char *, bool, SortConfig *);

#endif