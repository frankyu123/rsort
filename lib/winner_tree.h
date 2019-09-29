#ifndef _WINNE_TREE_H_
#define _WINNE_TREE_H_

#include "mergesort.h"
#include <stdbool.h>

extern void initWinnerTree(SortConfig *, int *);
extern bool checkWinnerTreeEmpty();
extern void winnerTreePop();

#endif