#ifndef _WINNE_TREE_H_
#define _WINNE_TREE_H_

#include "mergesort.h"
#include <stdbool.h>

#define _WINNER_TREE_SPLIT_FILE "split_text_"
#define _WINNER_TREE_RESULT_FILE "result.rec"

extern void initWinnerTree(SortArgs *, int *, size_t, bool);
extern bool checkWinnerTreeEmpty();
extern void winnerTreePop();

#endif