#ifndef _MAIN_H_
#define _MAIN_H_

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <limits.h>
#include "lib/mergesort.h"
#include "lib/winner_tree.h"

SortConfig *initSortConfig(int, char **);
int splitKFile(SortData **, int, int, SortConfig *);
void mergeKFile(int *, SortConfig *config);

#endif