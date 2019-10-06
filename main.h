#ifndef _MAIN_H_
#define _MAIN_H_

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include "lib/mergesort.h"
#include "lib/winner_tree.h"

SortConfig *initSortConfig(int, char **);
void splitKFile(SortData **, int, SortConfig *);

#endif