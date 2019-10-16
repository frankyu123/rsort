#ifndef _MAIN_H_
#define _MAIN_H_

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

#include <mergesort.h>
#include <winner_tree.h>

void usage();
SortConfig *initSortConfig(int, char **);
void splitKFile(SortData **, int, SortConfig *);

#endif