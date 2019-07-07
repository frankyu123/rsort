#ifndef _MAIN_H_
#define _MAIN_H_

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <limits.h>
#include "lib/mergesort.h"
#include "lib/winner_tree.h"

void initUserArgs(int, char **);
int splitKFile(SortFormat **, int, int);
void mergeKFile(int *);

#endif