/**
 * mergesort.c - implement internal mergesort
 * 
 * Author: Frank Yu <frank85515@gmail.com>
 * 
 * (C) Copyright 2019 Frank Yu
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mergesort.h>

#ifdef __linux__
#include <pthread.h>
#endif

#ifdef __APPLE__
#include <pthread_barrier.h>
#endif

/**
 * @low: begin idx for each unsorted segment
 * @hight: last idx for each unsorted segment
 */
typedef struct ThreadArgs {
    int low;
    int high;
} ThreadArgs;

static SortConfig *config;
static RecordList *result;
static int *idx = NULL;
static pthread_barrier_t _pbt;

SortConfig *initSortConfig()
{
    SortConfig *config = malloc(sizeof(SortConfig));
    config->keyTag = NULL;
    config->keyPos = 1;
    config->numeric = config->reverse = false;
    return config;
}

static char *getKeyCol(char *str)
{
    char *last = NULL;
    char *ptr = strstr(str, config->keyTag);

    int cnt = 1;
    while ((cnt < config->keyPos || config->keyPos == -1) && ptr != NULL) {
        last = ptr;
        ptr = strstr(ptr + 1, config->keyTag);
        ++cnt;
    }

    if (last != NULL && config->keyPos == -1) {
        ptr = last;
    }

    return ptr;
}

static int qcmp(const void *a,const void *b)
{
    int leftIdx = *(int *) a, rightIdx = *(int *) b;
    char *leftPtr, *rightPtr;
    if (config->keyTag == NULL) {
        if (config->numeric) {
            long leftNum = strtol(result[leftIdx].record, &leftPtr, 10);
            long rightNum = strtol(result[rightIdx].record, &rightPtr, 10);
            if (leftNum != rightNum) {
                return (!config->reverse) ? (int) rightNum - leftNum : (int) leftNum - rightNum; 
            } else {
                return (!config->reverse) ? (int) strcmp(leftPtr, rightPtr) : (int) strcmp(rightPtr, leftPtr);
            }
        } else {
            return (!config->reverse) ? (int) strcmp(result[leftIdx].record, result[rightIdx].record) : (int) strcmp(result[rightIdx].record, result[leftIdx].record);
        }
    } else {
        char *lptr, *rptr;
        if ((lptr = getKeyCol(result[leftIdx].record)) == NULL) {
            return (!config->reverse) ? 1 : -1;
        } else if ((rptr = getKeyCol(result[rightIdx].record)) == NULL) {
            return (!config->reverse) ? -1 : 1;
        }

         if (config->numeric) {
            long leftNum = strtol(lptr, &leftPtr, 10);
            long rightNum = strtol(rptr, &rightPtr, 10);
            if (leftNum != rightNum) {
                return (!config->reverse) ? (int) rightNum - leftNum : (int) leftNum - rightNum; 
            } else {
                return (!config->reverse) ? (int) strcmp(leftPtr, rightPtr) : (int) strcmp(rightPtr, leftPtr);
            }
        } else {
            if (!config->reverse) {
                return (int) strcmp(lptr, rptr);
            } else {
                return (int) strcmp(rptr, lptr);
            }
        }
    }
}

static bool mcmp(int leftPos, int rightPos)
{
    char *leftPtr, *rightPtr;
    if (config->keyTag == NULL) {
        if (config->numeric) {
            long leftNum = strtol(result[idx[leftPos]].record, &leftPtr, 10);
            long rightNum = strtol(result[idx[rightPos]].record, &rightPtr, 10);
            if (leftNum > rightNum || (leftNum == rightNum && strcmp(leftPtr, rightPtr) <= 0)) {
                return (!config->reverse) ? true : false; 
            } else {
                return (!config->reverse) ? false : true;
            }
        } else {
            if (strcmp(result[idx[leftPos]].record, result[idx[rightPos]].record) <= 0) {
                return (!config->reverse) ? true : false;
            } else {
                return (!config->reverse) ? false : true;
            }
        }
    } else {
        char *lptr, *rptr;
        if ((lptr = getKeyCol(result[idx[leftPos]].record)) == NULL) {
            return (!config->reverse) ? true : false;
        } else if ((rptr = getKeyCol(result[idx[rightPos]].record)) == NULL) {
            return (!config->reverse) ? false : true;
        }

        if (config->numeric) {
            long leftNum = strtol(lptr, &leftPtr, 10);
            long rightNum = strtol(rptr, &rightPtr, 10);
            if (leftNum > rightNum || (leftNum == rightNum && strcmp(leftPtr, rightPtr) <= 0)) {
                return (!config->reverse) ? true : false; 
            } else {
                return (!config->reverse) ? false :true;
            }
        } else {
            if (strcmp(lptr, rptr) <= 0) {
                return (!config->reverse) ? true : false;
            } else {
                return (!config->reverse) ? false : true;
            }
        }
    }
}

static void *job(void *data)
{
    ThreadArgs *args = (ThreadArgs *) data;
    int low = args->low;
    int high = args->high;
    qsort(idx + low, high - low + 1, sizeof(int), qcmp);
    pthread_barrier_wait(&_pbt);
    return NULL;
}

static void merge(int low, int mid, int high)
{
    int size = high - low + 1;
    int *tmp = (int *) malloc(size * sizeof(int));
    int mainPos = 0, leftPos = low, rightPos = mid + 1;

    for (int i = 0; i < size; i++) {
        if (leftPos <= mid && rightPos <= high) {
            if (mcmp(leftPos, rightPos)) {
                tmp[i] = idx[leftPos++];
            } else {
                tmp[i] = idx[rightPos++];
            }
        } else {
            if (leftPos <= mid) {
                tmp[i] = idx[leftPos++];
            } else if (rightPos <= high) {
                tmp[i] = idx[rightPos++];
            }
        }
    }

    for (int i = 0; i < size; i++) {
        idx[low + i] = tmp[i];
    }

    free(tmp);
}

void mergeSort(RecordList **data, int **originIdx, int size, int thread, SortConfig *userConfig)
{
    config = userConfig;
    result = *data;
    idx = (*originIdx);

    pthread_t tids[thread];
    pthread_barrier_init(&_pbt, NULL, thread + 1);

    // Partition qsort
    for (int i = 0; i < thread; i++) {
        ThreadArgs *args = (ThreadArgs *) malloc(sizeof(ThreadArgs));
        args->low = i * (size - 1) / thread;
        if (i != 0) {
            args->low += 1;
        }
        args->high = (i + 1) * (size - 1) / thread;
        pthread_create(&tids[i], NULL, job, args);
    }
    pthread_barrier_wait(&_pbt);

    for (int i = 0; i < thread; i++) {
        pthread_join(tids[i], NULL);
    }

    pthread_barrier_destroy(&_pbt);

    // Merge
    for (int i = thread / 2; i > 0; i /= 2) {
        for (int j = 0; j < i; j++) {
            int low, mid, high;
            low = j * (size - 1) / i;
            high = (j + 1)  * (size - 1) / i;
            if (j != 0) {
                low += 1;
            }
            mid = (j * 2 + 1) * (size - 1) / (i * 2);
            merge(low, mid, high);
        }
    }

    result = NULL;
    free(result);
    idx = NULL;
    free(idx);
    config = NULL;
    free(config);
}