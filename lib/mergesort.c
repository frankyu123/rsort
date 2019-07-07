#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "mergesort.h"

#ifdef __linux__
#include <pthread.h>
#endif

#ifdef __APPLE__
#include "pthread_barrier.h"
#endif

#define _THREAD_NUM 5

typedef struct ThreadArgs {
    int low;
    int high;
} ThreadArgs;

static SortArgs *sortArgs;
static pthread_barrier_t pbt;
static SortFormat *result;
static int *idx = NULL;

static int qcmp(const void *a,const void *b)
{
    int leftIdx = *(int *) a;
    int rightIdx = *(int *) b;
    char *leftPtr, *rightPtr;

    if (sortArgs->keyTag == NULL) {
        if (sortArgs->numeric) {
            long leftNum = strtol(result[leftIdx].recordBegin, &leftPtr, 10);
            long rightNum = strtol(result[rightIdx].recordBegin, &rightPtr, 10);
            if (leftNum != rightNum) {
                return (int) rightNum - leftNum; 
            } else {
                return (int) strcmp(leftPtr, rightPtr);
            }
        } else {
            return (int) strcmp(result[leftIdx].recordBegin, result[rightIdx].recordBegin);
        }
    } else {
         if (sortArgs->numeric) {
            long leftNum, rightNum;

            if (result[leftIdx].keyBegin != NULL) {
                leftNum = strtol(result[leftIdx].keyBegin, &leftPtr, 10);
            } else {
                leftNum = strtol(result[leftIdx].recordBegin, &leftPtr, 10);
            }

            if (result[rightIdx].keyBegin != NULL) {
                rightNum = strtol(result[rightIdx].keyBegin, &rightPtr, 10);
            } else {
                rightNum = strtol(result[rightIdx].recordBegin, &rightPtr, 10);
            }

            if (leftNum != rightNum) {
                return (int) rightNum - leftNum; 
            } else {
                return (int) strcmp(leftPtr, rightPtr);
            }
        } else {
            if (result[leftIdx].keyBegin != NULL) {
                leftPtr = result[leftIdx].keyBegin;
            } else {
                leftPtr = result[leftIdx].recordBegin;
            }

            if (result[rightIdx].keyBegin != NULL) {
                rightPtr = result[rightIdx].keyBegin;
            } else {
                rightPtr = result[rightIdx].recordBegin;
            }

            return (int) strcmp(leftPtr, rightPtr);
        }
    }
}

static bool mcmp(int leftPos, int rightPos)
{
    char *leftPtr, *rightPtr;

    if (sortArgs->keyTag == NULL) {
        if (sortArgs->numeric) {
            long leftNum = strtol(result[idx[leftPos]].recordBegin, &leftPtr, 10);
            long rightNum = strtol(result[idx[rightPos]].recordBegin, &rightPtr, 10);
            if (leftNum > rightNum || (leftNum == rightNum && strcmp(leftPtr, rightPtr) < 0)) {
                return true; 
            } else {
                return false;
            }
        } else {
            if (strcmp(result[idx[leftPos]].recordBegin, result[idx[rightPos]].recordBegin) <= 0) {
                return true;
            } else {
                return false;
            }
        }
    } else {
        if (sortArgs->numeric) {
            long leftNum, rightNum;

            if (result[idx[leftPos]].keyBegin != NULL) {
                leftNum = strtol(result[idx[leftPos]].keyBegin, &leftPtr, 10);
            } else {
                leftNum = strtol(result[idx[leftPos]].recordBegin, &leftPtr, 10);
            }

            if (result[idx[rightPos]].keyBegin != NULL) {
                rightNum = strtol(result[idx[rightPos]].keyBegin, &rightPtr, 10);
            } else {
                rightNum = strtol(result[idx[rightPos]].recordBegin, &rightPtr, 10);
            }

            if (leftNum > rightNum || (leftNum == rightNum && strcmp(leftPtr, rightPtr) < 0)) {
                return true; 
            } else {
                return false;
            }
        } else {
            if (result[idx[leftPos]].keyBegin != NULL) {
                leftPtr = result[idx[leftPos]].keyBegin;
            } else {
                leftPtr = result[idx[leftPos]].recordBegin;
            }

            if (result[idx[rightPos]].keyBegin != NULL) {
                rightPtr = result[idx[rightPos]].keyBegin;
            } else {
                rightPtr = result[idx[rightPos]].recordBegin;
            }

            if (strcmp(leftPtr, rightPtr) <= 0) {
                return true;
            } else {
                return false;
            }
        }
    }
}

static void *thread(void *data)
{
    ThreadArgs *args = (ThreadArgs *) data;
    int low = args->low;
    int high = args->high;
    qsort(idx + low, high - low + 1, sizeof(int), qcmp);
    pthread_barrier_wait(&pbt);
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

int *mergeSort(SortFormat **data, int **originIdx, int size, SortArgs *cmd)
{
    sortArgs = cmd;
    result = *data;
    idx = (*originIdx);

    pthread_t tid;
    pthread_barrier_init(&pbt, NULL, _THREAD_NUM);

    // Partition qsort
    for (int i = 0; i < _THREAD_NUM - 1; i++) {
        ThreadArgs *args = (ThreadArgs *) malloc(sizeof(ThreadArgs));
        args->low = i * (size - 1) / (_THREAD_NUM - 1);
        if (i != 0) {
            args->low += 1;
        }
        args->high = (i + 1) * (size - 1) / (_THREAD_NUM - 1);
        pthread_create(&tid, NULL, thread, args);
    }
    
    pthread_barrier_wait(&pbt);

    // Merge
    for (int i = (_THREAD_NUM - 1) / 2; i > 0; i /= 2) {
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

    return idx;
}