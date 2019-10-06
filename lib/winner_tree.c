#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "winner_tree.h"

#ifdef __linux__
#include <pthread.h>
#endif

#ifdef __APPLE__
#include "pthread_barrier.h"
#endif

#define _WINNER_TREE_SPLIT_FILE "split_text"
#define _WINNER_TREE_OFFSET_FILE "offset"

typedef struct ThreadArgs {
    int endIdx;
    int newFileNo;
} ThreadArgs;

typedef struct WinnerTree {
    int *nodeList;
    SortData *nodeValue;
} WinnerTree;

static int _fileNum;
static SortConfig *config;
static int _nodeNum = 1;
static pthread_barrier_t pbt;

static char *getSortData(FILE *fin, FILE *fmap)
{
    char *record = NULL;
    int size;

    if (fmap == NULL) {
        return NULL;
    }

    if (fscanf(fmap, "%d\n", &size) != EOF) {
        record = (char *) malloc(size + 2);
        memset(record, '\0', size + 2);
        fread(record, size, sizeof(char), fin);
    }
    return record;
}

static bool nodeCmp(SortData *left, SortData *right) 
{
    char *leftPtr, *rightPtr;
    if (config->keyTag == NULL) {
        if (config->numeric) {
            long leftNum = strtol(left->record, &leftPtr, 10);
            long rightNum = strtol(right->record, &rightPtr, 10);
            if (leftNum > rightNum || (leftNum == rightNum && strcmp(leftPtr, rightPtr) < 0)) {
                return (!config->reverse) ? true : false; 
            } else {
                return (!config->reverse) ? false : true;
            }
        } else {
            if (strcmp(left->record, right->record) <= 0) {
                return (!config->reverse) ? true : false;
            } else {
                return (!config->reverse) ? false : true;
            }
        }
    } else {
        if (strstr(left->record, config->keyTag) == NULL) {
            return (!config->reverse) ? true : false;
        } else if (strstr(right->record, config->keyTag) == NULL) {
            return (!config->reverse) ? false : true;
        }

        if (config->numeric) {
            long leftNum = strtol(strstr(left->record, config->keyTag), &leftPtr, 10);
            long rightNum = strtol(strstr(right->record, config->keyTag), &rightPtr, 10);
            if (leftNum > rightNum || (leftNum == rightNum && strcmp(leftPtr, rightPtr) < 0)) {
                return (!config->reverse) ? true : false; 
            } else {
                return (!config->reverse) ? false : true;
            }
        } else {
            if (strcmp(strstr(left->record, config->keyTag), strstr(right->record, config->keyTag)) <= 0) {
                return (!config->reverse) ? true : false;
            } else {
                return (!config->reverse) ? false : true;
            }
        }
    }
}

static void initWinnerTree(WinnerTree *tree, int nodeIdx, FILE **fin, FILE **fmap)
{
    if (nodeIdx >= _nodeNum) {
        return;
    }

    int leftIdx = 2 * nodeIdx + 1;
    int rightIdx = 2 * nodeIdx + 2;
    initWinnerTree(tree, leftIdx, fin, fmap);
    initWinnerTree(tree, rightIdx, fin, fmap);

    if (leftIdx >= _nodeNum && rightIdx >= _nodeNum) {
        int idx = nodeIdx - (_nodeNum - config->chunk);
        tree->nodeList[nodeIdx] = idx;
        char *data = getSortData(fin[idx], fmap[idx]);
        if (data == NULL) {
            tree->nodeList[nodeIdx] = -1;
        } else {
            tree->nodeValue[idx].record = (char *) malloc(strlen(data) + 1);
            strcpy(tree->nodeValue[idx].record, data);
        }
        free(data);
    } else {
		if (tree->nodeList[leftIdx] == -1 && tree->nodeList[rightIdx] == -1) {
			tree->nodeList[nodeIdx] = -1;
		} else if (tree->nodeList[leftIdx] != -1 && tree->nodeList[rightIdx] == -1) {
			tree->nodeList[nodeIdx] = tree->nodeList[leftIdx];
		} else if (tree->nodeList[leftIdx] == -1 && tree->nodeList[rightIdx] != -1) {
			tree->nodeList[nodeIdx] = tree->nodeList[rightIdx];
		} else {
			if (nodeCmp(&tree->nodeValue[tree->nodeList[leftIdx]], &tree->nodeValue[tree->nodeList[rightIdx]])) {
				tree->nodeList[nodeIdx] = tree->nodeList[leftIdx];
			} else {
				tree->nodeList[nodeIdx] = tree->nodeList[rightIdx];
			}
		}
    }
}

static void updateWinnerTree(WinnerTree *tree, int nodeIdx, int updateIdx, FILE **fin, FILE **fmap)
{
    if (nodeIdx >= _nodeNum) {
        return;
    }

    int leftIdx = 2 * nodeIdx + 1;
    int rightIdx = 2 * nodeIdx + 2;
    updateWinnerTree(tree, leftIdx, updateIdx, fin, fmap);
    updateWinnerTree(tree, rightIdx, updateIdx, fin, fmap);

	if (leftIdx >= _nodeNum &&  rightIdx >= _nodeNum && tree->nodeList[nodeIdx] == updateIdx) {
		char *newData = getSortData(fin[updateIdx], fmap[updateIdx]);
		if (newData != NULL) {
            free(tree->nodeValue[updateIdx].record);
            tree->nodeValue[updateIdx].record = (char *) malloc(strlen(newData) + 1);
			strcpy(tree->nodeValue[updateIdx].record, newData);
		} else {
            free(tree->nodeValue[updateIdx].record);
			tree->nodeList[nodeIdx] = -1;
		}
        free(newData);
	} else if (leftIdx < _nodeNum &&  rightIdx < _nodeNum && tree->nodeList[nodeIdx] != -1) {
		if (tree->nodeList[leftIdx] == -1 && tree->nodeList[rightIdx] == -1) {
			tree->nodeList[nodeIdx] = -1;
		} else if (tree->nodeList[leftIdx] != -1 && tree->nodeList[rightIdx] == -1) {
			tree->nodeList[nodeIdx] = tree->nodeList[leftIdx];
		} else if (tree->nodeList[leftIdx] == -1 && tree->nodeList[rightIdx] != -1) {
			tree->nodeList[nodeIdx] = tree->nodeList[rightIdx];
		} else {
			if (nodeCmp(&tree->nodeValue[tree->nodeList[leftIdx]], &tree->nodeValue[tree->nodeList[rightIdx]])) {
				tree->nodeList[nodeIdx] = tree->nodeList[leftIdx];
			} else {
				tree->nodeList[nodeIdx] = tree->nodeList[rightIdx];
			}
		}
	}
}

static void *job(void *argv)
{
    ThreadArgs *args = (ThreadArgs *) argv;

    FILE **fin = malloc((config->chunk + 1) * sizeof(FILE *));
    FILE **fmap = malloc((config->chunk + 1) * sizeof(FILE *));
    char splitFile[31], offsetFile[31], newFile[31], newMap[31];
    for (int i = 0; i < config->chunk; i++) {
        sprintf(splitFile, "%s_%d.rec", _WINNER_TREE_SPLIT_FILE, args->endIdx - (config->chunk - 1) + i);
        sprintf(offsetFile, "%s_%d.rec", _WINNER_TREE_OFFSET_FILE, args->endIdx - (config->chunk - 1) + i);
        fin[i] = fopen(splitFile, "r");
        fmap[i] = fopen(offsetFile, "r");
    }

    sprintf(newFile, "%s_%d.rec", _WINNER_TREE_SPLIT_FILE, args->newFileNo);
    sprintf(newMap, "%s_%d.rec", _WINNER_TREE_OFFSET_FILE, args->newFileNo);
    FILE *fout = fopen(newFile, "w");
    FILE *fnmap = fopen(newMap, "w");

    WinnerTree *tree = malloc(sizeof(WinnerTree));
    tree->nodeValue = (SortData *) malloc(config->chunk * sizeof(SortData));
    tree->nodeList = (int *) malloc(_nodeNum * sizeof(int));
    for (int i = 0; i < _nodeNum; i++) {
        tree->nodeList[i] = -1;
    }

    initWinnerTree(tree, 0, fin, fmap);
    while (true) {
        if (tree->nodeList[0] == -1) {
            free(tree->nodeList);
            free(tree->nodeValue);
            free(tree);
            break;
        }
        fprintf(fout, "%s", tree->nodeValue[tree->nodeList[0]].record);
        fprintf(fnmap, "%d\n", (int) strlen(tree->nodeValue[tree->nodeList[0]].record));
        updateWinnerTree(tree, 0, tree->nodeList[0], fin, fmap);
    }

    for (int i = 0; i < config->chunk; i++) {
        sprintf(splitFile, "%s_%d.rec", _WINNER_TREE_SPLIT_FILE, args->endIdx - (config->chunk - 1) + i);
        sprintf(offsetFile, "%s_%d.rec", _WINNER_TREE_OFFSET_FILE, args->endIdx - (config->chunk - 1) + i);
        fclose(fin[i]);
        fclose(fmap[i]);
        remove(splitFile);
        remove(offsetFile);
    }
    fclose(fout);
    fclose(fnmap);

    return NULL;
}

void mergeKFile(int fileNum, SortConfig *conf)
{
    config = conf;
    while (_nodeNum < config->chunk) {
        _nodeNum <<= 1;
    }
    _nodeNum = 2 * _nodeNum - 1;

    int cnt = 0;
    while (fileNum - cnt > config->chunk) {
        int threadNum = (fileNum - cnt) / config->chunk;

        if (threadNum > config->thread) {
            threadNum = config->thread;
        }

        pthread_t tids[threadNum];
        pthread_barrier_init(&pbt, NULL, threadNum + 1);
        for (int i = 0; i < threadNum; i++) {
            ThreadArgs *args = (ThreadArgs *) malloc(sizeof(ThreadArgs));
            args->endIdx = cnt + config->chunk * (i + 1);
            args->newFileNo = fileNum + (i + 1);
            pthread_create(&tids[i], NULL, job, args);
        }

        for (int i = 0; i < threadNum; i++) {
            pthread_join(tids[i], NULL);
        }

        pthread_barrier_destroy(&pbt);
        cnt += threadNum * config->chunk;
        fileNum += threadNum;
    }

    FILE **fin = malloc((config->chunk + 1) * sizeof(FILE *));
    FILE **fmap = malloc((config->chunk + 1) * sizeof(FILE *));
    char splitFile[31], offsetFile[31];
    for (int i = 0; i < config->chunk; i++) {
        sprintf(splitFile, "%s_%d.rec", _WINNER_TREE_SPLIT_FILE, cnt + i + 1);
        sprintf(offsetFile, "%s_%d.rec", _WINNER_TREE_OFFSET_FILE, cnt + i + 1);
        fin[i] = fopen(splitFile, "r");
        fmap[i] = fopen(offsetFile, "r");
    }
    FILE *fout = stdout;

    WinnerTree *tree = malloc(sizeof(WinnerTree));
    tree->nodeValue = (SortData *) malloc(config->chunk * sizeof(SortData));
    tree->nodeList = (int *) malloc(_nodeNum * sizeof(int));
    for (int i = 0; i < _nodeNum; i++) {
        tree->nodeList[i] = -1;
    }

    initWinnerTree(tree, 0, fin, fmap);
    while (true) {
        if (tree->nodeList[0] == -1) {
            free(tree->nodeList);
            free(tree->nodeValue);
            free(tree);
            break;
        }
        fprintf(fout, "%s", tree->nodeValue[tree->nodeList[0]].record);
        updateWinnerTree(tree, 0, tree->nodeList[0], fin, fmap);
    }

    for (int i = 0; i < config->chunk; i++) {
        sprintf(splitFile, "%s_%d.rec", _WINNER_TREE_SPLIT_FILE, cnt + i + 1);
        sprintf(offsetFile, "%s_%d.rec", _WINNER_TREE_OFFSET_FILE, cnt + i + 1);
        fclose(fin[i]);
        fclose(fmap[i]);
        remove(splitFile);
        remove(offsetFile);
    }
    fclose(fout);
}