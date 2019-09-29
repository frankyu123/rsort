#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include "winner_tree.h"

#define _DEFAULT_BUFFER_SIZE 1024
#define _WINNER_TREE_SPLIT_FILE "split_text_"
#define _WINNER_TREE_OFFSET_FILE "offset_"
#define _WINNER_TREE_RESULT_FILE "result.rec"

typedef struct WinnerTreeNode {
    SortData *value;
    int chunkIdx;
    int fileIdx;
} WinnerTreeNode;

// File variables
static FILE **fin;
static FILE **fmap;
static FILE *fout;
static int *fileNum;
static int currentFileIdx = 0;

// Tree variables
static SortConfig *config;
static int *nodeList;
static WinnerTreeNode *nodeValue;

static SortData *getSortData(int fileIdx)
{
    SortData *data = malloc(sizeof(SortData));
    int size;
    if (fscanf(fmap[fileIdx], "%d\n", &size) != EOF) {
        data->record = (char *) malloc(size + 2);
        memset(data->record, '\0', size + 2);
        fread(data->record, size, sizeof(char), fin[fileIdx]);
        return data;
    } else {
        return NULL;
    }
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

static void winnerTreeInsert(int nodeIdx, int totalNode)
{
    if (nodeIdx >= totalNode) {
        return;
    }

    int leftIdx = 2 * nodeIdx + 1;
    int rightIdx = 2 * nodeIdx + 2;
    winnerTreeInsert(leftIdx, totalNode);
    winnerTreeInsert(rightIdx, totalNode);

    if (leftIdx >= totalNode && rightIdx >= totalNode && currentFileIdx < config->chunk) {
        nodeList[nodeIdx] = currentFileIdx;
        nodeValue[currentFileIdx].value = getSortData(currentFileIdx);
        ++currentFileIdx;
    } else if (leftIdx >= totalNode && rightIdx >= totalNode && currentFileIdx >= config->chunk) {
        nodeList[nodeIdx] = -1;
    } else {
		if (nodeList[leftIdx] == -1 && nodeList[rightIdx] == -1) {
			nodeList[nodeIdx] = -1;
		} else if (nodeList[leftIdx] != -1 && nodeList[rightIdx] == -1) {
			nodeList[nodeIdx] = nodeList[leftIdx];
		} else if (nodeList[leftIdx] == -1 && nodeList[rightIdx] != -1) {
			nodeList[nodeIdx] = nodeList[rightIdx];
		} else {
			if (nodeCmp(nodeValue[nodeList[leftIdx]].value, nodeValue[nodeList[rightIdx]].value)) {
				nodeList[nodeIdx] = nodeList[leftIdx];
			} else {
				nodeList[nodeIdx] = nodeList[rightIdx];
			}
		}
    }
}

void initWinnerTree(SortConfig *conf, int *fileInfo)
{   
    config = conf;
    fileNum = fileInfo;

    fin = (FILE **) malloc((config->chunk + 1) * sizeof(FILE *));
    fmap = (FILE **) malloc((config->chunk + 1) * sizeof(FILE *));
    char splitFile[31], offsetFile[31]; 
    for (int i = 1; i <= config->chunk; i++) {
        sprintf(splitFile, "%s%d_%d.rec", _WINNER_TREE_SPLIT_FILE, i, 1);
        sprintf(offsetFile, "%s%d_%d.rec", _WINNER_TREE_OFFSET_FILE, i, 1);
        fin[i-1] = fopen(splitFile, "r");
        fmap[i-1] = fopen(offsetFile, "r");
    }
    fout = fopen(_WINNER_TREE_RESULT_FILE, "w");

    int nodeNum = 1;
    while (nodeNum < config->chunk) {
        nodeNum <<= 1;
    }

    nodeList = (int *) malloc((2 * nodeNum - 1) * sizeof(int));
    for (int i = 0; i < 2 * nodeNum - 1; i++) {
        nodeList[i] = -1;
    }

    nodeValue = (WinnerTreeNode *) malloc(config->chunk * sizeof(WinnerTreeNode));
    for (int i = 0; i < config->chunk; i++) {
        nodeValue[i].chunkIdx = i + 1;
        nodeValue[i].fileIdx = 1;
    }

    winnerTreeInsert(0, 2 * nodeNum - 1);
}

bool checkWinnerTreeEmpty()
{
    if (nodeList[0] == -1) {
        fflush(fout);
        fclose(fout);
        free(nodeList);
        free(nodeValue);
        return true;
    } else {
        return false;
    }
}

static void winnerTreeUpdate(int nodeIdx, int updateIdx, int totalNode)
{
    if (nodeIdx >= totalNode) {
        return;
    }

    int leftIdx = 2 * nodeIdx + 1;
    int rightIdx = 2 * nodeIdx + 2;
    winnerTreeUpdate(leftIdx, updateIdx, totalNode);
    winnerTreeUpdate(rightIdx, updateIdx, totalNode);

	if (leftIdx >= totalNode &&  rightIdx >= totalNode && nodeList[nodeIdx] == updateIdx) {
		SortData *newData = getSortData(updateIdx);
		if (newData != NULL) {
            free(nodeValue[updateIdx].value->record);
            free(nodeValue[updateIdx].value);
			nodeValue[updateIdx].value = newData;
		} else {
            char splitFile[31], offsetFile[31]; 
            sprintf(splitFile, "%s%d_%d.rec", _WINNER_TREE_SPLIT_FILE, nodeValue[updateIdx].chunkIdx, nodeValue[updateIdx].fileIdx);
            sprintf(offsetFile, "%s%d_%d.rec", _WINNER_TREE_OFFSET_FILE, nodeValue[updateIdx].chunkIdx, nodeValue[updateIdx].fileIdx);
            fclose(fin[updateIdx]);
            fclose(fmap[updateIdx]);

			if (nodeValue[updateIdx].fileIdx != fileNum[nodeValue[updateIdx].chunkIdx - 1]) {
                char newSplitFile[31], newOffsetFile[31];
                sprintf(newSplitFile, "%s%d_%d.rec", _WINNER_TREE_SPLIT_FILE, nodeValue[updateIdx].chunkIdx, nodeValue[updateIdx].fileIdx + 1);
                sprintf(newOffsetFile, "%s%d_%d.rec", _WINNER_TREE_OFFSET_FILE, nodeValue[updateIdx].chunkIdx, nodeValue[updateIdx].fileIdx + 1);
                fin[updateIdx] = fopen(newSplitFile, "r");
                fmap[updateIdx] = fopen(newOffsetFile, "r");

                free(nodeValue[updateIdx].value->record);
                free(nodeValue[updateIdx].value);

                nodeValue[updateIdx].value = getSortData(updateIdx);
                nodeValue[updateIdx].fileIdx += 1;
			} else {
                free(nodeValue[updateIdx].value->record);
                free(nodeValue[updateIdx].value);
				nodeList[nodeIdx] = -1;
			}

            free(newData);
			remove(splitFile);
            remove(offsetFile);
			printf("Sucessfully merge %s\n", splitFile);
		}
	} else if (leftIdx < totalNode &&  rightIdx < totalNode && nodeList[nodeIdx] != -1) {
		if (nodeList[leftIdx] == -1 && nodeList[rightIdx] == -1) {
			nodeList[nodeIdx] = -1;
		} else if (nodeList[leftIdx] != -1 && nodeList[rightIdx] == -1) {
			nodeList[nodeIdx] = nodeList[leftIdx];
		} else if (nodeList[leftIdx] == -1 && nodeList[rightIdx] != -1) {
			nodeList[nodeIdx] = nodeList[rightIdx];
		} else {
			if (nodeCmp(nodeValue[nodeList[leftIdx]].value, nodeValue[nodeList[rightIdx]].value)) {
				nodeList[nodeIdx] = nodeList[leftIdx];
			} else {
				nodeList[nodeIdx] = nodeList[rightIdx];
			}
		}
	}
}

void winnerTreePop()
{
    fwrite(nodeValue[nodeList[0]].value->record, strlen(nodeValue[nodeList[0]].value->record), sizeof(char), fout);

    int nodeNum = 1;
    while (nodeNum < config->chunk) {
        nodeNum <<= 1;
    }

    winnerTreeUpdate(0, nodeList[0], 2 * nodeNum - 1);
}