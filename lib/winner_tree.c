#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include "winner_tree.h"

#define _DEFAULT_BUFFER_SIZE 1024

typedef struct WinnerTreeNode {
    SortFormat *value;
    int chunkIdx;
    int fileIdx;
} WinnerTreeNode;

// File variables
static FILE **fin;
static FILE *fout;
static int *fileNum;
static int currentFileIdx = 0;

// Record variables
static char **recordTmp;
static int *recordMaxBufferSize;
static size_t fixBufferSize = 0;
static bool fixBufferMode = true;

// Tree variables
static SortArgs *treeArgs;
static int *nodeList;
static WinnerTreeNode *nodeValue;

static SortFormat *getSortData(int fileIdx)
{
    SortFormat *sortData = (SortFormat *) malloc(sizeof(SortFormat));
    sortData->keyBegin = NULL;

    char inputBuffer[_DEFAULT_BUFFER_SIZE];
    char *tmp = malloc(strlen(recordTmp[fileIdx]) + _DEFAULT_BUFFER_SIZE);
    strcpy(tmp, recordTmp[fileIdx]);
    size_t maxInputBufferSize = strlen(recordTmp[fileIdx]) + _DEFAULT_BUFFER_SIZE;
    bool findRecord = false;

    size_t readBuffer;
    if (fixBufferMode) {
        readBuffer = fixBufferSize;
    } else {
        readBuffer = _DEFAULT_BUFFER_SIZE;
    }
    
    while (fgets(inputBuffer, readBuffer, fin[fileIdx]) != NULL) {
        if (treeArgs->beginTag != NULL) {
            if (strcmp(recordTmp[fileIdx], "\0") == 0 && strstr(inputBuffer, treeArgs->beginTag) && !findRecord) {
                if (maxInputBufferSize <= strlen(tmp) + strlen(inputBuffer)) {
                    tmp = (char *) realloc(tmp, strlen(tmp) + strlen(inputBuffer) + _DEFAULT_BUFFER_SIZE);
                    maxInputBufferSize = strlen(tmp) + strlen(inputBuffer) + _DEFAULT_BUFFER_SIZE;
                }
                strcat(tmp, inputBuffer);
                findRecord = true;
            } else if (strstr(inputBuffer, treeArgs->beginTag) && findRecord) {
                if (recordMaxBufferSize[fileIdx] <= strlen(recordTmp[fileIdx]) + strlen(inputBuffer)) {
                    recordTmp[fileIdx] = (char *) realloc(recordTmp[fileIdx], strlen(recordTmp[fileIdx]) + strlen(inputBuffer) + _DEFAULT_BUFFER_SIZE);
                    recordMaxBufferSize[fileIdx] = strlen(recordTmp[fileIdx]) + strlen(inputBuffer) + _DEFAULT_BUFFER_SIZE;
                }
                strcpy(recordTmp[fileIdx], inputBuffer);
                break;
            } else {
                if (strcmp(recordTmp[fileIdx], "\0") != 0) {
                    findRecord = true;
                }

                if (maxInputBufferSize <= strlen(tmp) + strlen(inputBuffer)) {
                    tmp = (char *) realloc(tmp, strlen(tmp) + strlen(inputBuffer) + _DEFAULT_BUFFER_SIZE);
                    maxInputBufferSize = strlen(tmp) + strlen(inputBuffer) + _DEFAULT_BUFFER_SIZE;
                }

                strcat(tmp, inputBuffer);
            }
        } else {
            if (maxInputBufferSize <= strlen(tmp) + strlen(inputBuffer)) {
                tmp = (char *) realloc(tmp, strlen(tmp) + strlen(inputBuffer) + _DEFAULT_BUFFER_SIZE);
                maxInputBufferSize = strlen(tmp) + strlen(inputBuffer) + _DEFAULT_BUFFER_SIZE;
            }

            strcat(tmp, inputBuffer);

            if (tmp[strlen(tmp)-1] == '\n') {
                findRecord = true;
                break;
            }
        }
    }

    if (strlen(tmp) != 0 && fixBufferMode) {
        fseek(fin[fileIdx], fixBufferSize - strlen(tmp), SEEK_CUR);
    }

    if (!findRecord) {
        if (strcmp(tmp, recordTmp[fileIdx]) == 0) {
            strcpy(tmp, "\0");
        }
        free(recordTmp[fileIdx]);
        recordTmp[fileIdx] = (char *) malloc(_DEFAULT_BUFFER_SIZE);
        strcpy(recordTmp[fileIdx], "\0");
        recordMaxBufferSize[fileIdx] = _DEFAULT_BUFFER_SIZE;
    }

    if (treeArgs->keyTag != NULL) {
        char *keyPtr = strstr(tmp, treeArgs->keyTag);
        if (keyPtr != NULL) {
            sortData->keyBegin = (char *) malloc(strlen(keyPtr) + 1);
            strcpy(sortData->keyBegin, keyPtr + strlen(treeArgs->keyTag));
        } else {
            sortData->keyBegin = (char *) malloc(sizeof(char));
            strcpy(sortData->keyBegin, "\0");
        }
    }

    sortData->recordBegin = (char *) malloc(strlen(tmp) + 1);
    strcpy(sortData->recordBegin, tmp);

    free(tmp);
    return sortData;
}

static bool nodeCmp(SortFormat *left, SortFormat *right) 
{
    char *leftPtr, *rightPtr;
    if (treeArgs->keyTag == NULL) {
        if (treeArgs->numeric) {
            long leftNum = strtol(left->recordBegin, &leftPtr, 10);
            long rightNum = strtol(right->recordBegin, &rightPtr, 10);
            if (leftNum > rightNum || (leftNum == rightNum && strcmp(leftPtr, rightPtr) < 0)) {
                return true; 
            } else {
                return false;
            }
        } else {
            if (strcmp(left->recordBegin, right->recordBegin) <= 0) {
                return true;
            } else {
                return false;
            }
        }
    } else {
        if (treeArgs->numeric) {
            long leftNum, rightNum;

            if (left->keyBegin != NULL) {
                leftNum = strtol(left->keyBegin, &leftPtr, 10);
            } else {
                leftNum = strtol(left->recordBegin, &leftPtr, 10);
            }

            if (right->keyBegin != NULL) {
                rightNum = strtol(right->keyBegin, &rightPtr, 10);
            } else {
                rightNum = strtol(right->recordBegin, &rightPtr, 10);
            }

            if (leftNum > rightNum || (leftNum == rightNum && strcmp(leftPtr, rightPtr) < 0)) {
                return true; 
            } else {
                return false;
            }
        } else {
            if (left->keyBegin != NULL) {
                leftPtr = left->keyBegin;
            } else {
                leftPtr = left->recordBegin;
            }

            if (right->keyBegin != NULL) {
                rightPtr = right->keyBegin;
            } else {
                rightPtr = right->recordBegin;
            }

            if (strcmp(leftPtr, rightPtr) <= 0) {
                return true;
            } else {
                return false;
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

    if (leftIdx >= totalNode && rightIdx >= totalNode && currentFileIdx < treeArgs->chunk) {
        nodeList[nodeIdx] = currentFileIdx;
        nodeValue[currentFileIdx].value = getSortData(currentFileIdx);
        ++currentFileIdx;
    } else if (leftIdx >= totalNode && rightIdx >= totalNode && currentFileIdx >= treeArgs->chunk) {
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

void initWinnerTree(SortArgs *userArgs, int *fileInfo, size_t bufferSize, bool openFixBufferMode)
{   
    // Initialize global variables
    treeArgs = userArgs;
    fileNum = fileInfo;
    fixBufferSize = bufferSize;
    fixBufferMode = openFixBufferMode;

    recordTmp = (char **) malloc((treeArgs->chunk + 1) * sizeof(char *));
    for (int i = 0; i < treeArgs->chunk; i++) {
        recordTmp[i] = (char *) malloc(_DEFAULT_BUFFER_SIZE * sizeof(char));
        strcpy(recordTmp[i], "\0");
    }

    recordMaxBufferSize = (int *) malloc((treeArgs->chunk + 1) * sizeof(int));
    memset(recordMaxBufferSize, _DEFAULT_BUFFER_SIZE, (treeArgs->chunk + 1) * sizeof(int));

    fin = (FILE **) malloc((treeArgs->chunk + 1) * sizeof(FILE *));
    char filename[31]; 
    for (int i = 1; i <= treeArgs->chunk; i++) {
        sprintf(filename, "%s%d_%d.rec", _WINNER_TREE_SPLIT_FILE, i, 1);
        fin[i-1] = fopen(filename, "r");
    }

    fout = fopen(_WINNER_TREE_RESULT_FILE, "w");

    int nodeNum = 1;
    while (nodeNum < treeArgs->chunk) {
        nodeNum <<= 1;
    }

    nodeList = (int *) malloc((2 * nodeNum - 1) * sizeof(int));
    for (int i = 0; i < 2 * nodeNum - 1; i++) {
        nodeList[i] = -1;
    }

    nodeValue = (WinnerTreeNode *) malloc(treeArgs->chunk * sizeof(WinnerTreeNode));
    for (int i = 0; i < treeArgs->chunk; i++) {
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
        free(recordTmp);
        free(recordMaxBufferSize);
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
		SortFormat *newData = getSortData(updateIdx);
		if (strlen(newData->recordBegin) != 0) {
            free(nodeValue[updateIdx].value->recordBegin);
            if (treeArgs->keyTag != NULL) {
                free(nodeValue[updateIdx].value->keyBegin);
            }
            free(nodeValue[updateIdx].value);
			nodeValue[updateIdx].value = newData;
		} else {
            char filename[31]; 
            sprintf(filename, "%s%d_%d.rec", _WINNER_TREE_SPLIT_FILE, nodeValue[updateIdx].chunkIdx, nodeValue[updateIdx].fileIdx);
            fclose(fin[updateIdx]);

			if (nodeValue[updateIdx].fileIdx != fileNum[nodeValue[updateIdx].chunkIdx - 1]) {
                char newFilename[31];
                sprintf(newFilename, "%s%d_%d.rec", _WINNER_TREE_SPLIT_FILE, nodeValue[updateIdx].chunkIdx, nodeValue[updateIdx].fileIdx + 1);
                fin[updateIdx] = fopen(newFilename, "r");

                free(recordTmp[updateIdx]);
                recordTmp[updateIdx] = (char *) malloc(_DEFAULT_BUFFER_SIZE);
                strcpy(recordTmp[updateIdx], "\0");

                free(nodeValue[updateIdx].value->recordBegin);
                if (treeArgs->keyTag != NULL) {
                    free(nodeValue[updateIdx].value->keyBegin);
                }
                free(nodeValue[updateIdx].value);
                recordMaxBufferSize[updateIdx] = _DEFAULT_BUFFER_SIZE;
                nodeValue[updateIdx].value = getSortData(updateIdx);
                nodeValue[updateIdx].fileIdx += 1;
			} else {
                free(recordTmp[updateIdx]);
                free(nodeValue[updateIdx].value->recordBegin);
                if (treeArgs->keyTag != NULL) {
                    free(nodeValue[updateIdx].value->keyBegin);
                }
                free(nodeValue[updateIdx].value);
				nodeList[nodeIdx] = -1;
			}

            free(newData);
			remove(filename);
			printf("Sucessfully merge %s\n", filename);
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
    fwrite(nodeValue[nodeList[0]].value->recordBegin, strlen(nodeValue[nodeList[0]].value->recordBegin), sizeof(char), fout);

    int nodeNum = 1;
    while (nodeNum < treeArgs->chunk) {
        nodeNum <<= 1;
    }

    winnerTreeUpdate(0, nodeList[0], 2 * nodeNum - 1);
}