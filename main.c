#include "main.h"

#define _DEFAULT_BUFFER_SIZE 1024
#define _DEFAULT_SORT_DATA_SIZE 1024
#define _DEFAULT_FILE_SIZE 2000000000 // 2GB

SortArgs *userArgs;

int main(int argc, char *argv[])
{
    struct timeval start, end;
    long long startusec, endusec;
    double duration, totalDuration = 0;

    FILE *fin = fopen(argv[argc-1], "r");
    if (fin == NULL) {
        perror("Error fopen input file ");
        exit(0);
    }

    // Get input file size
    struct stat st;
    stat(argv[argc-1], &st);
    size_t fileSize = st.st_size;

    initUserArgs(argc, argv);

    size_t sortDataSize = _DEFAULT_SORT_DATA_SIZE;
    size_t maxInputBufferSize = _DEFAULT_BUFFER_SIZE;
    size_t maxRecordBufferSize = _DEFAULT_BUFFER_SIZE;

    SortFormat *sortData = (SortFormat *) malloc(sortDataSize * sizeof(SortFormat));
    char *inputBuffer = (char *) malloc(_DEFAULT_BUFFER_SIZE * sizeof(char));
    char *tmpInputBuffer = (char *) malloc(_DEFAULT_BUFFER_SIZE * sizeof(char));
    strcpy(tmpInputBuffer, "\0");
    char *tmpRecordBeginBuffer;
    tmpRecordBeginBuffer = (char *) malloc(_DEFAULT_BUFFER_SIZE * sizeof(char));
    strcpy(tmpRecordBeginBuffer, "\0");

    int count = 0, chunkIdx = 1, fileIdx[userArgs->chunk];
    size_t countSize = 0;
    bool findRecord = false;
    for (int i = 0; i < userArgs->chunk; i++) {
        fileIdx[i] = 0;
    }

    gettimeofday(&start, NULL);

    // Get input data
    while (fgets(inputBuffer, _DEFAULT_BUFFER_SIZE, fin) != NULL) {
        if (strlen(tmpInputBuffer) + strlen(inputBuffer) >= maxInputBufferSize) {
            tmpInputBuffer = (char *) realloc(tmpInputBuffer, strlen(tmpInputBuffer) + strlen(inputBuffer) + _DEFAULT_BUFFER_SIZE);
            maxInputBufferSize = strlen(tmpInputBuffer) + strlen(inputBuffer) + _DEFAULT_BUFFER_SIZE;
        }
        strcat(tmpInputBuffer, inputBuffer);

        if (tmpInputBuffer[strlen(tmpInputBuffer)-1] == '\n') {
            if (userArgs->beginTag == NULL) {
                if (count >= sortDataSize) {
                    sortData = (SortFormat *) realloc(sortData, (sortDataSize + _DEFAULT_SORT_DATA_SIZE) * sizeof(SortFormat));
                    sortDataSize += _DEFAULT_SORT_DATA_SIZE;
                }

                if (strlen(tmpInputBuffer) == 1 && strcmp(tmpInputBuffer, userArgs->endTag) == 0) {
                    if (userArgs->keyTag != NULL) {
                        sortData[count].keyBegin = (char *) malloc(sizeof(char));
                        strcpy(sortData[count].keyBegin, "\0");
                    }

                    sortData[count].recordBegin = (char *) malloc(strlen(tmpInputBuffer) + 1);
                    strcpy(sortData[count].recordBegin, tmpInputBuffer);
                    countSize += strlen(sortData[count].recordBegin);
                    ++count;
                    
                    if (countSize + 1000 > (fileSize * chunkIdx) / userArgs->chunk) {
                        fileIdx[chunkIdx-1] = splitKFile(&sortData, count, chunkIdx);

                        for (int i = 0; i < count; i++) {
                            free(sortData[i].recordBegin);
                            if (userArgs->keyTag != NULL) {
                                free(sortData[i].keyBegin);
                            }
                        }
                        free(sortData);

                        sortDataSize = _DEFAULT_SORT_DATA_SIZE;
                        sortData = (SortFormat *) malloc(sortDataSize * sizeof(SortFormat));

                        count = 0;
                        ++chunkIdx;
                    }
                } else {
                    char *endPtr = strtok(tmpInputBuffer, userArgs->endTag);
                    while (endPtr != NULL) {
                        if (userArgs->keyTag != NULL) {
                            char *keyPtr = strstr(endPtr, userArgs->keyTag);
                            if (keyPtr != NULL) {
                                sortData[count].keyBegin = (char *) malloc(strlen(keyPtr) + 1);
                                strcpy(sortData[count].keyBegin, keyPtr + strlen(userArgs->keyTag));
                            } else {
                                sortData[count].keyBegin = (char *) malloc(sizeof(char));
                                strcpy(sortData[count].keyBegin, "\0");
                            }
                        }

                        sortData[count].recordBegin = (char *) malloc(strlen(endPtr) + 1);
                        strcpy(sortData[count].recordBegin, endPtr);
                        countSize += strlen(sortData[count].recordBegin);
                        ++count;

                        if (countSize + 1000 > (fileSize * chunkIdx) / userArgs->chunk) {
                            fileIdx[chunkIdx-1] = splitKFile(&sortData, count, chunkIdx);

                            for (int i = 0; i < count; i++) {
                                free(sortData[i].recordBegin);
                                if (userArgs->keyTag != NULL) {
                                    free(sortData[i].keyBegin);
                                }
                            }
                            free(sortData);

                            sortDataSize = _DEFAULT_SORT_DATA_SIZE;
                            sortData = (SortFormat *) malloc(sortDataSize * sizeof(SortFormat));

                            count = 0;
                            ++chunkIdx;
                        }

                        endPtr = strtok(NULL, userArgs->endTag);
                    }
                }
            } else {
                char *beginPtr = strstr(tmpInputBuffer, userArgs->beginTag);
                if (beginPtr != NULL && findRecord == false && strcmp(tmpRecordBeginBuffer, "\0") == 0) {
                    if (maxRecordBufferSize <= strlen(tmpRecordBeginBuffer) + strlen(beginPtr)) {
                        tmpRecordBeginBuffer = (char *) realloc(tmpRecordBeginBuffer, strlen(tmpRecordBeginBuffer) + strlen(beginPtr) + _DEFAULT_BUFFER_SIZE);
                        maxRecordBufferSize = strlen(tmpRecordBeginBuffer) + strlen(beginPtr) + _DEFAULT_BUFFER_SIZE;
                    }
                    findRecord = true;
                    strcpy(tmpRecordBeginBuffer, beginPtr);
                } else if (beginPtr != NULL && findRecord == true && strcmp(tmpRecordBeginBuffer, "\0") != 0) {
                    if (count >= sortDataSize) {
                        sortData = (SortFormat *) realloc(sortData, (sortDataSize + _DEFAULT_SORT_DATA_SIZE) * sizeof(SortFormat));
                        sortDataSize += _DEFAULT_SORT_DATA_SIZE;
                    }

                    if (userArgs->keyTag != NULL) {
                        char *keyPtr = strstr(tmpRecordBeginBuffer, userArgs->keyTag);
                        if (keyPtr != NULL) {
                            sortData[count].keyBegin = (char *) malloc(strlen(keyPtr) + 1);
                            strcpy(sortData[count].keyBegin, keyPtr + strlen(userArgs->keyTag));
                        } else {
                            sortData[count].keyBegin = (char *) malloc(sizeof(char));
                            strcpy(sortData[count].keyBegin, "\0");
                        }
                    }

                    sortData[count].recordBegin = (char *) malloc(strlen(tmpRecordBeginBuffer) + 1);
                    strcpy(sortData[count].recordBegin, tmpRecordBeginBuffer);
                    countSize += strlen(sortData[count].recordBegin);
                    ++count;

                    if (countSize + 1000 > (fileSize * chunkIdx) / userArgs->chunk) {
                        fileIdx[chunkIdx-1] = splitKFile(&sortData, count, chunkIdx);

                        for (int i = 0; i < count; i++) {
                            free(sortData[i].recordBegin);
                            if (userArgs->keyTag != NULL) {
                                free(sortData[i].keyBegin);
                            }
                        }
                        free(sortData);

                        sortDataSize = _DEFAULT_SORT_DATA_SIZE;
                        sortData = (SortFormat *) malloc(sortDataSize * sizeof(SortFormat));

                        count = 0;
                        ++chunkIdx;
                    }

                    free(tmpRecordBeginBuffer);
                    maxRecordBufferSize = _DEFAULT_BUFFER_SIZE + (int) strlen(beginPtr);
                    tmpRecordBeginBuffer = (char *) malloc(maxRecordBufferSize * sizeof(char));
                    strcpy(tmpRecordBeginBuffer, "\0");
                    strcpy(tmpRecordBeginBuffer, beginPtr);
                } else if (beginPtr == NULL && findRecord == true) {
                    if (maxRecordBufferSize <= strlen(tmpRecordBeginBuffer) + strlen(tmpInputBuffer)) {
                        tmpRecordBeginBuffer = (char *) realloc(tmpRecordBeginBuffer, strlen(tmpRecordBeginBuffer) + strlen(tmpInputBuffer) + _DEFAULT_BUFFER_SIZE);
                        maxRecordBufferSize = strlen(tmpRecordBeginBuffer) + strlen(tmpInputBuffer) + _DEFAULT_BUFFER_SIZE;
                    }
                    strcat(tmpRecordBeginBuffer, tmpInputBuffer);
                }
            }

            free(tmpInputBuffer);
            maxInputBufferSize = _DEFAULT_BUFFER_SIZE;
            tmpInputBuffer = (char *) malloc(maxInputBufferSize * sizeof(char));
            strcpy(tmpInputBuffer, "\0");
        }
    }

    // Get last record in beginTag case
    if (userArgs->beginTag != NULL && findRecord == true && strcmp(tmpRecordBeginBuffer, "\0") != 0) {
        if (userArgs->keyTag != NULL) {
            char *keyPtr = strstr(tmpRecordBeginBuffer, userArgs->keyTag);
            if (keyPtr != NULL) {
                sortData[count].keyBegin = (char *) malloc(strlen(keyPtr) + 1);
                strcpy(sortData[count].keyBegin, keyPtr + strlen(userArgs->keyTag));
            } else {
                sortData[count].keyBegin = (char *) malloc(sizeof(char));
                strcpy(sortData[count].keyBegin, "\0");
            }
        }
        sortData[count].recordBegin = (char *) malloc(strlen(tmpRecordBeginBuffer) + 1);
        strcpy(sortData[count].recordBegin, tmpRecordBeginBuffer);
        ++count;
    }

    // Write last chunk in file
    if (count != 0 && chunkIdx <= userArgs->chunk) {
        fileIdx[chunkIdx-1] = splitKFile(&sortData, count, chunkIdx);
        for (int i = 0; i < count; i++) {
            free(sortData[i].recordBegin);
            if (userArgs->keyTag != NULL) {
                free(sortData[i].keyBegin);
            }
        }
        free(sortData);
    }

    free(inputBuffer);
    free(tmpInputBuffer);
    free(tmpRecordBeginBuffer);

    gettimeofday(&end, NULL);
    startusec = start.tv_sec * 1000000 + start.tv_usec;
    endusec = end.tv_sec * 1000000 + end.tv_usec;
    duration = (double) (endusec - startusec) / 1000000.0;
    totalDuration += duration;
    printf("Data processing spends %lf sec\n", duration);

    // Merge
    if (fileIdx[0] == 0) {
        perror("Error fgets or file is empty ");
        exit(0);
    } else if (fileIdx[0] == 1 && userArgs->chunk == 1) {
        char filename[31];
        sprintf(filename, "%s%d_%d.rec", _WINNER_TREE_SPLIT_FILE, 1, 1);
        rename(filename, _WINNER_TREE_RESULT_FILE);
        printf("Successfully rename split file\n");
    } else {
        gettimeofday(&start, NULL);
        mergeKFile(fileIdx);
        gettimeofday(&end, NULL);
        startusec = start.tv_sec * 1000000 + start.tv_usec;
        endusec = end.tv_sec * 1000000 + end.tv_usec;
        duration = (double) (endusec - startusec) / 1000000.0;
        totalDuration += duration;
        printf("Merge split files spends %lf sec\n", duration);
    }

    printf("Rsort spends %lf sec\n", totalDuration);
    return 0;
}

void initUserArgs(int argc, char *argv[])
{
    userArgs = (SortArgs *) malloc(sizeof(SortArgs));
    userArgs->beginTag = userArgs->keyTag = NULL;
    userArgs->endTag = (char *) malloc(10 * sizeof(char));
    strcpy(userArgs->endTag, "\n");
    userArgs->reverse = false;
    userArgs->numeric = false;
    userArgs->chunk = 1;
    userArgs->maxFileSize = _DEFAULT_FILE_SIZE;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "-rb") == 0) {
            userArgs->beginTag = (char *) malloc(30 * sizeof(char));
            strcpy(userArgs->beginTag, argv[i+1]);
        } else if (strcmp(argv[i], "-d") == 0) {
            strcpy(userArgs->endTag, argv[i+1]);
        } else if (strcmp(argv[i], "-k") == 0) {
            userArgs->keyTag = (char *) malloc(20 * sizeof(char));
            strcpy(userArgs->keyTag, argv[i+1]);
        } else if (strcmp(argv[i], "-n") == 0) {
            userArgs->numeric = true;
        } else if (strcmp(argv[i], "-r") == 0) {
            userArgs->reverse = true;
        } else if (strcmp(argv[i], "--chunk") == 0) {
            userArgs->chunk = strtol(argv[i+1], NULL, 10);
        } else if (strcmp(argv[i], "-s") == 0) {
            userArgs->maxFileSize = (size_t) strtol(argv[i+1], NULL, 10);
        }
    }
}

int splitKFile(SortFormat **data, int size, int chunkIdx)
{
    int fileNum = 1;
    struct timeval start, end;
    long long startusec, endusec;
    double duration;

    // Sort
    gettimeofday(&start, NULL);
    int *idx = (int *) malloc(size * sizeof(int));
    for (int i = 0; i < size; i++) {
        idx[i] = i;
    }
    mergeSort(data, &idx, size, userArgs);
    gettimeofday(&end, NULL);

    startusec = start.tv_sec * 1000000 + start.tv_usec;
    endusec = end.tv_sec * 1000000 + end.tv_usec;
    duration = (double) (endusec - startusec) / 1000000.0;
    printf("Mergesort for chunk %d spends %lf sec\n", chunkIdx, duration);

    // Write files
    gettimeofday(&start, NULL);
    char filename[31];
    sprintf(filename, "%s%d_%d.rec", _WINNER_TREE_SPLIT_FILE, chunkIdx, fileNum);
    size_t countSize = 0;

    FILE *fout = fopen(filename, "w");
    for (int i = 0; i < size; i++) {
        char *newContent = malloc(strlen((*data)[idx[i]].recordBegin) + 2);
        if (userArgs->beginTag == NULL && strcmp(userArgs->endTag, "\n") == 0 && (*data)[idx[i]].recordBegin[0] != '\n') {
            sprintf(newContent, "%s\n", (*data)[idx[i]].recordBegin);
        } else {
            sprintf(newContent, "%s", (*data)[idx[i]].recordBegin);
        }
        fwrite(newContent, sizeof(char), strlen(newContent), fout);
        countSize += strlen(newContent);

        if (i + 1 < size && countSize + 1000 > userArgs->maxFileSize) {
            fclose(fout);
            ++fileNum;
            sprintf(filename, "%s%d_%d.rec", _WINNER_TREE_SPLIT_FILE, chunkIdx, fileNum);
            fout = fopen(filename, "w");
            countSize = 0;
        }
        free(newContent);
    }
    
    fclose(fout);
    gettimeofday(&end, NULL);

    startusec = start.tv_sec * 1000000 + start.tv_usec;
    endusec = end.tv_sec * 1000000 + end.tv_usec;
    duration = (double) (endusec - startusec) / 1000000.0;
    printf("Write %d files for chunk %d spends %lf sec\n", fileNum, chunkIdx, duration);

    free(idx);
    return fileNum;
}

void mergeKFile(int *fileIdx)
{
    initWinnerTree(userArgs, fileIdx, 0, false);

    while (true) {
        if (checkWinnerTreeEmpty()) {
            break;
        }
        winnerTreePop();
    }
}