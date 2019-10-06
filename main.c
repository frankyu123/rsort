//***************************************************************************
// Rsort Command
// @Description : 
// case -rb             : record begin by specific tag
// case -d              : split by delimiters and sort
// case -k              : sort by specific key column
// case -n              : numerical comparison
// case -r              : reverse order
// case -chunk          : number of external chunk
// case -s              : limit file size (byte)
// case -parallel       : number of threads using in sort
//***************************************************************************

#include "main.h"

#define _DEFAULT_BUFFER_SIZE 1024
#define _SPLIT_FILE "split_text"
#define _OFFSET_FILE "offset"
#define _TERM_FILE "term.rec"

static int _fileNum = 0;

int main(int argc, char *argv[])
{
    SortConfig *config = initSortConfig(argc, argv);

    FILE *fin;
    if (config->isCutByDelim) {
        char cmd[300];
        sprintf(cmd, "./tools/segmentor/seg < %s > %s", argv[argc-1], _TERM_FILE);
        system(cmd);
        fin = fopen(_TERM_FILE, "r");
    } else {
        fin = fopen(argv[argc-1], "r");
        if (argc == 1 || fin == NULL) {
            fin = stdin;
        }
    }

    int size = _DEFAULT_BUFFER_SIZE;
    int maxInputBufferSize = _DEFAULT_BUFFER_SIZE;
    int maxRecordBufferSize = _DEFAULT_BUFFER_SIZE;

    char *inputBuffer = (char *) malloc(_DEFAULT_BUFFER_SIZE * sizeof(char));
    char *tmpInputBuffer = (char *) malloc(_DEFAULT_BUFFER_SIZE * sizeof(char));
    strcpy(tmpInputBuffer, "\0");
    char *tmpRecordBeginBuffer;
    tmpRecordBeginBuffer = (char *) malloc(_DEFAULT_BUFFER_SIZE * sizeof(char));
    strcpy(tmpRecordBeginBuffer, "\0");

    // Parser
    SortData *data = malloc(size * sizeof(SortData));
    int count = 0, memUsed = 0;
    while (fgets(inputBuffer, _DEFAULT_BUFFER_SIZE, fin) != NULL) {
        if (strlen(tmpInputBuffer) + strlen(inputBuffer) >= maxInputBufferSize) {
            maxInputBufferSize = strlen(tmpInputBuffer) + strlen(inputBuffer) + _DEFAULT_BUFFER_SIZE;
            tmpInputBuffer = (char *) realloc(tmpInputBuffer, maxInputBufferSize);
        }
        strcat(tmpInputBuffer, inputBuffer);

        if (tmpInputBuffer[strlen(tmpInputBuffer)-1] == '\n') {
            if (config->beginTag == NULL) {
                if (count >= size) {
                    size += _DEFAULT_BUFFER_SIZE;
                    data = (SortData *) realloc(data, size * sizeof(SortData));
                }

                data[count].record = (char *) malloc(strlen(tmpInputBuffer) + 1);
                strcpy(data[count].record, tmpInputBuffer);
                memUsed += strlen(data[count].record);
                ++count;
                
                if (memUsed >= config->maxFileSize) {
                    splitKFile(&data, count, config);

                    for (int i = 0; i < count; i++) {
                        free(data[i].record);
                    }
                    free(data);

                    size = _DEFAULT_BUFFER_SIZE;
                    data = (SortData *) malloc(size * sizeof(SortData));
                    count = memUsed = 0;
                }
            } else {
                bool findRecord = false;
                char *ptr = tmpInputBuffer, *rbPtr;
                while ((rbPtr = strstr(ptr, config->beginTag)) != NULL) {
                    if (strcmp(tmpRecordBeginBuffer, "\0") == 0) {
                        if (maxRecordBufferSize <= strlen(tmpRecordBeginBuffer) + strlen(rbPtr)) {
                            maxRecordBufferSize = strlen(tmpRecordBeginBuffer) + strlen(rbPtr) + _DEFAULT_BUFFER_SIZE;
                            tmpRecordBeginBuffer = (char *) realloc(tmpRecordBeginBuffer, maxRecordBufferSize);
                        }
                        strcpy(tmpRecordBeginBuffer, rbPtr);
                    } else {
                        if (count >= size) {
                            size += _DEFAULT_BUFFER_SIZE;
                            data = (SortData *) realloc(data, size * sizeof(SortData));
                        }
                        data[count].record = (char *) malloc(strlen(tmpRecordBeginBuffer) + 1);
                        strcpy(data[count].record, tmpRecordBeginBuffer);
                        memUsed += strlen(data[count].record);
                        ++count;

                        if (memUsed >= config->maxFileSize) {
                            splitKFile(&data, count, config);

                            for (int i = 0; i < count; i++) {
                                free(data[i].record);
                            }
                            free(data);

                            size = _DEFAULT_BUFFER_SIZE;
                            data = (SortData *) malloc(size * sizeof(SortData));
                            count = memUsed = 0;
                        }

                        free(tmpRecordBeginBuffer);
                        maxRecordBufferSize = _DEFAULT_BUFFER_SIZE + (int) strlen(rbPtr);
                        tmpRecordBeginBuffer = (char *) malloc(maxRecordBufferSize * sizeof(char));
                        memset(tmpRecordBeginBuffer, '\0', maxRecordBufferSize);
                        strcpy(tmpRecordBeginBuffer, rbPtr);
                    }
                    findRecord = true;
                    ptr = (rbPtr + strlen(config-> beginTag));
                }

                if (!findRecord && strcmp(tmpRecordBeginBuffer, "\0") != 0) {
                    if (maxRecordBufferSize <= strlen(tmpRecordBeginBuffer) + strlen(tmpInputBuffer)) {
                        maxRecordBufferSize = strlen(tmpRecordBeginBuffer) + strlen(tmpInputBuffer) + _DEFAULT_BUFFER_SIZE;
                        tmpRecordBeginBuffer = (char *) realloc(tmpRecordBeginBuffer, maxRecordBufferSize);
                    }
                    strcat(tmpRecordBeginBuffer, tmpInputBuffer);
                }
            }

            free(tmpInputBuffer);
            maxInputBufferSize = _DEFAULT_BUFFER_SIZE;
            tmpInputBuffer = (char *) malloc(maxInputBufferSize * sizeof(char));
            memset(tmpInputBuffer, '\0', maxInputBufferSize);
        }
    }

    // Get last record in -rb case
    if (config->beginTag != NULL && strcmp(tmpRecordBeginBuffer, "\0") != 0) {
        data[count].record = (char *) malloc(strlen(tmpRecordBeginBuffer) + 1);
        strcpy(data[count].record, tmpRecordBeginBuffer);
        ++count;
    }

    // Write remaining records in file
    if (count != 0) {
        splitKFile(&data, count, config);
        for (int i = 0; i < count; i++) {
            free(data[i].record);
        }
        free(data);
    }
    free(inputBuffer);
    free(tmpInputBuffer);
    free(tmpRecordBeginBuffer);
    fclose(fin);

    // Merge
    if (_fileNum == 0) {
        perror("Error fgets or file is empty ");
        exit(0);
    } else {
        mergeKFile(_fileNum, config);
    }

    if (config->isCutByDelim) {
        remove(_TERM_FILE);
    }

    return 0;
}

SortConfig *initSortConfig(int argc, char *argv[])
{
    SortConfig *config = malloc(sizeof(SortConfig));
    config->beginTag = config->keyTag = NULL;
    config->reverse = config->isCutByDelim = config->numeric = false;
    config->chunk = 1;
    config->maxFileSize = 1000000000; // Default approx. 1GB
    config->thread = 4;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "-rb") == 0) {
            config->beginTag = (char *) malloc(30 * sizeof(char));
            strcpy(config->beginTag, argv[i+1]);
            i += 1;
        } else if (strcmp(argv[i], "-d") == 0) {
            config->isCutByDelim = true;
        } else if (strcmp(argv[i], "-k") == 0) {
            config->keyTag = (char *) malloc(30 * sizeof(char));
            strcpy(config->keyTag, argv[i+1]);
            i += 1;
        } else if (strcmp(argv[i], "-n") == 0) {
            config->numeric = true;
        } else if (strcmp(argv[i], "-r") == 0) {
            config->reverse = true;
        } else if (strcmp(argv[i], "-chunk") == 0) {
            config->chunk = atoi(argv[i+1]);
            i += 1;
        } else if (strcmp(argv[i], "-s") == 0) {
            config->maxFileSize = atoi(argv[i+1]);
            i += 1;
        } else if (strcmp(argv[i], "-parallel") == 0) {
            config->thread = atoi(argv[i+1]);
            i += 1;
        }
    }

    if (config->thread < 2) {
        config->thread = 2;
    }

    return config;
}

void splitKFile(SortData **data, int size, SortConfig *config)
{
    ++_fileNum;

    int *idx = (int *) malloc(size * sizeof(int));
    for (int i = 0; i < size; i++) {
        idx[i] = i;
    }
    mergeSort(data, &idx, size, config);

    char splitFile[31], offsetFile[31];
    sprintf(splitFile, "%s_%d.rec", _SPLIT_FILE, _fileNum);
    sprintf(offsetFile, "%s_%d.rec", _OFFSET_FILE, _fileNum);

    FILE *fout = fopen(splitFile, "w");
    FILE *fmap = fopen(offsetFile, "w");
    for (int i = 0; i < size; i++) {
        fwrite((*data)[idx[i]].record, sizeof(char), strlen((*data)[idx[i]].record), fout);
        fprintf(fmap, "%d\n", (int) strlen((*data)[idx[i]].record));
    }
    
    fclose(fout);
    fclose(fmap);
    free(idx);
}