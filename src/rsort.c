/**
 * rsort.c - main driver file for rsort
 * 
 * Author: Frank Yu <frank85515@gmail.com>
 * 
 * (C) Copyright 2019 Frank Yu
 * 
 * External mergesort
 * 1) parse texts by giving OPTION
 * 2) split files in N size (-s OPTION) and sort ( internal mergesort )
 * 3) merge M files with K chunks ( winner tree )
 */

#include <rsort.h>

#define _BUFFER_SIZE 1024
#define _SPLIT_FILE "_split"
#define _OFFSET_FILE "_offset"
#define _TERM_FILE "_term_list"

int _fileNum = 0;

int main(int argc, char *argv[])
{
    RsortConfig *config = initRsortConfig(argc, argv);

    FILE *fin;
    if (config->input != NULL) {
        fin = fopen(config->input, "r");
        if (fin == NULL) {
            fprintf(stderr, "Error: file not found\n");
            exit(2);
        }
    } else {
        fin = stdin;
    }

    if (config->isCutByDelim) {
        if (access("./tools/segmentor/seg", F_OK) != 0) {
            fprintf(stderr, "Please compile tools/segmentor first\n");
            exit(2);
        } else {
            fclose(fin);
            char cmd[300];
            sprintf(cmd, "./tools/segmentor/seg < %s > %s", argv[argc-1], _TERM_FILE);
            system(cmd);
            fin = fopen(_TERM_FILE, "r");
        }
    }

    int maxInputBufferSize = _BUFFER_SIZE;
    int maxRecordBufferSize = _BUFFER_SIZE;
    char *inputBuffer = (char *) malloc(_BUFFER_SIZE * sizeof(char));
    char *tmpInputBuffer = (char *) malloc(_BUFFER_SIZE * sizeof(char));
    strcpy(tmpInputBuffer, "\0");
    char *tmpRecordBeginBuffer;
    tmpRecordBeginBuffer = (char *) malloc(_BUFFER_SIZE * sizeof(char));
    strcpy(tmpRecordBeginBuffer, "\0");

    // Parser
    int size = _BUFFER_SIZE;
    RecordList *data = malloc(size * sizeof(RecordList));
    int count = 0, memUsed = 0;
    while (fgets(inputBuffer, _BUFFER_SIZE, fin) != NULL) {
        if (strlen(tmpInputBuffer) + strlen(inputBuffer) >= maxInputBufferSize) {
            maxInputBufferSize = strlen(tmpInputBuffer) + strlen(inputBuffer) + _BUFFER_SIZE;
            tmpInputBuffer = (char *) realloc(tmpInputBuffer, maxInputBufferSize);
        }
        strcat(tmpInputBuffer, inputBuffer);

        if (tmpInputBuffer[strlen(tmpInputBuffer)-1] == '\n') {
            if (config->beginTag == NULL) {
                if (count >= size) {
                    size += _BUFFER_SIZE;
                    data = (RecordList *) realloc(data, size * sizeof(RecordList));
                }
                data[count].record = strdup(tmpInputBuffer);
                memUsed += strlen(data[count].record);
                ++count;
                
                if (memUsed >= config->maxFileSize) {
                    splitKFile(&data, count, config);

                    for (int i = 0; i < count; i++) {
                        free(data[i].record);
                    }
                    free(data);

                    size = _BUFFER_SIZE;
                    data = (RecordList *) malloc(size * sizeof(RecordList));
                    count = memUsed = 0;
                }
            } else {
                bool findRecord = false;
                char *ptr = tmpInputBuffer, *rbPtr;
                if ((rbPtr = strstr(ptr, config->beginTag)) != NULL) {
                    if (strcmp(tmpRecordBeginBuffer, "\0") == 0) {
                        if (maxRecordBufferSize <= strlen(tmpRecordBeginBuffer) + strlen(rbPtr)) {
                            maxRecordBufferSize = strlen(tmpRecordBeginBuffer) + strlen(rbPtr) + _BUFFER_SIZE;
                            tmpRecordBeginBuffer = (char *) realloc(tmpRecordBeginBuffer, maxRecordBufferSize);
                        }
                        strcpy(tmpRecordBeginBuffer, rbPtr);
                    } else {
                        if (count >= size) {
                            size += _BUFFER_SIZE;
                            data = (RecordList *) realloc(data, size * sizeof(RecordList));
                        }
                        data[count].record = strdup(tmpRecordBeginBuffer);
                        memUsed += strlen(data[count].record);
                        ++count;

                        if (memUsed >= config->maxFileSize) {
                            splitKFile(&data, count, config);

                            for (int i = 0; i < count; i++) {
                                free(data[i].record);
                            }
                            free(data);

                            size = _BUFFER_SIZE;
                            data = (RecordList *) malloc(size * sizeof(RecordList));
                            count = memUsed = 0;
                        }

                        free(tmpRecordBeginBuffer);
                        maxRecordBufferSize = _BUFFER_SIZE + (int) strlen(rbPtr);
                        tmpRecordBeginBuffer = (char *) malloc(maxRecordBufferSize * sizeof(char));
                        memset(tmpRecordBeginBuffer, '\0', maxRecordBufferSize);
                        strcpy(tmpRecordBeginBuffer, rbPtr);
                    }
                    findRecord = true;
                }

                if (!findRecord && strcmp(tmpRecordBeginBuffer, "\0") != 0) {
                    if (maxRecordBufferSize <= strlen(tmpRecordBeginBuffer) + strlen(tmpInputBuffer)) {
                        maxRecordBufferSize = strlen(tmpRecordBeginBuffer) + strlen(tmpInputBuffer) + _BUFFER_SIZE;
                        tmpRecordBeginBuffer = (char *) realloc(tmpRecordBeginBuffer, maxRecordBufferSize);
                    }
                    strcat(tmpRecordBeginBuffer, tmpInputBuffer);
                }
            }

            free(tmpInputBuffer);
            maxInputBufferSize = _BUFFER_SIZE;
            tmpInputBuffer = (char *) malloc(maxInputBufferSize * sizeof(char));
            memset(tmpInputBuffer, '\0', maxInputBufferSize);
        }
    }

    // Get last record in -rb case
    if (config->beginTag != NULL && strcmp(tmpRecordBeginBuffer, "\0") != 0) {
        data[count].record = strdup(tmpRecordBeginBuffer);
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
        fprintf(stderr, "Error: record not found\nPlease check -rb option\n");
        exit(2);
    } else {
        mergeKFile(_fileNum, config->chunk, config->thread, config->output, config->isUniquify, config->sort);
    }

    if (config->isCutByDelim) {
        remove(_TERM_FILE);
    }

    if (config->beginTag != NULL) {
        free(config->beginTag);
    }

    if (config->sort->keyTag != NULL) {
        free(config->sort->keyTag);
    }

    if (config->input != NULL) {
        free(config->input);
    }

    if (config->output != NULL) {
        free(config->output);
    }

    free(config);
    return 0;
}

RsortConfig *initRsortConfig(int argc, char *argv[])
{
    if (argc == 1) {
        fprintf(stderr, "Error: missing arguments\n");
        exit(2);
    }

    RsortConfig *config = malloc(sizeof(RsortConfig));
    config->beginTag = config->output = config->input = NULL;
    config->isCutByDelim = config->isUniquify = false;
    config->chunk = 4;
    config->maxFileSize = 500000000;
    config->thread = 4;
    config->sort = initSortConfig();

    for (int i = 0; i < argc; i++) {
        bool flag = false;

        if (strcmp(argv[i], "-rb") == 0) {
            config->beginTag = strdup(argv[i+1]);
            i += 1;
            flag = true;
        } else if (strcmp(argv[i], "-d") == 0) {
            config->isCutByDelim = true;
            flag = true;
        } else if (strstr(argv[i], "-k") != NULL) {
            config->sort->keyTag = strdup(argv[i+1]);
            if (strlen(argv[i]) == 3) {
                config->sort->keyPos = (argv[i][2] == 'r') ? -1 : atoi(argv[i] + 2);
            }
            i += 1;
            flag = true;
        } else if (strcmp(argv[i], "-n") == 0) {
            config->sort->numeric = true;
            flag = true;
        } else if (strcmp(argv[i], "-r") == 0) {
            config->sort->reverse = true;
            flag = true;
        } else if (strcmp(argv[i], "-chunk") == 0) {
            config->chunk = atoi(argv[i+1]);
            i += 1;
            flag = true;
        } else if (strcmp(argv[i], "-s") == 0) {
            config->maxFileSize = atoi(argv[i+1]);
            i += 1;
            flag = true;
        } else if (strcmp(argv[i], "-parallel") == 0) {
            config->thread = atoi(argv[i+1]);
            i += 1;
            flag = true;
        } else if (strcmp(argv[i], "--help") == 0) {
            usage();
        } else if (strcmp(argv[i], "-o") == 0) {
            config->output = strdup(argv[i+1]);
            i += 1;
            flag = true;
        } else if (strcmp(argv[i], "-u") == 0) {
            config->isUniquify = true;
            flag = true;
        }

        if (!flag && i == argc - 1) {
            config->input = strdup(argv[i]);
        }
    }

    if (config->thread < 1) {
        fprintf(stderr, "Error: thread can not smaller than 1\n");
        exit(2);
    }

    if (config->chunk < 2) {
        fprintf(stderr, "Error: chunk can not smaller than 2\n");
        exit(2);
    }

    return config;
}

void splitKFile(RecordList **data, int size, RsortConfig *config)
{
    ++_fileNum;

    int *idx = (int *) malloc(size * sizeof(int));
    for (int i = 0; i < size; i++) {
        idx[i] = i;
    }
    mergeSort(data, &idx, size, config->thread, config->sort);

    char splitFile[31], offsetFile[31];
    sprintf(splitFile, "%s_%d", _SPLIT_FILE, _fileNum);
    sprintf(offsetFile, "%s_%d", _OFFSET_FILE, _fileNum);

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

void usage()
{
    printf("Usage: rsort [OPTION]... [FILE]...\n");
    printf("Sort giving FILE by OPTION.\n");
    printf("Example: ./rsort -rb @GAISRec: text.txt\n\nCommand:\n");
    printf ("\
  -rb               obtain record begin by specific tag\n\
  -d                split giving FILE by delimiters and sort\n\
  -k{num}           sort by nth ocurrence of specific key column\n\
  -kr               sort by last occurrence of specific key column\n\
  -n                sort by numerical comparison\n\
  -r                output result in reverse order\n\
  -chunk            number of external chunks using in merge\n\
  -s                limit file size (byte)\n\
  -parallel         number of threads needed\n\
  -o                output in specific file\n\
  -u                remove duplicate records\n\
  --help            show rsort information\n\n");

  exit(0);
}