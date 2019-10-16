/**
 * rsort.c - main driver file for rsort
 * 
 * Author: Frank Yu <frank85515@gmail.com>
 * 
 * (C) Copyright 2019 Frank Yu
 * 
 * External mergesort
 * 1) parse texts by giving OPTION
 * 2) split files in N size (-s OPTION) and sort (internal mergesort)
 * 3) merge M files with K chunks (winner tree)
 */

#include <rsort.h>

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
        if (access("./tools/segmentor/seg", F_OK) != 0) {
            fprintf(stderr, "Please compile tools/segmentor first\n");
            exit(0);
        } else {
            char cmd[300];
            sprintf(cmd, "./tools/segmentor/seg < %s > %s", argv[argc-1], _TERM_FILE);
            system(cmd);
            fin = fopen(_TERM_FILE, "r");
        }
    } else {
        fin = fopen(argv[argc-1], "r");
        if (argc == 1 || fin == NULL) {
            fin = stdin;
        }
    }

    int maxInputBufferSize = _DEFAULT_BUFFER_SIZE;
    int maxRecordBufferSize = _DEFAULT_BUFFER_SIZE;
    char *inputBuffer = (char *) malloc(_DEFAULT_BUFFER_SIZE * sizeof(char));
    char *tmpInputBuffer = (char *) malloc(_DEFAULT_BUFFER_SIZE * sizeof(char));
    strcpy(tmpInputBuffer, "\0");
    char *tmpRecordBeginBuffer;
    tmpRecordBeginBuffer = (char *) malloc(_DEFAULT_BUFFER_SIZE * sizeof(char));
    strcpy(tmpRecordBeginBuffer, "\0");

    // Parser
    int size = _DEFAULT_BUFFER_SIZE;
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
        fprintf(stderr, "Error fgets or file is empty\n");
        exit(0);
    } else {
        mergeKFile(_fileNum, config);
    }

    if (config->isCutByDelim) {
        remove(_TERM_FILE);
    }

    if (config->beginTag != NULL) {
        free(config->beginTag);
    }

    if (config->keyTag != NULL) {
        free(config->keyTag);
    }

    if (config->output != NULL) {
        free(config->output);
    }

    free(config);
    return 0;
}

SortConfig *initSortConfig(int argc, char *argv[])
{
    SortConfig *config = malloc(sizeof(SortConfig));
    config->beginTag = config->keyTag = config->output = NULL;
    config->reverse = config->isCutByDelim = config->numeric = false;
    config->keyPos = 1;
    config->chunk = 4;
    config->maxFileSize = 500000000; // Default approx. 500MB
    config->thread = 4;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "-rb") == 0) {
            config->beginTag = strdup(argv[i+1]);
            i += 1;
        } else if (strcmp(argv[i], "-d") == 0) {
            config->isCutByDelim = true;
        } else if (strstr(argv[i], "-k") != NULL) {
            config->keyTag = strdup(argv[i+1]);
            if (strlen(argv[i]) == 3) {
                config->keyPos = (argv[i][2] == 'r') ? -1 : atoi(argv[i] + 2);
            }
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
        } else if (strcmp(argv[i], "--help") == 0) {
            usage();
        } else if (strcmp(argv[i], "-o") == 0) {
            config->output = strdup(argv[i+1]);
            i += 1;
        }
    }

    if (config->thread < 1) {
        config->thread = 1;
    }

    if (config->chunk < 2) {
        config->chunk = 2;
    }

    return config;
}

/**
 * splitKFile - main function for splitting file in specifice file size
 * @data: pointer to unsorted data
 * @size: total idx of unsorted data
 * @config: sort config
 */
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
  --help            show rsort information\n\n");

  exit(0);
}