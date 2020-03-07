/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   main.c
 * Author: umurotti
 *
 * Created on March 5, 2020, 12:40 AM
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
/*
 * 
 */
struct tuple {
    int column;
    int value;
};

struct IPCprotocol {
    int K;
    struct tuple **inters;
    int *interSizes;
} common;

struct mapper_thread_arg {
    int i;
    int noOfColumns;
    int noOfRows;
    char *splitFilePath;
    char *vectorFilePath;
};

struct reducer_thread_arg {
    char *resultFilePath;
    int noOfRows;
};

static const char *SPLIT_NAME = "split";
//static const char *RESULT_NAME = "result";
static char *resultName = "result00";
/*Check for null before assigning maxRowNumber*/
int findNumberOfElements_Matrix(int *maxRowNumber, int *maxColumnNumber, const char *matrixfile_path);
char* stringConcat(const char *str1, const char *str2);
int compareStrings(const char *str1, const char *str2);
char* integerToString(const int num);
void split(const int K, const int noOfElements, const char *matrixfile_path, const char *splitFilePath);
FILE* assignFile(const int n, const char *fileName, const char *filePath, const char *arg);
//
void createMappersThread(const int K, const int noOfRows, const int noOfColumns, char *splitfile_path, char *vectorfile_path);
void reduceThread(const int K, const int noOfRows, char *resultfile_path);
//
void *mapperRunner(void *params);
void *reducerRunner(void *params);
//
void readVector(int *arr, const char *vectorfile_path);

int main(int argc, char** argv) {
    int maxRowElement = 0, maxColumnElement = 0;
    int K = -1;
    //char *matrixFilePath = "/home/umurotti/Documents/CS342/Project1/matrixfile";
    char *matrixFilePath = "./matrixfile";
    //char *vectorFilePath = "/home/umurotti/Documents/CS342/Project1/vectorfile";
    char *vectorFilePath = "./vectorfile";
    //char *splitFilePath = "/home/umurotti/Documents/CS342/Project1/";
    char *splitFilePath = "./";
    //char *resultFilePath = "/home/umurotti/Documents/CS342/Project1/";
    char *resultFilePath = "./";
    //assign matrixfile path
    if(argv[1] != NULL) {
        matrixFilePath = argv[1];
    }
    //assign vrctorfile path
    if(argv[2] != NULL) {
        vectorFilePath = argv[2];
    }
    //assign resultfile path
    if(argv[3] != NULL) {
        resultName = argv[3];
    }
    //assign K
    if(argv[4] != NULL) {
        K = atoi(argv[4]);
    }
    //find number of elements
    int noOfElements = findNumberOfElements_Matrix(&maxRowElement, &maxColumnElement, matrixFilePath);
    //k-check
    if(K > noOfElements) {
        fprintf(stderr, "K must be less than or eq maximum number of non-zero elements in matrix\nmaximum number of non-zero elements in matrix: %d will be used\n", noOfElements);
        K = noOfElements;
    }
    //split
    split(K, noOfElements, matrixFilePath, splitFilePath);
    createMappersThread(K, maxRowElement, maxColumnElement, splitFilePath, vectorFilePath);
    
    for(int i = 0; i < K; i++) {
        printf("inter: %d\n", i);
        for(int j = 0; j < common.interSizes[i]; j++) {
            printf("column:%d\tvalue:%d\n", common.inters[i][j].column, common.inters[i][j].value);
        }
    }
    
    reduceThread(K, maxRowElement, resultFilePath);
    
    //free allocated memory locations
    for(int i = 0; i < common.K; i++) {
        free(common.inters[i]);
    }
    free(common.inters);
    free(common.interSizes);
    
    return (EXIT_SUCCESS);
}

void createMappersThread(const int K, const int noOfRows, const int noOfColumns, char *splitfile_path, char *vectorfile_path) {
    common.K = K;
    common.interSizes = malloc(sizeof(int *) * K);
    common.inters = malloc(sizeof(struct tuple*) * K);
    

    
    pthread_t workers[K];
    struct mapper_thread_arg params[K];
    for(int i = 0; i < K; i++) {
        params[i].i = i;
        params[i].noOfColumns = noOfColumns;
        params[i].noOfRows = noOfRows;
        params[i].splitFilePath = splitfile_path;
        params[i].vectorFilePath = vectorfile_path;
        pthread_create(&workers[i], NULL, mapperRunner, &params[i]);
/*
        printf("id:%d\n", workers[i]);
*/
        
    }
    for(int i = 0; i < K; i++)
        pthread_join(workers[i], NULL);

}

void reduceThread(const int K, const int noOfRows, char *resultfile_path) {
    pthread_t reducer;
    struct reducer_thread_arg params;
    params.noOfRows = noOfRows;
    params.resultFilePath = resultfile_path;
    pthread_create(&reducer, NULL, reducerRunner, &params);
    pthread_join(reducer, NULL);
    
}

void *reducerRunner(void *params) {
    struct reducer_thread_arg *args = params;
    int result[args->noOfRows];
    //fill with zeros
    for (int i = 0; i < args->noOfRows; i++) {
        result[i] = 0;
    }
    //use from common to read values
    for(int i = 0; i < common.K; i++) {
        for(int j = 0; j < common.interSizes[i]; j++) {
            result[common.inters[i][j].column - 1] += common.inters[i][j].value;
        }
    }

    //write to result file        
    char *resultFileFullPath = stringConcat(args->resultFilePath, resultName);
    FILE *resultPtr = fopen(resultFileFullPath, "w+");
    printf("result file path: %s\n", resultFileFullPath);
    //freeing tmp
    free(resultFileFullPath);

    for (int i = 0; i < args->noOfRows; i++) {
        fprintf(resultPtr, "%d %d\n", i + 1, result[i]);
    }
    fclose(resultPtr);
    // exit the current thread 
    printf("Exiting reduce runner\n");
    pthread_exit(NULL); 
}

void *mapperRunner(void *params) {
    //sleep(10);
    struct mapper_thread_arg *args = params;
    printf("Thread_args.i: %d\n",  args->i);
    //assign split file
    FILE *splitPtr = assignFile(args->i, SPLIT_NAME, args->splitFilePath, "r");
    //read vector
    int vector[args->noOfColumns];
    readVector(vector, args->vectorFilePath);

    //create partial result array
    int partialResult[args->noOfRows];
    //fill with zeros
    for (int i = 0; i < args->noOfRows; i++) {
        partialResult[i] = 0;
    }
    //read values
    int rowElement, columnElement, value;
    fscanf(splitPtr, "%d %d %d*[^\n]", &rowElement, &columnElement, &value);
    //multiply
    while (!feof(splitPtr)) {
        partialResult[rowElement - 1] += value * vector[columnElement - 1];
        fscanf(splitPtr, "%d %d %d*[^\n]", &rowElement, &columnElement, &value);
    }
    //close split file
    fclose(splitPtr);
    //write to common structure
    int interSize = 0;
    for (int i = 0; i < args->noOfRows; i++) {
        if (partialResult[i] != 0) {
            interSize++;
        }
    }
    common.interSizes[args->i] = interSize;
    struct tuple *tmp = malloc(sizeof(struct tuple) * interSize);
    common.inters[args->i] = tmp;
    
    //write partial results to common inters 2D array
    int count = 0;
    for (int i = 0; i < args->noOfRows; i++) {
        if (partialResult[i] != 0) {
            //fprintf(fout, "%d %d\n", i + 1, partialResult[i]);
            tmp[count].column = i + 1;
            tmp[count].value = partialResult[i];
            count++;
        }
    }
    // exit the current thread 
    printf("Exiting mapper runner\n");
    pthread_exit(NULL); 
}

/**
 * 
 * @param arr
 * @param vectorfile_path
 */
void readVector(int *arr, const char *vectorfile_path) {
    int rowIndex, value;
    FILE *fptr;
    fptr = fopen(vectorfile_path, "r");

    fscanf(fptr, "%d %d%*[^\n]", &rowIndex, &value);
    while (!feof(fptr)) {
        arr[rowIndex - 1] = value;
        fscanf(fptr, "%d %d%*[^\n]", &rowIndex, &value);
    }
    fclose(fptr);
}

FILE* assignFile(const int n, const char *fileName, const char *filePath, const char *arg) {
    char *tmp = integerToString(n);
    char *splitFileName = stringConcat(fileName, tmp);
    char *splitFileFullPath = stringConcat(filePath, splitFileName);

    FILE *result = fopen(splitFileFullPath, arg);

    free(tmp);
    free(splitFileName);
    free(splitFileFullPath);

    return result;
}

/**
 * 
 * @param K number of resulting split files
 * @param noOfElements number of elements in the matrix
 * @param matrixfile_path path to matrix file
 * @param splitFilePath path to split files
 */
void split(const int K, const int noOfElements, const char *matrixfile_path, const char *splitFilePath) {
    //int s = (noOfElements + K - 1) / K;
    int s = noOfElements / K;
    int countFile = 0;
    printf("split s=%d\n", s);

    FILE *matrixPtr;
    FILE *splitPtr;
    matrixPtr = fopen(matrixfile_path, "r");
    splitPtr = assignFile(countFile, SPLIT_NAME, splitFilePath, "w+");

    int row, column, value = 0;

    fscanf(matrixPtr, "%d%d%d", &row, &column, &value);

    int count = 1;

    while (!feof(matrixPtr)) {
        printf("row:%d\tcolumn:%d\tvalue:%d\n", row, column, value);
        fprintf(splitPtr, "%d %d %d\n", row, column, value);
        fscanf(matrixPtr, "%d%d%d", &row, &column, &value);

        if (count == s && countFile < K - 1) {
            countFile++;
            fclose(splitPtr);
            splitPtr = assignFile(countFile, SPLIT_NAME, splitFilePath, "w+");
            count = 0;
        }
        count++;
    }

    fclose(splitPtr);
    fclose(matrixPtr);
}

/**
 * 
 * @param maxRowNumber changes the value of pointed integer to number of rows
 * @param matrixfile_path path to matrixfile. Fİle must end with \n
 * @return number of nonzero elements in matrix
 */
int findNumberOfElements_Matrix(int *maxRowNumber, int *maxColumnNumber, const char *matrixfile_path) {
    int rowNo = 0;
    int maxRowElement = 0;
    int maxColumnElement = 0;

    FILE * fptr;
    fptr = fopen(matrixfile_path, "r");

    fscanf(fptr, "%d %d%*[^\n]", &maxRowElement, &maxColumnElement);
    int tmp1 = maxRowElement;
    int tmp2 = maxColumnElement;
    while (!feof(fptr)) {
        rowNo++;
        fscanf(fptr, "%d %d%*[^\n]", &tmp1, &tmp2);
        if (tmp1 > maxRowElement)
            maxRowElement = tmp1;
        if (tmp2 > maxColumnElement)
            maxColumnElement = tmp2;
    }
    printf("rowNo: %d\n", rowNo);
    printf("maxRowElement: %d\tmaxColumnElement: %d\n", maxRowElement, maxColumnElement);

    if (maxRowNumber != NULL)
        *maxRowNumber = maxRowElement;

    if (maxColumnNumber != NULL)
        *maxColumnNumber = maxColumnElement;

    fclose(fptr);
    return rowNo;

}

/**
 * 
 * @param str1
 * @param str2
 * @return 
 */
char* stringConcat(const char *str1, const char *str2) {
    int str1Size = 0, str2Size = 0;

    while (str1[str1Size] != '\0') {
        str1Size++;
    }

    while (str2[str2Size] != '\0') {
        str2Size++;
    }

    char *result = (char*) malloc(str1Size + str2Size + 1);

    int i = 0;
    for (i = 0; i < str1Size; i++) {
        result[i] = str1[i];
    }
    for (i = 0; i < str2Size; i++) {
        result[i + str1Size] = str2[i];
    }

    result[str1Size + str2Size] = '\0';

    return result;

}

/**
 * 
 * @param num
 * @return 
 */
char* integerToString(const int num) {
    size_t needed = snprintf(NULL, 0, "%d", num);
    char *numberArray = malloc(needed + 1);

    snprintf(numberArray, needed + 1, "%d", num);
    return numberArray;
}

/**
 * 
 * @param str1
 * @param str2
 * @return 
 */
int compareStrings(const char *str1, const char *str2) {
    int i = 0;
    while (str1[i] != '\0') {
        if (str1[i] != str2[i])
            return 0;
        i++;
    }
    return 1;
}