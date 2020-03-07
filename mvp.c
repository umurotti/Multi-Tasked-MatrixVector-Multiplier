/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   main.c
 * Author: umurotti
 *
 * Created on February 23, 2020, 2:32 AM
 */

#include <stdio.h>
#include <stdlib.h>

//open
#include <unistd.h>
#include <fcntl.h>

//wait
#include <sys/wait.h>

//Pipe
#define READ_END 0
#define WRITE_END 1

static const char *SPLIT_NAME = "split";
//static const char *RESULT_NAME = "result";
static char *resultName = "result00";
/*Check for null before assigning maxRowNumber*/
int findNumberOfElements_Matrix(int *maxRowNumber, int *maxColumnNumber, const char *matrixfile_path);
char* stringConcat(const char *str1, const char *str2);
int compareStrings(const char *str1, const char *str2);
char* integerToString(const int num);
void split(const int K, const int noOfElements, const char *matrixfile_path, const char *splitFilePath);
int** createMappersPipe(const int K, const int noOfRows, const int noOfColumns, const char *splitfile_path, const char *vectorfile_path);
FILE* assignFile(const int n, const char *fileName, const char *filePath, const char *arg);
void readVector(int *arr, const char *vectorfile_path);
void reducePipe(const int K, const int noOfRows, int **fileDescriptors, const char *resultfile_path);
/**/

/*
 * 
 */
int main(int argc, char** argv) {
    /*Main Preparation*/
    int maxRowElement = 0, maxColumnElement = 0;
    int K = -1;
    char *matrixFilePath = "/home/umurotti/Documents/CS342/Project1/matrixfile";
    char *vectorFilePath = "/home/umurotti/Documents/CS342/Project1/vectorfile";
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
    //create mappers
    printf("usingPipe\n");

    int **fileDescriptors = createMappersPipe(K, maxRowElement, maxColumnElement, splitFilePath, vectorFilePath);
    reducePipe(K, maxRowElement, fileDescriptors, resultFilePath);
    for(int i = 0; i < K; i++) {
        int *x = fileDescriptors[i];
        free(x);
    }
    free(fileDescriptors);
    return (EXIT_SUCCESS);
}

void reducePipe(const int K, const int noOfRows, int **fileDescriptors, const char *resultfile_path) {
    int pid;

    pid = fork();
    if (pid == 0) {
        //child process
        //TO-DO 
        //create result array
        int result[noOfRows];
        //fill with zeros
        for (int i = 0; i < noOfRows; i++) {
            result[i] = 0;
        }
        //read from pipe
        int rowElement, value;
        
        for (int i = 0; i < K; i++) {
            close(fileDescriptors[i][WRITE_END]);
            FILE *fin = fdopen(fileDescriptors[i][READ_END], "r");
            
            int interSize;
            fscanf(fin, "%d*[^\n]", &interSize);

            printf("intersize: %d\n", interSize);
            int j = 0;
            while (j < interSize) {
                fscanf(fin, "%d %d*[^\n]", &rowElement, &value);
                result[rowElement - 1] += value;
                printf("in for: i = %d %d %d\n", i, rowElement, value);
                j++;
            }
            fclose(fin);

        }
        

        //write to result file        
        char *resultFileFullPath = stringConcat(resultfile_path, resultName);
        FILE *resultPtr = fopen(resultFileFullPath, "w+");
        //freeing tmp
        free(resultFileFullPath);

        for (int i = 0; i < noOfRows; i++) {
            fprintf(resultPtr, "%d %d\n", i + 1, result[i]);
        }
        fclose(resultPtr);


        exit(0);
    }
    
    wait(NULL);
    printf("ReducePipe finished executing\n");
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

int** createMappersPipe(const int K, const int noOfRows, const int noOfColumns, const char *splitfile_path, const char *vectorfile_path) {
    int pid;
    int** fileDestinations = malloc(sizeof(int *) * K);

    for (int i = 0; i < K; i++) {
        //allocate memory for each pipe write-read end
        fileDestinations[i] = malloc(sizeof(int) * 2);
        //create pipe
        if (pipe(fileDestinations[i]) == -1) {
            fprintf(stderr, "Pipe Failed");
            //return 1;
        }
        pid = fork();
        if (pid == 0) {
            //child process
            printf("[son] pid %d from [parent] pid %d\n", getpid(), getppid());

            //TO-DO
            //handle pipe
            close(fileDestinations[i][READ_END]);

            //assign split file
            FILE *splitPtr = assignFile(i, SPLIT_NAME, splitfile_path, "r");

            //read vector
            int vector[noOfColumns];
            readVector(vector, vectorfile_path);

            for (int i = 0; i < noOfColumns; i++) {
                printf("%d: %d\n", getpid(), vector[i]);
            }

            //create partial result array
            int partialResult[noOfRows];
            //fill with zeros
            for (int i = 0; i < noOfRows; i++) {
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
            
            //write to pipe and fill interSizes
            int interSize = 0;
            FILE *fout = fdopen(fileDestinations[i][WRITE_END], "w");
            for (int i = 0; i < noOfRows; i++) {
                if (partialResult[i] != 0) {
                    interSize++;
                }
            }
            printf("intersize: %d\n", interSize);
            //write size of intermediate to first line for each pipe
            fprintf(fout, "%d\n", interSize);
            printf("intersize written\n");
            //write partial results to pipe line by line
            for (int i = 0; i < noOfRows; i++) {
                if (partialResult[i] != 0) {
                    fprintf(fout, "%d %d\n", i + 1, partialResult[i]);
                }
            }
            fclose(fout);
            exit(0);
        }
    }
    for (int i = 0; i < K; i++) {
        wait(NULL);
    }
    printf("MapperPipe finished executing\n");
    return fileDestinations;
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