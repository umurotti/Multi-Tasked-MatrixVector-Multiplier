#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>

struct line {
    int row;
    int column;
    int value;
};

struct IPCprotocol {
    int K;
    int B;
    int *vector;
    struct line **buffers;
    int *buffer_positions;
    sem_t *emptys_array;
    sem_t *fulls_array;
    sem_t *mutex_array;
} common;

struct mapper_thread_arg {
    int i;
    int noOfColumns;
    int noOfRows;
    char *splitFilePath;
};

struct reducer_thread_arg {
    char *resultFilePath;
    int noOfColumns;
    int noOfRows;
    int noOfElements;
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
void readVector(int *arr, const char *vectorfile_path);
//
void createThreads(const int K, const int B,const int noOfElements, const int noOfRows, const int noOfColumns, char *splitfile_path, char *resultfile_path);
void reduceThread(const int K, const int noOfElements, const int noOfRows, const int noOfColumns, char *resultfile_path);
//
void *mapperRunner(void *params);
void *reducerRunner(void *params);
//

int main(int argc, char** argv) {
    int maxRowElement = 0, maxColumnElement = 0;
    int K = -1;
    int B = -1;
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
    //assign B
    if(argv[5] != NULL) {
        B = atoi(argv[5]);
    }
    //find number of elements
    int noOfElements = findNumberOfElements_Matrix(&maxRowElement, &maxColumnElement, matrixFilePath);
    //k-check
    if(K > noOfElements) {
        fprintf(stderr, "K must be less than or eq maximum number of non-zero elements in matrix\nmaximum number of non-zero elements in matrix: %d will be used\n", noOfElements);
        K = noOfElements;
    }
    //b-check
    if(!(100 <= B && B <= 10000)) {
        fprintf(stderr, "B must be between 100 and 10000. Default value: B = 100 is set.\n");
        B = 100;
    }
    //split
    split(K,noOfElements, matrixFilePath, splitFilePath);
    //read vector
    common.vector = malloc(sizeof(int) * maxColumnElement);
    readVector(common.vector, vectorFilePath);
    createThreads(K, B, noOfElements, maxRowElement, maxColumnElement, splitFilePath, resultFilePath);
/*
    reduceThread(K, noOfElements, maxRowElement, maxColumnElement, resultFilePath, vectorFilePath);
*/
    free(common.vector);
    return (EXIT_SUCCESS);
}

void createThreads(const int K, const int B,const int noOfElements, const int noOfRows, const int noOfColumns, char *splitfile_path, char *resultfile_path) {
    common.B = B;
    common.K = K;
    common.buffers = malloc(sizeof(struct line *) * K);
    common.buffer_positions = malloc(sizeof(int) * K);
    common.emptys_array = malloc(sizeof(sem_t) * K);
    common.fulls_array = malloc(sizeof(sem_t) * K);
    common.mutex_array = malloc(sizeof(sem_t) * K);
    
    
    
    pthread_t workers[K];
    struct mapper_thread_arg params[K];
    for(int i = 0; i < K; i++) {
        //initialize semaphores
        sem_init(&common.emptys_array[i], 0, B);
        sem_init(&common.fulls_array[i], 0, 0);
        sem_init(&common.mutex_array[i], 0, 1);
        //initialize buffers
        common.buffers[i] = malloc(sizeof(struct line) * B);
        common.buffer_positions[i] = 0;
        //initialize arguments for mapper thread
        params[i].i = i;
        params[i].noOfColumns = noOfColumns;
        params[i].noOfRows = noOfRows;
        params[i].splitFilePath = splitfile_path;
        pthread_create(&workers[i], NULL, mapperRunner, &params[i]);
    }
    reduceThread(K, noOfElements, noOfRows, noOfColumns, resultfile_path);
    
    for(int i = 0; i < K; i++)
        pthread_join(workers[i], NULL);
    
    //free allocated spaces
    free(common.buffer_positions);
    free(common.emptys_array);
    free(common.fulls_array);
    free(common.mutex_array);
    for(int i = 0; i < K; i++)
        free(common.buffers[i]);
    free(common.buffers);
}

void *mapperRunner(void *params) {
    struct mapper_thread_arg *args = params;
    printf("Thread_args.i: %d\n",  args->i);
    FILE *splitPtr = assignFile(args->i, SPLIT_NAME, args->splitFilePath, "r");
    //read values
    struct line nl;
    fscanf(splitPtr, "%d %d %d*[^\n]", &nl.row, &nl.column, &nl.value);
    while (!feof(splitPtr)) {
        // critical section
        sem_wait(&common.emptys_array[args->i]);
        sem_wait(&common.mutex_array[args->i]);
        //
        printf("positions: %d\n", common.buffer_positions[args->i]);
        common.buffers[args->i][common.buffer_positions[args->i]] = nl;
        
        common.buffer_positions[args->i]++;
        //
        sem_post(&common.mutex_array[args->i]);
        sem_post(&common.fulls_array[args->i]);
        // end of critical section
        fscanf(splitPtr, "%d %d %d*[^\n]", &nl.row, &nl.column, &nl.value);
    }
    //close split file
    fclose(splitPtr);
    // exit the current thread 
    printf("Exiting mapper runner\n");
    pthread_exit(NULL); 
}

void reduceThread(const int K, const int noOfElements, const int noOfRows, const int noOfColumns, char *resultfile_path) {
    pthread_t reducer;
    struct reducer_thread_arg params;
    params.noOfRows = noOfRows;
    params.resultFilePath = resultfile_path;
    params.noOfColumns = noOfColumns;
    params.noOfElements = noOfElements;
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
    
    struct line next_consumed;
    int totalConsumed = 0;
    do {
        int i = 0;
        while (i < common.K) {
            int tmp = 0;
            sem_getvalue(&common.fulls_array[i], &tmp);
            printf("\nfulls: %d\n", tmp);
            sem_getvalue(&common.emptys_array[i], &tmp);
            printf("emptyss: %d\n", tmp);
            sem_getvalue(&common.mutex_array[i], &tmp);
            printf("mutex: %d\n", tmp);
            printf("positions: %d\n", common.buffer_positions[i]);
            if (!sem_trywait(&common.fulls_array[i])) {
                //there is item to consume
                sem_wait(&common.mutex_array[i]);
                ////
                common.buffer_positions[i]--;
                next_consumed = common.buffers[i][common.buffer_positions[i]];                
                ////
                sem_post(&common.mutex_array[i]);
                sem_post(&common.emptys_array[i]);
                totalConsumed++;
                //multiply
                result[next_consumed.row - 1] += next_consumed.value * common.vector[next_consumed.column - 1];
            } else {
                i++;
            }
        }
    } while (totalConsumed != args->noOfElements);
    
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