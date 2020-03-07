#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <math.h>
#include <unistd.h>
//wait
#include <sys/wait.h>

//global time measurement variables
struct timeval timer_start, timer_end;
double timer_spent;
static const int NO_OF_TESTS = 3;
static const char* MATRIXFILE_NAME = "test_matrix";
static const char* VECTORFILE_NAME = "test_vector";
//function declarations
void myExec(char **arg, const char *func_name, const int size);
void calculateTime(double *timer_spent, struct timeval *timer_start, struct timeval *timer_end, const char *func_name);
FILE* assignFile(const int n, const char *fileName, const char *filePath, const char *arg);
char* stringConcat(const char *str1, const char *str2);
char* integerToString(const int num);
void cleanInter(const int size);
void cleanSplit(const int size);

int main(int argc, char** argv) {


    for (int i = 0; i < NO_OF_TESTS; i++) {
        FILE *matrixPtr = assignFile(i, MATRIXFILE_NAME, "./", "w+");
        FILE *vectorPtr = assignFile(i, VECTORFILE_NAME, "./", "w+");
        int size = pow(10, i + 1);
        //int size = 5;
        for (int row = 1; row < size; row++) {
            for (int column = 1; column < size; column++) {
                fprintf(matrixPtr, "%d %d %d\n", row, column, (row * column) % 100);
            }
            fprintf(vectorPtr, "%d %d\n", row, (row * row) % 100);
        }
        fclose(matrixPtr);
        fclose(vectorPtr);

        char *tmp = integerToString(i);
        char *matrixFile = stringConcat(MATRIXFILE_NAME, tmp);
        char *vectorFile = stringConcat(VECTORFILE_NAME, tmp);
        free(tmp);
        char *argmv[] = {"./mv", matrixFile, vectorFile, "srs", "10", 0};
        myExec(argmv, "mv", size);
        cleanInter(size);
        cleanSplit(size);
        char *argmvp[] = {"./mvp", matrixFile, vectorFile, "srs", "10", 0};
        myExec(argmvp, "mvp", size);
        cleanSplit(size);
        char *argmvt[] = {"./mvt", matrixFile, vectorFile, "srs", "10", 0};
        myExec(argmvt, "mvt", size);
        cleanSplit(size);
    }



    return (EXIT_SUCCESS);
}

void cleanInter(const int size) {
    for (int i = 0; i < 10; i++) {
        char *tmp = integerToString(i);
        char *tmp2 = stringConcat("inter", tmp);
        remove(tmp2);
        free(tmp);
        free(tmp2);
    }
}

void cleanSplit(const int size) {
    for (int i = 0; i < 10; i++) {
        char *tmp = integerToString(i);
        char *tmp2 = stringConcat("split", tmp);
        remove(tmp2);
        free(tmp);
        free(tmp2);
    }
}

void myExec(char **arg, const char *func_name, const int size) {
    gettimeofday(&timer_start, NULL);
    if (!fork()) {
        execvp(arg[0], arg);
    }
    wait(NULL);
    gettimeofday(&timer_end, NULL);
    char *tmp = integerToString(size);
    calculateTime(&timer_spent, &timer_start, &timer_end, stringConcat(func_name, tmp));
    free(tmp);
}

void calculateTime(double *timer_spent, struct timeval *timer_start, struct timeval *timer_end, const char *func_name) {
    *timer_spent = ((*timer_end).tv_sec - (*timer_start).tv_sec) * 1000.0;
    *timer_spent += ((*timer_end).tv_usec - (*timer_start).tv_usec) / 1000.0;
    printf("Time spent %s: %.6f (ms)\n", func_name, *timer_spent);
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
