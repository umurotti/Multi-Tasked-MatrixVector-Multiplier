# Multi-Tasked-MatrixVector-Multiplier
 ---------------------------------------
|	Matrix-Vector Multiplier	|
|---------------------------------------|
|	Name:		Umur		|
|	Surname:	Göğebakan	|
 ---------------------------------------

To obtain executables type make in commandline.
Executables should appear as mv, mvp, mvt and test.

Arguments are passed to program from commandline (not stdin).
</br>
mv < matrixFilePath >* < vectorFilePath >** < resultFileName >*** < K >\*\*\*\*</br>
mvp < matrixFilePath >* < vectorFilePath >** < resultFileName >*** < K >\*\*\*\*</br>
mvt < matrixFilePath >* < vectorFilePath >** < resultFileName >*** < K >*\*\*\*</br>
mvt_s < matrixFilePath >* < vectorFilePath >** < resultFileName >*** < K >**** < B > *****</br>

\*	< matrixFilePath > is path to matrixfile eg. "/home/umurotti/Documents/CS342/Project1/matrixfile"</br>
**	< vectorFilePath > is path to vectorfile eg. "/home/umurotti/Documents/CS342/Project1/vectorfile"</br>
***	< resultFileName > is the name of the file for result to be written.
	It is placed into current running directory with given name. If the file exists, it should overwrite. Otherwise, it 		should create a new one</br>
****	< K > is an integer which specifies the number of splits</br>
*****	< B > is an integer which specifies the fixed-size of each buffer

Example command:*</br>
	./mvt myMatrix myVector result 5</br>
*	Here since the myMatrix and myVector are not full path, they should be under the current running directory.

					TEST
- There is a C program called Test.c to run timimng tests
- This program creates three different sized matrices and vector where n = 10, 100, 1000
- Then each program (mv, mvp and mvt) are tested to do the multiplication and their turnaorund time are printed on commandline
- To run the test program manually, executables should be obtained beforehand (using make command)
- Or a short BASH script is writeen to run tests directly.
- To run runTest.sh give executable permissions (chmod +x runTest.sh)
- Then ./runTest.sh should call make, run tests, print results and clean all related files to test.

!!! For matrix and vector files to be read correctly, last line should be an empty line, ie. there should be an empty line without any value at the end. !!!
