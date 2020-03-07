all: mv mvp mvt test
mv: mv.c
	gcc -Wall -g -o mv mv.c
mvp: mvp.c
	gcc -Wall -g -o mvp mvp.c
mvt: mvt.c
	gcc -Wall -g -o mvt mvt.c -pthread
test: test.c
	gcc -Wall -g -o test test.c -lm
clean:
	rm -rf mv mv.o mvp mvp.o mvt mvt.o test test.o *~
