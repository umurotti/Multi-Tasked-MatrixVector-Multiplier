all: mv mvp mvt
mv: mv.c
	gcc -Wall -g -o mv mv.c
mvp: mvp.c
	gcc -Wall -g -o mvp mvp.c
mvt: mvt.c
	gcc -Wall -g -o mvt mvt.c -pthread
clean:
	rm -rf mv mv.o mvp mvp.o mvt mvt.o *~
