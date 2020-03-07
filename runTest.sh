#!/bin/bash
make
echo "running tests..."
./test | grep Time
echo "test finished"
make clean
VAR1="test_matrix"
VAR2="test_vector"
for i in {0..3}
do
	rm -rf $VAR1$i $VAR2$i
done
rm -rf "srs"