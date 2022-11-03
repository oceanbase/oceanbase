#! /bin/bash
for f in `ls *.tmp`
do
echo $f
ff=`echo ${f%%.tmp}`
cp $f $ff.result
done
