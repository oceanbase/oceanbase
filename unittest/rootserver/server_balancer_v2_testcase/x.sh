# cp *.tmp to *.result
for f in `ls *.tmp`
do
echo $f
ff=`echo ${f%%.tmp}`
mv $f $ff.result
rm -f $f
done
