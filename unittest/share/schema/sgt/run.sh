
rm -rf *.sql
for ((i=0; i<$1; i++))
do
echo "looping $i"
sh -x ./schema.sh &
sleep 10
done
wait
echo "all done"

