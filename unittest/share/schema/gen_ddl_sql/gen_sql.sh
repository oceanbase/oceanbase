count=50
for ((i=1;i<=$count;i++))
do
  sed "s/000/$i/" trade10.sql >> trade.sql
done
