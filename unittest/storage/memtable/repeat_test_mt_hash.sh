
c=0
echo "" > fail_page.log
while true;
do
  echo "=============== $c ======"
  echo "=============== $c ======"
  echo "=============== $c ======"
  rm mt_hash.log -f
  echo "=============== $c ======" >> run.log
  echo "=============== $c ======" >> run.log
  echo "=============== $c ======" >> run.log
  ./test_mt_hash >> run.log
  if [ $? -ne 0 ];
  then
    exit
  fi
  let c++
  tail -n 5000000 mt_hash.log | grep fail >> fail_page.log
done
