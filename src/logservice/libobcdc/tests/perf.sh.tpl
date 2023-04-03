port=$1
if [ -z $port ]; then
  port=39411
fi;
pid=`cat run/liboblog.pid`

#sudo perf record -e cycles -c 100000000 -p $pid -g -- sleep 20
#sudo perf script -F ip,sym -f > data.viz

#sudo perf record -F 99 -p $pid -g -- sleep 20
#sudo perf script > flame.viz

#pstack $pid > ps.viz

echo http://$(hostname -i):$port/data.viz
echo http://$(hostname -i):$port/flame.viz
echo http://$(hostname -i):$port/ps.viz
sudo python -m SimpleHTTPServer $port
