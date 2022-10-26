# !/bin/Bash

cd ~/DeepOLA/deepola/wake/examples/tpch_polars
echo > log.txt

# get the log info
for((j=0;j<5;j++))
do
	# clear cache
	sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
	
	# get query time 
	beginTime=`date +%s%N`
	RUST_LOG=info cargo run --release --no-default-features --example main -- query $1 10 \../../resources/tpc-h/data/scale=1/partition=10/$2/ | tee log.txt 
	endTime=`date +%s%N`
	echo query time: $(($endTime-$beginTime))

	# deal with the log document
	num=0 # line
	sumS=0 # second/run
	sumNS=0 # nanosecond/run
	for i in `cat log.txt`
	do
		((num++))
		if [ $(($(($num-3))%14)) = 1 ]
		then
			#echo de s $i
			sumS=$(($sumS-${i:0:${#i}-1}))
		elif [ $(($(($num-5))%14)) = 1 ]
		then
			#echo de ns $i
			sumNS=$(($sumNS-$i))
		elif [ $(($(($num-10))%14)) = 1 ]
		then
			#echo add s $i
			sumS=$(($sumS+${i:0:${#i}-1}))
		elif [ $(($(($num-12))%14)) = 1 ]
		then
			#echo add ns $i
			sumNS=$(($sumNS+$i))
		fi
		#echo $i		
	done
	#echo $sumS + $sumNS
	echo file read time: $((sumS*1000000000+sumNS))
done



