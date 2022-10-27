# !/bin/Bash

cd ~/DeepOLA/deepola/wake/examples/tpch_polars
echo > log.txt

q=("q1" "q14" "qa" "qb" "qc" "qd")
type=("parquet" "tbl")
meansQ=()
stdsQ=()
meansF=()
stdsF=()
index=0

for((m=0;m<6;m++))
do
	for((n=0;n<2;n++))
	do
		# get the log info
		query=()
		fileRead=()
		for((j=0;j<5;j++))
		do
        		# clear cache
        		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

        		# get query time 
        		beginTime=`date +%s%N`
        		RUST_LOG=info cargo run --release --no-default-features --example main -- query ${q[m]} 10 \../../resources/tpc-h/data/scale=1/partition=10/${type[n]}/ | tee log.txt
        		endTime=`date +%s%N`
        		#echo query time: $(($endTime-$beginTime))
			query[j]=$(($endTime-$beginTime))

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
        		#echo file read time: $((sumS*1000000000+sumNS))
			fileRead[j]=$((sumS*1000000000+sumNS))
		done
		# calculate the mean and std
		meanSum=$((${query[0]}+${query[1]}+${query[2]}+${query[3]}+${query[4]}))
		meansQ[index]=$(($meanSum/5))
		num=$(((${query[0]}-${meansQ[index]})*(${query[0]}-${meansQ[index]})+(${query[1]}-${meansQ[index]})*(${query[1]}-${meansQ[index]})+(${query[2]}-${meansQ[index]})*(${query[2]}-${meansQ[index]})+(${query[3]}-${meansQ[index]})*(${query[3]}-${meansQ[index]})+(${query[4]}-${meansQ[index]})*(${query[4]}-${meansQ[index]})))
		num=$(($num/5))
		#echo $num
		stdsQ[index]=$(awk -v x=$num 'BEGIN{printf("%d",sqrt(x))}')
		#echo ${stdsQ[index]}

		meanSum=$((${fileRead[0]}+${fileRead[1]}+${fileRead[2]}+${fileRead[3]}+${fileRead[4]}))
		meansF[index]=$(($meanSum/5))
		num=$(((${fileRead[0]}-${meansF[index]})*(${fileRead[0]}-${meansF[index]})+(${fileRead[1]}-${meansF[index]})*(${fileRead[1]}-${meansF[index]})+(${fileRead[2]}-${meansF[index]})*(${fileRead[2]}-${meansF[index]})+(${fileRead[3]}-${meansF[index]})*(${fileRead[3]}-${meansF[index]})+(${fileRead[4]}-${meansF[index]})*(${fileRead[4]}-${meansF[index]})))
		num=$(($num/5))
		#echo $num
		stdsF[index]=$(awk -v x=$num 'BEGIN{printf("%d",sqrt(x))}')
		#echo ${stdsF[index]}
		((index++))
	done
done

#for((i=0;i<12;i++))
#do
#	echo $i
#	echo ${meansQ[i]}
#	echo ${stdsQ[i]}
#	echo ${meansF[i]}
#	echo ${stdsF[i]}
#	echo ------------------
#done

for((i=0;i<12;i++))
do
	len=${#meansF[i]}
	if [ $len -lt 9 ]
	then
		num0=$((9-${#meansF[i]}))
		for((j=0;j<$num0;j++))
		do
			meansF[i]=0${meansF[i]}
		done
		meansF[i]=0.${meansF[i]}
		meansF[i]=${meansF[i]:0:5}
	elif [ $len -eq 9 ]
	then
		meansF[i]=0.${meansF[i]}
		meansF[i]=${meansF[i]:0:5}
	else
		len1=$((${#meansF[i]}-9))
		meansF[i]=${meansF[i]:0:$len1}.${meansF[i]:$len1:3}
	fi

	len=${#meansQ[i]}
	if [ $len -lt 9 ]
	then
		num0=$((9-${#meansQ[i]}))
		for((j=0;j<$num0;j++))
		do
			meansQ[i]=0${meansQ[i]}
		done
		meansQ[i]=0.${meansQ[i]}
		meansQ[i]=${meansQ[i]:0:5}
	elif [ $len -eq 9 ]
	then
		meansQ[i]=0.${meansQ[i]}
		meansQ[i]=${meansQ[i]:0:5}
	else
		len1=$((${#meansQ[i]}-9))
		meansQ[i]=${meansQ[i]:0:$len1}.${meansQ[i]:$len1:3}
	fi

	len=${#stdsQ[i]}
	if [ $len -lt 9 ]
	then
		num0=$((9-${#stdsQ[i]}))
		for((j=0;j<$num0;j++))
		do
			stdsQ[i]=0${stdsQ[i]}
		done
		stdsQ[i]=0.${stdsQ[i]}
		stdsQ[i]=${stdsQ[i]:0:5}
	elif [ $len -eq 9 ]
	then
		stdsQ[i]=0.${stdsQ[i]}
		stdsQ[i]=${stdsQ[i]:0:5}
	else
		len1=$((${#stdsQ[i]}-9))
		stdsQ[i]=${stdsQ[i]:0:$len1}.${stdsQ[i]:$len1:3}
	fi

	len=${#stdsF[i]}
	if [ $len -lt 9 ]
	then
		num0=$((9-${#stdsF[i]}))
		for((j=0;j<$num0;j++))
		do
			stdsF[i]=0${stdsF[i]}
		done
		stdsF[i]=0.${stdsF[i]}
		stdsF[i]=${stdsF[i]:0:5}
	elif [ $len -eq 9 ]
	then
		stdsF[i]=0.${stdsF[i]}
		stdsF[i]=${stdsF[i]:0:5}
	else
		len1=$((${#stdsF[i]}-9))
		stdsF[i]=${stdsF[i]:0:$len1}.${stdsF[i]:$len1:3}
	fi

done

echo ------------------------------------------------------------------------------------------------------------------------------------------
echo "|          | Parquet Format Query Latency | Parquet Format File Load Duration | TBL Format Query Latency | TBL Format File Load Duration |"
echo ------------------------------------------------------------------------------------------------------------------------------------------
echo "|   q1     |        ${meansQ[0]} ± ${stdsQ[0]}         |           ${meansF[0]} ± ${stdsF[0]}           |       ${meansQ[1]} ± ${stdsQ[1]}      |         ${meansF[1]} ± ${stdsF[1]}         |"
echo ------------------------------------------------------------------------------------------------------------------------------------------
echo "|   q14    |        ${meansQ[2]} ± ${stdsQ[2]}         |           ${meansF[2]} ± ${stdsF[2]}           |       ${meansQ[3]} ± ${stdsQ[3]}      |         ${meansF[3]} ± ${stdsF[3]}         |"
echo ------------------------------------------------------------------------------------------------------------------------------------------
echo "|   qa     |        ${meansQ[4]} ± ${stdsQ[4]}         |           ${meansF[4]} ± ${stdsF[4]}           |       ${meansQ[5]} ± ${stdsQ[5]}      |         ${meansF[5]} ± ${stdsF[5]}         |"
echo ------------------------------------------------------------------------------------------------------------------------------------------
echo "|   qb     |        ${meansQ[6]} ± ${stdsQ[6]}         |           ${meansF[6]} ± ${stdsF[6]}           |       ${meansQ[7]} ± ${stdsQ[7]}      |         ${meansF[7]} ± ${stdsF[7]}         |"
echo ------------------------------------------------------------------------------------------------------------------------------------------
echo "|   qc     |        ${meansQ[8]} ± ${stdsQ[8]}         |           ${meansF[8]} ± ${stdsF[8]}           |       ${meansQ[9]} ± ${stdsQ[9]}      |         ${meansF[9]} ± ${stdsF[9]}         |"
echo ------------------------------------------------------------------------------------------------------------------------------------------
echo "|   qd     |        ${meansQ[10]} ± ${stdsQ[10]}         |           ${meansF[10]} ± ${stdsF[10]}           |       ${meansQ[11]} ± ${stdsQ[11]}      |         ${meansF[11]} ± ${stdsF[11]}         |"
echo ------------------------------------------------------------------------------------------------------------------------------------------
