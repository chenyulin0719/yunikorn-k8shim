#/bin/bash

# bash run_benchmark.sh YUNIKORN-2329
# bash run_benchmark.sh YUNIKORN-2329-KEEP-GO-ROUTINE

echo $1
git -C /home/chenyulin/yunikorn/yunikorn-core checkout $1

REGISTRY=local VERSION=latest make clean sched_image
kind load docker-image local/yunikorn:scheduler-amd64-latest  --name yunikorn

current_branch=$(git -C ~/yunikorn/yunikorn-core branch --show-current)
current_node_count=$(kubectl get nodes --no-headers | wc -l)

benchmark_result_file="benchmark_result_${current_branch}_${current_node_count}.txt"
rm $benchmark_result_file

echo "Current Core branch: ${current_branch}" >> $benchmark_result_file
echo "Current Node count: ${current_node_count}" >> $benchmark_result_file



for i in {1..20}
do
    echo "Loop: $i times, deleting scheduler pod" >> $benchmark_result_file
    kubectl delete pods -l component=yunikorn-scheduler -n yunikorn
    echo "Sleep 90 seconds..." >> $benchmark_result_file
    sleep 90
    kubectl logs $(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n yunikorn | grep ^yunikorn-scheduler) -n yunikorn | grep "nodeUpdateRequestCounter" >> $benchmark_result_file
done

# git -C /home/chenyulin/yunikorn/yunikorn-core YUNIKORN-2329-KEEP-GO-ROUTINE
# git -C /home/chenyulin/yunikorn/yunikorn-core YUNIKORN-2329
