#!/bin/bash
kubectl delete -f sleeppods.yaml
while true; do
  kubectl apply -f sleeppods.yaml
  sleep 5
  kubectl delete -f sleeppods.yaml
done

