#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
source k8s/bootstrap-cluster.sh

RELEASE=upbit
CHART=charts/upbit-price-stream

echo "==> 1/3 클러스터/이미지 준비"
bootstrap_cluster

echo "==> 2/3 Strimzi Cluster Operator 설치"
kubectl -n "$NAMESPACE" apply -f "https://strimzi.io/install/latest?namespace=${NAMESPACE}"
kubectl -n "$NAMESPACE" wait deployment/strimzi-cluster-operator --for=condition=Available --timeout=300s

echo "==> 3/3 Helm 차트 배포 (Kafka + 앱 3개 + HPA)"
helm upgrade --install "$RELEASE" "$CHART" -n "$NAMESPACE" --create-namespace
kubectl -n "$NAMESPACE" wait kafka/upbit-kafka --for=condition=Ready --timeout=300s
kubectl -n "$NAMESPACE" wait --for=condition=Available deployment --all --timeout=300s

echo "==> 완료. 확인:"
echo "  kubectl -n ${NAMESPACE} get pods"
echo "  kubectl -n ${NAMESPACE} get hpa"
echo "  kubectl -n ${NAMESPACE} port-forward svc/api-server 18081:8080"
