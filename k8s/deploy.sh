#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

CLUSTER=upbit
NAMESPACE=upbit
RELEASE=upbit
CHART=charts/upbit-price-stream
GRADLE_IMAGE=eclipse-temurin:21-jdk
STRIMZI_VERSION=1.1.0
STRIMZI_KAFKA_VERSION=4.2.1
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

# k3d's tag-based image import is unreliable on this host (docker's
# containerd-snapshotter storage driver produces multi-platform manifests that
# k3d's importer can't fully resolve — "content digest ... not found"). Saving
# a single-platform tarball first and importing that works around it, and also
# sidesteps this network's TLS-inspecting proxy (host docker trusts its CA,
# the k3d node's containerd does not) since the pull happens on the host.
import_image() {
  local image="$1"
  local tarball="$TMPDIR/$(echo "$image" | tr '/:' '__').tar"
  docker save --platform linux/amd64 "$image" -o "$tarball"
  k3d image import "$tarball" -c "$CLUSTER"
}

echo "==> 1/7 docker-compose 스택 정리 (리소스 확보)"
docker compose down 2>/dev/null || true

echo "==> 2/7 k3d 클러스터 준비"
if ! k3d cluster list | grep -q "^${CLUSTER} "; then
  k3d cluster create "$CLUSTER" --servers 1 --agents 0
else
  echo "k3d cluster '${CLUSTER}' already exists, skipping create"
fi
kubectl config use-context "k3d-${CLUSTER}"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

echo "==> 3/7 Strimzi 이미지 사전 확보 (레지스트리 직접 접근 회피)"
docker pull "quay.io/strimzi/operator:${STRIMZI_VERSION}"
docker pull "quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${STRIMZI_KAFKA_VERSION}"
import_image "quay.io/strimzi/operator:${STRIMZI_VERSION}"
import_image "quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${STRIMZI_KAFKA_VERSION}"

echo "==> 4/7 Strimzi Cluster Operator 설치"
kubectl -n "$NAMESPACE" apply -f "https://strimzi.io/install/latest?namespace=${NAMESPACE}"
kubectl -n "$NAMESPACE" wait deployment/strimzi-cluster-operator --for=condition=Available --timeout=300s

echo "==> 5/7 앱 이미지 빌드 + 클러스터로 import"
docker run --rm --network host \
  -v "$(pwd)":/workspace -w /workspace \
  -v gradle-cache:/root/.gradle \
  "$GRADLE_IMAGE" ./gradlew build
docker compose build
import_image upbit-price-stream-collector:latest
import_image upbit-price-stream-stream-processor:latest
import_image upbit-price-stream-api-server:latest

echo "==> 6/7 Helm 차트 배포 (Kafka + 앱 3개 + HPA)"
helm upgrade --install "$RELEASE" "$CHART" -n "$NAMESPACE" --create-namespace
kubectl -n "$NAMESPACE" wait kafka/upbit-kafka --for=condition=Ready --timeout=300s

echo "==> 7/7 앱 기동 대기"
kubectl -n "$NAMESPACE" wait --for=condition=Available deployment --all --timeout=300s

echo "==> 완료. 확인:"
echo "  kubectl -n ${NAMESPACE} get pods"
echo "  kubectl -n ${NAMESPACE} get hpa"
echo "  kubectl -n ${NAMESPACE} port-forward svc/api-server 18081:8080"
