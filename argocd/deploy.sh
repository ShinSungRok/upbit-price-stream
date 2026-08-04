#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
source k8s/bootstrap-cluster.sh

ARGOCD_VERSION=v3.5.0
ARGOCD_NAMESPACE=argocd

echo "==> 1/5 클러스터/이미지 준비"
bootstrap_cluster

echo "==> 2/5 ArgoCD 이미지 사전 확보 (레지스트리 직접 접근 회피)"
docker pull "quay.io/argoproj/argocd:${ARGOCD_VERSION}"
docker pull ghcr.io/dexidp/dex:v2.45.0
docker pull public.ecr.aws/docker/library/redis:8.2.3-alpine
import_image "quay.io/argoproj/argocd:${ARGOCD_VERSION}"
import_image ghcr.io/dexidp/dex:v2.45.0
import_image public.ecr.aws/docker/library/redis:8.2.3-alpine

echo "==> 3/5 ArgoCD 설치"
kubectl create namespace "$ARGOCD_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
# --server-side avoids the last-applied-configuration annotation kubectl apply
# normally writes, which overflows the 256KB annotation limit on the (large)
# ApplicationSet CRD. The upstream manifest also hardcodes imagePullPolicy:
# Always on every container, which ignores our pre-imported local images and
# always tries (and, on this network, fails) a registry pull — patch it to
# IfNotPresent before applying.
curl -sL "https://raw.githubusercontent.com/argoproj/argo-cd/${ARGOCD_VERSION}/manifests/install.yaml" \
  | sed 's/imagePullPolicy: Always/imagePullPolicy: IfNotPresent/g' \
  > "$TMPDIR/argocd-install.yaml"
kubectl apply --server-side --force-conflicts -n "$ARGOCD_NAMESPACE" -f "$TMPDIR/argocd-install.yaml"
kubectl -n "$ARGOCD_NAMESPACE" wait deployment/argocd-server deployment/argocd-repo-server \
  --for=condition=Available --timeout=300s

echo "==> 4/5 Application 등록 (strimzi-operator, upbit-price-stream)"
kubectl apply -f argocd/applications/

echo "==> 5/5 Sync 대기"
for app in strimzi-operator upbit-price-stream; do
  echo "  ${app} 동기화 대기 중..."
  for i in $(seq 1 40); do
    sync=$(kubectl get application "$app" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.sync.status}' 2>/dev/null || true)
    health=$(kubectl get application "$app" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.health.status}' 2>/dev/null || true)
    echo "    [$i] sync=${sync:-<none>} health=${health:-<none>}"
    if [ "$sync" = "Synced" ] && [ "$health" = "Healthy" ]; then
      break
    fi
    sleep 15
  done
done

echo "==> 완료. 확인:"
echo "  kubectl get application -n ${ARGOCD_NAMESPACE}"
echo "  kubectl -n upbit get pods"
echo "  kubectl -n upbit get hpa"
echo "  kubectl -n ${ARGOCD_NAMESPACE} port-forward svc/argocd-server 18082:443"
