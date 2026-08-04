# Kubernetes 배포 (Phase 2)

Strimzi Operator로 Kafka를 K8s 네이티브 리소스(KafkaNodePool + Kafka)로 띄우고,
collector/stream-processor/api-server를 Helm 차트([`charts/upbit-price-stream`](../charts/upbit-price-stream))로
배포한다. 로컬 검증은 [k3d](https://k3d.io/)(Docker 위에서 도는 경량 k3s) 기준.

Strimzi Operator 설치 파일 자체는 이 저장소에 커밋하지 않는다 — 외부 프로젝트가
배포하는 리소스이므로 `deploy.sh`가 공식 설치 URL(`strimzi.io/install/latest`)에서
직접 받아 적용한다. `charts/upbit-price-stream`은 우리가 소유하는 설정(Kafka 클러스터
스펙, 앱 Deployment/HPA/Service)만 담는다.

이 아래는 **직접 `helm upgrade --install`을 실행하는 경로**다. git을 소스오브트루스로
삼아 ArgoCD가 자동으로 동기화하는 GitOps 경로는 [`argocd/README.md`](../argocd/README.md)
참고 — 둘 다 클러스터/이미지 준비 로직(`bootstrap-cluster.sh`)은 공유한다.

## 실행

```bash
./k8s/deploy.sh
```

`docker compose down` → k3d 클러스터 생성 → Strimzi 이미지 사전 확보(아래 참고) →
Strimzi Operator 설치 → 앱 3개 이미지 빌드/import → `helm upgrade --install` → 기동
대기, 순서로 진행한다. 이미 설치된 릴리스에 다시 실행해도 안전하다(idempotent).

## 리소스 관련 메모

- 로컬 데모 목적이라 Kafka는 **ephemeral 스토리지**(PVC 없음, 파드 재시작 시
  토픽 데이터 초기화), 컨트롤러+브로커 겸용 단일 노드로 구성했다.
- **Entity Operator(Topic/User Operator)는 껐다** — 메모리를 아끼기 위함이고,
  대신 브로커 레벨 `auto.create.topics.enable=true`로 토픽을 자동 생성한다
  (docker-compose와 동일 방식).
- Kafka 브로커 JVM 힙은 384Mi로 제한. 리소스가 더 넉넉한 환경이면
  `charts/upbit-price-stream/values.yaml`의 `kafka.resources`/`kafka.jvmHeap`을
  올려도 된다.
- 앱 3개 모두 `autoscaling.enabled: true`(기본 `minReplicas: 1, maxReplicas: 3,
  targetCPUUtilizationPercentage: 70`)로 HPA가 붙어 있다. `stream-processor`는
  토픽이 3파티션이라 최대 3 replica까지 의미가 있다. 실제로 JVM 기동 시 CPU가
  잠깐 튀면서 3개까지 스케일 아웃되는 걸 확인했다 — 데모/기동 초기엔 리소스를
  더 쓴다는 뜻이니, 리소스가 빠듯하면 `values.yaml`에서 꺼도 된다.

## 이 환경에서 만난 이슈 (해결됨, `deploy.sh`에 반영)

- **`k3d image import <image-name>`이 이 환경에서 일반적으로 깨져 있음**
  (`ctr: content digest ...: not found`) — Strimzi 이미지뿐 아니라 `alpine`
  같은 아무 이미지에서도 재현됨. 원인은 호스트 Docker의 containerd-snapshotter
  스토리지 드라이버가 만드는 멀티플랫폼 매니페스트를 k3d importer가 못 읽는 것.
  `docker save --platform linux/amd64 <image> -o x.tar` 후 `k3d image import
  x.tar`로 우회(`deploy.sh`의 `import_image` 함수).
- **사내망 등에서 TLS 검사 프록시를 쓰는 경우, k3d 노드(containerd)가 quay.io
  인증서를 신뢰하지 않아 Strimzi 이미지 pull이 막힐 수 있음** (호스트 Docker는
  정상 pull됨). `deploy.sh`가 Strimzi 이미지를 호스트에서 먼저 `docker pull`한
  뒤 위 tar 방식으로 import해서 이 문제를 피한다.

## 확인

```bash
kubectl -n upbit get pods
kubectl -n upbit get hpa
helm list -n upbit
kubectl -n upbit logs deploy/stream-processor
kubectl -n upbit logs deploy/api-server
kubectl -n upbit port-forward svc/api-server 18081:8080 &
curl -i --http1.1 -N \
  -H "Connection: Upgrade" -H "Upgrade: websocket" \
  -H "Sec-WebSocket-Version: 13" -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
  http://localhost:18081/ws/stream
```

collector는 업비트 라이브 WebSocket에 접속해야 해서, 접속 환경에 따라(사내망 등)
막혀있으면 SSL 에러가 나며 재연결을 반복한다 — docker-compose 로컬 실행 때와
동일한 제약이다.
