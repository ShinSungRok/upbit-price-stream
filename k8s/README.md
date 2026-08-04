# Kubernetes 배포 (Phase 2, Step 1)

Strimzi Operator로 Kafka를 K8s 네이티브 리소스(KafkaNodePool + Kafka)로 띄우고,
collector/stream-processor/api-server를 Deployment로 배포한다. 로컬 검증은
[k3d](https://k3d.io/)(Docker 위에서 도는 경량 k3s) 기준.

Strimzi Operator 설치 파일 자체는 이 저장소에 커밋하지 않는다 — 외부 프로젝트가
배포하는 리소스이므로 `deploy.sh`가 공식 설치 URL(`strimzi.io/install/latest`)에서
직접 받아 적용한다. `k8s/*.yaml`은 우리가 소유하는 설정(Kafka 클러스터 스펙, 앱
Deployment)만 담는다.

## 실행

```bash
./k8s/deploy.sh
```

`docker compose down` → k3d 클러스터 생성 → Strimzi Operator 설치 → Kafka 클러스터
기동 대기 → 앱 3개 이미지 빌드/import → Deployment 적용, 순서로 진행한다.

## 리소스 관련 메모

- 로컬 데모 목적이라 Kafka는 **ephemeral 스토리지**(PVC 없음, 파드 재시작 시
  토픽 데이터 초기화), 컨트롤러+브로커 겸용 단일 노드로 구성했다.
- **Entity Operator(Topic/User Operator)는 껐다** — 메모리를 아끼기 위함이고,
  대신 브로커 레벨 `auto.create.topics.enable=true`로 토픽을 자동 생성한다
  (docker-compose와 동일 방식).
- Kafka 브로커 JVM 힙은 384Mi로 제한. 리소스가 더 넉넉한 환경이면
  `k8s/kafka-nodepool.yaml`의 `resources`/`jvmOptions`를 올려도 된다.

## 확인

```bash
kubectl -n upbit get pods
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
