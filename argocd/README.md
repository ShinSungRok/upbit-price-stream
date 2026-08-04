# ArgoCD GitOps 배포 (Phase 2, Step 3)

[Helm 차트](../charts/upbit-price-stream)로 패키징된 스택을 ArgoCD로 GitOps 배포한다.
git(`main` 브랜치)이 소스오브트루스가 되고, ArgoCD가 클러스터 상태를 git과 자동으로
동기화한다 — `k8s/deploy.sh`(직접 `helm upgrade --install`)와는 별개의 배포 경로다.

## Application 2개

- **`strimzi-operator`**: Strimzi Kafka Operator. 원래는 공식 OCI Helm 차트
  (`oci://quay.io/strimzi-helm/strimzi-kafka-operator`)를 라이브로 가리키려 했으나,
  `argocd-repo-server`가 클러스터 안에서 직접 quay.io에 접속하다가 이 네트워크의
  TLS 검사 프록시에 막히는 것 확인함(이미지 pull과 달리, 실행 중인 파드가 만드는
  런타임 HTTPS 호출이라 이미지 pre-import 방식으로 우회 불가능). GitHub git fetch는
  문제없이 되는 것 확인했으므로, 차트를 [`argocd/vendor/strimzi-kafka-operator`](vendor)에
  받아 커밋해두고 그걸 git 소스로 가리키는 방식으로 우회했다 — 여전히 git만 보고
  동작하는 GitOps이고, 다만 그 git이 upstream이 아니라 우리 레포일 뿐이다.
- **`upbit-price-stream`**: 이 저장소의 `charts/upbit-price-stream`을 `main` 브랜치
  기준으로 배포. `syncPolicy.automated.selfHeal`이 켜져 있어 클러스터를 수동으로
  건드려도 git 상태로 되돌아간다.

두 Application 다 `destination.namespace: upbit`로 맞춰서 Step 1/2와 동일한
네임스페이스를 쓴다. ArgoCD 자체 설치 파일은 (Strimzi와 같은 이유로) 이 저장소에
커밋하지 않는다.

## 실행

```bash
./argocd/deploy.sh
```

`k8s/bootstrap-cluster.sh`로 클러스터/이미지를 준비한 뒤, ArgoCD 이미지를 사전
확보하고(같은 TLS 이슈 회피), ArgoCD를 설치하고, 두 Application을 등록한 뒤
`Synced`+`Healthy`가 될 때까지 대기한다.

## 이 환경에서 만난 이슈 (해결됨, `deploy.sh`에 반영)

- **ApplicationSet CRD가 너무 커서 일반 `kubectl apply -f`가 실패함**
  (`metadata.annotations: Too long: must have at most 262144 bytes`) —
  `kubectl apply`가 쓰는 `last-applied-configuration` annotation이 256KB 제한을
  넘어서 생기는, 큰 CRD에서 흔한 문제. `kubectl apply --server-side`로 우회.
- **ArgoCD 공식 설치 매니페스트가 모든 컨테이너에 `imagePullPolicy: Always`를
  박아놨음** — 로컬로 미리 import해둔 이미지를 무시하고 매번 레지스트리를 다시
  치다가 이 네트워크에서 실패함. 적용 전에 `sed`로 `IfNotPresent`로 바꿔서 우회.
- **Strimzi 오퍼레이터 OCI Helm 차트를 ArgoCD가 라이브로 못 가져옴** — 위
  "Application 2개" 설명 참고, `argocd/vendor/`로 우회.

## 검증

```bash
kubectl get application -n argocd
kubectl -n upbit get pods
kubectl -n upbit get hpa
kubectl -n argocd port-forward svc/argocd-server 18082:443
```

**GitOps가 실제로 동작하는지 확인하는 가장 확실한 방법**: `charts/upbit-price-stream/values.yaml`
아무 값이나 바꿔서 커밋+push한 뒤, 클러스터를 손대지 않고 `kubectl get application
upbit-price-stream -n argocd -o jsonpath='{.status.sync.revision}'`가 새 커밋
해시로, 실제 리소스(`kubectl -n upbit get deploy ...`)도 바뀐 값으로 자동
반영되는지 지켜본다(기본 polling 주기 ~3분). 이 세션에서 `apiServer.resources.requests.memory`를
256Mi→288Mi로 바꿔 실제로 이렇게 동작하는 것을 확인했다.
