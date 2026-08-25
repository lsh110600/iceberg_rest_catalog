# 폐쇄망 배포 가이드

이 프로젝트의 기본 폐쇄망 배포 방식은 **인터넷 연결 구간에서 이미지를 완성하고, 폐쇄망에서는 이미지를 다시 빌드하지 않는 방식**이다. Python 패키지, npm 패키지, 운영체제 패키지는 실행 시 다운로드되지 않는다.

```text
인터넷 연결 빌드 호스트
  └─ source + Docker registry/package registry
       └─ versioned images + manifest + SHA-256
            └─ 승인된 이동 매체/전송 구간
                 └─ 폐쇄망 Docker 호스트
                      └─ docker load -> Compose (pull_policy: never)
```

## 1. 사전 조건

- 연결 구간: Docker Engine/BuildKit, Docker Compose v2, 소스 저장소 및 외부 registry 접근
- 폐쇄망: Docker Engine과 Docker Compose v2가 사전 설치되어 있어야 한다.
- 대상 호스트의 Linux CPU 아키텍처를 확인한다(`amd64` 또는 `arm64`). 이미지 bundle은 OS/아키텍처별로 만든다.
- 폐쇄망 내부의 Iceberg REST Catalog, HDFS, Spark/Trino/Flink endpoint, DNS, NTP, 사내 CA 접근 경로를 방화벽 정책에 반영한다.
- 실제 운영 전 내부 TLS reverse proxy와 사내 OIDC/SSO를 준비한다. Livy Spark executor는 구현되어 있지만 현재 코드의 OIDC/RBAC는 아직 구현 전이므로 승인 경로를 조직 인증 체계로 보강하기 전에는 운영 변경 작업을 활성화하지 않는다.

## 2. 연결 구간에서 bundle 만들기

기본 버전과 빌드 호스트 아키텍처로 생성한다.

```bash
./scripts/build-airgap-bundle.sh 0.1.0
```

다른 Linux 아키텍처와 출력 디렉터리를 지정할 수 있다.

```bash
./scripts/build-airgap-bundle.sh 0.1.0 ./dist amd64
```

기본 bundle은 용량을 줄이기 위해 Playground를 포함하지 않는다. 폐쇄망에서도 로컬 HDFS/Spark 실습 환경이 필요할 때만 다음과 같이 추가한다.

```bash
INCLUDE_PLAYGROUND=true ./scripts/build-airgap-bundle.sh 0.1.0 ./dist amd64
```

결과물:

```text
dist/iceberg-ops-airgap-0.1.0-linux-amd64.tar.gz
dist/iceberg-ops-airgap-0.1.0-linux-amd64.tar.gz.sha256
```

bundle에는 backend/frontend/PostgreSQL 이미지, 폐쇄망 전용 Compose, 설정 예시, 이미지 manifest와 내부 파일 checksum이 포함된다. `INCLUDE_PLAYGROUND=true`이면 Spark/Iceberg runner, 로컬 Livy와 Hadoop 이미지도 포함되고 manifest의 `playground_included`가 `true`가 된다. 빌드 후 조직 표준 도구로 이미지 vulnerability scan, SBOM 생성, 악성코드 검사, artifact 서명을 수행하고 승인된 저장소에 보관한다.

운영 Livy 제출용 `spark-jobs/iceberg_maintenance.py`는 Playground 포함 여부와 관계없이 bundle에 포함된다. 이 파일은 반입 승인 후 Livy/Spark가 접근 가능한 내부 HDFS에 별도로 배포한다.

## 3. 반입과 설치

반입 전에 전송 파일의 SHA-256을 연결 구간에서 기록하고, 폐쇄망에서 별도 전달된 값과 비교한다.

```bash
sha256sum --check iceberg-ops-airgap-0.1.0-linux-amd64.tar.gz.sha256
tar -xzf iceberg-ops-airgap-0.1.0-linux-amd64.tar.gz
cd iceberg-ops-airgap-0.1.0-linux-amd64
./install.sh
```

최초 실행은 내부 checksum과 이미지를 확인·적재한 뒤 `.env`를 만들고 중단한다. `.env`의 `POSTGRES_PASSWORD`, bind 주소, 포트, 공개 origin을 설정한 후 다시 실행한다.

```bash
vi .env
./install.sh
```

Playground 포함 bundle을 사용한다면 `.env`의 `PLAYGROUND_ENABLED=true`로 바꾸고 `PLAYGROUND_RUNNER_TOKEN`에 임의의 긴 값을 설정한 뒤 설치한다. 설치 스크립트가 HDFS/Spark/Livy와 실제 큐를 소비하는 `operation-worker`를 포함한 `playground` Compose profile을 활성화한다. 읽기 전용 SQL은 Spark runner가 직접 수행하고 rewrite 등 프로시저는 로컬 Livy batch로 제출한다. 포함하지 않은 bundle에서 이 값을 켜면 로컬 Hadoop/Spark/Livy 이미지가 없으므로 시작에 실패한다.

운영 Spark 실행은 기존 내부 Livy에 연결한다. 반입된 작업 파일을 클러스터 관리 호스트에서 배포한다.

```bash
hdfs dfs -mkdir -p /apps/iceberg-ops
hdfs dfs -put -f spark-jobs/iceberg_maintenance.py /apps/iceberg-ops/
```

`.env`의 `LIVY_URL`, `LIVY_JOB_FILE`, TLS CA/auth, proxy user, YARN queue와 `LIVY_CONF_JSON`을 내부 환경에 맞게 설정한 후 Livy profile을 시작한다.

```bash
docker compose --env-file .env -f compose.airgap.yaml --profile livy up -d livy-worker
```

Livy는 배포 시 외부에서 다운로드하지 않으며 기존 폐쇄망 서비스로 전제한다. Iceberg runtime JAR, Hadoop/S3 connector와 catalog 설정도 Livy가 사용하는 Spark classpath 또는 승인된 내부 artifact 경로에 미리 준비되어야 한다. Playground와 Livy worker는 같은 heartbeat key를 사용하므로 한 배포에서는 한 profile만 실행한다.

기본 bind 주소는 `127.0.0.1`이다. 사내 reverse proxy가 같은 호스트에서 TLS를 종료하는 구성을 권장한다. 직접 노출해야 한다면 승인된 관리망 IP만 지정한다. PostgreSQL 포트는 호스트에 공개하지 않는다.

상태 확인과 로그 조회:

```bash
docker compose --env-file .env -f compose.airgap.yaml ps
docker compose --env-file .env -f compose.airgap.yaml logs -f backend frontend
```

Playground 상태와 로그는 profile을 지정해 확인한다.

```bash
docker compose --env-file .env -f compose.airgap.yaml --profile playground ps
docker compose --env-file .env -f compose.airgap.yaml --profile playground logs -f operation-worker livy playground-runner spark-master spark-worker
```

## 4. 내부 registry를 사용하는 경우

여러 호스트에 배포한다면 연결 구간 bundle 대신 폐쇄망 내부 registry로 동일한 version tag의 이미지를 배포할 수 있다. 반입 절차에서 이미지를 scan/sign한 후 내부 registry에 push하고 `.env`의 `IMAGE_REPOSITORY`를 `registry.internal.example/iceberg-ops`처럼 바꾼다.

전용 Compose의 `pull_policy: never`는 기본적으로 registry 접근을 막는다. registry에서 최초 배포하려면 운영자가 이미지를 미리 `docker pull`하거나 승인된 별도 배포 단계에서 적재한 뒤 Console을 시작한다. 애플리케이션 시작 과정은 외부 registry에 접근하지 않는다.

## 5. 업그레이드와 롤백

1. 새 버전을 다른 tag로 반입하고 checksum/서명/취약점 정책을 검증한다.
2. PostgreSQL의 일관된 backup과 복원 테스트를 먼저 수행한다.
3. `.env`의 `APP_VERSION`을 바꾸고 `docker compose ... up -d --pull never`를 실행한다.
4. health, migration, 핵심 API와 UI를 검증한다.
5. 애플리케이션 이미지는 이전 tag로 되돌릴 수 있지만 DB migration은 자동 역변환되지 않는다. schema가 변경된 release의 rollback은 사전에 검증한 DB 복원 절차를 따른다.

운영 bundle, 설정 변경 이력, 승인자, checksum, image ID, DB backup ID를 배포 감사 기록으로 남긴다.

## 6. 보안 및 운영 체크리스트

- 기본 비밀번호 사용 금지, `.env` 권한 제한 또는 조직 secret 관리 도구 사용
- 사내 CA가 포함된 별도 이미지 variant와 TLS 인증서 교체 절차 마련
- 사내 OIDC/SSO, RBAC/ABAC, 요청자·승인자 분리 구현 후 변경 작업 활성화
- Catalog/HDFS/engine credential은 사용자 인증과 분리하고 최소 권한·단기 credential 사용
- image tag뿐 아니라 image ID/digest, SBOM, 서명, SHA-256을 release 증적으로 보존
- 운영망 egress allowlist 적용 및 예상하지 않은 DNS/HTTP 호출 모니터링
- 내부 NTP, DNS, 로그 수집, Prometheus, backup target의 장애 시나리오 검증
- HDFS Kerberos keytab 및 사내 CA를 image에 굽지 않고 runtime secret로 제공

## 7. 완전한 오프라인 재빌드가 필요한 경우

기본 bundle은 **오프라인 실행**을 보장하는 배포 산출물이며, 폐쇄망 내부 소스 재빌드까지 포함하지 않는다. 내부에서 재빌드해야 한다면 별도 공급망 bundle이 필요하다.

- digest로 고정한 Python/Node/PostgreSQL base images
- OS/CPU 아키텍처가 일치하는 Python wheelhouse와 hash가 고정된 requirements
- npm lockfile에 대응하는 검증된 npm cache 또는 사내 npm mirror
- BuildKit/buildx 도구, compiler와 OS package mirror
- SBOM, license 목록, provenance/서명 검증 키

wheelhouse와 native dependency는 대상 OS/CPU에 종속될 수 있으므로 아키텍처별로 생성하고 실제 폐쇄망과 같은 환경에서 재현 빌드를 검증한다.

## 8. 공식 참고 자료

- [Docker image save/load](https://docs.docker.com/reference/cli/docker/image/)
- [Docker Compose service `pull_policy`](https://docs.docker.com/reference/compose-file/services/)
- [pip repeatable installs와 wheelhouse](https://pip.pypa.io/en/stable/topics/repeatable-installs/)
- [Apache Livy REST API](https://livy.apache.org/docs/latest/rest-api.html)
- [Apache Livy Getting Started](https://livy.apache.org/get-started/)
