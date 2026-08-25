# Iceberg Ops Console

HDFS의 Apache Iceberg 테이블을 관측하고 Spark, Trino, Flink 유지보수 작업을 요청하기 위한 운영 Control Plane MVP입니다. 향후 S3 storage profile과 HDFS→S3 migration workflow를 확장할 수 있는 데이터 모델을 포함합니다.

## 현재 구현 범위

- 테이블 운영 현황 대시보드와 table detail
- HDFS/S3 storage profile 조회
- Spark/Trino/Flink engine capability 조회
- Iceberg `ScanReport`/`CommitReport` REST endpoint 수신과 민감 필드 제거
- 유지보수 작업 요청, 위험도 판정, 승인/반려, 감사 이벤트
- PostgreSQL + Alembic migration
- FastAPI OpenAPI 문서
- Next.js 웹 UI
- Docker Compose 실행
- 로컬 HDFS + Spark + Iceberg 샘플 테이블 Playground
- Apache Livy batch 기반 운영 Spark 작업 제출·상태 추적·로그 수집
- 버전 고정 이미지 bundle 기반 폐쇄망 배포

Playground와 운영 catalog의 승인된 프로시저 작업은 Livy `/batches` adapter로 제출합니다. Playground의 단순 SQL과 Iceberg metadata 조회만 상시 Spark runner에서 직접 실행합니다. Trino/Flink 실행 adapter는 아직 구현 전입니다.

화면별 단일/Bulk 사용 절차와 결과 해석은 [사용 가이드](GUIDE.md), 폐쇄망 설치 절차는 [폐쇄망 배포 가이드](AIR_GAPPED.md)를 참고합니다.

## 빠른 시작

필요 조건은 Docker와 Docker Compose뿐입니다.

```bash
cp .env.example .env
docker compose up --build -d
```

- Web: <http://localhost:3000>
- FastAPI Swagger: <http://localhost:8088/docs>
- API health: <http://localhost:8088/healthz>
- Prometheus metrics: <http://localhost:8088/metrics>

종료:

```bash
docker compose down
```

## Playground

Playground는 로컬 학습·통합 테스트용 Compose profile이다. Hadoop 3.5.0 NameNode/DataNode, Spark 3.5.9 standalone cluster, Iceberg 1.11.0 runner와 Apache Livy 0.9.0을 시작한다.

```bash
make playground-up
```

첫 실행은 Hadoop/Spark 이미지 다운로드 때문에 시간이 걸릴 수 있다. 준비되면 <http://localhost:3000/playground>에서 HDFS Hadoop Catalog의 이름과 warehouse URI를 등록하고, 자주 사용하는 Namespace를 Catalog별로 등록한다. 이후 등록 값을 선택하고 `table_name`을 입력하면 기존 HDFS Iceberg 테이블의 정의와 `partitions`·`snapshots`·`files` metadata table에서 파티션, snapshot, 파일 메트릭을 조회할 수 있다. Bulk 모드에서는 최대 200개 table 이름을 DB 기반 비동기 Job으로 처리하고 진행률, 공통 메트릭 비교표, 선택 테이블 상세 패널, 테이블별 실패 목록을 표시한다. Catalog와 Namespace, Bulk Job 결과는 PostgreSQL에 유지된다. SQL 입력은 `SELECT`, `SHOW`, `DESCRIBE`, `EXPLAIN`, 읽기 전용 `WITH`만 허용하고 결과는 최대 200행으로 제한한다.

Warehouse URI의 HDFS host 또는 nameservice는 `playground-runner` 컨테이너에서 접근 가능해야 한다. 같은 Catalog 이름을 다른 warehouse URI로 다시 등록하는 요청은 기존 연결이 조용히 바뀌지 않도록 거부한다.

테스트 테이블이 필요한 경우 **Sample table 생성**을 누르면 `playground.demo.orders`에 8개 주문과 2개 Iceberg snapshot이 생성된다. 기본 `playground` Catalog와 `demo` Namespace는 자동 등록된다.

<http://localhost:3000/operations>에서도 HDFS Catalog와 Namespace를 등록할 수 있다. Bulk 작업은 실행 전에 영향 범위와 위험도를 계산하고 테이블별 독립 승인 요청을 생성한다. 테이블 실행 잠금과 idempotency key가 동시·중복 실행을 차단하고, Livy batch 취소와 worker 재시작 시 기존 batch 재연결을 지원한다. 작업 그룹 진행률, 그룹 취소/실패 재시도, cursor pagination과 SSE 상태 갱신을 UI에서 제공한다.

현재 로컬 adapter가 실행하는 command:

- Compute table statistics
- Rewrite data files/manifests/position deletes
- Expire snapshots
- Remove orphan files (`dryRun=true` 기본, 최소 72시간 보존)
- Rollback to snapshot

요청 시 기록한 snapshot과 실행 직전 snapshot이 다르면 작업을 실패 처리한다. 기존 production demo record는 관측 화면 예시일 뿐 실제 HDFS 테이블이 아니므로 로컬 worker가 소비하지 않는다.

- Playground: <http://localhost:3000/playground>
- HDFS NameNode UI: <http://localhost:9870>
- Spark master UI: <http://localhost:19090>
- Spark worker UI: <http://localhost:19091>
- Livy REST API: <http://localhost:18998>

상태 확인과 Playground 서비스 중지:

```bash
make playground-status
make playground-down
```

HDFS 데이터는 `playground_hdfs_namenode`, `playground_hdfs_datanode` named volume에 유지된다. `make reset`은 PostgreSQL을 포함한 모든 named volume을 삭제하므로 테스트 데이터 보존이 필요하면 사용하지 않는다.

## 운영 Spark / Apache Livy

운영 모드는 backend가 SparkContext를 만들지 않고 기존 Livy 서비스의 batch REST API에 PySpark 작업을 제출한다. 제출 후 batch ID와 애플리케이션 ID를 실행 이력에 기록하고, terminal state까지 polling한 다음 Livy log에서 구조화된 실행 결과를 회수해 snapshot 전후 상태를 검증한다.

먼저 bundle에 포함된 작업 파일을 Livy와 Spark driver가 읽을 수 있는 HDFS 경로에 배포한다.

```bash
hdfs dfs -mkdir -p /apps/iceberg-ops
hdfs dfs -put -f spark-jobs/iceberg_maintenance.py /apps/iceberg-ops/
```

`.env`에서 `LIVY_URL`, 인증/TLS, queue, resource와 catalog Spark conf를 설정한다. 다음은 REST Catalog 예시다.

```dotenv
SPARK_ENGINE_NAME=spark-production
LIVY_URL=https://livy.internal.example:8998
LIVY_JOB_FILE=hdfs:///apps/iceberg-ops/iceberg_maintenance.py
LIVY_PROXY_USER=iceberg-ops
LIVY_QUEUE=iceberg-maintenance
LIVY_CONF_JSON={"spark.sql.extensions":"org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions","spark.sql.catalog.production":"org.apache.iceberg.spark.SparkCatalog","spark.sql.catalog.production.type":"rest","spark.sql.catalog.production.uri":"https://catalog.internal.example:8181"}
```

Livy worker만 시작한다.

```bash
docker compose --profile livy up -d --build livy-worker
```

현재 UI의 worker heartbeat는 단일 Spark worker를 전제로 하므로 동일 배포에서 `operation-worker`와 `livy-worker`를 동시에 실행하지 않는다. Playground 배포는 `playground`, 운영 배포는 `livy` profile을 선택한다.

DB volume까지 초기화:

```bash
docker compose down -v
```

## 서비스 구성

```text
browser -> Next.js :3000 -> FastAPI :8088 -> PostgreSQL :5432
                              |
                              ├─ read-only SQL/metadata -> Playground runner -> Spark
                              └─ operation queue -> worker -> Livy batch -> Spark procedure

Spark -> Iceberg HadoopCatalog -> HDFS
```

Docker 기본값은 `DEMO_DATA=true`이며 HDFS 테이블 4개와 engine/storage 예시를 넣습니다. 실제 환경에서는 `.env`에서 `DEMO_DATA=false`로 변경합니다.

## 주요 API

```text
GET  /ops/api/v1/dashboard
GET  /ops/api/v1/tables
GET  /ops/api/v1/tables/{table_id}
GET  /ops/api/v1/tables/{table_id}/metrics
GET  /ops/api/v1/engines
GET  /ops/api/v1/storage-profiles
GET  /ops/api/v1/operation-requests
GET  /ops/api/v1/operation-requests/page
GET  /ops/api/v1/operation-stream
POST /ops/api/v1/operation-plans
POST /ops/api/v1/operation-requests
POST /ops/api/v1/operation-requests/bulk
POST /ops/api/v1/operation-requests/{request_id}/approvals
GET  /ops/api/v1/operation-requests/{request_id}
POST /ops/api/v1/operation-requests/{request_id}/retry
POST /ops/api/v1/operation-requests/{request_id}/cancel
GET  /ops/api/v1/bulk-jobs
GET  /ops/api/v1/bulk-jobs/{job_id}
POST /ops/api/v1/bulk-jobs/{job_id}/cancel
POST /ops/api/v1/bulk-jobs/{job_id}/retry-failed
GET  /ops/api/v1/workers/spark

GET  /ops/api/v1/playground/status
GET  /ops/api/v1/playground/catalogs
POST /ops/api/v1/playground/catalogs
POST /ops/api/v1/playground/namespaces
POST /ops/api/v1/playground/tables
POST /ops/api/v1/playground/tables/bulk
POST /ops/api/v1/playground/tables/bulk-jobs
POST /ops/api/v1/playground/bootstrap
POST /ops/api/v1/playground/query

POST /v1/namespaces/{namespace}/tables/{table}/metrics
POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics
```

## 개발 테스트

```bash
make test
```

이 명령은 production 이미지와 분리된 Docker `test` stage에서 Ruff/pytest 의존성을 사용합니다.

전체 설계와 향후 migration workflow는 [DESIGN.md](./DESIGN.md)를 참고하세요.

## 폐쇄망 배포

인터넷 연결 구간에서 backend/frontend/PostgreSQL 이미지를 하나의 checksum 포함 bundle로 만들고, 폐쇄망에서는 `docker load` 후 로컬 이미지만 실행합니다. 전용 Compose에는 `pull_policy: never`가 적용되며 DB 포트는 호스트에 공개하지 않습니다.

```bash
make airgap-validate
./scripts/build-airgap-bundle.sh 0.1.0 ./dist amd64
```

Playground까지 반입할 때만 대용량 Hadoop/Spark 이미지를 선택적으로 포함한다.

```bash
INCLUDE_PLAYGROUND=true ./scripts/build-airgap-bundle.sh 0.1.0 ./dist amd64
```

CPU 아키텍처별 bundle 생성, 반입 검증, 내부 registry, 업그레이드/rollback 및 보안 체크리스트는 [AIR_GAPPED.md](./AIR_GAPPED.md)를 참고하세요.
