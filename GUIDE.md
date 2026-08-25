# Iceberg Ops Console 사용 가이드

이 문서는 기존 HDFS Iceberg 테이블을 등록하고 메타데이터를 조회하거나 유지보수 작업을 요청하는 방법을 설명한다. Catalog와 Namespace는 등록 후 재사용하고, table은 단일 또는 Bulk 방식으로 선택한다.

## 시작하기

Playground를 포함한 전체 로컬 환경을 시작한다.

```bash
docker compose --profile playground up --build -d
```

- Playground: <http://localhost:3000/playground>
- Operations: <http://localhost:3000/operations>
- API 문서: <http://localhost:8088/docs>

## 테이블 운영 현황 확인

첫 화면의 **테이블 운영 현황**에는 관리 테이블, 긴급 확인, 주의 필요, 정상,
상태 미확인, 진행 중 작업이 요약된다. 각 카드에 마우스를 올리거나 키보드로 초점을
옮기면 판정 의미와 대표 테이블 최대 3개를 확인할 수 있다.

카드를 클릭하면 카드 아래에 해당 테이블 전체 목록이 열린다. 목록은 한 번에 100개씩
표시하며, 대상이 더 있으면 **테이블 100개 더 보기**로 이어서 조회한다. 테이블 이름을
누르면 개별 메트릭 화면으로 이동한다. **진행 중 작업** 카드는 클릭 후 작업 관리 화면으로
이동할 수 있다.

| 화면 표시 | 의미 |
|---|---|
| 긴급 확인 | 데이터 유실 위험이나 freshness SLO 위반처럼 즉시 조치가 필요한 상태 |
| 주의 필요 | 작은 파일, delete 증폭 등 성능·운영 점검이 필요한 상태 |
| 정상 | 현재 적용된 상태 규칙에서 경고나 긴급 조건이 없는 상태 |
| 상태 미확인 | 최근 관측값 또는 판정에 필요한 메트릭이 부족한 상태 |

상태는 Console에 마지막으로 수집된 관측값을 기준으로 한다. 데모 데이터가 활성화된
환경의 예시 테이블과 용량은 실제 HDFS 사용량이 아니며, Playground에서 기존 테이블을
등록·조회한 결과와 구분해야 한다.

## Catalog와 Namespace 등록

Playground와 Operations 화면에서 같은 등록 정보를 공유한다.

1. **Catalog / Namespace 등록**을 연다.
2. Catalog 이름과 HDFS warehouse URI를 입력한다.
3. 등록된 Catalog를 선택하고 자주 사용하는 Namespace를 등록한다.
4. 이후에는 드롭다운에서 기존 값을 선택해 재사용한다.

Catalog 이름은 영문자 또는 `_`로 시작하고 영문자, 숫자, `_`만 사용할 수 있다. Namespace는 `analytics.daily`처럼 여러 단계로 등록할 수 있다. Warehouse URI는 `playground-runner`와 Spark/Livy가 접근할 수 있는 절대 `hdfs://` URI여야 한다.

같은 Catalog 이름을 다른 warehouse URI로 다시 등록하면 기존 연결이 암묵적으로 변경되지 않도록 요청을 거부한다.

## Playground에서 테이블 조회

### 단일 테이블 SQL

1. **단일 테이블 SQL**을 선택한다.
2. Catalog, Namespace와 `table_name`을 선택·입력한다.
3. 메타데이터, 파티션 통계, 스냅샷 이력 또는 파일 메트릭 preset을 선택한다.
4. **Run query**를 누른다.

SQL은 읽기 전용 단일 statement만 허용하며 결과는 최대 200행이다.

### Bulk 메트릭

1. **Bulk 메트릭**을 선택한다.
2. Catalog와 Namespace를 선택한다.
3. `table_name 목록`에 최대 200개 테이블 이름을 입력한다.
4. **테이블 조회**를 누른다.

테이블 이름은 줄바꿈, 공백 또는 쉼표로 구분할 수 있으며 중복은 제거된다.
Bulk 조회는 비동기 Job으로 등록된다. 화면에는 `처리 완료/전체`, 성공, 실패, 취소 수가
갱신되며 브라우저 요청 시간이 길어져도 서버의 Job과 테이블별 결과는 PostgreSQL에 남는다.

```text
orders
events
customers
```

결과는 다음 세 영역으로 나뉜다.

| 영역 | 내용 |
|---|---|
| 비교표 | Format version, Partition 수, File 수, Record 수, 크기, Snapshot 수, 마지막 commit |
| 선택 테이블 상세 | Current snapshot, Small file 비율, Delete file 비율, 관측 시각 |
| 실패 목록 | 존재하지 않거나 읽을 수 없는 테이블 이름과 개별 오류 |

비교표의 행을 선택하면 오른쪽 상세 패널이 바뀐다. **이 테이블 SQL 상세 조회**를 누르면 해당 테이블을 단일 SQL 모드에서 이어서 조회할 수 있다. 일부 테이블이 실패해도 성공한 테이블 결과는 유지된다.

Small file 비율은 128 MiB보다 작은 DATA 파일 비율이다. Delete file 비율은 전체 DATA/DELETE 파일 중 DELETE 파일이 차지하는 비율이다.

## Operations에서 Bulk 작업 요청

### 작업 대상 일괄 등록

1. **Catalog / Namespace / table 등록**을 연다.
2. Catalog와 Namespace를 선택한다.
3. 테이블 등록 방식을 **Bulk**로 바꾼다.
4. 최대 200개의 `table_name`을 입력한다.
5. **일괄 확인·등록**을 누른다.

각 테이블의 실제 Iceberg 메타데이터를 확인한 뒤 성공한 테이블만 작업 대상 목록에 추가한다. 실패 개수는 화면 메시지에 별도로 표시된다.

### 여러 테이블에 작업 요청

1. 작업 대상 선택 방식을 **Bulk 작업**으로 바꾼다.
2. 이름으로 테이블을 검색하고 체크박스로 선택한다. 검색 결과 전체 선택도 가능하다.
3. Command, 옵션과 Reason을 입력한다.
4. **영향 범위 계산**을 눌러 파일 수, 예상 rewrite 크기, 위험도와 경고를 확인한다.
5. **승인 요청 등록**을 누른다.

Bulk 요청은 하나의 거대한 작업이 아니라 테이블마다 독립된 승인 요청을 생성한다. 따라서 snapshot 계획 조건, 승인, 실행 상태, 실패와 재시도가 테이블별로 관리된다. Bulk 등록만으로 승인되거나 유지보수 작업이 실행되지는 않는다.

승인된 요청은 Worker가 순서대로 처리하며 다음 상태를 거친다.

```text
WAITING_APPROVAL → QUEUED → RUNNING → VERIFYING → SUCCEEDED / FAILED
                                      ↘ CANCEL_REQUESTED → CANCELED
```

요청 시점 snapshot과 실행 직전 snapshot이 다르면 테이블을 변경하기 전에 실패 처리한다.
같은 테이블은 하나의 승인된 작업만 실행 잠금을 소유할 수 있다. 중복 제출은
`Idempotency-Key`로 같은 결과를 반환하며, 다른 payload에 같은 키를 재사용하면 거부된다.
실행 중 취소는 Livy batch에 전달된다. 이미 Iceberg commit이 완료된 변경을 자동 rollback한다는
의미는 아니므로 결과와 snapshot 상태를 확인해야 한다.

Bulk 작업은 **작업 그룹** 카드에서 전체 진행률을 확인하고 그룹 취소 또는 실패 항목만 재시도할
수 있다. 작업 목록은 cursor pagination의 **이전 작업 더 보기**로 확장되며 최신 상태는 SSE로
자동 갱신된다.

## Bulk API 예시

테이블 메트릭을 확인하고 작업 대상으로 등록한다.

```bash
curl -X POST http://localhost:8088/ops/api/v1/playground/tables/bulk-jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: inspect-demo-20260825' \
  -d '{
    "catalogName": "playground",
    "namespaceName": "demo",
    "tableNames": ["orders", "events", "customers"]
  }'
```

응답의 Job ID를 조회한다.

```bash
curl http://localhost:8088/ops/api/v1/bulk-jobs/JOB_UUID
```

등록된 여러 테이블에 독립된 Operations 승인 요청을 만든다.

```bash
curl -X POST http://localhost:8088/ops/api/v1/operation-requests/bulk \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: compute-stats-20260825' \
  -d '{
    "tableIds": ["TABLE_UUID_1", "TABLE_UUID_2"],
    "command": "COMPUTE_STATS",
    "preferredEngine": "SPARK",
    "parameters": {},
    "reason": "정기 통계 갱신",
    "requester": "data.operator"
  }'
```

## 제한 및 문제 해결

- 한 번의 Bulk 요청은 최대 200개 테이블이다.
- 현재 등록형 Catalog는 HDFS Hadoop Catalog만 지원한다.
- Playground SQL은 읽기 전용이며 운영 보안 경계로 사용하면 안 된다.
- 테이블이 없다는 오류가 나오면 선택한 Catalog, Namespace, warehouse 경로를 확인한다.
- 전체 테이블이 실패하면 HDFS/Spark 상태와 `playground-runner` 로그를 확인한다.
- Bulk 처리가 멈춘 것처럼 보이면 `bulk-worker` 상태와 로그를 확인한다.
- 작업 요청 버튼이 비활성화되면 Worker 상태와 현재 Worker의 Catalog scope를 확인한다.

```bash
docker compose --profile playground ps
docker compose --profile playground logs -f playground-runner bulk-worker operation-worker livy
```
