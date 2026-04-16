# 데이터 보관 등록 프로그램 전체 설계 (Spark + Delta Lake + Airflow)

## 0) 전제 및 목표

본 설계는 다음 기술 스택을 기준으로 합니다.

- 실행 엔진: **Apache Spark (PySpark)**
- 저장 포맷: **Delta Lake** (Unity Catalog 또는 Hive Metastore)
- 오케스트레이션: **Apache Airflow**
- 배포 환경: **on-premise (YARN 또는 Kubernetes)**

핵심 목표는 다음과 같습니다.

1. 테이블별 보관 정책을 중앙 관리(등록/수정/비활성화)
2. 정책 기반 TTL 만료 데이터 자동 탐지 및 삭제/이관
3. 배치 실행 결과를 감사 로그로 추적
4. 실패 감지 및 즉시 알림

---

## 1) 보관 정책 메타데이터 스키마 설계

### 1.1 Delta Lake DDL (retention_policy)

> 카탈로그/스키마는 환경에 맞춰 조정하세요. (예: `main.governance`)

```sql
CREATE TABLE IF NOT EXISTS main.governance.retention_policy (
    table_name        STRING COMMENT '대상 테이블명',
    db_name           STRING COMMENT '대상 DB/Schema명',
    retention_days    INT    COMMENT '보관 일수',
    storage_tier      STRING COMMENT 'hot/warm/cold/archive',
    is_active         BOOLEAN COMMENT '정책 활성화 여부',
    owner             STRING COMMENT '정책 소유자(팀/담당자)',
    registered_at     TIMESTAMP COMMENT '정책 최초 등록 시각',
    last_executed_at  TIMESTAMP COMMENT '마지막 정책 실행 시각'
)
USING DELTA
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
);
```

### 1.2 정책 CRUD 인터페이스 설계 (Python Class)

```python
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class RetentionPolicy:
    """단일 보관 정책 엔티티.

    Attributes:
        db_name: 대상 DB/Schema
        table_name: 대상 테이블
        retention_days: 보관 일수
        storage_tier: hot/warm/cold/archive 중 하나
        is_active: 활성화 여부
        owner: 정책 소유자
        registered_at: 등록 시각
        last_executed_at: 마지막 실행 시각
    """

    db_name: str
    table_name: str
    retention_days: int
    storage_tier: str
    is_active: bool = True
    owner: str = "unknown"
    registered_at: Optional[datetime] = None
    last_executed_at: Optional[datetime] = None


class RetentionPolicyService:
    """보관 정책 CRUD 서비스.

    Delta Table(main.governance.retention_policy)을 단일 소스로 사용한다.
    """

    def __init__(self, spark, policy_table: str = "main.governance.retention_policy"):
        """서비스 초기화.

        Args:
            spark: SparkSession
            policy_table: 정책 메타데이터 Delta 테이블 전체 경로
        """
        self.spark = spark
        self.policy_table = policy_table

    def create_policy(self, policy: RetentionPolicy) -> None:
        """정책 신규 등록(upsert 가능)."""

    def get_policy(self, db_name: str, table_name: str) -> Optional[RetentionPolicy]:
        """단일 정책 조회."""

    def list_policies(self, active_only: bool = True) -> List[RetentionPolicy]:
        """정책 목록 조회."""

    def update_policy(self, db_name: str, table_name: str, **fields) -> None:
        """보관일수/티어/소유자 등 정책 수정."""

    def deactivate_policy(self, db_name: str, table_name: str) -> None:
        """정책 비활성화(is_active=false)."""

    def delete_policy(self, db_name: str, table_name: str) -> None:
        """물리 삭제(권장: 운영에서는 soft delete 우선)."""
```

### 1.3 REST API 대안(선택)

- `POST /policies` : 정책 등록
- `GET /policies/{db}/{table}` : 단일 조회
- `GET /policies?active=true` : 목록 조회
- `PATCH /policies/{db}/{table}` : 정책 수정
- `POST /policies/{db}/{table}/deactivate` : 비활성화

---

## 2) 보관 등록 프로그램 구현

### 2.1 RetentionRegistry 클래스 (PySpark)

```python
from datetime import datetime, timedelta
from pyspark.sql import DataFrame


class RetentionRegistry:
    """보관 정책 실행 엔진.

    주요 기능:
      1) 정책 upsert (MERGE INTO)
      2) 만료 파티션 스캔
      3) 삭제/이관 실행
      4) last_executed_at 및 감사 로그 기록
    """

    def __init__(
        self,
        spark,
        policy_table: str = "main.governance.retention_policy",
        audit_table: str = "main.governance.audit_log",
    ):
        self.spark = spark
        self.policy_table = policy_table
        self.audit_table = audit_table

    def upsert_policy(self, policy_df: DataFrame) -> None:
        """MERGE INTO 기반 정책 upsert.

        policy_df 스키마는 retention_policy와 동일해야 한다.
        """
        merge_sql = f"""
        MERGE INTO {self.policy_table} t
        USING (
          SELECT
            db_name,
            table_name,
            retention_days,
            storage_tier,
            is_active,
            owner,
            COALESCE(registered_at, current_timestamp()) AS registered_at,
            last_executed_at
          FROM incoming_policy_view
        ) s
        ON t.db_name = s.db_name AND t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET
          t.retention_days = s.retention_days,
          t.storage_tier = s.storage_tier,
          t.is_active = s.is_active,
          t.owner = s.owner,
          t.last_executed_at = s.last_executed_at
        WHEN NOT MATCHED THEN INSERT *
        """
        policy_df.createOrReplaceTempView("incoming_policy_view")
        self.spark.sql(merge_sql)

    def load_active_policies(self) -> DataFrame:
        """활성화 정책 조회."""
        return self.spark.sql(
            f"SELECT * FROM {self.policy_table} WHERE is_active = true"
        )

    def build_expired_partition_query(
        self, full_table_name: str, partition_col: str, retention_days: int
    ) -> str:
        """파티션 pruning 기반 만료 데이터 조회 SQL 생성.

        예시 결과:
          SELECT * FROM db.tbl
          WHERE dt < date_sub(current_date(), 30)
        """
        return f"""
        SELECT *
        FROM {full_table_name}
        WHERE {partition_col} < date_sub(current_date(), {retention_days})
        """

    def execute_retention(
        self,
        db_name: str,
        table_name: str,
        partition_col: str,
        retention_days: int,
        mode: str = "hard_delete",
        archive_target: str | None = None,
    ) -> dict:
        """단일 정책 실행.

        Args:
            mode: soft_delete | hard_delete | archive
        Returns:
            실행 메트릭(dict): affected_rows, status, message
        """
        full_table = f"{db_name}.{table_name}"
        expired_sql = self.build_expired_partition_query(full_table, partition_col, retention_days)

        if mode == "hard_delete":
            delete_sql = f"""
            DELETE FROM {full_table}
            WHERE {partition_col} < date_sub(current_date(), {retention_days})
            """
            self.spark.sql(delete_sql)
            return {"affected_rows": -1, "status": "SUCCESS", "message": "hard_delete done"}

        if mode == "archive":
            if not archive_target:
                raise ValueError("archive_target is required for archive mode")
            self.spark.sql(f"INSERT INTO {archive_target} {expired_sql}")
            self.spark.sql(
                f"DELETE FROM {full_table} WHERE {partition_col} < date_sub(current_date(), {retention_days})"
            )
            return {"affected_rows": -1, "status": "SUCCESS", "message": "archive done"}

        # soft_delete는 예: deleted_flag 컬럼 업데이트 전략(테이블 스키마 의존)
        return {"affected_rows": -1, "status": "SUCCESS", "message": "soft_delete placeholder"}

    def update_last_executed_at(self, db_name: str, table_name: str) -> None:
        """정책 실행 후 last_executed_at 갱신."""
        self.spark.sql(f"""
        UPDATE {self.policy_table}
        SET last_executed_at = current_timestamp()
        WHERE db_name = '{db_name}' AND table_name = '{table_name}'
        """)

    def append_audit_log(
        self,
        dag_run_id: str,
        table_name: str,
        action_type: str,
        affected_rows: int,
        status: str,
    ) -> None:
        """감사 로그 Delta append."""
        self.spark.sql(f"""
        INSERT INTO {self.audit_table}
        VALUES (
          '{dag_run_id}',
          '{table_name}',
          '{action_type}',
          {affected_rows},
          current_timestamp(),
          '{status}'
        )
        """)
```

### 2.2 TTL 만료 식별 핵심 포인트

- 테이블은 가급적 `dt`(DATE) 또는 `yyyymmdd` 기반 파티셔닝
- 조건식은 **파티션 컬럼 직접 비교**로 구성 (`dt < date_sub(...)`)해 partition pruning 유도
- 파일 정리 비용 관리를 위해 Delta `OPTIMIZE`/`VACUUM` 운영 정책 병행

---

## 3) Airflow 연동 설계

### 3.1 3단계 DAG 구조

1. **정책 로딩** (`PythonOperator`)
2. **만료 파티션 스캔** (`PythonOperator`)
3. **삭제/이관 Spark 작업 실행** (`SparkSubmitOperator`)

### 3.2 DAG 코드 스켈레톤

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule


def load_policies(**context):
    """활성 보관 정책을 로딩해 XCom에 저장."""
    # 예: 정책 조회 후 JSON 직렬화하여 push
    pass


def scan_expired_partitions(**context):
    """정책별 만료 대상 파티션/조건을 계산하고 XCom에 저장."""
    # 예: [{db_name, table_name, partition_col, retention_days, expired_filter}, ...]
    pass


def on_failure_callback(context):
    """실패 시 슬랙/메일 알림을 트리거."""
    # SlackWebhookOperator or custom notifier
    pass


with DAG(
    dag_id="data_retention_registry_dag",
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",  # schedule_interval
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "on_failure_callback": on_failure_callback,
    },
    params={
        "retention_mode": "hard_delete",  # soft_delete | hard_delete | archive
    },
    tags=["governance", "retention", "delta"],
) as dag:

    t1_load_policies = PythonOperator(
        task_id="load_policies",
        python_callable=load_policies,
    )

    t2_scan_expired = PythonOperator(
        task_id="scan_expired_partitions",
        python_callable=scan_expired_partitions,
    )

    t3_execute_spark = SparkSubmitOperator(
        task_id="execute_retention_job",
        application="/opt/airflow/jobs/run_retention.py",
        name="run_retention_job",
        conn_id="spark_default",
        application_args=[
            "--retention-mode", "{{ params.retention_mode }}",
            "--dag-run-id", "{{ run_id }}",
        ],
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    t1_load_policies >> t2_scan_expired >> t3_execute_spark
```

### 3.3 YARN/K8s 배포 포인트

- YARN: `--master yarn --deploy-mode cluster`, queue/resource 설정 분리
- Kubernetes: Spark Operator 사용 시 `SparkApplication` CRD 기반 제출 또는 Airflow에서 spark-submit to k8s
- 공통: Airflow Connection/Secrets Backend로 자격증명 분리

---

## 4) 감사 로그(audit_log) 설계

### 4.1 Delta Lake DDL

```sql
CREATE TABLE IF NOT EXISTS main.governance.audit_log (
    dag_run_id    STRING COMMENT 'Airflow run_id',
    table_name    STRING COMMENT '대상 테이블',
    action_type   STRING COMMENT 'soft_delete/hard_delete/archive',
    affected_rows BIGINT COMMENT '영향 받은 행 수(미측정시 -1)',
    executed_at   TIMESTAMP COMMENT '실행 시각',
    status        STRING COMMENT 'SUCCESS/FAILED'
)
USING DELTA
PARTITIONED BY (status)
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
);
```

### 4.2 append 로깅 규칙

- 정책 단위 실행마다 1건 이상 append
- 실패 시에도 반드시 기록(`status=FAILED`, message는 별도 운영 로그/메트릭 시스템에 저장)
- 필요 시 `error_code`, `error_message`, `duration_ms` 컬럼 확장

---

## 5) 에러 처리 및 알림 설계

### 5.1 Airflow 실패 콜백

- DAG 레벨 `on_failure_callback` + Task 레벨 override 가능
- 콜백 내용:
  - DAG ID / Task ID / Run ID / 실행 시각 / 예외 메시지
  - 영향 테이블(가능하면)
  - 재실행 링크

### 5.2 Slack / Email

- Slack: `SlackWebhookOperator` 또는 custom webhook client
- Email: `EmailOperator`
- 우선순위:
  1. Task 실패 즉시 Slack
  2. DAG 최종 실패 시 Email 요약

### 5.3 재처리 전략

- 일시 장애(네트워크/세션): Airflow retry로 자동 복구
- 데이터 오류(스키마 mismatch): fail-fast + 수동 조치
- 멱등성: DELETE 조건/ARCHIVE INSERT를 날짜 조건 기반으로 고정해 재실행 안정성 확보

---

## 6) 컴포넌트 간 데이터 흐름 다이어그램 (텍스트)

```text
[User/API]
   |
   | (CRUD: create/update/deactivate)
   v
[RetentionPolicyService]
   |
   | upsert/read
   v
[Delta: retention_policy]
   |
   | (Airflow DAG Step1: load active policies)
   v
[Airflow PythonOperator: load_policies]
   |
   | (Step2: compute expired partitions/filters)
   v
[Airflow PythonOperator: scan_expired_partitions]
   |
   | (Step3: spark-submit with retention_mode)
   v
[Spark RetentionRegistry Job]
   |---------------------------|
   |                           |
   v                           v
[Target Delta Tables]      [Delta: audit_log append]
   |
   | update last_executed_at
   v
[Delta: retention_policy]

(Any failure)
   |
   v
[Airflow on_failure_callback] --> [Slack/Email]
```

---

## 7) 운영 체크리스트 (권장)

- 파티션 컬럼 표준화(`dt`) 및 UTC 기준 일자 계산 통일
- Delta `VACUUM` 보존기간과 컴플라이언스 요구사항 정합성 검토
- Unity Catalog 사용 시 권한 모델(`USE CATALOG`, `SELECT`, `MODIFY`) 선반영
- 감사 로그 모니터링 대시보드(성공률/삭제량/실패 추이) 구축

