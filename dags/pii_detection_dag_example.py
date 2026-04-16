"""
dags/pii_detection_dag_example.py
───────────────────────────────────
PIIDetectionOperator 사용 예시 DAG 모음

총 3개의 DAG 예시:
  1. pii_regex_report_dag    — regex 탐지 + report 액션 (기본 패턴)
  2. pii_presidio_mask_dag   — presidio 탐지 + mask 액션
  3. pii_custom_quarantine_dag — custom UDF + quarantine 액션

공통 설계:
  - on_failure_callback 으로 Slack/이메일 알림 연동 포인트 제공
  - XCom 으로 탐지 결과를 하위 태스크에 전달하는 패턴 시연
  - 다운스트림 태스크(데이터 품질 체크, 알림)와의 연결 예시 포함
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── 커스텀 오퍼레이터 임포트 ─────────────────────────────────
from airflow.operators.pii_detection import PIIDetectionOperator

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────
# 공통 설정
# ──────────────────────────────────────────────────────────
DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# Spark 환경 설정 (필요에 따라 조정)
SPARK_CONF: Dict[str, str] = {
    "spark.executor.memory": "4g",
    "spark.executor.cores": "2",
    "spark.sql.shuffle.partitions": "200",
}

# ──────────────────────────────────────────────────────────
# on_failure_callback 예시
# ──────────────────────────────────────────────────────────
def _notify_pii_failure(context: Dict) -> None:
    """
    PII 탐지 또는 파이프라인 실패 시 호출되는 콜백.

    실제 운영 환경에서는 SlackWebhookOperator, EmailOperator 등으로
    교체하거나 PagerDuty API 를 직접 호출합니다.
    """
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    exception = context.get("exception")

    logger.error(
        "[PII 알림] DAG='%s' | Task='%s' | 실패 원인: %s",
        dag_id,
        task_instance.task_id if task_instance else "unknown",
        exception,
    )
    # TODO: 실제 알림 채널 연동
    # slack_client.chat_postMessage(channel="#data-security-alerts", text=msg)


# ──────────────────────────────────────────────────────────
# 공통 하위 태스크 함수
# ──────────────────────────────────────────────────────────
def _handle_detection_result(**context) -> None:
    """
    PIIDetectionOperator 의 XCom 결과를 수신하여 후처리하는 예시.

    탐지 결과를 읽어 로그로 출력하고, 탐지율이 높은 컬럼에 대해
    추가 경보 임계값 체크를 수행합니다.
    """
    ti = context["ti"]
    results: List[Dict] = ti.xcom_pull(
        key="pii_detection_result",
        task_ids="detect_pii",
    ) or []

    if not results:
        logger.info("PII 탐지 결과 없음 — 정상 데이터셋")
        return

    logger.warning("=== PII 탐지 결과 요약 ===")
    for r in results:
        logger.warning(
            "컬럼: %-20s | PII 유형: %-15s | 탐지 행: %6d / %6d (%.2f%%)",
            r["column_name"],
            r["pii_type"],
            r["sample_count"],
            r["total_rows"],
            r["detection_ratio"] * 100,
        )

    # 임계값(5%) 초과 컬럼은 추가 경보
    HIGH_RISK_THRESHOLD = 0.05
    high_risk = [r for r in results if r["detection_ratio"] >= HIGH_RISK_THRESHOLD]
    if high_risk:
        logger.error(
            "고위험 컬럼 탐지! 탐지율 %.0f%% 초과: %s",
            HIGH_RISK_THRESHOLD * 100,
            [r["column_name"] for r in high_risk],
        )
        # TODO: 긴급 알림 발송


# ──────────────────────────────────────────────────────────
# DAG 1: regex 탐지 + report 액션
# ──────────────────────────────────────────────────────────
with DAG(
    dag_id="pii_regex_report_dag",
    description="regex 엔진으로 한국 개인정보 탐지 후 결과 리포트",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",   # 매일 02:00 실행
    catchup=False,
    tags=["pii", "security", "regex"],
) as dag1:

    # ── Task 1: PII 탐지 (report) ────────────────────────
    detect_pii = PIIDetectionOperator(
        task_id="detect_pii",
        # Jinja 템플릿으로 실행 날짜 기반 동적 경로 지정 가능
        # source_table="/data/delta/customers_{{ ds_nodash }}",
        source_table="/data/delta/customers",
        detection_engine="regex",
        action_on_detect="report",
        # target_columns=None → StringType 컬럼 전체 자동 탐색
        target_columns=None,
        spark_conf=SPARK_CONF,
        output_table="/data/delta/pii_detection_results",
        on_failure_callback=_notify_pii_failure,
        execution_timeout=timedelta(hours=2),
    )

    # ── Task 2: 탐지 결과 후처리 ─────────────────────────
    handle_result = PythonOperator(
        task_id="handle_detection_result",
        python_callable=_handle_detection_result,
        on_failure_callback=_notify_pii_failure,
    )

    # ── DAG 흐름 ─────────────────────────────────────────
    detect_pii >> handle_result


# ──────────────────────────────────────────────────────────
# DAG 2: presidio 탐지 + mask 액션
# ──────────────────────────────────────────────────────────
with DAG(
    dag_id="pii_presidio_mask_dag",
    description="Presidio 엔진으로 PII 탐지 후 소스 테이블 마스킹",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["pii", "security", "presidio", "mask"],
) as dag2:

    mask_pii = PIIDetectionOperator(
        task_id="mask_pii",
        source_table="/data/delta/user_profiles",
        detection_engine="presidio",
        action_on_detect="mask",
        target_columns=["name", "email", "address", "phone"],
        spark_conf=SPARK_CONF,
        confidence_threshold=0.75,        # presidio 탐지 신뢰도 임계값
        mask_char="[REDACTED]",            # 마스킹 대체 문자
        output_table="/data/delta/pii_detection_results",
        on_failure_callback=_notify_pii_failure,
        execution_timeout=timedelta(hours=3),
    )

    notify_masked = PythonOperator(
        task_id="notify_mask_complete",
        python_callable=_handle_detection_result,
    )

    mask_pii >> notify_masked


# ──────────────────────────────────────────────────────────
# DAG 3: custom UDF + quarantine 액션
# ──────────────────────────────────────────────────────────

def _custom_pii_detector(value: str | None) -> list[str]:
    """
    사용자 정의 PII 탐지 함수 예시.

    내부 시스템 특화 패턴(예: 사원번호, 내부 ID 체계)을 탐지하는 경우
    이 함수를 교체하여 사용합니다.

    반환값: 탐지된 PII 유형 이름 목록
    """
    import re

    if not value or not isinstance(value, str):
        return []

    detected = []

    # 예시: 내부 사원번호 패턴 (EMP-XXXXXXXX)
    if re.search(r"EMP-\d{8}", value):
        detected.append("사원번호")

    # 예시: 주민번호 (regex 엔진과 중복 탐지 가능)
    if re.search(r"\d{6}-[1-4]\d{6}", value):
        detected.append("주민등록번호")

    return detected


with DAG(
    dag_id="pii_custom_quarantine_dag",
    description="커스텀 UDF 로 PII 탐지 후 격리 테이블로 이동",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@monthly",
    catchup=False,
    tags=["pii", "security", "custom", "quarantine"],
) as dag3:

    quarantine_pii = PIIDetectionOperator(
        task_id="quarantine_pii",
        source_table="/data/delta/hr_records",
        detection_engine="custom",
        action_on_detect="quarantine",
        custom_udf=_custom_pii_detector,
        spark_conf=SPARK_CONF,
        output_table="/data/delta/pii_detection_results",
        quarantine_table="/data/delta/hr_records_quarantine",
        on_failure_callback=_notify_pii_failure,
        execution_timeout=timedelta(hours=4),
    )

    post_quarantine = PythonOperator(
        task_id="post_quarantine_report",
        python_callable=_handle_detection_result,
    )

    quarantine_pii >> post_quarantine


# ──────────────────────────────────────────────────────────
# DAG 4: fail 액션 — 데이터 품질 게이트용
# ──────────────────────────────────────────────────────────
with DAG(
    dag_id="pii_quality_gate_dag",
    description="데이터 파이프라인 진입 전 PII 포함 시 DAG 실패 처리",
    default_args={
        **DEFAULT_ARGS,
        # PII 탐지로 실패 시 반드시 on_failure_callback 으로 알림
        "on_failure_callback": _notify_pii_failure,
    },
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,    # 외부 트리거(예: 데이터 랜딩 후 TriggerDagRunOperator)
    catchup=False,
    tags=["pii", "security", "gate", "fail"],
) as dag4:

    # PII 가 발견되면 AirflowException 으로 파이프라인 자체를 중단
    pii_gate = PIIDetectionOperator(
        task_id="pii_quality_gate",
        source_table="{{ dag_run.conf.get('source_table', '/data/delta/incoming') }}",
        detection_engine="regex",
        action_on_detect="fail",
        output_table="/data/delta/pii_detection_results",
    )

    # pii_gate 가 통과해야만 실행되는 하위 태스크들
    downstream_etl = PythonOperator(
        task_id="run_etl",
        python_callable=lambda **kw: logger.info("ETL 파이프라인 실행 중..."),
    )

    pii_gate >> downstream_etl
