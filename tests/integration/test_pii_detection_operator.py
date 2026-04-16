"""
tests/integration/test_pii_detection_operator.py
──────────────────────────────────────────────────
PIIDetectionOperator 통합 테스트

테스트 전략:
  - 로컬 SparkSession + 임시 Delta 테이블로 Operator execute() 엔드-투-엔드 검증
  - 각 detection_engine (regex / custom) 과 action_on_detect (report / mask /
    quarantine / fail) 조합을 독립 테스트 케이스로 분리
  - 픽스처(fixture) 패턴으로 SparkSession 과 임시 디렉터리를 공유

사전 조건:
  - pyspark, delta-spark, pytest 설치
  - JAVA_HOME 설정 필요
  - 실행: pytest tests/integration/ -v --timeout=120

설계 결정:
  - `tmp_path` (pytest 내장 픽스처) 로 테스트별 격리된 임시 디렉터리 사용
  - SparkSession 은 모듈 범위 픽스처로 한 번만 생성 → 테스트 스위트 속도 최적화
  - Airflow context 는 MagicMock 으로 모킹 → Airflow 서버 없이 execute() 호출 가능
"""

from __future__ import annotations

import os
import sys
from typing import Dict, List
from unittest.mock import MagicMock, patch

import pytest

# 프로젝트 루트를 sys.path 에 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


# ──────────────────────────────────────────────────────────
# pyspark / delta 임포트 가드 (미설치 시 테스트 전체 skip)
# ──────────────────────────────────────────────────────────
pyspark = pytest.importorskip("pyspark", reason="pyspark 미설치 → 통합 테스트 skip")
delta = pytest.importorskip("delta", reason="delta-spark 미설치 → 통합 테스트 skip")


# ──────────────────────────────────────────────────────────
# 픽스처: SparkSession (모듈 범위)
# ──────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def spark():
    """
    Delta Lake 지원 로컬 SparkSession 생성.

    scope="module" → 모듈 내 모든 테스트가 동일한 세션 공유
    """
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("PIIDetectionOperator_IntegrationTest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        # 테스트 중 불필요한 로그 억제
        .config("spark.ui.showConsoleProgress", "false")
    )

    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")

    yield spark_session
    spark_session.stop()


# ──────────────────────────────────────────────────────────
# 픽스처: 샘플 Delta 테이블 생성 헬퍼
# ──────────────────────────────────────────────────────────
@pytest.fixture
def sample_delta_table(spark, tmp_path):
    """
    PII 데이터가 혼합된 샘플 Delta 테이블 생성.

    스키마: id(int), name(str), contact(str), notes(str)
    - contact 컬럼: 전화번호·이메일·계좌번호 포함 행 존재
    - name 컬럼: 주민번호 포함 행 존재
    """
    table_path = str(tmp_path / "sample_table")
    data = [
        # id, name,                      contact,                          notes
        (1,  "홍길동 901231-1234567",   "010-1234-5678",                  "일반 메모"),
        (2,  "김영희",                  "user@example.com",               "이메일 고객"),
        (3,  "이철수",                  "110-123-456789",                  "계좌 등록"),
        (4,  "박민수",                  "010-9876-5432",                   "VIP 고객"),
        (5,  "정수진",                  "M12345678",                       "여권 등록"),
        (6,  "clean_user",             "일반 연락처",                      "정상 데이터"),
        (7,  "another_clean",          "정상 값",                          "이상 없음"),
    ]

    df = spark.createDataFrame(data, ["id", "name", "contact", "notes"])
    df.write.format("delta").mode("overwrite").save(table_path)

    return table_path


@pytest.fixture
def clean_delta_table(spark, tmp_path):
    """PII 가 전혀 없는 정상 Delta 테이블."""
    table_path = str(tmp_path / "clean_table")
    data = [
        (1, "일반 사용자", "서울시 강남구", "정상 메모"),
        (2, "테스트 계정", "경기도 수원시", "테스트"),
        (3, "데이터 없음", None,           "null 값 포함"),
    ]
    df = spark.createDataFrame(data, ["id", "name", "address", "notes"])
    df.write.format("delta").mode("overwrite").save(table_path)
    return table_path


# ──────────────────────────────────────────────────────────
# 픽스처: Airflow context 모킹
# ──────────────────────────────────────────────────────────
@pytest.fixture
def mock_context():
    """Airflow execute() 에 전달되는 context dict 모킹."""
    ti_mock = MagicMock()
    ti_mock.xcom_push = MagicMock()

    dag_mock = MagicMock()
    dag_mock.dag_id = "test_dag"

    return {
        "ti": ti_mock,
        "dag": dag_mock,
        "run_id": "test_run_20240101T000000",
    }


# ──────────────────────────────────────────────────────────
# 헬퍼: Operator 인스턴스 생성 (task_id 자동 주입)
# ──────────────────────────────────────────────────────────
def _make_operator(**kwargs):
    """PIIDetectionOperator 를 테스트용 기본값으로 생성."""
    from airflow.operators.pii_detection import PIIDetectionOperator

    defaults = {
        "task_id": "test_pii_task",
        "detection_engine": "regex",
        "action_on_detect": "report",
    }
    defaults.update(kwargs)
    return PIIDetectionOperator(**defaults)


# ──────────────────────────────────────────────────────────
# 1. 파라미터 유효성 검증 테스트
# ──────────────────────────────────────────────────────────
class TestOperatorParameterValidation:
    """__init__ 단계의 파라미터 검증 로직 테스트."""

    def test_invalid_detection_engine_raises(self):
        with pytest.raises(ValueError, match="detection_engine"):
            _make_operator(source_table="/any", detection_engine="invalid")

    def test_invalid_action_raises(self):
        with pytest.raises(ValueError, match="action_on_detect"):
            _make_operator(source_table="/any", action_on_detect="delete")

    def test_custom_engine_without_udf_raises(self):
        with pytest.raises(ValueError, match="custom_udf"):
            _make_operator(
                source_table="/any",
                detection_engine="custom",
                custom_udf=None,
            )

    def test_valid_params_no_exception(self):
        op = _make_operator(source_table="/any/path")
        assert op.source_table == "/any/path"
        assert op.detection_engine == "regex"
        assert op.action_on_detect == "report"

    def test_template_fields_defined(self):
        from airflow.operators.pii_detection import PIIDetectionOperator

        assert "source_table" in PIIDetectionOperator.template_fields
        assert "output_table" in PIIDetectionOperator.template_fields


# ──────────────────────────────────────────────────────────
# 2. regex + report 액션 통합 테스트
# ──────────────────────────────────────────────────────────
class TestRegexReportAction:
    """regex 엔진 + report 액션의 엔드-투-엔드 검증."""

    def test_detect_pii_returns_results(
        self, spark, sample_delta_table, mock_context, tmp_path
    ):
        """PII 포함 테이블에서 탐지 결과가 반환되어야 함."""
        output_path = str(tmp_path / "results")
        op = _make_operator(
            source_table=sample_delta_table,
            detection_engine="regex",
            action_on_detect="report",
            output_table=output_path,
        )

        with patch(
            "airflow.operators.pii_detection.PIIDetectionOperator"
            "._get_or_create_spark_session",
            return_value=spark,
        ):
            results = op.execute(mock_context)

        assert isinstance(results, list)
        assert len(results) > 0

        pii_types_found = {r["pii_type"] for r in results}
        # contact 컬럼에 전화번호·이메일·계좌번호·여권번호 존재
        # name 컬럼에 주민등록번호 존재
        assert len(pii_types_found) >= 2, f"최소 2가지 PII 타입 탐지 필요, 실제: {pii_types_found}"

    def test_xcom_push_called_with_results(
        self, spark, sample_delta_table, mock_context, tmp_path
    ):
        """execute() 후 XCom push 가 올바른 키로 호출되어야 함."""
        op = _make_operator(
            source_table=sample_delta_table,
            output_table=str(tmp_path / "results"),
        )

        with patch(
            "airflow.operators.pii_detection.PIIDetectionOperator"
            "._get_or_create_spark_session",
            return_value=spark,
        ):
            op.execute(mock_context)

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="pii_detection_result",
            value=mock_context["ti"].xcom_push.call_args[1]["value"],
        )

    def test_result_schema_fields(
        self, spark, sample_delta_table, mock_context, tmp_path
    ):
        """탐지 결과 dict 의 필수 필드가 모두 포함되어야 함."""
        op = _make_operator(
            source_table=sample_delta_table,
            output_table=str(tmp_path / "results"),
        )

        with patch(
            "airflow.operators.pii_detection.PIIDetectionOperator"
            "._get_or_create_spark_session",
            return_value=spark,
        ):
            results = op.execute(mock_context)

        required_fields = {
            "column_name", "pii_type", "sample_count",
            "total_rows", "detection_ratio",
        }
        for r in results:
            assert required_fields.issubset(r.keys()), (
                f"필수 필드 누락: {required_fields - r.keys()}"
            )
            assert r["total_rows"] == 7  # 샘플 테이블 총 행 수
            assert 0.0 <= r["detection_ratio"] <= 1.0

    def test_output_delta_table_created(
        self, spark, sample_delta_table, mock_context, tmp_path
    ):
        """output_table 에 Delta 파일이 실제로 생성되어야 함."""
        output_path = str(tmp_path / "output_results")
        op = _make_operator(
            source_table=sample_delta_table,
            output_table=output_path,
        )

        with patch(
            "airflow.operators.pii_detection.PIIDetectionOperator"
            "._get_or_create_spark_session",
            return_value=spark,
        ):
            op.execute(mock_context)

        # Delta 테이블은 _delta_log 디렉터리로 식별
        assert os.path.exists(os.path.join(output_path, "_delta_log"))


# ──────────────────────────────────────────────────────────
# 3. regex + mask 액션 통합 테스트
# ──────────────────────────────────────────────────────────
class TestRegexMaskAction:
    """regex 엔진 + mask 액션의 소스 테이블 마스킹 검증."""

    def test_masked_table_has_no_original_pii(
        self, spark, sample_delta_table, mock_context
    ):
        """마스킹 후 소스 테이블에서 원본 PII 값이 사라져야 함."""
        import re as _re

        op = _make_operator(
            source_table=sample_delta_table,
            action_on_detect="mask",
            target_columns=["contact"],
        )

        with patch(
            "airflow.operators.pii_detection.PIIDetectionOperator"
            "._get_or_create_spark_session",
            return_value=spark,
        ):
            op.execute(mock_context)

        # 마스킹 후 소스 테이블 재읽기
        masked_df = spark.read.format("delta").load(sample_delta_table)
        contacts = [
            row["contact"]
            for row in masked_df.select("contact").collect()
            if row["contact"] is not None
        ]

        phone_pattern = _re.compile(r"01[0-9]-\d{3,4}-\d{4}")
        email_pattern = _re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

        for contact in contacts:
            assert not phone_pattern.search(contact), f"마스킹 실패: {contact}"
            assert not email_pattern.search(contact), f"마스킹 실패: {contact}"


# ──────────────────────────────────────────────────────────
# 4. custom + quarantine 액션 통합 테스트
# ──────────────────────────────────────────────────────────
class TestCustomQuarantineAction:
    """custom UDF + quarantine 액션의 행 분리 동작 검증."""

    def test_quarantine_moves_pii_rows(
        self, spark, sample_delta_table, mock_context, tmp_path
    ):
        """PII 포함 행이 격리 테이블로 이동되고 소스에서 제거되어야 함."""
        quarantine_path = str(tmp_path / "quarantine")

        # "010-" 포함 행을 PII 로 판단하는 커스텀 UDF
        def phone_only_detector(val):
            import re
            if val and re.search(r"01[0-9]-\d{3,4}-\d{4}", val):
                return ["전화번호"]
            return []

        op = _make_operator(
            source_table=sample_delta_table,
            detection_engine="custom",
            action_on_detect="quarantine",
            custom_udf=phone_only_detector,
            target_columns=["contact"],
            quarantine_table=quarantine_path,
        )

        with patch(
            "airflow.operators.pii_detection.PIIDetectionOperator"
            "._get_or_create_spark_session",
            return_value=spark,
        ):
            op.execute(mock_context)

        # 격리 테이블 존재 및 행 수 확인
        assert os.path.exists(os.path.join(quarantine_path, "_delta_log"))
        quarantine_df = spark.read.format("delta").load(quarantine_path)
        assert quarantine_df.count() == 2  # "010-1234-5678", "010-9876-5432" 2개

        # 소스 테이블 행 수 감소 확인
        source_df = spark.read.format("delta").load(sample_delta_table)
        assert source_df.count() == 5  # 7 - 2 = 5


# ──────────────────────────────────────────────────────────
# 5. fail 액션 통합 테스트
# ──────────────────────────────────────────────────────────
class TestFailAction:
    """fail 액션에서 AirflowException 발생 검증."""

    def test_fail_action_raises_airflow_exception(
        self, spark, sample_delta_table, mock_context
    ):
        """PII 탐지 시 AirflowException 이 발생해야 함."""
        from airflow.exceptions import AirflowException

        op = _make_operator(
            source_table=sample_delta_table,
            action_on_detect="fail",
        )

        with patch(
            "airflow.operators.pii_detection.PIIDetectionOperator"
            "._get_or_create_spark_session",
            return_value=spark,
        ):
            with pytest.raises(AirflowException, match="PII 탐지로 인한 DAG 실패"):
                op.execute(mock_context)

    def test_fail_action_exception_message_contains_column_info(
        self, spark, sample_delta_table, mock_context
    ):
        """AirflowException 메시지에 탐지 컬럼 정보가 포함되어야 함."""
        from airflow.exceptions import AirflowException

        op = _make_operator(
            source_table=sample_delta_table,
            action_on_detect="fail",
            target_columns=["contact"],
        )

        with patch(
            "airflow.operators.pii_detection.PIIDetectionOperator"
            "._get_or_create_spark_session",
            return_value=spark,
        ):
            with pytest.raises(AirflowException) as exc_info:
                op.execute(mock_context)

        assert "contact" in str(exc_info.value)


# ──────────────────────────────────────────────────────────
# 6. 정상 데이터(PII 없음) 처리 테스트
# ──────────────────────────────────────────────────────────
class TestCleanDataHandling:
    """PII 가 없는 정상 데이터에서 액션이 수행되지 않아야 함."""

    def test_clean_table_returns_empty_results(
        self, spark, clean_delta_table, mock_context
    ):
        """PII 미탐지 시 빈 결과 반환 및 XCom push."""
        op = _make_operator(source_table=clean_delta_table)

        with patch(
            "airflow.operators.pii_detection.PIIDetectionOperator"
            "._get_or_create_spark_session",
            return_value=spark,
        ):
            results = op.execute(mock_context)

        assert results == []
        mock_context["ti"].xcom_push.assert_called_with(
            key="pii_detection_result", value=[]
        )

    def test_fail_action_no_exception_on_clean_data(
        self, spark, clean_delta_table, mock_context
    ):
        """PII 없는 테이블에서 fail 액션은 예외를 발생시키지 않아야 함."""
        op = _make_operator(
            source_table=clean_delta_table,
            action_on_detect="fail",
        )

        with patch(
            "airflow.operators.pii_detection.PIIDetectionOperator"
            "._get_or_create_spark_session",
            return_value=spark,
        ):
            # 예외 없이 정상 완료되어야 함
            results = op.execute(mock_context)

        assert results == []


# ──────────────────────────────────────────────────────────
# 7. target_columns 자동 탐지 테스트
# ──────────────────────────────────────────────────────────
class TestColumnAutoDetection:
    """target_columns=None 시 StringType 컬럼 자동 탐지 검증."""

    def test_auto_detects_string_columns(self, spark, tmp_path):
        """StringType 컬럼만 자동 선택, 숫자/날짜 컬럼은 제외."""
        from pyspark.sql.types import (
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("age", IntegerType(), True),
        ])
        data = [(1, "홍길동", "hong@test.com", 30)]
        df = spark.createDataFrame(data, schema)

        table_path = str(tmp_path / "mixed_type_table")
        df.write.format("delta").mode("overwrite").save(table_path)

        op = _make_operator(source_table=table_path)

        with patch(
            "airflow.operators.pii_detection.PIIDetectionOperator"
            "._get_or_create_spark_session",
            return_value=spark,
        ):
            resolved = op._resolve_target_columns(
                spark.read.format("delta").load(table_path)
            )

        assert set(resolved) == {"name", "email"}
        assert "id" not in resolved
        assert "age" not in resolved
