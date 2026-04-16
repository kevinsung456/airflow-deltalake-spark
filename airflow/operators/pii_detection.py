"""
airflow/operators/pii_detection.py
────────────────────────────────────
Apache Airflow Custom Operator — PIIDetectionOperator

Delta Lake 테이블에서 PII(개인식별정보)를 탐지하고 정책에 따라 처리하는
Airflow Custom Operator.

지원 탐지 엔진:
  - regex    : 한국 개인정보 정규식 (주민번호·전화·이메일·계좌·여권)
  - presidio : Microsoft Presidio AnalyzerEngine
  - custom   : 사용자 정의 callable UDF

탐지 후 액션:
  - report     : 결과를 output_table (Delta)에 MERGE upsert + XCom push
  - mask       : 소스 테이블 PII 값 마스킹 후 Delta 덮어쓰기
  - quarantine : PII 포함 행을 격리 테이블로 이동, 소스는 정제본으로 교체
  - fail       : AirflowException 발생으로 DAG 실패 처리

설계 결정 근거:
  1. template_fields 에 source_table / output_table 포함
     → Jinja 매크로({{ ds }}, {{ run_id }} 등)로 동적 경로 지정 가능
  2. SparkSession 은 getActiveSession() 우선 → 없으면 신규 생성
     → Airflow-on-Spark 환경과 독립 실행 환경 모두 지원
  3. _save_detection_results 는 MERGE INTO 시도 → 테이블 미존재 시
     overwrite 로 폴백하여 첫 실행도 멱등성 보장
  4. mask 액션은 regex 엔진일 때 Spark 네이티브 regexp_replace 사용
     → UDF 오버헤드 없이 처리, presidio/custom 은 UDF 결과 기반 셀 단위 마스킹
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

logger = logging.getLogger(__name__)


class PIIDetectionOperator(BaseOperator):
    """
    Delta Lake 테이블에서 PII 를 탐지하는 Airflow Custom Operator.

    Parameters
    ----------
    source_table : str
        읽을 Delta 테이블 경로 또는 메타스토어 테이블명.
        Jinja 템플릿 사용 가능.
    detection_engine : {'regex', 'presidio', 'custom'}
        PII 탐지 엔진 선택.
    action_on_detect : {'report', 'mask', 'quarantine', 'fail'}
        PII 탐지 시 수행할 액션.
    target_columns : list[str] | None
        탐지 대상 컬럼 목록. None 이면 StringType 컬럼 전체 자동 탐색.
    spark_conf : dict | None
        신규 SparkSession 생성 시 추가할 Spark 설정.
    confidence_threshold : float
        presidio 엔진 신뢰도 임계값 (기본 0.8).
    custom_udf : callable | None
        detection_engine='custom' 일 때 필수.
        시그니처: (value: str | None) -> list[str]
    output_table : str | None
        탐지 결과를 저장할 Delta 테이블 경로. Jinja 템플릿 사용 가능.
    mask_char : str
        마스킹 대체 문자열 (기본 '***').
    quarantine_table : str | None
        격리 테이블 경로. None 이면 source_table + '_quarantine' 사용.
    """

    # Airflow UI 에서 오퍼레이터를 구분하는 색상
    ui_color = "#FF6B6B"
    ui_fgcolor = "#FFFFFF"

    # Jinja 템플릿 적용 필드
    template_fields = ("source_table", "output_table", "quarantine_table")

    _VALID_ENGINES = frozenset({"regex", "presidio", "custom"})
    _VALID_ACTIONS = frozenset({"report", "mask", "quarantine", "fail"})

    def __init__(
        self,
        *,
        source_table: str,
        detection_engine: str,
        action_on_detect: str,
        target_columns: Optional[List[str]] = None,
        spark_conf: Optional[Dict[str, str]] = None,
        confidence_threshold: float = 0.8,
        custom_udf: Optional[Callable] = None,
        output_table: Optional[str] = None,
        mask_char: str = "***",
        quarantine_table: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        # ── 파라미터 유효성 검증 ────────────────────────────
        if detection_engine not in self._VALID_ENGINES:
            raise ValueError(
                f"detection_engine 은 {self._VALID_ENGINES} 중 하나여야 합니다. "
                f"입력값: '{detection_engine}'"
            )
        if action_on_detect not in self._VALID_ACTIONS:
            raise ValueError(
                f"action_on_detect 는 {self._VALID_ACTIONS} 중 하나여야 합니다. "
                f"입력값: '{action_on_detect}'"
            )
        if detection_engine == "custom" and custom_udf is None:
            raise ValueError(
                "detection_engine='custom' 사용 시 custom_udf 를 반드시 제공해야 합니다."
            )

        self.source_table = source_table
        self.detection_engine = detection_engine
        self.action_on_detect = action_on_detect
        self.target_columns = target_columns
        self.spark_conf: Dict[str, str] = spark_conf or {}
        self.confidence_threshold = confidence_threshold
        self.custom_udf = custom_udf
        self.output_table = output_table
        self.mask_char = mask_char
        self.quarantine_table = quarantine_table

    # ──────────────────────────────────────────────────────
    # SparkSession 관리
    # ──────────────────────────────────────────────────────
    def _get_or_create_spark_session(self):
        """
        활성 SparkSession 을 재사용하거나 없으면 신규 생성.

        설계 결정:
          - getActiveSession() 우선 → 이미 Spark 클러스터에서 실행 중인
            경우(예: Airflow-on-Spark, Livy) 기존 세션을 그대로 사용
          - Delta Lake 카탈로그 확장은 신규 세션 생성 시에만 적용
        """
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:
            raise AirflowException(
                "pyspark 가 설치되어 있지 않습니다. 'pip install pyspark' 를 실행하세요."
            ) from exc

        spark = SparkSession.getActiveSession()
        if spark is not None:
            logger.info("기존 SparkSession 재사용: appId=%s", spark.sparkContext.applicationId)
            return spark

        logger.info("신규 SparkSession 생성 중 (appName=PIIDetection_%s)", self.task_id)
        builder = (
            SparkSession.builder
            .appName(f"PIIDetection_{self.task_id}")
            # Delta Lake SQL 확장 등록
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            # Delta Lake 카탈로그 설정
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        for key, value in self.spark_conf.items():
            builder = builder.config(key, value)

        spark = builder.getOrCreate()
        logger.info("SparkSession 생성 완료: appId=%s", spark.sparkContext.applicationId)
        return spark

    # ──────────────────────────────────────────────────────
    # Delta 테이블 읽기
    # ──────────────────────────────────────────────────────
    def _read_source_table(self, spark):
        """
        소스 Delta 테이블을 DataFrame 으로 읽음.

        시도 순서:
          1. spark.read.format("delta").load(path)  — 절대/상대 경로
          2. spark.table(name)                       — Hive/Unity 카탈로그 테이블명
        """
        logger.info("소스 Delta 테이블 읽는 중: %s", self.source_table)
        try:
            return spark.read.format("delta").load(self.source_table)
        except Exception:
            pass
        try:
            return spark.table(self.source_table)
        except Exception as exc:
            raise AirflowException(
                f"소스 테이블 '{self.source_table}' 읽기 실패: {exc}"
            ) from exc

    # ──────────────────────────────────────────────────────
    # 대상 컬럼 추론
    # ──────────────────────────────────────────────────────
    def _resolve_target_columns(self, df) -> List[str]:
        """
        target_columns 미지정 시 StringType 컬럼을 자동 탐색.

        설계 결정:
          - StringType 만 필터링 → PII 는 문자열로 저장되는 것이 일반적
          - 사용자가 target_columns 를 명시한 경우 스키마 검증은 생략
            (Spark 단에서 컬럼 없을 시 AnalysisException 발생)
        """
        if self.target_columns is not None:
            return list(self.target_columns)

        from pyspark.sql.types import StringType

        cols = [
            f.name
            for f in df.schema.fields
            if isinstance(f.dataType, StringType)
        ]
        logger.info("StringType 컬럼 자동 탐지: %s", cols)
        return cols

    # ──────────────────────────────────────────────────────
    # UDF 선택
    # ──────────────────────────────────────────────────────
    def _get_detect_udf(self):
        """탐지 엔진에 따라 적절한 pandas_udf 반환."""
        from utils.pii_detector import (
            make_custom_detect_udf,
            make_presidio_detect_udf,
            make_regex_detect_udf,
        )

        if self.detection_engine == "regex":
            logger.info("탐지 엔진: regex (한국 개인정보 패턴)")
            return make_regex_detect_udf()
        elif self.detection_engine == "presidio":
            logger.info(
                "탐지 엔진: presidio (confidence_threshold=%.2f)",
                self.confidence_threshold,
            )
            return make_presidio_detect_udf(
                confidence_threshold=self.confidence_threshold
            )
        else:  # custom
            logger.info("탐지 엔진: custom (사용자 UDF)")
            return make_custom_detect_udf(self.custom_udf)  # type: ignore[arg-type]

    # ──────────────────────────────────────────────────────
    # 결과 저장 (report / mask / quarantine 공통)
    # ──────────────────────────────────────────────────────
    def _save_detection_results(
        self,
        spark,
        detection_results: List[Dict],
        context: Dict,
    ) -> None:
        """
        탐지 결과를 output_table Delta 테이블에 MERGE INTO upsert.

        Merge 키: (run_id, column_name, pii_type)
          → 동일 run_id 재실행 시 중복 없이 갱신

        output_table 미지정 또는 탐지 결과 없으면 skip.
        """
        if not self.output_table or not detection_results:
            return

        from pyspark.sql.types import (
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        schema = StructType(
            [
                StructField("run_id", StringType(), False),
                StructField("dag_id", StringType(), False),
                StructField("task_id", StringType(), False),
                StructField("source_table", StringType(), False),
                StructField("column_name", StringType(), False),
                StructField("pii_type", StringType(), False),
                StructField("sample_count", LongType(), False),
                StructField("total_rows", LongType(), False),
                StructField("detection_ratio", DoubleType(), False),
                StructField("detected_at", TimestampType(), False),
                StructField("action_taken", StringType(), False),
            ]
        )

        run_id: str = str(context.get("run_id", "unknown"))
        dag_id: str = context["dag"].dag_id if context.get("dag") else "unknown"
        now = datetime.now(tz=timezone.utc)

        rows = [
            (
                run_id,
                dag_id,
                self.task_id,
                self.source_table,
                r["column_name"],
                r["pii_type"],
                r["sample_count"],
                r["total_rows"],
                r["detection_ratio"],
                now,
                self.action_on_detect,
            )
            for r in detection_results
        ]

        results_df = spark.createDataFrame(rows, schema)
        results_df.createOrReplaceTempView("__pii_results_staging__")

        merge_sql = f"""
            MERGE INTO delta.`{self.output_table}` AS target
            USING __pii_results_staging__ AS source
            ON  target.run_id      = source.run_id
            AND target.column_name = source.column_name
            AND target.pii_type    = source.pii_type
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """

        try:
            spark.sql(merge_sql)
            logger.info(
                "탐지 결과 %d 건을 '%s' 에 MERGE 완료",
                len(detection_results),
                self.output_table,
            )
        except Exception as merge_err:
            # 테이블 미존재 시 신규 생성 (첫 실행 폴백)
            logger.warning(
                "MERGE 실패 (%s) → Delta 테이블 신규 생성 후 쓰기 진행", merge_err
            )
            results_df.write.format("delta").mode("overwrite").save(self.output_table)
            logger.info(
                "output_table '%s' 신규 생성 및 %d 건 저장 완료",
                self.output_table,
                len(detection_results),
            )

    # ──────────────────────────────────────────────────────
    # 액션: mask
    # ──────────────────────────────────────────────────────
    def _apply_mask(self, spark, df, target_columns: List[str], detect_udf) -> None:
        """
        PII 값을 mask_char 로 치환하고 소스 Delta 테이블을 덮어씀.

        엔진별 전략:
          - regex   : Spark 네이티브 regexp_replace() 사용
                      → 셀 내 PII 패턴 부분만 치환 (부분 마스킹)
          - presidio / custom : UDF 로 탐지 → PII 존재 셀 전체를 mask_char 로 교체
                      → Presidio 는 오프셋 기반 부분 마스킹도 가능하나
                        복잡도 증가로 셀 단위 마스킹으로 단순화
        """
        from pyspark.sql import functions as F

        masked_df = df

        if self.detection_engine == "regex":
            # 정규식 패턴 직접 적용 (UDF 오버헤드 없음)
            from utils.pii_detector import KOREAN_PII_PATTERNS

            for col_name in target_columns:
                masked_col = F.col(col_name)
                for _, pattern in KOREAN_PII_PATTERNS.items():
                    masked_col = F.regexp_replace(masked_col, pattern, self.mask_char)
                masked_df = masked_df.withColumn(col_name, masked_col)
        else:
            # UDF 탐지 결과 기반 셀 단위 마스킹
            from utils.pii_detector import build_pii_flag_column

            for col_name in target_columns:
                tmp_flag = f"__mask_flag_{col_name}__"
                # 컬럼별 개별 플래그 생성
                from pyspark.sql import functions as F2
                from utils.pii_detector import make_regex_detect_udf

                col_flag_df = masked_df.withColumn(
                    tmp_flag,
                    F.size(detect_udf(F.col(col_name))) > 0,
                )
                masked_df = col_flag_df.withColumn(
                    col_name,
                    F.when(F.col(tmp_flag), F.lit(self.mask_char)).otherwise(
                        F.col(col_name)
                    ),
                ).drop(tmp_flag)

        masked_df.write.format("delta").mode("overwrite").save(self.source_table)
        logger.info(
            "마스킹 완료 → '%s' 에 덮어쓰기 (컬럼: %s)", self.source_table, target_columns
        )

    # ──────────────────────────────────────────────────────
    # 액션: quarantine
    # ──────────────────────────────────────────────────────
    def _apply_quarantine(
        self, spark, df, target_columns: List[str], detect_udf
    ) -> int:
        """
        PII 포함 행을 격리 테이블로 이동하고 소스는 정제본으로 교체.

        반환값: 격리된 행 수
        """
        from pyspark.sql import functions as F
        from utils.pii_detector import build_pii_flag_column

        quarantine_path = self.quarantine_table or f"{self.source_table}_quarantine"
        flag_col = "__has_pii__"

        df_flagged = build_pii_flag_column(df, target_columns, detect_udf, flag_col)

        quarantine_df = df_flagged.filter(F.col(flag_col)).drop(flag_col)
        clean_df = df_flagged.filter(~F.col(flag_col)).drop(flag_col)

        quarantine_count = quarantine_df.count()

        # 격리 테이블에 append (기존 격리 데이터 보존)
        quarantine_df.write.format("delta").mode("append").save(quarantine_path)
        logger.info(
            "격리 완료: %d 행 → '%s'", quarantine_count, quarantine_path
        )

        # 소스를 정제본으로 교체
        clean_df.write.format("delta").mode("overwrite").save(self.source_table)
        logger.info("소스 테이블 정제본 기록 완료: '%s'", self.source_table)

        return quarantine_count

    # ──────────────────────────────────────────────────────
    # execute (Airflow 진입점)
    # ──────────────────────────────────────────────────────
    def execute(self, context: Dict) -> List[Dict]:
        """
        Operator 실행 메서드.

        흐름:
          1. SparkSession 획득/생성
          2. 소스 Delta 테이블 읽기
          3. 대상 컬럼 결정 (명시 or 자동 탐색)
          4. 탐지 UDF 선택
          5. PII 탐지 및 집계
          6. action_on_detect 에 따라 분기 처리
          7. XCom push

        반환값:
          탐지 결과 dict list (key='pii_detection_result' 로 XCom 에도 push)
        """
        from utils.pii_detector import detect_pii_in_dataframe

        logger.info(
            "PIIDetectionOperator 시작 | engine=%s | action=%s | source=%s",
            self.detection_engine,
            self.action_on_detect,
            self.source_table,
        )

        # ── 1. SparkSession ────────────────────────────────
        spark = self._get_or_create_spark_session()

        # ── 2. 소스 테이블 읽기 ────────────────────────────
        df = self._read_source_table(spark)
        total_rows: int = df.count()
        logger.info("소스 테이블 로드 완료: %d 행", total_rows)

        if total_rows == 0:
            logger.warning("소스 테이블이 비어 있습니다. 탐지를 건너뜁니다.")
            context["ti"].xcom_push(key="pii_detection_result", value=[])
            return []

        # ── 3. 대상 컬럼 결정 ─────────────────────────────
        target_columns = self._resolve_target_columns(df)
        if not target_columns:
            logger.warning("탐지 대상 컬럼이 없습니다.")
            context["ti"].xcom_push(key="pii_detection_result", value=[])
            return []

        logger.info("탐지 대상 컬럼: %s", target_columns)

        # ── 4. UDF 선택 ───────────────────────────────────
        detect_udf = self._get_detect_udf()

        # ── 5. PII 탐지 집계 ──────────────────────────────
        detection_results = detect_pii_in_dataframe(
            df, target_columns, detect_udf, total_rows
        )
        logger.info(
            "탐지 완료: 컬럼/PII타입 조합 %d 건 발견", len(detection_results)
        )

        # ── 6. 액션 분기 ──────────────────────────────────
        if detection_results:
            if self.action_on_detect == "report":
                self._save_detection_results(spark, detection_results, context)

            elif self.action_on_detect == "mask":
                self._apply_mask(spark, df, target_columns, detect_udf)
                self._save_detection_results(spark, detection_results, context)

            elif self.action_on_detect == "quarantine":
                quarantined = self._apply_quarantine(
                    spark, df, target_columns, detect_udf
                )
                # 격리 건수를 결과에 메타 정보로 추가
                for r in detection_results:
                    r["quarantined_rows"] = quarantined
                self._save_detection_results(spark, detection_results, context)

            elif self.action_on_detect == "fail":
                summary = ", ".join(
                    f"{r['column_name']}/{r['pii_type']} ({r['sample_count']}건)"
                    for r in detection_results
                )
                raise AirflowException(
                    f"PII 탐지로 인한 DAG 실패 | 소스: '{self.source_table}' | "
                    f"탐지 항목: [{summary}]"
                )
        else:
            logger.info("PII 미탐지 — 추가 액션 없음")

        # ── 7. XCom push ──────────────────────────────────
        context["ti"].xcom_push(
            key="pii_detection_result", value=detection_results
        )
        logger.info("XCom push 완료 (key='pii_detection_result')")

        return detection_results
