"""
utils/pii_detector.py
─────────────────────
PII 검출 UDF 모듈 — PIIDetectionOperator 에서 사용하는 핵심 검출 로직

설계 결정 근거:
  1. 패턴 dict를 모듈 상수로 정의 → Spark 직렬화 시 UDF 클로저에
     포함되어 executor 에서 재컴파일 없이 사용 가능
  2. make_*_detect_udf() 팩토리 패턴 → Operator 가 엔진별로
     동일한 pandas_udf 반환 타입을 보장하면서 구현을 교체 가능
  3. pandas_udf(ArrayType(StringType())) 선택 이유:
     - Arrow 직렬화로 대량 데이터 처리 성능 최적화
     - 셀 당 여러 PII 타입을 배열로 반환 → 집계 시 explode 활용
  4. presidio AnalyzerEngine 은 pandas_udf 내부에서 지연 초기화
     → 직렬화 불가 객체를 executor 프로세스별로 한 번만 생성
"""

from __future__ import annotations

import re
from typing import Callable, Dict, List, Optional

import pandas as pd
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import ArrayType, StringType

# ──────────────────────────────────────────────
# 한국 개인정보 정규식 패턴
# ──────────────────────────────────────────────
KOREAN_PII_PATTERNS: Dict[str, str] = {
    # 주민등록번호: 생년월일(6자리) + 하이픈 + 성별코드(1~4) + 6자리
    "주민등록번호": r"\d{6}-[1-4]\d{6}",
    # 휴대전화: 01X-NNN(N)-NNNN
    "전화번호": r"01[0-9]-\d{3,4}-\d{4}",
    # 일반 이메일 주소
    "이메일": r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    # 은행 계좌번호: NNN(N)-NN(NNN)-NN(NNNN) 형식
    "계좌번호": r"\d{3,6}-\d{2,6}-\d{2,6}",
    # 한국 여권번호: 영문 1~2자 + 숫자 7~8자 (예: M12345678)
    "여권번호": r"[A-Z]{1,2}\d{7,8}",
}

# ──────────────────────────────────────────────
# 마스킹용 정규식(regex 모드 mask action에서 Spark native 사용)
# ──────────────────────────────────────────────
MASK_REPLACEMENT = "***"


# ──────────────────────────────────────────────
# UDF 팩토리: regex 모드
# ──────────────────────────────────────────────
def make_regex_detect_udf():
    """
    한국 개인정보 정규식 기반 pandas UDF 생성.

    반환 타입: ArrayType(StringType())
      → 셀 내에서 탐지된 PII 타입 이름 목록 (예: ["주민등록번호", "이메일"])
      → 탐지 없으면 빈 리스트 []

    설계 결정:
      - 패턴 dict 를 클로저로 캡처하여 executor 에서 재임포트 없이 사용
      - 패턴은 UDF 내부에서 한 번만 컴파일 → 파티션 내 재사용
    """
    # 클로저로 캡처할 패턴 사본(원본 dict 변경 방지)
    _patterns = dict(KOREAN_PII_PATTERNS)

    @F.pandas_udf(ArrayType(StringType()))  # type: ignore[misc]
    def _detect_pii_regex(series: pd.Series) -> pd.Series:
        # executor 프로세스별 최초 호출 시 한 번만 컴파일
        import re as _re

        compiled = {k: _re.compile(v) for k, v in _patterns.items()}

        results: List[List[str]] = []
        for val in series:
            detected: List[str] = []
            if val and isinstance(val, str):
                for pii_type, pattern in compiled.items():
                    if pattern.search(val):
                        detected.append(pii_type)
            results.append(detected)
        return pd.Series(results)

    return _detect_pii_regex


# ──────────────────────────────────────────────
# UDF 팩토리: presidio 모드
# ──────────────────────────────────────────────
def make_presidio_detect_udf(
    confidence_threshold: float = 0.8,
    language: str = "ko",
) -> "pyspark.sql.functions.UserDefinedFunction":  # noqa: F821
    """
    Microsoft Presidio AnalyzerEngine 기반 pandas UDF 생성.

    설계 결정:
      - AnalyzerEngine 은 직렬화 불가 → UDF 내부에서 지연 초기화
      - _ANALYZER 모듈 수준 캐시 변수로 executor 프로세스당 1회 초기화
      - confidence_threshold / language 는 클로저로 캡처
    """
    _threshold = confidence_threshold
    _lang = language

    @F.pandas_udf(ArrayType(StringType()))  # type: ignore[misc]
    def _detect_pii_presidio(series: pd.Series) -> pd.Series:
        # executor 프로세스별 지연 초기화 (모듈 캐시)
        import sys

        cache_key = f"_presidio_analyzer_{_lang}"
        if cache_key not in sys.modules:
            from presidio_analyzer import AnalyzerEngine

            sys.modules[cache_key] = AnalyzerEngine()  # type: ignore[assignment]
        analyzer = sys.modules[cache_key]

        results: List[List[str]] = []
        for val in series:
            detected: List[str] = []
            if val and isinstance(val, str):
                try:
                    analysis = analyzer.analyze(  # type: ignore[union-attr]
                        text=val,
                        language=_lang,
                        score_threshold=_threshold,
                    )
                    # 중복 제거 후 entity_type 목록 반환
                    detected = list({r.entity_type for r in analysis})
                except Exception:
                    pass
            results.append(detected)
        return pd.Series(results)

    return _detect_pii_presidio


# ──────────────────────────────────────────────
# UDF 팩토리: custom 모드
# ──────────────────────────────────────────────
def make_custom_detect_udf(custom_fn: Callable) -> "pyspark.sql.functions.UserDefinedFunction":  # noqa: F821
    """
    사용자 정의 함수를 pandas UDF 로 래핑.

    custom_fn 시그니처:
        (value: str | None) -> List[str]
        예: lambda val: ["커스텀PII"] if val and "secret" in val else []

    설계 결정:
      - 사용자 함수를 클로저로 캡처하여 Spark 직렬화 대상에 포함
      - 예외는 조용히 처리하여 파이프라인 중단 방지
    """
    _fn = custom_fn

    @F.pandas_udf(ArrayType(StringType()))  # type: ignore[misc]
    def _detect_pii_custom(series: pd.Series) -> pd.Series:
        results: List[List[str]] = []
        for val in series:
            try:
                detected = _fn(val) or []
            except Exception:
                detected = []
            results.append(list(detected))
        return pd.Series(results)

    return _detect_pii_custom


# ──────────────────────────────────────────────
# 핵심 집계 함수
# ──────────────────────────────────────────────
def detect_pii_in_dataframe(
    df: DataFrame,
    target_columns: List[str],
    detect_udf,
    total_rows: int,
) -> List[Dict]:
    """
    대상 컬럼 각각에 detect_udf 를 적용하고 PII 탐지 결과를 집계.

    반환 구조:
        [
            {
                "column_name": str,
                "pii_type":    str,
                "sample_count": int,   # 해당 PII 타입이 탐지된 행 수
                "total_rows":  int,    # 전체 행 수
                "detection_ratio": float  # sample_count / total_rows
            },
            ...
        ]

    설계 결정:
      - 컬럼별 UDF 적용 후 explode() → groupBy() 순서로 집계
        → 단일 Spark Job 으로 컬럼 수에 상관없이 확장 가능
      - 빈 배열 필터링으로 불필요한 셔플 최소화
    """
    results: List[Dict] = []

    for col_name in target_columns:
        detected_col = f"__pii__{col_name}__"

        # UDF 적용 → 셀당 탐지 PII 타입 배열
        df_with_detected = df.select(
            F.explode(
                detect_udf(F.col(col_name))
            ).alias("pii_type")
        ).filter(F.col("pii_type").isNotNull() & (F.col("pii_type") != ""))

        # PII 타입별 카운트 집계
        pii_counts = (
            df_with_detected
            .groupBy("pii_type")
            .agg(F.count("*").alias("sample_count"))
            .collect()
        )

        for row in pii_counts:
            detection_ratio = (
                round(row["sample_count"] / total_rows, 6)
                if total_rows > 0
                else 0.0
            )
            results.append(
                {
                    "column_name": col_name,
                    "pii_type": row["pii_type"],
                    "sample_count": int(row["sample_count"]),
                    "total_rows": total_rows,
                    "detection_ratio": detection_ratio,
                }
            )

    return results


# ──────────────────────────────────────────────
# 헬퍼: 행 레벨 PII 존재 여부 판별 (quarantine / mask 용)
# ──────────────────────────────────────────────
def build_pii_flag_column(
    df: DataFrame,
    target_columns: List[str],
    detect_udf,
    flag_col: str = "__has_pii__",
) -> DataFrame:
    """
    각 행에 PII 존재 여부를 나타내는 boolean 컬럼을 추가.

    - presidio / custom 엔진에서 mask·quarantine 처리 시 사용
    - 각 target_col 에 UDF 를 적용한 뒤 OR 연산으로 합산
    """
    tmp_cols: List[str] = []
    for col_name in target_columns:
        tmp = f"__pii_tmp_{col_name}__"
        df = df.withColumn(tmp, detect_udf(F.col(col_name)))
        tmp_cols.append(tmp)

    # 하나라도 탐지된 배열이 비어있지 않으면 True
    pii_condition = F.lit(False)
    for tmp in tmp_cols:
        pii_condition = pii_condition | (F.size(F.col(tmp)) > 0)

    df = df.withColumn(flag_col, pii_condition)
    df = df.drop(*tmp_cols)
    return df
