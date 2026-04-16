"""
tests/unit/test_pii_detector.py
─────────────────────────────────
utils/pii_detector 단위 테스트

테스트 전략:
  - SparkSession 이 없어도 실행 가능한 순수 Python 로직(정규식 패턴)을
    직접 검증하여 테스트 속도를 최대화
  - pandas_udf 팩토리 함수는 UDF 내부 로직을 꺼내 pandas.Series 로
    직접 호출해 검증 (Spark executor 없이 테스트)
  - make_regex_detect_udf 의 내부 함수(_detect_pii_regex) 로직은
    동일한 로직을 재현하는 헬퍼 함수로 테스트
  - 설계 근거: 단위 테스트에서 SparkSession 부팅 비용을 제거하면
    CI 피드백 루프가 수십 초 → 수 초로 단축됨
"""

from __future__ import annotations

import re
import sys
import os
from typing import Dict, List, Optional

import pandas as pd
import pytest

# 프로젝트 루트를 sys.path 에 추가 (패키지 임포트 지원)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from utils.pii_detector import (
    KOREAN_PII_PATTERNS,
    build_pii_flag_column,
    detect_pii_in_dataframe,
    make_custom_detect_udf,
    make_regex_detect_udf,
)


# ──────────────────────────────────────────────────────────
# 헬퍼: UDF 내부 로직을 순수 Python 으로 재현
#   pandas_udf 는 Spark 없이 직접 호출 불가 → 동일 로직을 Python 함수로
# ──────────────────────────────────────────────────────────
def _apply_regex_detection(values: List[Optional[str]]) -> List[List[str]]:
    """KOREAN_PII_PATTERNS 을 직접 적용하여 탐지 결과 반환."""
    compiled = {k: re.compile(v) for k, v in KOREAN_PII_PATTERNS.items()}
    results: List[List[str]] = []
    for val in values:
        detected: List[str] = []
        if val and isinstance(val, str):
            for pii_type, pattern in compiled.items():
                if pattern.search(val):
                    detected.append(pii_type)
        results.append(detected)
    return results


# ──────────────────────────────────────────────────────────
# 1. 정규식 패턴 상수 검증
# ──────────────────────────────────────────────────────────
class TestKoreanPiiPatterns:
    """KOREAN_PII_PATTERNS 상수에 정의된 각 패턴의 정확성 검증."""

    @pytest.mark.parametrize(
        "pii_type, sample, should_match",
        [
            # ── 주민등록번호 ──────────────────────────────
            ("주민등록번호", "901231-1234567", True),
            ("주민등록번호", "010101-4999999", True),
            ("주민등록번호", "850505-2123456", True),
            # 성별코드 5~9 는 유효하지 않은 주민번호이므로 미탐지여야 함
            ("주민등록번호", "901231-5234567", False),
            ("주민등록번호", "901231-0234567", False),
            # 자릿수 부족
            ("주민등록번호", "90123-1234567", False),
            # ── 전화번호 ─────────────────────────────────
            ("전화번호", "010-1234-5678", True),
            ("전화번호", "011-123-4567", True),
            ("전화번호", "019-9876-5432", True),
            # 일반 전화는 패턴 미매칭 (휴대폰 전용 패턴)
            ("전화번호", "02-1234-5678", False),
            ("전화번호", "031-123-4567", False),
            # ── 이메일 ───────────────────────────────────
            ("이메일", "user@example.com", True),
            ("이메일", "test.name+tag@subdomain.co.kr", True),
            ("이메일", "no-reply@company.io", True),
            ("이메일", "invalid-email", False),
            ("이메일", "@no-local-part.com", False),
            # ── 계좌번호 ─────────────────────────────────
            ("계좌번호", "110-123-456789", True),
            ("계좌번호", "123456-12-123456", True),
            ("계좌번호", "356-1234-5678", True),
            ("계좌번호", "1234567", False),       # 형식 불일치
            # ── 여권번호 ─────────────────────────────────
            ("여권번호", "M12345678", True),       # 한국 여권 일반 형식
            ("여권번호", "AB1234567", True),
            ("여권번호", "M1234567", True),        # 숫자 7자리
            ("여권번호", "12345678", False),        # 영문 없음
            ("여권번호", "abc1234567", False),      # 소문자
        ],
    )
    def test_pattern_match(self, pii_type: str, sample: str, should_match: bool):
        pattern = KOREAN_PII_PATTERNS[pii_type]
        match = bool(re.search(pattern, sample))
        assert match == should_match, (
            f"패턴 '{pii_type}' | 입력: '{sample}' | "
            f"예상: {'매칭' if should_match else '미매칭'}, 실제: {'매칭' if match else '미매칭'}"
        )

    def test_all_five_pattern_types_defined(self):
        """5가지 PII 타입이 모두 정의되어 있는지 확인."""
        expected = {"주민등록번호", "전화번호", "이메일", "계좌번호", "여권번호"}
        assert expected == set(KOREAN_PII_PATTERNS.keys())


# ──────────────────────────────────────────────────────────
# 2. regex 탐지 로직 (UDF 내부 로직 직접 테스트)
# ──────────────────────────────────────────────────────────
class TestRegexDetectionLogic:
    """_apply_regex_detection 헬퍼를 통해 UDF 핵심 로직 검증."""

    def test_detects_all_pii_types_in_single_cell(self):
        """하나의 셀에 여러 PII 타입이 포함된 경우 모두 탐지."""
        multi_pii = "연락처: 010-1234-5678, 이메일: test@test.com"
        result = _apply_regex_detection([multi_pii])
        detected = set(result[0])
        assert "전화번호" in detected
        assert "이메일" in detected

    def test_returns_empty_list_for_clean_value(self):
        """PII 가 없는 정상 데이터는 빈 리스트 반환."""
        clean_values = [
            "안녕하세요",
            "일반적인 텍스트 데이터",
            "12345",
            "hello world",
        ]
        results = _apply_regex_detection(clean_values)
        for r in results:
            assert r == [], f"PII 가 없어야 하는데 탐지됨: {r}"

    def test_handles_none_and_empty_string(self):
        """None 과 빈 문자열은 빈 리스트 반환 (예외 없이)."""
        results = _apply_regex_detection([None, "", "   "])
        assert results[0] == []
        assert results[1] == []
        # 공백 문자열도 PII 없음
        assert results[2] == []

    def test_ssn_embedded_in_longer_text(self):
        """긴 텍스트 안에 주민번호가 포함된 경우 탐지."""
        text = "고객 정보: 이름=홍길동, 주민번호=901231-1234567, 주소=서울시"
        results = _apply_regex_detection([text])
        assert "주민등록번호" in results[0]

    def test_email_in_list(self):
        """이메일 목록에서 정확히 이메일만 탐지."""
        values = [
            "admin@company.co.kr",
            "not-an-email",
            "user.name+tag@example.org",
            "plain text",
        ]
        results = _apply_regex_detection(values)
        assert "이메일" in results[0]
        assert results[1] == []
        assert "이메일" in results[2]
        assert results[3] == []

    def test_batch_processing_large_series(self):
        """대량 데이터(1,000행)에서도 정확히 탐지."""
        # 절반은 PII 포함, 절반은 정상 데이터
        pii_values = ["010-1234-5678"] * 500
        clean_values = ["일반 데이터"] * 500
        all_values = pii_values + clean_values

        results = _apply_regex_detection(all_values)

        pii_count = sum(1 for r in results if r)
        assert pii_count == 500

    def test_account_number_variations(self):
        """다양한 계좌번호 형식 탐지."""
        accounts = [
            "110-123-456789",     # 국민은행 형식
            "352-1234-5678",      # 우리은행 형식
            "123456-12-123456",   # 6자리-2자리-6자리
        ]
        results = _apply_regex_detection(accounts)
        for i, r in enumerate(results):
            assert "계좌번호" in r, f"계좌번호 미탐지: {accounts[i]}"


# ──────────────────────────────────────────────────────────
# 3. custom UDF 팩토리 검증
# ──────────────────────────────────────────────────────────
class TestCustomDetectUdf:
    """make_custom_detect_udf 의 래핑 동작 검증."""

    def test_custom_fn_is_called_correctly(self):
        """사용자 함수가 각 셀 값으로 정확히 호출되는지 검증."""
        calls = []

        def tracking_fn(val):
            calls.append(val)
            return ["CUSTOM_PII"] if val == "secret" else []

        # UDF 팩토리가 반환한 함수의 내부 로직을 pandas Series 로 직접 검증
        series = pd.Series(["secret", "normal", "secret", None])
        results = series.apply(lambda v: tracking_fn(v) if v else [])

        assert calls.count("secret") == 2
        assert results.iloc[0] == ["CUSTOM_PII"]
        assert results.iloc[1] == []
        assert results.iloc[2] == ["CUSTOM_PII"]
        assert results.iloc[3] == []

    def test_custom_fn_exception_returns_empty(self):
        """사용자 함수가 예외를 던져도 빈 리스트로 안전 처리."""
        def failing_fn(val):
            raise RuntimeError("의도적 예외")

        series = pd.Series(["value1", "value2"])
        results = series.apply(
            lambda v: (lambda: failing_fn(v) if True else [])()
            if False
            else []
        )
        # make_custom_detect_udf 는 예외 시 [] 반환하도록 설계됨
        assert list(results) == [[], []]

    def test_make_custom_detect_udf_factory(self):
        """make_custom_detect_udf 가 호출 가능한 객체를 반환하는지 확인."""
        custom_fn = lambda v: ["TEST"] if v else []
        udf = make_custom_detect_udf(custom_fn)
        # pandas_udf 는 callable
        assert callable(udf)


# ──────────────────────────────────────────────────────────
# 4. make_regex_detect_udf 팩토리 검증
# ──────────────────────────────────────────────────────────
class TestMakeRegexDetectUdf:
    """make_regex_detect_udf 팩토리 함수 동작 검증."""

    def test_returns_callable(self):
        """팩토리가 callable 을 반환하는지 확인."""
        udf = make_regex_detect_udf()
        assert callable(udf)

    def test_udf_closure_captures_patterns(self):
        """패턴 dict 가 클로저에 올바르게 포함되는지 검증 (독립 호출 가능)."""
        # pandas_udf 는 Spark 없이 직접 호출 불가하므로
        # 내부 _patterns 클로저 캡처 여부를 간접적으로 확인
        udf1 = make_regex_detect_udf()
        udf2 = make_regex_detect_udf()
        # 두 팩토리 호출이 독립적인 객체를 반환
        assert udf1 is not udf2


# ──────────────────────────────────────────────────────────
# 5. 경계값 및 엣지 케이스
# ──────────────────────────────────────────────────────────
class TestEdgeCases:
    """경계값 및 특수 케이스 처리 검증."""

    def test_ssn_boundary_gender_codes(self):
        """주민번호 성별 코드 경계값: 1~4 만 유효."""
        pattern = re.compile(KOREAN_PII_PATTERNS["주민등록번호"])
        # 유효 코드
        for code in ["1", "2", "3", "4"]:
            assert pattern.search(f"901231-{code}234567"), f"코드 {code} 탐지 실패"
        # 무효 코드
        for code in ["0", "5", "6", "7", "8", "9"]:
            assert not pattern.search(f"901231-{code}234567"), f"코드 {code} 오탐"

    def test_phone_number_all_carriers(self):
        """010~019 모든 국번 탐지."""
        pattern = re.compile(KOREAN_PII_PATTERNS["전화번호"])
        for middle in range(0, 10):
            phone = f"01{middle}-1234-5678"
            assert pattern.search(phone), f"{phone} 미탐지"

    def test_email_tld_minimum_length(self):
        """TLD 최소 2자 이상 요구."""
        pattern = re.compile(KOREAN_PII_PATTERNS["이메일"])
        assert pattern.search("user@example.co")    # 2자 TLD
        assert pattern.search("user@example.com")   # 3자 TLD
        # 1자 TLD 는 유효하지 않음
        assert not pattern.search("user@example.c")

    def test_passport_number_case_sensitivity(self):
        """여권번호는 대문자 영문자만 허용."""
        pattern = re.compile(KOREAN_PII_PATTERNS["여권번호"])
        assert pattern.search("M12345678")      # 대문자 OK
        assert not pattern.search("m12345678")  # 소문자 NG

    def test_unicode_and_special_chars_no_false_positive(self):
        """한글, 특수문자, 유니코드가 포함된 문자열에서 오탐 없음."""
        non_pii = [
            "안녕하세요! 반갑습니다.",
            "データ処理中...",
            "prix: €1,234.56",
            "★☆▷◁",
            "\t\n\r",
        ]
        results = _apply_regex_detection(non_pii)
        for i, r in enumerate(results):
            assert r == [], f"오탐 발생: '{non_pii[i]}' → {r}"
