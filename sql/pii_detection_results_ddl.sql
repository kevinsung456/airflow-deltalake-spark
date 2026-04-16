-- ============================================================
-- PII 탐지 결과 저장 Delta Table DDL
-- sql/pii_detection_results_ddl.sql
--
-- 설계 결정 근거:
--   1. PARTITIONED BY (dag_id, DATE(detected_at))
--      → DAG 별·날짜별 쿼리 패턴에 최적화된 파티션 전략
--        (예: 특정 DAG 의 최근 7일치 탐지 이력 조회)
--   2. delta.autoOptimize.optimizeWrite = true
--      → 소규모 파일 자동 병합으로 Small File Problem 예방
--   3. delta.autoOptimize.autoCompact = true
--      → 백그라운드 Compaction 으로 읽기 성능 유지
--   4. delta.logRetentionDuration = 'interval 30 days'
--      → 30일치 트랜잭션 로그 보존 (Time Travel, RESTORE 가능 범위)
--   5. MERGE 키: (run_id, column_name, pii_type)
--      → 같은 실행(run_id)에서 동일 컬럼/타입 조합은 upsert 처리
--        → 재실행(Airflow Clear) 시에도 결과 중복 없이 갱신
-- ============================================================

-- ──────────────────────────────────────────────────────────
-- 1. 결과 저장 테이블
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pii_detection_results (
    -- Airflow 실행 컨텍스트
    run_id          STRING  NOT NULL COMMENT 'Airflow DAG Run ID',
    dag_id          STRING  NOT NULL COMMENT 'Airflow DAG ID',
    task_id         STRING  NOT NULL COMMENT 'Airflow Task ID',

    -- 탐지 대상
    source_table    STRING  NOT NULL COMMENT '소스 Delta 테이블 경로 또는 이름',
    column_name     STRING  NOT NULL COMMENT 'PII 가 탐지된 컬럼명',
    pii_type        STRING  NOT NULL COMMENT '탐지된 PII 유형 (예: 주민등록번호, EMAIL)',

    -- 탐지 통계
    sample_count    BIGINT  NOT NULL COMMENT '해당 PII 유형이 탐지된 행 수',
    total_rows      BIGINT  NOT NULL COMMENT '전체 행 수',
    detection_ratio DOUBLE  NOT NULL COMMENT '탐지 비율 = sample_count / total_rows',

    -- 메타
    detected_at     TIMESTAMP NOT NULL COMMENT '탐지 수행 시각 (UTC)',
    action_taken    STRING  NOT NULL COMMENT '수행된 액션 (report|mask|quarantine|fail)'
)
USING DELTA
PARTITIONED BY (dag_id)
COMMENT 'PIIDetectionOperator 탐지 결과 이력 테이블'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.logRetentionDuration'       = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);


-- ──────────────────────────────────────────────────────────
-- 2. MERGE INTO 템플릿
--    PIIDetectionOperator._save_detection_results() 에서 사용하는 패턴
--    (참고용 — 실제 실행은 Operator Python 코드에서 동적 생성)
-- ──────────────────────────────────────────────────────────
/*
MERGE INTO pii_detection_results AS target
USING (
    SELECT
        '<run_id>'           AS run_id,
        '<dag_id>'           AS dag_id,
        '<task_id>'          AS task_id,
        '<source_table>'     AS source_table,
        '<column_name>'      AS column_name,
        '<pii_type>'         AS pii_type,
        <sample_count>       AS sample_count,
        <total_rows>         AS total_rows,
        <detection_ratio>    AS detection_ratio,
        current_timestamp()  AS detected_at,
        '<action_taken>'     AS action_taken
) AS source
ON  target.run_id      = source.run_id
AND target.column_name = source.column_name
AND target.pii_type    = source.pii_type
WHEN MATCHED THEN
    UPDATE SET
        target.sample_count    = source.sample_count,
        target.total_rows      = source.total_rows,
        target.detection_ratio = source.detection_ratio,
        target.detected_at     = source.detected_at,
        target.action_taken    = source.action_taken
WHEN NOT MATCHED THEN
    INSERT *;
*/


-- ──────────────────────────────────────────────────────────
-- 3. 운영 쿼리 예시
-- ──────────────────────────────────────────────────────────

-- 최근 7일간 PII 탐지율 TOP 10 컬럼 조회
/*
SELECT
    source_table,
    column_name,
    pii_type,
    MAX(detection_ratio)   AS max_detection_ratio,
    MAX(sample_count)      AS max_sample_count,
    COUNT(DISTINCT run_id) AS detection_count
FROM pii_detection_results
WHERE detected_at >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
GROUP BY source_table, column_name, pii_type
ORDER BY max_detection_ratio DESC
LIMIT 10;
*/

-- 특정 DAG 의 탐지 이력 Time Travel 조회 (Delta 기능)
/*
SELECT *
FROM pii_detection_results
VERSION AS OF 5          -- 5번째 버전 시점의 스냅샷
WHERE dag_id = 'my_pipeline_dag';
*/

-- OPTIMIZE + ZORDER (운영 주기 실행 권장: 주 1회)
/*
OPTIMIZE pii_detection_results
ZORDER BY (dag_id, detected_at);
*/
