-- ═══════════════════════════════════════════════════════════════
-- VIEW: mart_level_funnel_enriched
-- ═══════════════════════════════════════════════════════════════
-- Mục đích: Bổ sung tất cả calculated fields cần thiết cho
--           Drop Rate Analysis trên Looker Studio.
--           Connect view này vào Looker Studio thay vì bảng gốc.
--
-- Cách dùng:
--   1. Thay YOUR_PROJECT_ID bằng GCP Project ID thực tế
--   2. Chạy trên BigQuery Console
--   3. Trong Looker Studio → Add Data → BigQuery → chọn view này
--
-- Fields gốc (giữ nguyên từ mart_level_funnel):
--   level_number, users_completed, total_users,
--   completion_rate, avg_attempts, avg_best_score,
--   avg_days_to_complete
--
-- Fields mới (calculated):
--   prev_completion_rate  — completion_rate của level trước
--   drop_rate             — absolute drop (level N-1 → N)
--   drop_rate_pct         — relative drop (% so với level trước)
--   avg_drop_rate         — trung bình drop rate toàn bộ levels
--   drop_vs_avg           — drop_rate / avg_drop_rate (bội số)
--   difficulty_index      — composite score: attempts × (1 − rate)
--   pacing_gap            — chênh lệch ngày giữa 2 levels
--   severity              — phân loại: severe / warning / normal
--   severity_order        — sort helper: 1=severe, 2=warning, 3=normal
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `game-analytics-22.game_marts.mart_level_funnel_enriched`
AS
WITH base AS (
    SELECT
        level_number,
        users_completed,
        total_users,
        completion_rate,
        avg_attempts,
        avg_best_score,
        avg_days_to_complete,

        -- ── Previous level values (dùng LAG window function) ──
        LAG(completion_rate) OVER (ORDER BY level_number)
            AS prev_completion_rate,

        LAG(avg_days_to_complete) OVER (ORDER BY level_number)
            AS prev_days_to_complete,

    FROM `game-analytics-22.game_marts.mart_level_funnel`
),

with_drop AS (
    SELECT
        *,

        -- ══════════════════════════════════════════════
        -- 1. DROP RATE (absolute)
        -- Bao nhiêu % user base bị mất giữa level N-1 → N
        -- ══════════════════════════════════════════════
        ROUND(
            IFNULL(prev_completion_rate - completion_rate, 0),
            4
        ) AS drop_rate,

        -- ══════════════════════════════════════════════
        -- 2. DROP RATE PCT (relative)
        -- % user mất TƯƠNG ĐỐI so với level trước
        -- Ví dụ: level trước 40%, level này 30%
        --        → relative drop = (40-30)/40 = 25%
        -- ══════════════════════════════════════════════
        ROUND(
            SAFE_DIVIDE(
                prev_completion_rate - completion_rate,
                prev_completion_rate
            ),
            4
        ) AS drop_rate_pct,

        -- ══════════════════════════════════════════════
        -- 3. DIFFICULTY INDEX (composite)
        -- Kết hợp attempts + drop thành 1 chỉ số
        -- Cao = level vừa khó vừa nhiều người bỏ
        -- ══════════════════════════════════════════════
        ROUND(
            avg_attempts * (1 - completion_rate),
            4
        ) AS difficulty_index,

        -- ══════════════════════════════════════════════
        -- 4. PACING GAP
        -- Mất bao nhiêu ngày THÊM từ level trước → level này
        -- Spike = user bị kẹt quá lâu
        -- ══════════════════════════════════════════════
        ROUND(
            IFNULL(
                avg_days_to_complete - prev_days_to_complete,
                0
            ),
            2
        ) AS pacing_gap,

    FROM base
),

with_avg AS (
    SELECT
        *,

        -- ══════════════════════════════════════════════
        -- 5. AVG DROP RATE (global average)
        -- Mức drop trung bình qua tất cả levels
        -- Dùng làm reference line trên chart
        -- ══════════════════════════════════════════════
        ROUND(
            AVG(drop_rate) OVER (),
            4
        ) AS avg_drop_rate,

    FROM with_drop
)

SELECT
    -- ── Fields gốc ──
    level_number,
    users_completed,
    total_users,
    ROUND(completion_rate * 100, 2)   AS completion_rate_pct,   -- dạng % (0-100)
    completion_rate,                                             -- dạng ratio (0-1)
    avg_attempts,
    avg_best_score,
    avg_days_to_complete,

    -- ── Calculated fields ──
    prev_completion_rate,

    ROUND(drop_rate * 100, 2)         AS drop_rate_pct_display, -- dạng % (0-100) cho hiển thị
    drop_rate,                                                   -- dạng ratio cho tính toán

    ROUND(drop_rate_pct * 100, 2)     AS relative_drop_pct,     -- % tương đối

    ROUND(avg_drop_rate * 100, 2)     AS avg_drop_rate_pct,     -- AVG dạng %
    avg_drop_rate,

    -- ══════════════════════════════════════════════
    -- 6. DROP VS AVG (bội số)
    -- drop_rate / avg_drop_rate
    -- > 2.0 = severe, > 1.5 = warning
    -- ══════════════════════════════════════════════
    ROUND(
        SAFE_DIVIDE(drop_rate, avg_drop_rate),
        2
    ) AS drop_vs_avg,

    ROUND(difficulty_index * 100, 2)  AS difficulty_index,

    pacing_gap,

    -- ══════════════════════════════════════════════
    -- 7. SEVERITY (phân loại tự động)
    -- Dùng làm breakdown dimension trên Looker Studio
    -- để tô màu bars khác nhau
    -- ══════════════════════════════════════════════
    CASE
        WHEN SAFE_DIVIDE(drop_rate, avg_drop_rate) >= 2.0
            THEN 'severe'
        WHEN SAFE_DIVIDE(drop_rate, avg_drop_rate) >= 1.5
            THEN 'warning'
        ELSE 'normal'
    END AS severity,

    -- Sort helper: dùng để sort severity trong Looker
    CASE
        WHEN SAFE_DIVIDE(drop_rate, avg_drop_rate) >= 2.0 THEN 1
        WHEN SAFE_DIVIDE(drop_rate, avg_drop_rate) >= 1.5 THEN 2
        ELSE 3
    END AS severity_order,

    -- ══════════════════════════════════════════════
    -- 8. PLAYERS LOST (số tuyệt đối)
    -- Bao nhiêu user bị mất tại level này
    -- ══════════════════════════════════════════════
    ROUND(drop_rate * total_users)    AS players_lost,

FROM with_avg
ORDER BY level_number
;
