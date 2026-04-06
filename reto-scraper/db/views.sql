-- Vistas para anonimización y reporting.

CREATE OR REPLACE VIEW v_message_anonymized AS
WITH salt AS (
    SELECT COALESCE(current_setting('app.anonym_salt', true), '') AS val
)
SELECT
    rm.message_uuid,
    rm.network,
    encode(digest(COALESCE(rm.author_id, rm.author_handle, 'unknown') || salt.val, 'sha256'), 'hex') AS author_hash,
    encode(digest(COALESCE(rm.network_message_id, rm.message_uuid::TEXT) || salt.val, 'sha256'), 'hex') AS message_hash,
    rm.content,
    rm.content_clean,
    rm.language,
    rm.published_at,
    rm.collected_at,
    rm.permalink,
    rm.like_count,
    rm.reply_count,
    rm.repost_count,
    rm.quote_count,
    rm.extra,
    th.term,
    th.term_variant,
    th.match_type,
    th.snippet
FROM raw_messages rm
LEFT JOIN term_hits th ON th.message_uuid = rm.message_uuid,
salt;

CREATE OR REPLACE VIEW v_dashboard_summary AS
SELECT
    date_trunc('day', rm.collected_at) AS collected_day,
    rm.network,
    COUNT(DISTINCT rm.message_uuid) AS messages_collected,
    COUNT(th.hit_id) AS term_hits_total,
    COUNT(DISTINCT th.term) AS unique_terms,
    COUNT(DISTINCT ld.labeled_id) AS labeled_messages,
    SUM(CASE WHEN ld.label = 'hate' THEN 1 ELSE 0 END) AS labeled_hate,
    AVG(EXTRACT(EPOCH FROM (rm.collected_at - rm.published_at))) FILTER (
        WHERE rm.published_at IS NOT NULL
    ) / 60 AS avg_latency_minutes,
    COUNT(*) FILTER (WHERE cj.status = 'failed') AS failed_jobs
FROM raw_messages rm
LEFT JOIN term_hits th ON th.message_uuid = rm.message_uuid
LEFT JOIN labeled_data ld ON ld.message_uuid = rm.message_uuid
LEFT JOIN crawl_jobs cj ON cj.job_id = rm.job_id
GROUP BY collected_day, rm.network
ORDER BY collected_day DESC, rm.network;

