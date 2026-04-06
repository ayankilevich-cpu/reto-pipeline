-- Funciones auxiliares para inserciones idempotentes.

CREATE OR REPLACE FUNCTION upsert_crawl_job(
    p_job_id UUID,
    p_network TEXT,
    p_term TEXT,
    p_search_window tstzrange,
    p_status crawl_job_status DEFAULT 'pending',
    p_metadata JSONB DEFAULT '{}'::JSONB
) RETURNS UUID AS $$
BEGIN
    INSERT INTO crawl_jobs (job_id, network, term, search_window, status, metadata)
    VALUES (COALESCE(p_job_id, uuid_generate_v4()), p_network, p_term, p_search_window, p_status, p_metadata)
    ON CONFLICT (job_id) DO UPDATE
      SET network       = EXCLUDED.network,
          term          = EXCLUDED.term,
          search_window = EXCLUDED.search_window,
          status        = EXCLUDED.status,
          metadata      = COALESCE(crawl_jobs.metadata, '{}'::JSONB) || EXCLUDED.metadata,
          updated_at    = NOW()
    RETURNING job_id INTO p_job_id;
    RETURN p_job_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION upsert_raw_message(p_payload JSONB)
RETURNS UUID AS $$
DECLARE
    v_uuid UUID;
BEGIN
    INSERT INTO raw_messages (
        network,
        network_message_id,
        job_id,
        author_id,
        author_handle,
        author_name,
        content,
        content_clean,
        language,
        published_at,
        collected_at,
        permalink,
        like_count,
        reply_count,
        repost_count,
        quote_count,
        extra
    )
    VALUES (
        p_payload->>'network',
        p_payload->>'network_message_id',
        (p_payload->>'job_id')::UUID,
        p_payload->>'author_id',
        p_payload->>'author_handle',
        p_payload->>'author_name',
        p_payload->>'content',
        p_payload->>'content_clean',
        p_payload->>'language',
        (p_payload->>'published_at')::timestamptz,
        COALESCE((p_payload->>'collected_at')::timestamptz, NOW()),
        p_payload->>'permalink',
        NULLIF(p_payload->>'like_count', '')::INTEGER,
        NULLIF(p_payload->>'reply_count', '')::INTEGER,
        NULLIF(p_payload->>'repost_count', '')::INTEGER,
        NULLIF(p_payload->>'quote_count', '')::INTEGER,
        COALESCE(p_payload->'extra', '{}'::JSONB)
    )
    ON CONFLICT (network, network_message_id) DO UPDATE
      SET job_id         = EXCLUDED.job_id,
          author_id      = EXCLUDED.author_id,
          author_handle  = EXCLUDED.author_handle,
          author_name    = EXCLUDED.author_name,
          content        = EXCLUDED.content,
          content_clean  = EXCLUDED.content_clean,
          language       = EXCLUDED.language,
          published_at   = COALESCE(EXCLUDED.published_at, raw_messages.published_at),
          collected_at   = LEAST(raw_messages.collected_at, EXCLUDED.collected_at),
          permalink      = EXCLUDED.permalink,
          like_count     = GREATEST(COALESCE(raw_messages.like_count, 0), COALESCE(EXCLUDED.like_count, 0)),
          reply_count    = GREATEST(COALESCE(raw_messages.reply_count, 0), COALESCE(EXCLUDED.reply_count, 0)),
          repost_count   = GREATEST(COALESCE(raw_messages.repost_count, 0), COALESCE(EXCLUDED.repost_count, 0)),
          quote_count    = GREATEST(COALESCE(raw_messages.quote_count, 0), COALESCE(EXCLUDED.quote_count, 0)),
          extra          = COALESCE(raw_messages.extra, '{}'::JSONB) || COALESCE(EXCLUDED.extra, '{}'::JSONB),
          updated_at     = NOW()
    RETURNING message_uuid INTO v_uuid;

    RETURN v_uuid;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_term_hits(p_message_uuid UUID, p_hits JSONB)
RETURNS INT AS $$
DECLARE
    v_count INT := 0;
    v_hit JSONB;
BEGIN
    IF p_hits IS NULL OR jsonb_typeof(p_hits) <> 'array' THEN
        RETURN 0;
    END IF;

    FOR v_hit IN SELECT jsonb_array_elements(p_hits)
    LOOP
        INSERT INTO term_hits (
            message_uuid,
            job_id,
            term,
            term_variant,
            match_type,
            match_start,
            match_end,
            snippet
        )
        VALUES (
            p_message_uuid,
            NULLIF(v_hit->>'job_id', '')::UUID,
            v_hit->>'term',
            v_hit->>'variant',
            COALESCE(v_hit->>'match_type', 'exact'),
            NULLIF(v_hit->>'match_start', '')::INTEGER,
            NULLIF(v_hit->>'match_end', '')::INTEGER,
            v_hit->>'snippet'
        )
        ON CONFLICT DO NOTHING;
        v_count := v_count + 1;
    END LOOP;

    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

