/*
Propensão a churn (queda de uso) — PostgreSQL / BigQuery
Objetivo: sinalizar contas com risco com base em queda de uso + tickets + NPS negativo.

Dependências (exemplo):
- events(account_id, event_name, event_time)
- tickets(account_id, opened_at)
- nps(account_id, survey_date, score)  -- score: 0..10
- subscriptions(account_id, mrr, status)

Parâmetros (ajuste):
- janela_30d vs janela_7d para comparar uso recente vs base.
*/

-- BigQuery: remova a função DATE_TRUNC('month', ...) e use DATE_TRUNC(date, MONTH) quando necessário.
-- Postgres: ok como está.

WITH
base AS (
  SELECT
    e.account_id,
    COUNTIF(e.event_time >= CURRENT_DATE - INTERVAL '30 day') AS ev_30d,
    COUNTIF(e.event_time >= CURRENT_DATE - INTERVAL '7 day')  AS ev_7d
  FROM events e
  WHERE e.event_name IN ('feature_used','session_start')
  GROUP BY 1
),
queda AS (
  SELECT
    account_id,
    ev_30d,
    ev_7d,
    CASE
      WHEN ev_30d = 0 THEN 1.0
      ELSE (ev_7d::numeric / NULLIF(ev_30d/4.0,0)) -- normaliza 7d vs média semanal de 30d
    END AS uso_ratio_semana
  FROM base
),
sinais AS (
  SELECT
    a.account_id,
    COALESCE(q.uso_ratio_semana, 0) AS uso_ratio_semana,
    COALESCE(t.tickets_30d, 0)      AS tickets_30d,
    COALESCE(n.nps_neg_90d, 0)      AS nps_neg_90d,
    s.mrr,
    s.status
  FROM (SELECT DISTINCT account_id FROM events) a
  LEFT JOIN queda q USING (account_id)
  LEFT JOIN (
    SELECT account_id, COUNT(*) AS tickets_30d
    FROM tickets
    WHERE opened_at >= CURRENT_DATE - INTERVAL '30 day'
    GROUP BY 1
  ) t USING (account_id)
  LEFT JOIN (
    SELECT account_id, COUNTIF(score <= 6) AS nps_neg_90d
    FROM nps
    WHERE survey_date >= CURRENT_DATE - INTERVAL '90 day'
    GROUP BY 1
  ) n USING (account_id)
  LEFT JOIN subscriptions s USING (account_id)
),
score AS (
  SELECT
    account_id,
    status,
    mrr,
    -- Scoring simples (0..100). Ajuste pesos conforme seu produto.
    LEAST(
      100,
      (CASE WHEN uso_ratio_semana < 0.4 THEN 60
            WHEN uso_ratio_semana < 0.7 THEN 40
            WHEN uso_ratio_semana < 0.9 THEN 20
            ELSE 0 END)
      + (LEAST(tickets_30d, 5) * 4)      -- até 20 pontos
      + (LEAST(nps_neg_90d, 3) * 6)      -- até 18 pontos
    ) AS churn_score
  FROM sinais
)
SELECT
  account_id,
  status,
  mrr,
  churn_score,
  CASE
    WHEN churn_score >= 60 THEN 'alto'
    WHEN churn_score >= 35 THEN 'medio'
    ELSE 'baixo'
  END AS risco_churn
FROM score
WHERE status = 'active'
ORDER BY churn_score DESC;
