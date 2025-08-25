/*
Cohort de retenção mensal — PostgreSQL / BigQuery
Objetivo: medir % de contas ativas por mês desde o mês de aquisição.

Dependências:
- accounts(account_id, signup_date)
- events(account_id, event_time, event_name)  -- sinal de atividade (ex.: 'session_start' ou 'feature_used')

Definições:
- Ativo em M = teve ao menos 1 evento no mês M.
*/

WITH
cohort AS (
  SELECT
    account_id,
    DATE_TRUNC('month', signup_date)::date AS cohort_month
  FROM accounts
),
atividade AS (
  SELECT
    e.account_id,
    DATE_TRUNC('month', e.event_time)::date AS evt_month,
    COUNT(*) AS evts
  FROM events e
  WHERE e.event_name IN ('session_start','feature_used')
  GROUP BY 1,2
),
calendario AS (
  SELECT generate_series(
    (SELECT MIN(cohort_month) FROM cohort),
    DATE_TRUNC('month', CURRENT_DATE)::date,
    INTERVAL '1 month'
  )::date AS month
),
joined AS (
  SELECT
    c.account_id,
    coh.cohort_month,
    cal.month AS ref_month,
    (EXTRACT(YEAR FROM cal.month) - EXTRACT(YEAR FROM coh.cohort_month)) * 12
      + (EXTRACT(MONTH FROM cal.month) - EXTRACT(MONTH FROM coh.cohort_month)) AS month_index,
    COALESCE(a.evts, 0) AS evts
  FROM cohort coh
  CROSS JOIN calendario cal
  JOIN accounts c ON c.account_id = coh.account_id
  LEFT JOIN atividade a ON a.account_id = coh.account_id AND a.evt_month = cal.month
  WHERE cal.month >= coh.cohort_month
),
agg AS (
  SELECT
    cohort_month,
    month_index,
    COUNT(*) FILTER (WHERE month_index = 0) AS base_cohort,
    COUNT(*) FILTER (WHERE evts > 0)     AS ativos
  FROM joined
  GROUP BY 1,2
)
SELECT
  cohort_month,
  month_index,
  CASE WHEN base_cohort = 0 THEN 0 ELSE ROUND(100.0 * ativos / base_cohort, 2) END AS retention_pct
FROM agg
ORDER BY cohort_month, month_index;
