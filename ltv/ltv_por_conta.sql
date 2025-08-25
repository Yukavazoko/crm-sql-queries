/*
LTV por conta — PostgreSQL / BigQuery
Objetivo: calcular LTV com base em MRR e churn, por segmento.

Dependências:
- subscriptions(account_id, start_date, cancel_date, mrr, plan, segment)
- payments(account_id, paid_at, amount)  -- opcional para receita real

Estratégias:
- LTV_mrr = MRR * 1/churn_rate_mensal (método de aproximação)
- Opcional: LTV_real = SUM(amount) ao longo da vida (se tiver payments completos)
*/

WITH
base AS (
  SELECT
    account_id,
    segment,
    plan,
    mrr,
    start_date,
    COALESCE(cancel_date, CURRENT_DATE) AS end_date,
    GREATEST(1, DATE_PART('month', AGE(COALESCE(cancel_date, CURRENT_DATE), start_date))) AS meses_vida
  FROM subscriptions
),
churn_rate AS (
  -- churn mensal por segmento (simples). Ajuste janela e filtro.
  SELECT
    segment,
    NULLIF(
      AVG(
        CASE WHEN cancel_date IS NOT NULL THEN 1 ELSE 0 END
      ), 0
    ) AS churn_rate_mensal
  FROM subscriptions
  GROUP BY 1
),
ltv_calc AS (
  SELECT
    b.account_id,
    b.segment,
    b.plan,
    b.mrr,
    b.meses_vida,
    cr.churn_rate_mensal,
    CASE
      WHEN cr.churn_rate_mensal IS NULL THEN b.mrr * b.meses_vida
      ELSE b.mrr * (1.0 / cr.churn_rate_mensal)
    END AS ltv_mrr_estimado
  FROM base b
  LEFT JOIN churn_rate cr USING (segment)
),
ltv_real AS (
  SELECT account_id, SUM(amount) AS ltv_real
  FROM payments
  GROUP BY 1
)
SELECT
  c.account_id,
  c.segment,
  c.plan,
  c.mrr,
  c.meses_vida,
  ROUND(c.ltv_mrr_estimado, 2) AS ltv_mrr_estimado,
  ROUND(r.ltv_real, 2)         AS ltv_real_opcional
FROM ltv_calc c
LEFT JOIN ltv_real r USING (account_id)
ORDER BY ltv_mrr_estimado DESC NULLS LAST;
