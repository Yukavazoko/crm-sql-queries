/*
Lead Scoring híbrido (fit + comportamento) — PostgreSQL / BigQuery
Objetivo: pontuar leads com base em ICP (fit) e sinais de intenção (comportamento).

Dependências:
- leads(lead_id, email, company_size, industry, role, country, source, created_at)
- web_events(lead_id, event_name, page, event_time)
- emails(lead_id, event, occurred_at)  -- event: delivered/open/click/optout

Regras exemplo (ajuste pesos):
- Fit: tamanho da empresa/indústria/papel
- Comportamento: visita /pricing, clique em email, form submit
*/

WITH
fit AS (
  SELECT
    l.lead_id,
    -- pesos de ICP (exemplos)
    (CASE
      WHEN l.company_size BETWEEN 11 AND 200 THEN 15
      WHEN l.company_size > 200 THEN 10
      ELSE 5 END) +
    (CASE
      WHEN l.industry IN ('SaaS','EduTech','FinTech') THEN 10 ELSE 5 END) +
    (CASE
      WHEN l.role IN ('Head','Manager','Director','Founder') THEN 10 ELSE 4 END)
    AS fit_score
  FROM leads l
),
beh AS (
  SELECT
    w.lead_id,
    SUM(CASE WHEN w.page LIKE '%/pricing%' THEN 15 ELSE 0 END) +
    SUM(CASE WHEN w.page LIKE '%/produto%' THEN 10 ELSE 0 END) +
    SUM(CASE WHEN w.event_name = 'form_submitted' THEN 20 ELSE 0 END) +
    SUM(CASE WHEN e.event = 'click' THEN 5 ELSE 0 END)
    AS beh_score
  FROM web_events w
  LEFT JOIN emails e ON e.lead_id = w.lead_id AND e.occurred_at::date = w.event_time::date
  WHERE w.event_time >= CURRENT_DATE - INTERVAL '30 day'
  GROUP BY 1
),
score AS (
  SELECT
    coalesce(f.lead_id, b.lead_id) AS lead_id,
    COALESCE(f.fit_score, 0) AS fit_score,
    COALESCE(b.beh_score, 0) AS beh_score,
    LEAST(100, COALESCE(f.fit_score,0) + COALESCE(b.beh_score,0)) AS total_score
  FROM fit f
  FULL OUTER JOIN beh b ON b.lead_id = f.lead_id
)
SELECT
  s.lead_id,
  s.fit_score,
  s.beh_score,
  s.total_score,
  CASE
    WHEN s.total_score >= 70 THEN 'MQL'
    WHEN s.total_score >= 40 THEN 'Quase MQL'
    ELSE 'Nurture'
  END AS stage_sugestao
FROM score s
ORDER BY total_score DESC;
