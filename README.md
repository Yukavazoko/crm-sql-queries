# 📊 CRM SQL Snippets

Este repositório reúne **consultas SQL práticas** usadas em estratégias de CRM e Growth:

- **Segmentação de churn** → identificar contas com risco de cancelamento.  
- **LTV por cliente** → cálculo de lifetime value por segmento.  
- **Scoring de leads** → pontuação baseada em comportamento e fit.  
- **Análises de retenção** → cohort analysis e curvas de engajamento.  

---

### 🎯 Objetivo
Criar uma base simples e prática de queries que podem ser adaptadas para **B2B SaaS** e usadas em ferramentas como **BigQuery, PostgreSQL e MySQL**.

---

## 🔧 Convenções
- Ajuste os nomes de tabelas/campos conforme seu DW/CRM:
  - `events` (eventos do produto/marketing)
  - `accounts` (contas/clientes)
  - `subscriptions` (assinaturas/planos)
  - `leads` (contatos/leads)
- Datas em UTC.  
- Onde houver `-- TODO:` personalize para seu caso.

💡 Este repositório complementa o [CRM Playbook](https://github.com/Yukavazoko/crm-playbook).

---

### 📂 Estrutura do Repositório
```text
sql-snippets/
├─ churn/
│  └─ propensao_churn.sql
├─ ltv/
│  └─ ltv_por_conta.sql
├─ scoring/
│  └─ lead_scoring.sql
└─ retention/
   └─ cohort_retention.sql
