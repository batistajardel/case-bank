-- Active: 1692300127744@@127.0.0.1@5432@transactions@public
-- Criando tabela de associados agregada

CREATE TABLE Associado_Info AS 
WITH AssociadoTransacoes AS (
    SELECT
        t.associado_id,
        COUNT(DISTINCT DATE_TRUNC('month', t.dat_transacao)) AS meses_transacao
    FROM Transacoes t
    WHERE t.dat_transacao >= CURRENT_DATE - INTERVAL '3 months'
    GROUP BY t.associado_id
),

AssociadoCredito AS (
    SELECT DISTINCT associado_id
    FROM Transacoes
    WHERE nom_modalidade = 'CREDITO'
      AND dat_transacao >= CURRENT_DATE - INTERVAL '3 months'
),

AssociadoDebito AS (
    SELECT DISTINCT associado_id
    FROM Transacoes
    WHERE nom_modalidade = 'DEBITO'
      AND dat_transacao >= CURRENT_DATE - INTERVAL '3 months'
)
SELECT
    a.num_cpf_cnpj,
    a.des_nome_associado,
    a.dat_associacao,
    a.cod_faixa_renda,
    a.des_faixa_renda,
    CASE
        WHEN at.meses_transacao >= 3 THEN True
        ELSE False
    END AS associado_frequente,
    CASE
        WHEN ac.associado_id IS NOT NULL THEN True
        ELSE False
    END AS associado_ativo_credito,
    CASE
        WHEN ad.associado_id IS NOT NULL THEN True
        ELSE False
    END AS associado_ativo_debito
FROM Associado a
LEFT JOIN AssociadoTransacoes at ON a.num_cpf_cnpj = at.associado_id
LEFT JOIN AssociadoCredito ac ON a.num_cpf_cnpj = ac.associado_id
LEFT JOIN AssociadoDebito ad ON a.num_cpf_cnpj = ad.associado_id;