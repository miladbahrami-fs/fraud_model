WITH trades AS (
    SELECT date
         , binary_user_id
         , platform
         , 'trade' AS action
         , SUM(number_of_trades) AS action_count
         , SUM(CAST(closed_pnl_usd AS INT64)) AS value
         -- action suspiciency
      FROM bi.trades
     WHERE date = '2022-01-01'
     GROUP BY date, binary_user_id, platform, action)
, pp_success AS (
    SELECT date
         , daily.payment_processor
         , success_rate
         , COALESCE(rk.risk_rank,1) AS risk_rank
         , COALESCE(rk.level_rank,'low') AS level_rank
      FROM ( 
            SELECT date
                 , payment_processor
                 , SUM(approved_count) OVER w AS approved 
                 , SUM(all_count) OVER w AS all_transactions
                 , SAFE_DIVIDE(SUM(approved_count) OVER w, SUM(all_count) OVER w) AS success_rate
              FROM (
                    SELECT date_actual AS date
                         , CASE
                            WHEN REGEXP_CONTAINS(pp, "CardPay") then "CardPay"
                            WHEN REGEXP_CONTAINS(pp, "Acquired") then "Acquired"
                            WHEN REGEXP_CONTAINS(pp, "VirtualPay") then "VirtualPay"
                            WHEN REGEXP_CONTAINS(pp, "Ecommpay") then "Ecommpay"
                            WHEN REGEXP_CONTAINS(pp, "ISignThis") then "ISignThis"
                            WHEN REGEXP_CONTAINS(pp, "DCash") then "DCash"
                            WHEN REGEXP_CONTAINS(pp, "Apexx") then "Apexx"
                            WHEN REGEXP_CONTAINS(pp, "Fiserv") then "Fiserv"
                            ELSE 'PreAuth' 
                           END AS payment_processor_group
                         , REGEXP_EXTRACT(pp,r'\b(\w+)$') AS payment_processor
                         , SUM(IF(COALESCE(Approved,'N')='Y',1,0)) AS approved_count
                         , SUM(IF(TraceID IS NULL,0,1)) AS all_count
                      FROM bi.dimension_calendar dc
                      LEFT JOIN bi.premier_cashier_transactions transactions on transactions.transaction_date = dc.date_actual
                     GROUP BY 1,2,3
                    )
           WINDOW w AS (PARTITION BY payment_processor ORDER BY date ROWS BETWEEN 30 PRECEDING and CURRENT ROW) 
           ) AS daily
      LEFT JOIN doughflow.payment_processor_risk_category_external rk 
        ON daily.payment_processor = rk.payment_processor
       AND date between rk.date_risk_start and rk.date_risk_end
)
, payments AS (
    SELECT date
         , binary_user_id
         , platform
         , action
         , SUM(action_count) AS action_count
         , SUM(value) AS value
          -- action suspiciency
      FROM (
            SELECT DATE(transaction_time) AS date
                 , binary_user_id
                 , CASE 
                    WHEN ds.offerings = 'deriv_mobile_multipliers' THEN 'Deriv Go'
                    WHEN ds.offerings IS NULL THEN 'others'
                    ELSE ds.offerings END AS platform
                 , CASE 
                    WHEN category IN ('Client Deposit','Payment Agent Deposit') THEN 'deposit'
                    WHEN category IN ('Client Withdrawal','Payment Agent Withdrawal') THEN 'withdrawal' END AS action
                 , 1 AS action_count
                 , amount_usd AS value
              FROM bi.bo_payment_model payment
              LEFT JOIN bi.dict_source ds on ds.id = payment.source
             WHERE category IN ('Client Withdrawal', 'Payment Agent Withdrawal'
                              , 'Client Deposit', 'Payment Agent Deposit')
               AND DATE(transaction_time) >= '2022-01-01'
        ) AS daily
     GROUP BY date, binary_user_id, platform, action
)
-- , pp AS (
--   SELECT * 
--     FROM payments
--     LEFT JOIN doughflow.payment_processor_risk_category_external rk 
--       ON payments.payment_processor = rk.payment_processor
--    AND DATE(bo.transaction_date) between rk.date_risk_start and rk.date_risk_end
-- )
SELECT * FROM trades
UNION ALL
SELECT * FROM payments