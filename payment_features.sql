WITH trades AS (
    SELECT trades.date
         , binary_user_id
         , platform
         , 'trade' AS action
         , SUM(COALESCE(number_of_trades,0)) AS action_count
         , SUM(CAST(COALESCE(closed_pnl_usd,0) AS INT64)) AS value
         , MAX(COALESCE(risk.risk_factor,0)) AS action_suspiciency
      FROM bi.trades
      LEFT JOIN (
            SELECT date
                 , asset
                 , SUM(clinet_win_count) OVER w
                 , SUM(total_contracts) OVER w
                 , ROUND(SAFE_DIVIDE(SUM(clinet_win_count) OVER w, SUM(total_contracts) OVER w),2) AS risk_factor
              FROM (
                    SELECT date_actual AS date
                         , asset
                         , SUM(COALESCE(client_win_count,0)) AS clinet_win_count
                         , SUM(COALESCE(total_contracts,0)) AS total_contracts
                      FROM bi.dimension_calendar dc
                      LEFT JOIN (
                            SELECT pnl.sell_txn_date
                                 , pnl.underlying_symbol
                                 , client_win_count
                                 , total_contracts
                                 , us.display_name AS asset
                              FROM bi.mv_bo_pnl_summary pnl
                              JOIN dictionary.underlying_symbol us ON pnl.underlying_symbol = us.symbol
                             WHERE year_month >= '2022-01-01'
                              ) AS asset_pnl ON dc.date_actual = asset_pnl.sell_txn_date
                     GROUP BY 1, 2
                     ORDER BY 2 DESC
                   )
             WINDOW w AS (PARTITION BY asset ORDER BY date ROWS BETWEEN 30 PRECEDING and CURRENT ROW )
          ) AS risk
        ON trades.asset = risk.asset AND trades.date = risk.date
     WHERE trades.date >= '2022-01-01'
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
                 , ROUND(SAFE_DIVIDE(SUM(approved_count) OVER w, SUM(all_count) OVER w),2) AS success_rate
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
                      WHERE dc.date_actual >= '2022-01-01' AND dc.date_actual <= current_date
                        AND transactions.transaction_date >= '2022-01-01'
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
         , SUM(COALESCE(action_count,0)) AS action_count
         , SUM(COALESCE(value,0)) AS value
         , MAX(COALESCE(risk_rank,1)) AS action_suspiciency
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
                 , payment.currency_code
                 , pp_success.risk_rank
              FROM bi.bo_payment_model payment
              LEFT JOIN pp_success ON pp_success.payment_processor = payment.payment_processor and DATE(payment.transaction_time) = pp_success.date 
              LEFT JOIN bi.dict_source ds ON ds.id = payment.source
             WHERE category IN ('Client Withdrawal', 'Payment Agent Withdrawal'
                              , 'Client Deposit', 'Payment Agent Deposit')
               AND DATE(transaction_time) >= '2022-01-01'
        ) AS daily
     GROUP BY date, binary_user_id, platform, action
)
SELECT * FROM trades
UNION ALL
SELECT * FROM payments