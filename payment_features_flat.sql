WITH trades AS (
    SELECT trades.date
         , binary_user_id
         , SUM(COALESCE(number_of_trades, 0)) AS action_count
         , SUM(CAST(COALESCE(closed_pnl_usd, 0) AS INT64)) AS value
         , MAX(COALESCE(risk.risk_factor, 0)) AS action_suspiciency
         , APPROX_TOP_COUNT(asset_type, 1)[offset(0)].value AS major_asset_type
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
                         , SUM(COALESCE(client_win_count, 0)) AS clinet_win_count
                         , SUM(COALESCE(total_contracts, 0)) AS total_contracts
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
     GROUP BY date, binary_user_id )
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
                            WHEN REGEXP_CONTAINS(pp, "CardPay") THEN "CardPay"
                            WHEN REGEXP_CONTAINS(pp, "Acquired") THEN "Acquired"
                            WHEN REGEXP_CONTAINS(pp, "VirtualPay") THEN "VirtualPay"
                            WHEN REGEXP_CONTAINS(pp, "Ecommpay") THEN "Ecommpay"
                            WHEN REGEXP_CONTAINS(pp, "ISignThis") THEN "ISignThis"
                            WHEN REGEXP_CONTAINS(pp, "DCash") THEN "DCash"
                            WHEN REGEXP_CONTAINS(pp, "Apexx") THEN "Apexx"
                            WHEN REGEXP_CONTAINS(pp, "Fiserv") THEN "Fiserv"
                            ELSE 'PreAuth' 
                           END AS payment_processor_group
                         , REGEXP_EXTRACT(pp, r'\b(\w+)$') AS payment_processor
                         , SUM(IF(COALESCE(Approved,'N')='Y', 1, 0)) AS approved_count
                         , SUM(IF(TraceID IS NULL, 0, 1)) AS all_count
                      FROM bi.dimension_calendar dc
                      LEFT JOIN bi.premier_cashier_transactions transactions on transactions.transaction_date = dc.date_actual
                     WHERE dc.date_actual >= '2022-01-01' AND dc.date_actual <= current_date
                       AND transactions.transaction_date >= '2022-01-01'
                     GROUP BY 1,2,3
                    )
           WINDOW w AS (PARTITION BY payment_processor ORDER BY date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) 
           ) AS daily
      LEFT JOIN doughflow.payment_processor_risk_category_external rk 
        ON daily.payment_processor = rk.payment_processor
       AND date between rk.date_risk_start and rk.date_risk_end
)
, card_issuer_pp AS (
    SELECT residence
         , card_issuer
         , cnt
         , 1 - ROUND(SUM(COALESCE(cnt)) OVER(PARTITION BY residence,card_issuer)/SUM(COALESCE(cnt)) OVER(PARTITION BY residence), 2) AS card_issuer_risk
      FROM (
            SELECT residence
                 , card_issuer
                 , COUNT(card_issuer) cnt
              FROM bi.premier_cashier_transactions pct
              JOIN bi.bo_client ON bo_client.loginid = pct.PIN
             WHERE card_issuer IS NOT NULL AND residence IS NOT NULL
             GROUP BY residence, card_issuer
      )
)
, payments AS (
    SELECT date
         , binary_user_id
         , action
         , SUM(COALESCE(action_count, 0)) AS action_count
         , SUM(COALESCE(value, 0)) AS value
         , MAX(COALESCE(risk_rank, 1)) AS action_suspiciency
         , APPROX_TOP_COUNT(currency_code, 1)[offset(0)].value AS major_currency
         , APPROX_TOP_COUNT(payment_processor, 1)[offset(0)].value AS major_payment_processor
         , MAX(COALESCE(card_issuer_risk,0)) AS card_issuer_popularity
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
                 , payment.payment_processor
                 , pp_success.risk_rank
                 , card_issuer_pp.card_issuer_risk
              FROM (
                    SELECT transaction_time
                         , payment.binary_user_id
                         , category,amount_usd
                         , currency_code
                         , source
                         , payment_processor
                         , user_profile.residence
                      FROM bi.bo_payment_model payment 
                      JOIN bi.user_profile ON user_profile.binary_user_id = payment.binary_user_id
                    ) AS payment
              JOIN card_issuer_pp ON card_issuer_pp.residence = payment.residence 
              LEFT JOIN pp_success ON pp_success.payment_processor = payment.payment_processor 
                   AND DATE(payment.transaction_time) = pp_success.date 
              LEFT JOIN bi.dict_source ds ON ds.id = payment.source
             WHERE category IN ('Client Withdrawal', 'Payment Agent Withdrawal'
                              , 'Client Deposit', 'Payment Agent Deposit')
               AND DATE(transaction_time) >= '2022-01-01'
        ) AS daily
     GROUP BY date, binary_user_id, action
)
SELECT COALESCE(trades.date, payments_deposit.date, payments_withdrawal.date) AS date
     , COALESCE(trades.binary_user_id, payments_deposit.binary_user_id, payments_withdrawal.binary_user_id) AS binary_user_id
     , payments_deposit.action_count AS deposit_count
     , payments_deposit.value AS deposit_value
     , payments_deposit.action_suspiciency AS deposit_risk
     , payments_deposit.major_currency AS deposit_major_currency
     , payments_deposit.major_payment_processor AS deposit_major_pp
     , payments_withdrawal.action_count AS withdrawal_count
     , -payments_withdrawal.value AS withdrawal_value
     , payments_withdrawal.action_suspiciency AS withdrawal_risk 
     , payments_withdrawal.major_currency AS withdrawal_major_currency
     , payments_withdrawal.major_payment_processor AS withdrawal_major_pp
     , trades.action_count AS trade_count
     , trades.value AS trade_pnl
     , trades.action_suspiciency AS trade_risk
     , trades.major_asset_type AS trade_major_asset_type
     
FROM trades
FULL JOIN (SELECT * FROM payments WHERE action='deposit') AS payments_deposit 
      ON payments_deposit.binary_user_id = trades.binary_user_id AND payments_deposit.date = trades.date
FULL JOIN (SELECT * FROM payments WHERE action='withdrawal') AS payments_withdrawal 
      ON payments_withdrawal.binary_user_id = trades.binary_user_id AND payments_withdrawal.date = trades.date
ORDER BY 2,1