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
             , CASE WHEN ds.offerings = 'deriv_mobile_multipliers' THEN 'Deriv Go'
                    WHEN ds.offerings IS NULL THEN 'others'
                    ELSE ds.offerings END AS platform
             , CASE WHEN category IN ('Client Deposit','Payment Agent Deposit') THEN 'deposit'
                    WHEN category IN ('Client Withdrawal','Payment Agent Withdrawal') THEN 'withdrawal' END AS action
             , 1 AS action_count
             , amount_usd AS value
          FROM `business-intelligence-240201.bi.bo_payment_model` payment
          LEFT JOIN bi.dict_source ds on ds.id = payment.source
         WHERE category IN ('Client Withdrawal', 'Payment Agent Withdrawal'
                          , 'Client Deposit', 'Payment Agent Deposit')
               AND DATE(transaction_time) >= '2022-01-01') AS daily
GROUP BY date, binary_user_id, platform, action)
SELECT * FROM trades
UNION ALL
SELECT * FROM payments