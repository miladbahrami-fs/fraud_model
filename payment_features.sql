WITH trades AS (
SELECT date
     , binary_user_id
     , platform
     , 'trade' AS action
     , number_of_trades AS action_count
     , CAST(closed_pnl_usd AS INT64) AS value
     -- asset suspiciuncy
  FROM bi.trades
 WHERE date = '2022-01-01')
, payments AS (
SELECT date
     , binary_user_id
  FROM(
SELECT DATE(transaction_time) AS date
     , binary_user_id
     , SUM(IF(category IN ('Client Deposit','Payment Agent Deposit')
             , amount_usd
             , 0)) AS deposit_usd
     , SUM(IF(category IN ('Client Withdrawal','Payment Agent Withdrawal')
             , amount_usd
             , 0)) AS withdrawal_usd
     , SUM(IF(category IN ('Client Deposit','Payment Agent Deposit')
             , 1
             , 0)) AS deposit_count
     , SUM(IF(category IN ('Client Withdrawal','Payment Agent Withdrawal')
             , 1
             , 0)) AS withdrawal_count   
  FROM `business-intelligence-240201.bi.bo_payment_model`
 WHERE category IN ('Client Withdrawal', 'Payment Agent Withdrawal'
                   , 'Client Deposit', 'Payment Agent Deposit')
       AND DATE(transaction_time)>='2022-01-01'
 GROUP BY 1, 2
  ) AS daily)