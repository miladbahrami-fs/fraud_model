WITH money_in AS (
    SELECT to_binary_user_id
         , SUM(amount_usd) AS amount_usd
         , COUNT(order_id) AS cnt
         , COUNT(DISTINCT from_binary_user_id) AS user_count  
      FROM bi.bo_p2p_transaction 
     WHERE DATE(transaction_time) >= '2022-01-01'
     GROUP BY 1
)
, money_out AS (
    SELECT from_binary_user_id
         , SUM(amount_usd) AS amount_usd
         , COUNT(order_id) AS cnt
         , COUNT(DISTINCT to_binary_user_id) AS user_count  
      FROM bi.bo_p2p_transaction 
     WHERE DATE(transaction_time) >= '2022-01-01'
     GROUP BY 1 
)
SELECT COALESCE(from_binary_user_id, to_binary_user_id) AS binary_user_id
     , money_in.amount_usd AS amount_usd_in
     , money_in.cnt AS order_count_in
     , money_in.user_count AS distinct_sender_count
     , money_out.amount_usd AS amount_usd_out
     , money_out.cnt AS order_count_out
     , money_out.user_count AS distinct_receiver_count
  FROM money_in
  FULL JOIN money_out ON money_in.to_binary_user_id = money_out.from_binary_user_id
 ORDER BY amount_usd_in DESC