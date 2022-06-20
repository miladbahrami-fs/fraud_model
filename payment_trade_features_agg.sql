WITH user_daily_summary AS (
    WITH bo_trades AS (
        SELECT binary_user_id
             , SUM(sum_buy_price_usd) AS bo_turnover_usd
             , SUM(winning_turnover_usd) AS bo_winning_turnover_usd
             , SUM(client_win_count) As bo_win_count
             , -SUM(sum_buy_price_usd_minus_sell_price_usd) AS bo_pnl_usd
             , SUM(total_contracts) AS bo_contract_count
          FROM bi.mv_bo_pnl_summary
         WHERE sell_txn_date >= '2022-06-01'
         GROUP BY 1
    )
   , payments AS (
        SELECT binary_user_id
             , SUM(IF(category IN ('Client Deposit'), amount_usd, 0)) AS deposit_usd
             , SUM(IF(category IN ('Client Withdrawal'), -amount_usd, 0)) AS withdrawal_usd
             , SUM(IF(category IN ('Client Deposit'), 1, 0)) AS deposit_count
             , SUM(IF(category IN ('Client Withdrawal'), 1, 0)) AS withdrawal_count
          FROM `business-intelligence-240201.bi.bo_payment_model`
         WHERE category IN ('Client Withdrawal', 'Client Deposit')
           AND DATE(transaction_time) >= '2022-06-01'
         GROUP BY 1
      )
   , mt5_trades AS (
       SElECT binary_user_id
            , -ROUND(SUM(CASE WHEN mv_mt5_deal_aggregated.entry IN ('out','out_by')
               THEN (mv_mt5_deal_aggregated.sum_profit + mv_mt5_deal_aggregated.sum_storage) * r.rate
               ELSE 0 END),4) AS mt5_pnl_usd
            , SUM(count_win_deals) AS mt5_win_count
            , SUM(count_deals) AS mt5_number_of_trades
         FROM bi.mv_mt5_deal_aggregated
         JOIN bi.mt5_user ON mt5_user.login=mv_mt5_deal_aggregated.login
         LEFT JOIN bi.mt5_trading_group g ON mt5_user.group = g.group AND mt5_user.srvid = g.srvid
         LEFT JOIN bi.bo_exchange_rate r ON DATE(r.date) = DATE_SUB(DATE(mv_mt5_deal_aggregated.deal_date), INTERVAL 1 DAY)
          AND g.currency = r.source_currency
          AND r.target_currency = 'USD'
        WHERE DATE(deal_date) >= '2022-06-01'
        GROUP BY 1
      )
   SELECT COALESCE(bo_trades.binary_user_id, payments.binary_user_id, mt5_trades.binary_user_id) AS binary_user_id
        , COALESCE(bo_trades.bo_turnover_usd, 0) AS bo_turnover_usd
        , COALESCE(bo_trades.bo_winning_turnover_usd, 0) AS bo_winning_turnover_usd
        , COALESCE(bo_trades.bo_pnl_usd, 0) AS bo_pnl_usd
        , COALESCE(bo_trades.bo_win_count, 0) AS bo_win_count
        , COALESCE(bo_trades.bo_contract_count, 0) AS bo_contract_count
        , COALESCE(payments.deposit_usd, 0) AS deposit_usd
        , COALESCE(payments.withdrawal_usd, 0) AS withdrawal_usd
        , COALESCE(payments.deposit_count, 0) AS deposit_count
        , COALESCE(payments.withdrawal_count, 0) AS withdrawal_count
        , COALESCE(mt5_trades.mt5_pnl_usd, 0) AS mt5_pnl_usd
        , COALESCE(mt5_trades.mt5_win_count, 0) AS mt5_win_count
        , COALESCE(mt5_trades.mt5_number_of_trades, 0) AS mt5_number_of_trades
     FROM bo_trades
     FULL JOIN payments
          ON payments.binary_user_id = bo_trades.binary_user_id
     FULL JOIN mt5_trades
          ON mt5_trades.binary_user_id = COALESCE(bo_trades.binary_user_id, payments.binary_user_id)
) 
, card_issuer_dict AS (
    SELECT residence
         , card_issuer
         , cnt
         , SUM(COALESCE(cnt)) OVER(PARTITION BY residence,card_issuer) AS re_ca
         , SUM(COALESCE(cnt)) OVER(PARTITION BY residence) AS res
         , ROUND(SUM(COALESCE(cnt)) OVER(PARTITION BY residence,card_issuer)/SUM(COALESCE(cnt)) OVER(PARTITION BY residence), 2) AS card_issuer_popularity
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
, card_issuer_popularity AS (
    SELECT binary_user_id
         , MIN(COALESCE(card_issuer_dict.card_issuer_popularity,1)) AS card_issuer_popularity
      FROM (   
            SELECT binary_user_id
                 , card_issuer
                 , residence 
              FROM bi.bo_client
              LEFT JOIN bi.premier_cashier_transactions pct ON pct.PIN = bo_client.loginid 
          ) AS foo
      LEFT JOIN card_issuer_dict ON card_issuer_dict.residence = foo.residence AND card_issuer_dict.card_issuer = foo.card_issuer
     GROUP BY 1 
)
SELECT user_daily_summary.* 
     , COALESCE(card_issuer_popularity.card_issuer_popularity, 1) AS card_issuer_popularity
  FROM user_daily_summary
  LEFT JOIN card_issuer_popularity ON card_issuer_popularity.binary_user_id = user_daily_summary.binary_user_id
