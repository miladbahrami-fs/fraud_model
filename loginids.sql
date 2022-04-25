WITH logins AS (
 SELECT * 
   FROM fraud_detection.colombia_external
UNION ALL 	
 SELECT * 
   FROM fraud_detection.ua_delay_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.apm_2020_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.turkish_fraud_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.more_than_5_cards_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.mt5_external
UNION ALL	
 SELECT *
   FROM fraud_detection.2019_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.2021_bd_group_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.ukraine_same_ip_login_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.mexican_fraud_external --email
UNION ALL	
 SELECT * 
   FROM fraud_detection.cards_2020_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.stolen_lost_pickup_restricted_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.ultrashort_suspicious_clients_external
UNION ALL
 SELECT * 
   FROM fraud_detection.kz_clients_external
UNION ALL
 SELECT * 
   FROM fraud_detection.turkish_3nd_trend_external
UNION ALL
 SELECT * 
   FROM fraud_detection.chinese_webmoney_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.ecuador_fraud_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.togo_clients_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.india_fraud_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.sudden_big_trades_external -- contain nulls
UNION ALL	
 SELECT * 
   FROM fraud_detection.disposable_emails_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.indonesian_fraud_external
UNION ALL	
-- SELECT * FROM fraud_detection.client_list_testing_external -- no access
-- UNION ALL	
 SELECT * 
   FROM fraud_detection.br_web_perfect_money_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.ru_related_email_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.brazil_fraud_external -- strange logins
UNION ALL	
 SELECT * 
   FROM fraud_detection.free_gifts_external -- multipe entry in one row
UNION ALL	
-- SELECT * FROM fraud_detection.nigaria_fraud_external -- Unable to parse range: NIGARIA FRAUD!A1:A10000
-- UNION ALL	
-- SELECT * FROM fraud_detection.bangladesh_suspicious_group_external -- Spreadsheet not found.
-- UNION ALL	
 SELECT * 
   FROM fraud_detection.indo_affiliates_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.financial_fraud_details_external -- contains nulls
UNION ALL	
 SELECT * 
   FROM fraud_detection.indian_crypto_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.ip_check_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.cards_2021_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.dominican_republic_fraud_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.unmatch_residence_and_login_ip_external
UNION ALL
 SELECT * 
   FROM fraud_detection.apm_2021_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.bo_external -- multiple entry in one row
UNION ALL	
 SELECT * 
   FROM fraud_detection.pa_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.turkish_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.usd_deposits_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.swiss_clients_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.bo_affiliate_check_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.american_names_among_afghanistan_client_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.turkish_2nd_trend_external
-- UNION ALL	
-- SELECT * FROM fraud_detection.apm_check_external -- Unable to parse range: APM CHECK!A1:A10000
UNION ALL	
 SELECT * 
   FROM fraud_detection.declined_by_antifraud_external
UNION ALL	
 SELECT * 
   FROM fraud_detection.india_paymero_external
)
SELECT DISTINCT loginid 
  FROM (SELECT REGEXP_EXTRACT_ALL(loginid,r'\w+') AS loginid_list FROM logins), UNNEST(loginid_list) AS loginid