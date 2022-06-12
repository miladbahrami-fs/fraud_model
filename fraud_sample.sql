  SELECT binary_user_id
        , 1 AS label
    FROM development.fraud_detected_users

  UNION ALL

  SELECT user_profile.binary_user_id 
       , 0 AS label
    FROM bi.user_profile TABLESAMPLE SYSTEM (5 PERCENT)
    LEFT JOIN development.fraud_detected_users fraud ON fraud.binary_user_id = user_profile.binary_user_id
   WHERE user_date.joined >= '2022-01-01' AND fraud.binary_user_id IS NULL