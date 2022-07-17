SELECT status_code 
  FROM development.fraud_dataset 
  LEFT JOIN bi.bo_client_status bcs ON bcs.binary_user_id = fraud_dataset.binary_user_id 
----------------------------------------------------------------------------------------------------------------------
SELECT MAX(user_profile.user_date.last_withdrawal) 
  FROM development.fraud_dataset 
  JOIN bi.user_profile ON user_profile.binary_user_id = fraud_dataset.binary_user_id
----------------------------------------------------------------------------------------------------------------------
SELECT user_profile.* 
  FROM development.fraud_dataset 
  JOIN `business-intelligence-240201.bi.user_profile` user_profile ON user_profile.binary_user_id = fraud_dataset.binary_user_id
 WHERE TRUE
QUALIFY ROW_NUMBER() OVER (ORDER BY user_profile.user_date.last_withdrawal DESC) = 1;
----------------------------------------------------------------------------------------------------------------------
SELECT DATE_TRUNC(date_joined, YEAR) AS year
     , count(*) AS count 
  FROM `business-intelligence-240201.development.fraud_dataset`
  GROUP BY 1