WITH loginids AS (
   SELECT loginid 
     FROM development.fraud_detected
    WHERE REGEXP_CONTAINS(loginid, r'(CR|MX|MLT|MF)+\d+')
)
, emails AS (
    SELECT loginid AS email
      FROM development.fraud_detected
     WHERE loginid LIKE '%@%'
)
, buis AS (
    SELECT binary_user_id 
      FROM development.fraud_detected
      JOIN bi.user_profile ON CAST(user_profile.binary_user_id AS STRING) = fraud_detected.loginid
     WHERE REGEXP_CONTAINS(loginid, r'^[0-9]{7,8}$')
)
SELECT DISTINCT bc.binary_user_id
  FROM loginids
  JOIN bi.bo_client bc ON bc.loginid = loginids.loginid
UNION DISTINCT
SELECT DISTINCT bc.binary_user_id
  FROM emails
  JOIN bi.bo_client bc ON bc.email = emails.email
UNION DISTINCT
SELECT binary_user_id
  FROM buis
