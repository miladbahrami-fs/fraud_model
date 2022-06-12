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
SELECT DISTINCT bc.binary_user_id
  FROM loginids
  JOIN bi.bo_client bc ON bc.loginid = loginids.loginid
UNION DISTINCT
SELECT DISTINCT bc.binary_user_id
  FROM emails
  JOIN bi.bo_client bc ON bc.email = emails.email
