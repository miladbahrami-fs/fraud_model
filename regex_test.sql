SELECT loginid, REGEXP_EXTRACT_ALL(loginid, r'[^,\s]+')
   FROM fraud_detection.mexican_fraud_external --email
-- select loginid,REGEXP_EXTRACT_ALL(loginid,r'\S+') from fraud_detection.free_gifts_external
-- select loginid,REGEXP_EXTRACT_ALL(loginid,r'[a-zA-Z0-9-@.]+') from fraud_detection.free_gifts_external
-- select loginid,REGEXP_EXTRACT_ALL(loginid,r'[^,\s]+') from fraud_detection.free_gifts_external