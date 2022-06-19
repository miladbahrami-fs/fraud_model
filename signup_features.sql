WITH bo_account AS (
SELECT DISTINCT binary_user_id
     , COUNT(loginid) OVER w AS count_real_bo_loginid
     , MAX(same_joined_birth) OVER w AS same_joined_birth
     , COUNT(is_fiat) OVER w AS count_fiat_bo_accounts
     , COUNT(is_crypto) OVER w AS count_crypto_bo_accounts
  FROM (
    SELECT bo_client.binary_user_id 
         , bo_client.loginid
         , bo_client.date_joined
         , CASE 
             WHEN bo_client.currency_code IN
                  ('BTC', 'ETH', 'ETC', 'LTC', 'BCH', 'IDK', 'UST', 'USB', 'DAI', 'PAX', 'BUSD', 'EURS', 'SAI', 'TUSD', 'USDC', 'USDK', 'eUSDT')
             THEN 1
             ELSE 0 END AS is_crypto
         , CASE 
             WHEN bo_client.currency_code NOT IN 
                  ('BTC', 'ETH', 'ETC', 'LTC', 'BCH', 'IDK', 'UST', 'USB', 'DAI', 'PAX', 'BUSD', 'EURS', 'SAI', 'TUSD', 'USDC', 'USDK', 'eUSDT')
             THEN 1 
             ELSE 0 END AS is_fiat
         , CASE 
           WHEN EXTRACT(DAY FROM bo_client.date_joined) = EXTRACT(DAY FROM bo_client.date_of_birth) 
                AND EXTRACT(MONTH FROM bo_client.date_joined)=EXTRACT(MONTH FROM bo_client.date_of_birth) 
           THEN 1 
           ELSE 0 END AS same_joined_birth,
      FROM bi.bo_client
     WHERE bo_client.binary_user_id IS NOT NULL
     ) AS bc
WINDOW w AS (PARTITION BY binary_user_id ORDER BY date_joined ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  
)
, mt5_account AS (
SELECT DISTINCT (mt5_user.binary_user_id)
     , FIRST_VALUE(mt5_user.registration_ts) OVER w AS first_real_mt5_registration_ts
     , COUNT(login) OVER w as count_real_mt5_login
  FROM bi.mt5_user
 WHERE binary_user_id IS NOT NULL
WINDOW w AS (PARTITION BY binary_user_id ORDER BY registration_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  

)
, profile AS (
SELECT binary_user_id
     , CASE WHEN aff_status='affiliated' THEN 1 ELSE 0 END AS is_affiliated
     , residence
     , gender
     , email
     , IF(COALESCE(email_verified,FALSE), 1, 0) AS email_verified
     , IF(COALESCE(has_social_signup,FALSE), 1 ,0) AS has_social_signup
     , IF(COALESCE(isUser.PA,FALSE), 1, 0) AS is_pa
     , IF(COALESCE(isUser.Affiliate,FALSE), 1 ,0) AS is_affiliate
     , user_date.joined AS date_joined
     , IF(user_date.first_trade IS NOT NULL, 1, 0) AS has_trade
     , IF(user_date.first_deposit IS NOT NULL, 1, 0) AS has_deposit
     , IF(user_date.first_withdrawal IS NOT NULL, 1, 0) AS has_withdrawal
  FROM bi.user_profile
)
, domain_popularity AS (
SELECT residence
     , domain
     , ROUND(1 * cnt / SUM(cnt) OVER (PARTITION BY residence),5) AS domain_popularity,    
  FROM (
        SELECT residence
             , REGEXP_EXTRACT(email, r'@(.+)') domain
             , COUNT(*) as cnt
          FROM bi.user_profile
         WHERE residence IS NOT NULL
         GROUP BY 1,2
      )
)
SELECT profile.binary_user_id
     , profile.residence
     , profile.date_joined
     , profile.gender
     , profile.email
     , domain_popularity.domain_popularity
     , COALESCE(profile.email_verified, 0) AS email_verified
     , COALESCE(profile.has_social_signup, 0) AS has_social_signup
     , COALESCE(profile.is_affiliated, 0) AS is_affiliated
     , COALESCE(profile.is_pa, 0) AS is_pa
     , COALESCE(profile.is_affiliate, 0) AS is_affiliate
     , COALESCE(profile.has_trade, 0) AS has_trade
     , COALESCE(profile.has_deposit, 0) AS has_deposit
     , COALESCE(profile.has_withdrawal, 0) AS has_withdrawal
     , COALESCE(bo_account.count_real_bo_loginid, 0) AS count_real_bo_accounts 
     , COALESCE(bo_account.count_crypto_bo_accounts, 0) AS count_crypto_bo_accounts
     , COALESCE(bo_account.count_fiat_bo_accounts, 0) AS count_fiat_bo_accounts
     , COALESCE(bo_account.same_joined_birth, 0) AS same_joined_birth
    --  , DATE(mt5_account.first_real_mt5_registration_ts) AS real_mt5_date_joined
     , COALESCE(mt5_account.count_real_mt5_login, 0) AS count_real_mt5_login
  FROM profile
  LEFT JOIN bo_account 
    ON bo_account.binary_user_id = profile.binary_user_id
  LEFT JOIN mt5_account 
    ON mt5_account.binary_user_id = profile.binary_user_id
  LEFT JOIN domain_popularity 
    ON profile.residence = domain_popularity.residence 
   AND REGEXP_EXTRACT(profile.email, r'@(.+)') = domain_popularity.domain
