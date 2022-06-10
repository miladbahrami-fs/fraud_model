# Fraud Detection Model
## Fraud Model Features
### Signup Features
- residence
- date_joined
- gender
- email
- domain_popularity
- email_verified
- has_social_signup
- is_affiliated
- is_pa
- is_affiliate
- has_trade
- has_deposit
- has_withdrawal
- count_real_bo_accounts
- count_crypto_bo_accounts
- count_fiat_bo_accounts
- count_real_mt5_login
- same_joined_birth

### Trade & Payment Features
#### First Model
- date
- binary_user_id
- platform
- action
- action_count
- value
- action_suspiciency
#### Second Model
- date
- binary_user_id
- deposit_count
- withdrawal_count
- deposit_value
- withdrawal_value
- deposit_risk
- withdrawal_risk
- deposit_major_currency
- withdrawal_major_currency
- deposit_major_pp
- withdrawal_major_pp
- deposit_card_issuer_risk
- withdrawal_card_issuer_risk
- trade_count
- trade_pnl
- trade_risk
- trade_major_asset_type