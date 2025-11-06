-- Users (with referrals)
CREATE TABLE IF NOT EXISTS users (
    id BIGINT PRIMARY KEY,
    balance DECIMAL(15,2) DEFAULT 0.00,
    balance_in_process DECIMAL(15,2) DEFAULT 0.00,
    daily_profit DECIMAL(15,2) DEFAULT 0.00,
    total_profit DECIMAL(15,2) DEFAULT 0.00,
    referral_count INTEGER DEFAULT 0,
    referral_earnings DECIMAL(15,2) DEFAULT 0.00,
    referrer_id BIGINT,
    wallet_address TEXT,
    network TEXT,
    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transactions
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    user_id BIGINT,
    type TEXT,
    amount DECIMAL(15,2),
    status TEXT,
    txid TEXT,
    wallet TEXT,
    network TEXT,
    screenshot_path TEXT,
    admin_note TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
