-- Migration: Add notification preference columns to users table
-- Date: 2025-11-07
-- Description: Adds mute_trade_notifications and mute_daily_summary columns

ALTER TABLE users 
ADD COLUMN IF NOT EXISTS mute_trade_notifications BOOLEAN DEFAULT FALSE;

ALTER TABLE users 
ADD COLUMN IF NOT EXISTS mute_daily_summary BOOLEAN DEFAULT FALSE;
