-- Homework 2: performance-oriented indexes for analytical queries
-- Database: adtech

CREATE INDEX idx_impressions_time_campaign
    ON impressions (impression_time, campaign_id);

CREATE INDEX idx_impressions_campaign_user
    ON impressions (campaign_id, user_id);

CREATE INDEX idx_clicks_time_impression
    ON clicks (click_time, impression_id);

CREATE INDEX idx_campaigns_advertiser
    ON campaigns (advertiser_id);

CREATE INDEX idx_users_location
    ON users (location_id);

CREATE INDEX idx_locations_name
    ON locations (location_name);

-- Optional extension for Question 7 (device performance comparison).
-- Uncomment only if your Homework 1 schema does not yet include device_type.
-- ALTER TABLE impressions
-- ADD COLUMN device_type ENUM('mobile', 'desktop', 'tablet') NOT NULL DEFAULT 'desktop';
--
-- CREATE INDEX idx_impressions_device_time
--     ON impressions (device_type, impression_time);
