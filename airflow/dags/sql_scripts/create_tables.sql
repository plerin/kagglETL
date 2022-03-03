BEGIN;

DROP TABLE IF EXISTS olympics.korea_medal;

CREATE TABLE IF NOT EXISTS olympics.korea_medal (
    sport VARCHAR(255),
    gold bigint NOT NULL,
    silver bigint NOT NULL,
    bronze bigint NOT NULL,
    total bigint NOT NULL,
    primary key (sport)
) diststyle key distkey(sport);

END;