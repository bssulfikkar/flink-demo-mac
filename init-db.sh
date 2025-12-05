#!/bin/bash

echo "Initializing PostgreSQL database..."

docker exec -i flink-demo-postgres-1 psql -U flink -d flink_demo << EOF
-- Create table for category statistics
CREATE TABLE IF NOT EXISTS category_stats (
    category VARCHAR(100),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    transaction_count BIGINT,
    total_amount DOUBLE PRECISION,
    avg_amount DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (category, window_start)
);

-- Create index for better query performance
CREATE INDEX IF NOT EXISTS idx_category_stats_window
ON category_stats(window_start, window_end);

-- Create index for category lookups
CREATE INDEX IF NOT EXISTS idx_category_stats_category
ON category_stats(category);

EOF

echo "Database initialized successfully!"
echo ""
echo "You can query the data with:"
echo "docker exec -it flink-demo-postgres-1 psql -U flink -d flink_demo -c 'SELECT * FROM category_stats;'"
