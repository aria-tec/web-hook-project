-- Migration: 000001_init_schema.down.sql

DROP TABLE IF EXISTS delivery_attempts;
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS endpoints;
DROP TABLE IF EXISTS tenants;
