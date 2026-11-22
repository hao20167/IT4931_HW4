DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
CREATE TABLE state(id int PRIMARY KEY CHECK(id=1), h bigint NOT NULL);
INSERT INTO state VALUES(1,0);
CREATE TABLE batch_agg(category text PRIMARY KEY,n bigint NOT NULL,total bigint NOT NULL);
CREATE TABLE batch_events(event_id text PRIMARY KEY,seq bigint UNIQUE NOT NULL);
CREATE TABLE speed_events(event_id text PRIMARY KEY,seq bigint UNIQUE NOT NULL,category text NOT NULL,amount_cents bigint NOT NULL,produced_at double precision NOT NULL);
CREATE VIEW q1 AS
WITH contributions AS (
 SELECT category,n,total FROM batch_agg
 UNION ALL
 SELECT category,count(*)::bigint,sum(amount_cents)::bigint
 FROM speed_events WHERE seq>(SELECT h FROM state WHERE id=1) GROUP BY category
)
SELECT category,sum(n)::bigint AS n,sum(total)::bigint AS total FROM contributions GROUP BY category;
CREATE VIEW q2 AS SELECT coalesce(sum(n),0)::bigint AS n,coalesce(sum(total),0)::bigint AS total FROM q1;
CREATE VIEW visible AS
SELECT event_id,seq FROM batch_events
UNION ALL
SELECT event_id,seq FROM speed_events WHERE seq>(SELECT h FROM state WHERE id=1);
