-- Brings into the repo the indexes and default that were added to the prod database by hand
-- and so never existed anywhere else. Stage lacked all of them, so it could not reproduce
-- prod's query plans, and a prod rebuild from migrations would have silently dropped them.
--
-- Every statement is a no-op where the object already exists. The names match prod's
-- exactly, so on prod IF NOT EXISTS finds each index already there and skips it, and the
-- default is re-set to the value it already has. Nothing on prod is rebuilt or locked.
--
-- Not CONCURRENTLY: that cannot run inside the transaction Prisma applies a migration in.
-- Where these do get built (stage, a fresh database) the tables are small enough for that
-- not to matter; on prod they are never built at all.

-- Contact lookups by (instanceId, remoteJid) that only need the id — the hottest index on
-- prod (13.7M scans). INCLUDE (id) lets those be answered from the index alone. Prisma's
-- schema cannot express INCLUDE, so this one lives only here.
CREATE INDEX IF NOT EXISTS "Contact_instanceId_remoteJid_inc_id_idx"
  ON "Contact" USING btree ("instanceId", "remoteJid") INCLUDE (id);

CREATE INDEX IF NOT EXISTS "idx_chat_instanceid" ON "Chat" USING btree ("instanceId");

CREATE INDEX IF NOT EXISTS "idx_message_instanceid" ON "Message" USING btree ("instanceId");

CREATE INDEX IF NOT EXISTS "idx_activitylogs_instanceid" ON "ActivityLogs" USING btree ("instanceId");

CREATE INDEX IF NOT EXISTS "idx_activitylogs_datetime" ON "ActivityLogs" USING btree ("dateTime");

ALTER TABLE "Instance" ALTER COLUMN "externalAttributes" SET DEFAULT '{}';
