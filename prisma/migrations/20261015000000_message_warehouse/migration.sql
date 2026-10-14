-- The message warehouse: messages of numbers flagged for it (externalAttributes.warehouse) are
-- kept here, each once, and read back per chat by time.
-- On prod the table is empty, so these are instant; elsewhere duplicates (same number, same
-- WhatsApp message id) are removed first, keeping the newest row (its status rows go with it).

DELETE FROM "Message" a
USING "Message" b
WHERE a."instanceId" = b."instanceId" AND a."keyId" = b."keyId" AND a.id < b.id;

-- One row per message per number: re-fetched history and replays are no-ops.
CREATE UNIQUE INDEX IF NOT EXISTS "Message_instanceId_keyId_key" ON "Message" ("instanceId", "keyId");

-- A chat's history, newest first.
CREATE INDEX IF NOT EXISTS "Message_instanceId_keyRemoteJid_messageTimestamp_idx"
  ON "Message" ("instanceId", "keyRemoteJid", "messageTimestamp" DESC);
