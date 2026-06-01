-- Deduplicate existing rows before adding constraint
DELETE FROM "Chat"
WHERE id NOT IN (
  SELECT DISTINCT ON ("instanceId", "remoteJid") id
  FROM "Chat"
  ORDER BY "instanceId", "remoteJid", id DESC
);

-- AddUniqueConstraint
ALTER TABLE "Chat" ADD CONSTRAINT "Chat_instanceId_remoteJid_key" UNIQUE ("instanceId", "remoteJid");
