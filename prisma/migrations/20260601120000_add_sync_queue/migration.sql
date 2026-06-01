CREATE TABLE IF NOT EXISTS "SyncQueue" (
  "id"         SERIAL PRIMARY KEY,
  "type"       VARCHAR(20)  NOT NULL,
  "remoteJid"  VARCHAR(100) NOT NULL,
  "data"       JSONB,
  "instanceId" INTEGER      NOT NULL,
  "createdAt"  TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "SyncQueue_instanceId_type_remoteJid_key" UNIQUE ("instanceId", "type", "remoteJid")
);

CREATE INDEX IF NOT EXISTS "SyncQueue_instanceId_type_idx" ON "SyncQueue"("instanceId", "type");
