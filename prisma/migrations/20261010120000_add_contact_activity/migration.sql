-- Who each connected number has chatted 1:1 with. Written in batches by
-- src/utils/contact-activity.ts; read by POST /chat/contactActivity/{instanceName}.
-- A new, empty table: nothing existing is rewritten or locked.

-- CreateTable
CREATE TABLE "ContactActivity" (
    "ownerNumber" VARCHAR(32) NOT NULL,
    "peerJid" VARCHAR(100) NOT NULL,
    "peerLid" VARCHAR(100),
    "firstAt" TIMESTAMPTZ(3) NOT NULL,
    "lastAt" TIMESTAMPTZ(3) NOT NULL,
    "fromMeCount" INTEGER NOT NULL DEFAULT 0,
    "fromThemCount" INTEGER NOT NULL DEFAULT 0,
    "source" VARCHAR(16),
    "createdAt" TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMPTZ(3) NOT NULL,

    CONSTRAINT "ContactActivity_pkey" PRIMARY KEY ("ownerNumber","peerJid")
);

-- CreateIndex
CREATE INDEX "ContactActivity_ownerNumber_peerLid_idx" ON "ContactActivity"("ownerNumber", "peerLid");
