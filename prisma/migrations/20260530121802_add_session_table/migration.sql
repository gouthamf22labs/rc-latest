-- CreateTable
CREATE TABLE "Session" (
    "id" SERIAL NOT NULL,
    "sessionKey" VARCHAR(300) NOT NULL,
    "data" JSONB NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "instanceId" INTEGER NOT NULL,

    CONSTRAINT "Session_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "Session_instanceId_idx" ON "Session"("instanceId");

-- CreateIndex
CREATE UNIQUE INDEX "Session_instanceId_sessionKey_key" ON "Session"("instanceId", "sessionKey");

-- AddForeignKey
ALTER TABLE "Session" ADD CONSTRAINT "Session_instanceId_fkey" FOREIGN KEY ("instanceId") REFERENCES "Instance"("id") ON DELETE CASCADE ON UPDATE CASCADE;
