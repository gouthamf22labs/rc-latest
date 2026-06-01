-- CreateIndex
CREATE INDEX IF NOT EXISTS "Contact_instanceId_remoteJid_idx" ON "Contact"("instanceId", "remoteJid");

-- CreateIndex
CREATE INDEX IF NOT EXISTS "Chat_instanceId_remoteJid_idx" ON "Chat"("instanceId", "remoteJid");

-- CreateIndex
CREATE INDEX IF NOT EXISTS "Message_instanceId_keyId_idx" ON "Message"("instanceId", "keyId");

-- CreateIndex
CREATE INDEX IF NOT EXISTS "MessageUpdate_messageId_idx" ON "MessageUpdate"("messageId");
