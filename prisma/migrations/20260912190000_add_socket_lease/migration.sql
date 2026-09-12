-- CreateTable
CREATE TABLE "SocketLease" (
    "id" VARCHAR(64) NOT NULL,
    "owner" VARCHAR(200) NOT NULL,
    "expiresAt" TIMESTAMPTZ(3) NOT NULL,

    CONSTRAINT "SocketLease_pkey" PRIMARY KEY ("id")
);
