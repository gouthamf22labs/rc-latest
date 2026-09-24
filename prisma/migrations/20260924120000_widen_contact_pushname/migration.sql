-- contact.name is the address-book entry, which has no length limit; VarChar(100) made
-- long names fail the whole Contact insert. varchar -> text is metadata-only (no rewrite).
ALTER TABLE "Contact" ALTER COLUMN "pushName" SET DATA TYPE TEXT;
