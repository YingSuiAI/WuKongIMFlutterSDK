-- Existing native identities are conservative immutable receipts. New outgoing
-- INSERTs explicitly write 0. Only old rows without native identity are pending.
ALTER TABLE message ADD COLUMN payload_committed INTEGER NOT NULL DEFAULT 1;
UPDATE message SET payload_committed = 0
WHERE message_id IS NULL OR message_id IN ('', '0');
