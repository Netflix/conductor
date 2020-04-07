ALTER TABLE `queue_message` ADD COLUMN `priority` TINYINT DEFAULT 0 AFTER `message_id`;
