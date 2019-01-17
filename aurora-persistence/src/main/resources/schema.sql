create table logs
(
	log_time timestamp,
	severity varchar,
	hostname varchar,
	fromhost varchar,
	logger varchar,
	owner varchar,
	message text
);