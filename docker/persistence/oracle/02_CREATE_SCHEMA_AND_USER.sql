--ALTER SESSION SET CONTAINER=ORCLPDB1;
ALTER SESSION SET CONTAINER=CONDUCTOR;
CREATE USER conductor IDENTIFIED BY conductor;
GRANT CONNECT, RESOURCE TO conductor;
ALTER USER conductor QUOTA UNLIMITED ON USERS;
exit;
