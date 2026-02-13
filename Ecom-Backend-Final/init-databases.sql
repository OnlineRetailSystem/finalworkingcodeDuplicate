-- Create all required databases
CREATE DATABASE IF NOT EXISTS authservice;
CREATE DATABASE IF NOT EXISTS productdb;
CREATE DATABASE IF NOT EXISTS orderdb;
CREATE DATABASE IF NOT EXISTS cartdb;
CREATE DATABASE IF NOT EXISTS categorydb;
CREATE DATABASE IF NOT EXISTS notificationdb;

-- Grant privileges
GRANT ALL PRIVILEGES ON authservice.* TO 'root'@'%';
GRANT ALL PRIVILEGES ON productdb.* TO 'root'@'%';
GRANT ALL PRIVILEGES ON orderdb.* TO 'root'@'%';
GRANT ALL PRIVILEGES ON cartdb.* TO 'root'@'%';
GRANT ALL PRIVILEGES ON categorydb.* TO 'root'@'%';
GRANT ALL PRIVILEGES ON notificationdb.* TO 'root'@'%';
FLUSH PRIVILEGES;
