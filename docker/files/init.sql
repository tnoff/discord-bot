CREATE USER discord WITH PASSWORD 'password';
CREATE USER backup_user WITH PASSWORD 'password';

CREATE DATABASE discord;
GRANT ALL PRIVILEGES ON DATABASE discord TO discord;
ALTER DATABASE discord OWNER TO discord;
