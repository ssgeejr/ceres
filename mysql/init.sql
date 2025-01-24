SET GLOBAL time_zone = 'America/Chicago';

USE ceresdb;

CREATE TABLE users (
    user_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    email VARCHAR(64) UNIQUE NOT NULL,
    department VARCHAR64255) NOT NULL
);

CREATE TABLE user_reports (
    user_id INT UNSIGNED NOT NULL,
    seen_date DATE NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);
