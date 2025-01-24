SET GLOBAL time_zone = 'America/Chicago';

USE ceresdb;

-- Create users table
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    email VARCHAR(64) UNIQUE NOT NULL,
    department VARCHAR(64) NOT NULL
);

-- Create user_reports table
CREATE TABLE user_reports (
    user_id INT NOT NULL,
    seen_date DATE NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
);
