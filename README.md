# Rustgress

Minimal database written in Rust. Inspired by [PostgreSQL](https://github.com/postgres/postgres).

# Goal:

The main goal of this project is to build a simplified SQL database that implements low-level storage concepts such as pages and tuples, along with basic maintenance mechanisms such as vacuuming and checksums. The project will also include a simple way to interact with the database to allow natural usage and testing of the system. As this is a university project, some design decisions will go beyond the immediate needs of the implementation in order to keep the architecture extensible and allow future expansion.

Currently working syntax:

```
CREATE TABLE users (id INT, ime VARCHAR, aktiven BOOLEAN)
SELECT * FROM messages WHERE user_id = 1
INSERT INTO messages VALUES (1000, 'My first message!')
SELECT * FROM rg_attribute WHERE attrelid = 2 AND atttypid = 50 AND attname = 'attnum' 
SELECT attrelid, attname FROM rg_attribute WHERE attrelid = 2 LIMIT 3
CREATE TABLE users (id INT, ime VARCHAR, aktiven BOOLEAN);
INSERT INTO users VALUES (1002, 'Gasper', true), (1003, 'Gorazd', true)
SELECT * FROM users
DROP TABLE users;
DELETE FROM users WHERE ime = 'Gasper'
SELECT * FROM rg_attribute ORDER BY atttypid ASC
SELECT * FROM rg_attribute ORDER BY atttypid DESC
UPDATE users SET ime = 'Gasper II.' WHERE id = 1002
SELECT * FROM usa_state_info where state < 'G' and illiteracy > 1.01 order by frost asc, illiteracy asc
SELECT * FROM usa_state_info where population < income
SELECT * FROM usa_state_info where income / 2 < 1900 - 200 + frost order by income desc
```


<p align="center">

<img src="https://img.shields.io/badge/dynamic/json?url=https://api.codetabs.com/v1/loc/?github=LukaLavs/Rustgress&query=$[0].linesOfCode&label=Lines%20of%20Code&color=blue&style=for-the-badge" />

<img src="https://img.shields.io/github/languages/code-size/LukaLavs/Rustgress?style=for-the-badge" />

</p>