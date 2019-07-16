CREATE DATABASE enterprise;
use enterprise;

CREATE TABLE customer (
  id VARCHAR(20),
  name VARCHAR(40)
);

INSERT INTO customer
  (id, name)
VALUES
  ('1', 'balaji'),
('2', 'apoorva');