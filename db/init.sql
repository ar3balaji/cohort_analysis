CREATE DATABASE enterprise;
use enterprise;

CREATE TABLE customer (
  id VARCHAR(75),
  created timestamp
);

CREATE TABLE app_order (
  id VARCHAR(75),
  order_number varchar(75),
  user_id varchar(75),
  created timestamp
);