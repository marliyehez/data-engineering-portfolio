SELECT pg_catalog.set_config('search_path', 'star_schema', false);


-- DDL
CREATE TABLE IF NOT EXISTS dim_product(
	product_id SERIAL PRIMARY KEY,
	name_ VARCHAR(25),
	category VARCHAR(25),
);


CREATE TABLE IF NOT EXISTS dim_customer(
	customer_id SERIAL PRIMARY KEY,
	first_name VARCHAR(25),
	last_name VARCHAR(25),
	sex VARCHAR(1), -- M/F
	city VARCHAR(25)
);


CREATE TABLE IF NOT EXISTS dim_date(
	date_id SERIAL PRIMARY KEY,
	year_ INT,
	month_ INT,
	day_ INT,
	weekday_ BOOLEAN,
	holiday_flag BOOLEAN
);


CREATE TABLE IF NOT EXISTS sale(
	sale_id SERIAL PRIMARY KEY,
	date_id INT NOT NULL,  
	product_id INT NOT NULL,
	customer_id INT NOT NULL,
	units INT NOT NULL,
	price INT NOT NULL,
	CONSTRAINT fk_date 
		FOREIGN KEY(date_id) REFERENCES dim_date(date_id) ON DELETE CASCADE,
	CONSTRAINT fk_product
		FOREIGN KEY(product_id) REFERENCES dim_product(product_id) ON DELETE CASCADE,
	CONSTRAINT fk_customer
		FOREIGN KEY(customer_id) REFERENCES dim_customer(customer_id) ON DELETE CASCADE
);


-- DML
INSERT INTO dim_product
VALUES 
  (1, 'Pulpen', 'Alat Tulis dan Penghapus'),
  (2, 'Pensil', 'Alat Tulis dan Penghapus'),
  (3, 'Rautan', 'Alat Tulis dan Penghapus'),
  (4, 'Kertas A4', 'Buku dan Kertas'),
  (5, 'Penghapus', 'Alat Tulis dan Penghapus'),
  (6, 'Buku Tulis', 'Buku dan Kertas'),
  (7, 'Gunting', 'Gunting dan Lem');


INSERT INTO dim_customer
VALUES 
  (1, 'Bowie', 'Quincey', 'M', 'Tangerang Selatan'),
  (2, 'Kehlani', 'Ellisson', 'F', 'DKI Jakarta'),
  (3, 'Freddie', 'Hepburn', 'M', 'DKI Jakarta'),
  (4, 'Ebba', 'Ayers', 'F', 'Bandung');


INSERT INTO dim_date
VALUES 
  (1, 2023, 1, 13, TRUE, FALSE),
  (2, 2023, 1, 14, FALSE, FALSE),
  (3, 2023, 1, 15, FALSE, FALSE);


INSERT INTO sale
VALUES 
  (1, 1, 2, 2, 3, 3000),
  (2, 1, 3, 2, 1, 2500),
  (3, 2, 4, 1, 20, 200),
  (4, 2, 7, 1, 1, 7000),
  (5, 2, 1, 1, 1, 5000),
  (6, 2, 2, 3, 1, 3000),
  (7, 3, 5, 3, 2, 2500),
  (8, 3, 6, 2, 1, 4000),
  (9, 3, 7, 2, 1, 7000),
  (10, 3, 6, 4, 3, 4000);