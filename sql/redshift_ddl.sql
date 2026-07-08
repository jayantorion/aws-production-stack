-- =============================================================================
-- Redshift DDL — staging + final tables for all retail entities
-- Optimization: DISTKEY on join grain, SORTKEY on filter columns, AUTO encode.
-- Staging tables: DISTSTYLE EVEN, no sortkey (fast COPY/DELETE/INSERT).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ----------------------------- departments ----------------------------------
CREATE TABLE IF NOT EXISTS staging.departments (
    department_id   INT NOT NULL,
    department_name VARCHAR(256)
) DISTSTYLE EVEN;

CREATE TABLE IF NOT EXISTS analytics.departments (
    department_id   INT NOT NULL,
    department_name VARCHAR(256),
    batch_id        CHAR(32)
) DISTSTYLE ALL;                       -- tiny dim: replicated to every node

-- ----------------------------- categories -----------------------------------
CREATE TABLE IF NOT EXISTS staging.categories (
    category_id          INT NOT NULL,
    category_department_id INT,
    category_name        VARCHAR(256)
) DISTSTYLE EVEN;

CREATE TABLE IF NOT EXISTS analytics.categories (
    category_id          INT NOT NULL,
    category_department_id INT,
    category_name        VARCHAR(256),
    batch_id             CHAR(32)
) DISTSTYLE ALL;

-- ----------------------------- customers ------------------------------------
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id       INT NOT NULL,
    customer_fname    VARCHAR(64),
    customer_lname    VARCHAR(64),
    customer_email    CHAR(64),          -- sha256-masked at silver (PII)
    customer_password CHAR(64),
    customer_street   VARCHAR(256),
    customer_city     VARCHAR(64),
    customer_state    VARCHAR(8),
    customer_zipcode  VARCHAR(16)
) DISTSTYLE EVEN;

CREATE TABLE IF NOT EXISTS analytics.customers (
    customer_id       INT NOT NULL,
    customer_fname    VARCHAR(64),
    customer_lname    VARCHAR(64),
    customer_email    CHAR(64),
    customer_password CHAR(64),
    customer_street   VARCHAR(256),
    customer_city     VARCHAR(64),
    customer_state    VARCHAR(8),
    customer_zipcode  VARCHAR(16),
    batch_id          CHAR(32)
) DISTSTYLE KEY DISTKEY (customer_id) SORTKEY (customer_id);

-- ----------------------------- products -------------------------------------
CREATE TABLE IF NOT EXISTS staging.products (
    product_id          INT NOT NULL,
    product_category_id INT,
    product_name        VARCHAR(512),
    product_description VARCHAR(MAX),
    product_price       DOUBLE PRECISION,
    product_image       VARCHAR(MAX)
) DISTSTYLE EVEN;

CREATE TABLE IF NOT EXISTS analytics.products (
    product_id          INT NOT NULL,
    product_category_id INT,
    product_name        VARCHAR(512),
    product_description VARCHAR(MAX),
    product_price       DOUBLE PRECISION,
    product_image       VARCHAR(MAX),
    batch_id            CHAR(32)
) DISTSTYLE KEY DISTKEY (product_id) SORTKEY (product_id);

-- ----------------------------- orders ---------------------------------------
CREATE TABLE IF NOT EXISTS staging.orders (
    order_id          INT NOT NULL,
    order_date        TIMESTAMP,
    order_customer_id INT,
    order_status      VARCHAR(64)
) DISTSTYLE EVEN;

CREATE TABLE IF NOT EXISTS analytics.orders (
    order_id          INT NOT NULL,
    order_date        TIMESTAMP,
    order_customer_id INT,
    order_status      VARCHAR(64),
    batch_id          CHAR(32)
) DISTSTYLE KEY DISTKEY (order_customer_id)      -- joins to customers
  SORTKEY (order_date);                          -- date-range BI scans

-- ----------------------------- order_items ----------------------------------
CREATE TABLE IF NOT EXISTS staging.order_items (
    order_item_id            INT NOT NULL,
    order_item_order_id      INT,
    order_item_product_id    INT,
    order_item_quantity      INT,
    order_item_subtotal      DOUBLE PRECISION,
    order_item_product_price DOUBLE PRECISION
) DISTSTYLE EVEN;

CREATE TABLE IF NOT EXISTS analytics.order_items (
    order_item_id            INT NOT NULL,
    order_item_order_id      INT,
    order_item_product_id    INT,
    order_item_quantity      INT,
    order_item_subtotal      DOUBLE PRECISION,
    order_item_product_price DOUBLE PRECISION,
    batch_id                 CHAR(32)
) DISTSTYLE KEY DISTKEY (order_item_order_id)    -- co-located with orders
  SORTKEY (order_item_order_id);
