CREATE TABLE IF NOT EXISTS anymarket_marketplaces (
  id INT AUTO_INCREMENT PRIMARY KEY,
  code VARCHAR(50) NOT NULL UNIQUE,
  name VARCHAR(120) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS anymarket_orders (
  id BIGINT PRIMARY KEY,
  marketplace_id INT NOT NULL,
  marketplace_order_id VARCHAR(120) NULL,
  channel VARCHAR(60) NULL,
  fulfillment_type ENUM('FULFILLMENT','OWN_LOGISTICS') NULL,
  status VARCHAR(60) NOT NULL,
  substatus VARCHAR(60) NULL,
  buyer_name VARCHAR(190) NULL,
  buyer_document VARCHAR(40) NULL,
  buyer_email VARCHAR(190) NULL,
  total_amount DECIMAL(15,2) NULL,
  total_discount DECIMAL(15,2) NULL,
  freight_amount DECIMAL(15,2) NULL,
  created_at_marketplace DATETIME(6) NULL,
  approved_at DATETIME(6) NULL,
  cancelled_at DATETIME(6) NULL,
  shipped_at DATETIME(6) NULL,
  delivered_at DATETIME(6) NULL,
  created_at DATETIME(6) NULL,
  updated_at DATETIME(6) NULL,
  extra_json JSON NULL,
  KEY idx_status (status, substatus),
  KEY idx_dates (approved_at, cancelled_at, shipped_at, delivered_at),
  KEY idx_marketplace_order (marketplace_order_id),
  CONSTRAINT fk_orders_marketplace FOREIGN KEY (marketplace_id) REFERENCES anymarket_marketplaces(id)
);

CREATE TABLE IF NOT EXISTS anymarket_order_items (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_id BIGINT NOT NULL,
  sku_id BIGINT NULL,
  marketplace_item_id VARCHAR(120) NULL,
  title VARCHAR(255) NULL,
  quantity INT NOT NULL,
  unit_price DECIMAL(15,2) NULL,
  total_price DECIMAL(15,2) NULL,
  external_id VARCHAR(120) NULL,
  extra_json JSON NULL,
  KEY idx_items_order (order_id),
  CONSTRAINT fk_items_order FOREIGN KEY (order_id) REFERENCES anymarket_orders(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS anymarket_order_payments (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_id BIGINT NOT NULL,
  method VARCHAR(60) NULL,
  installments INT NULL,
  amount DECIMAL(15,2) NULL,
  transaction_id VARCHAR(120) NULL,
  status VARCHAR(60) NULL,
  authorized_at DATETIME(6) NULL,
  paid_at DATETIME(6) NULL,
  canceled_at DATETIME(6) NULL,
  extra_json JSON NULL,
  KEY idx_pay_order (order_id),
  CONSTRAINT fk_pay_order FOREIGN KEY (order_id) REFERENCES anymarket_orders(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS anymarket_order_shipping (
  order_id BIGINT PRIMARY KEY,
  carrier VARCHAR(120) NULL,
  service VARCHAR(120) NULL,
  tracking_code VARCHAR(120) NULL,
  promised_delivery DATETIME(6) NULL,
  shipped_at DATETIME(6) NULL,
  delivered_at DATETIME(6) NULL,
  receiver_name VARCHAR(190) NULL,
  address_street VARCHAR(190) NULL,
  address_number VARCHAR(30) NULL,
  address_comp VARCHAR(120) NULL,
  address_district VARCHAR(120) NULL,
  address_city VARCHAR(120) NULL,
  address_state VARCHAR(2) NULL,
  address_zip VARCHAR(20) NULL,
  extra_json JSON NULL,
  CONSTRAINT fk_ship_order FOREIGN KEY (order_id) REFERENCES anymarket_orders(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS anymarket_order_invoices (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_id BIGINT NOT NULL,
  is_invoiced TINYINT(1) NOT NULL DEFAULT 0,
  invoice_key VARCHAR(60) NULL UNIQUE,
  number VARCHAR(20) NULL,
  series VARCHAR(10) NULL,
  issued_at DATETIME(6) NULL,
  xml_url VARCHAR(500) NULL,
  pdf_url VARCHAR(500) NULL,
  extra_json JSON NULL,
  KEY idx_inv_order (order_id),
  KEY idx_inv_issued (issued_at),
  CONSTRAINT fk_inv_order FOREIGN KEY (order_id) REFERENCES anymarket_orders(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS anymarket_order_returns (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_id BIGINT NOT NULL,
  status VARCHAR(60) NULL,
  reason VARCHAR(255) NULL,
  requested_at DATETIME(6) NULL,
  approved_at DATETIME(6) NULL,
  received_at DATETIME(6) NULL,
  extra_json JSON NULL,
  KEY idx_ret_order (order_id),
  CONSTRAINT fk_ret_order FOREIGN KEY (order_id) REFERENCES anymarket_orders(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS anymarket_order_status_history (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_id BIGINT NOT NULL,
  status VARCHAR(60) NOT NULL,
  substatus VARCHAR(60) NULL,
  source VARCHAR(60) NULL,
  occurred_at DATETIME(6) NOT NULL,
  payload JSON NULL,
  KEY idx_hist_order (order_id),
  KEY idx_hist_when (occurred_at),
  CONSTRAINT fk_hist_order FOREIGN KEY (order_id) REFERENCES anymarket_orders(id) ON DELETE CASCADE
);

INSERT INTO anymarket_marketplaces (code, name) VALUES
  ('MAGALU','Magazine Luiza'),
  ('MERCADOLIVRE','Mercado Livre'),
  ('VIAVAREJO','Via Varejo')
ON DUPLICATE KEY UPDATE name=VALUES(name);
