CREATE TABLE NEW_ORDER (
  NO_O_ID INTEGER DEFAULT '0' NOT NULL,
  NO_D_ID TINYINT DEFAULT '0' NOT NULL,
  NO_W_ID TINYINT DEFAULT '0' NOT NULL,
  CONSTRAINT NO_PK_TREE PRIMARY KEY (NO_D_ID,NO_W_ID,NO_O_ID)
);

CREATE TABLE FIVEK_STRING (
  ID INTEGER DEFAULT '0' NOT NULL,
  P INTEGER DEFAULT '0' NOT NULL,
  CVALUE VARCHAR(60000) DEFAULT '' NOT NULL,
  PRIMARY KEY (ID)
);

CREATE TABLE FIVEK_STRING_WITH_INDEX (
  ID INTEGER DEFAULT '0' NOT NULL,
  CVALUE VARCHAR(5000) DEFAULT '' NOT NULL,
  PRIMARY KEY (ID),
  UNIQUE (CVALUE)
);

CREATE TABLE WIDE (
    ID INTEGER DEFAULT '0' NOT NULL,
    P INTEGER DEFAULT '0' NOT NULL,
    CVALUE1 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE2 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE3 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE4 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE5 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE6 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE7 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE8 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE9 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE10 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE11 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE12 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE13 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE14 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE15 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE16 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE17 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE18 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE19 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE20 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE21 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE22 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE23 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE24 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE25 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE26 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE27 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE28 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE29 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE30 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE31 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE32 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE33 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE34 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE35 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE36 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE37 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE38 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE39 VARCHAR(62) DEFAULT '' NOT NULL,
    CVALUE40 VARCHAR(62) DEFAULT '' NOT NULL,
);

CREATE TABLE WAREHOUSE (
  W_ID SMALLINT DEFAULT '0' NOT NULL,
  W_NAME VARCHAR(16) DEFAULT NULL,
  W_STREET_1 VARCHAR(32) DEFAULT NULL,
  W_STREET_2 VARCHAR(32) DEFAULT NULL,
  W_CITY VARCHAR(32) DEFAULT NULL,
  W_STATE VARCHAR(2) DEFAULT NULL,
  W_ZIP VARCHAR(9) DEFAULT NULL,
  W_TAX FLOAT DEFAULT NULL,
  W_YTD FLOAT DEFAULT NULL,
  CONSTRAINT W_PK_ARRAY PRIMARY KEY (W_ID)
);

CREATE TABLE DISTRICT (
  D_ID TINYINT DEFAULT '0' NOT NULL,
  D_W_ID SMALLINT DEFAULT '0' NOT NULL,
  D_NAME VARCHAR(16) DEFAULT NULL,
  D_STREET_1 VARCHAR(32) DEFAULT NULL,
  D_STREET_2 VARCHAR(32) DEFAULT NULL,
  D_CITY VARCHAR(32) DEFAULT NULL,
  D_STATE VARCHAR(2) DEFAULT NULL,
  D_ZIP VARCHAR(9) DEFAULT NULL,
  D_TAX FLOAT DEFAULT NULL,
  D_YTD FLOAT DEFAULT NULL,
  D_NEXT_O_ID INT DEFAULT NULL,
  PRIMARY KEY (D_W_ID,D_ID)
);

CREATE TABLE ITEM (
  I_ID INTEGER DEFAULT '0' NOT NULL,
  I_IM_ID INTEGER DEFAULT NULL,
  I_NAME VARCHAR(32) DEFAULT NULL,
  I_PRICE FLOAT DEFAULT NULL,
  I_DATA VARCHAR(64) DEFAULT NULL,
  CONSTRAINT I_PK_ARRAY PRIMARY KEY (I_ID)
);

CREATE TABLE CUSTOMER (
  C_ID INTEGER DEFAULT '0' NOT NULL,
  C_D_ID TINYINT DEFAULT '0' NOT NULL,
  C_W_ID SMALLINT DEFAULT '0' NOT NULL,
  C_FIRST VARCHAR(32) DEFAULT NULL,
  C_MIDDLE VARCHAR(2) DEFAULT NULL,
  C_LAST VARCHAR(32) DEFAULT NULL,
  C_STREET_1 VARCHAR(32) DEFAULT NULL,
  C_STREET_2 VARCHAR(32) DEFAULT NULL,
  C_CITY VARCHAR(32) DEFAULT NULL,
  C_STATE VARCHAR(2) DEFAULT NULL,
  C_ZIP VARCHAR(9) DEFAULT NULL,
  C_PHONE VARCHAR(32) DEFAULT NULL,
  C_SINCE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  C_CREDIT VARCHAR(2) DEFAULT NULL,
  C_CREDIT_LIM FLOAT DEFAULT NULL,
  C_DISCOUNT FLOAT DEFAULT NULL,
  C_BALANCE FLOAT DEFAULT NULL,
  C_YTD_PAYMENT FLOAT DEFAULT NULL,
  C_PAYMENT_CNT INTEGER DEFAULT NULL,
  C_DELIVERY_CNT INTEGER DEFAULT NULL,
  C_DATA VARCHAR(500),
  PRIMARY KEY (C_W_ID,C_D_ID,C_ID),
  UNIQUE (C_W_ID,C_D_ID,C_LAST,C_FIRST)
);
CREATE INDEX IDX_CUSTOMER ON CUSTOMER (C_W_ID,C_D_ID,C_LAST);

CREATE TABLE CUSTOMER_NAME (
  C_ID INTEGER DEFAULT '0' NOT NULL,
  C_D_ID TINYINT DEFAULT '0' NOT NULL,
  C_W_ID SMALLINT DEFAULT '0' NOT NULL,
  C_FIRST VARCHAR(32) DEFAULT NULL,
  C_LAST VARCHAR(32) DEFAULT NULL
);
CREATE INDEX IDX_CUSTOMER_NAME ON CUSTOMER_NAME (C_W_ID,C_D_ID,C_LAST);

CREATE TABLE HISTORY (
  H_C_ID INTEGER DEFAULT NULL,
  H_C_D_ID TINYINT DEFAULT NULL,
  H_C_W_ID SMALLINT DEFAULT NULL,
  H_D_ID TINYINT DEFAULT NULL,
  H_W_ID SMALLINT DEFAULT '0' NOT NULL,
  H_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  H_AMOUNT FLOAT DEFAULT NULL,
  H_DATA VARCHAR(32) DEFAULT NULL
);

CREATE TABLE STOCK (
  S_I_ID INTEGER DEFAULT '0' NOT NULL,
  S_W_ID SMALLINT DEFAULT '0 ' NOT NULL,
  S_QUANTITY INTEGER DEFAULT '0' NOT NULL,
  S_DIST_01 VARCHAR(32) DEFAULT NULL,
  S_DIST_02 VARCHAR(32) DEFAULT NULL,
  S_DIST_03 VARCHAR(32) DEFAULT NULL,
  S_DIST_04 VARCHAR(32) DEFAULT NULL,
  S_DIST_05 VARCHAR(32) DEFAULT NULL,
  S_DIST_06 VARCHAR(32) DEFAULT NULL,
  S_DIST_07 VARCHAR(32) DEFAULT NULL,
  S_DIST_08 VARCHAR(32) DEFAULT NULL,
  S_DIST_09 VARCHAR(32) DEFAULT NULL,
  S_DIST_10 VARCHAR(32) DEFAULT NULL,
  S_YTD INTEGER DEFAULT NULL,
  S_ORDER_CNT INTEGER DEFAULT NULL,
  S_REMOTE_CNT INTEGER DEFAULT NULL,
  S_DATA VARCHAR(64) DEFAULT NULL,
  PRIMARY KEY (S_W_ID,S_I_ID)
);

CREATE TABLE ORDERS (
  O_ID INTEGER DEFAULT '0' NOT NULL,
  O_D_ID TINYINT DEFAULT '0' NOT NULL,
  O_W_ID SMALLINT DEFAULT '0' NOT NULL,
  O_C_ID INTEGER DEFAULT NULL,
  O_ENTRY_D TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  O_CARRIER_ID INTEGER DEFAULT NULL,
  O_OL_CNT INTEGER DEFAULT NULL,
  O_ALL_LOCAL INTEGER DEFAULT NULL,
  PRIMARY KEY (O_W_ID,O_D_ID,O_ID),
  UNIQUE (O_W_ID,O_D_ID,O_C_ID,O_ID)
);
CREATE INDEX IDX_ORDERS ON ORDERS (O_W_ID,O_D_ID,O_C_ID);

CREATE TABLE ORDER_LINE (
  OL_O_ID INTEGER DEFAULT '0' NOT NULL,
  OL_D_ID TINYINT DEFAULT '0' NOT NULL,
  OL_W_ID SMALLINT DEFAULT '0' NOT NULL,
  OL_NUMBER INTEGER DEFAULT '0' NOT NULL,
  OL_I_ID INTEGER DEFAULT NULL,
  OL_SUPPLY_W_ID SMALLINT DEFAULT NULL,
  OL_DELIVERY_D TIMESTAMP DEFAULT NULL,
  OL_QUANTITY INTEGER DEFAULT NULL,
  OL_AMOUNT FLOAT DEFAULT NULL,
  OL_DIST_INFO VARCHAR(32) DEFAULT NULL,
  PRIMARY KEY (OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER)
);
--CREATE INDEX IDX_ORDER_LINE_3COL ON ORDER_LINE (OL_W_ID,OL_D_ID,OL_O_ID);
--CREATE INDEX IDX_ORDER_LINE_2COL ON ORDER_LINE (OL_W_ID,OL_D_ID);
CREATE INDEX IDX_ORDER_LINE_TREE ON ORDER_LINE (OL_W_ID,OL_D_ID,OL_O_ID);
