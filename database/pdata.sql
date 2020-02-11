CREATE DATABASE IF NOT EXISTS PDATA;
USE PDATA;

CREATE TABLE classification (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT,
	initials VARCHAR(5) NOT NULL,
	description VARCHAR(50) NOT NULL,

	CONSTRAINT pk_classification
		PRIMARY KEY (id),
	CONSTRAINT ck_classification_initials
		CHECK ( TRIM(initials) <> '' ),
	CONSTRAINT ck_classification_description
		CHECK ( TRIM(description) <> '')
);

INSERT INTO classification(initials, description) VALUES
('EC1', 'Crime de Nivel 1'),('EC2', 'Crime de Nivel 2'),('EC3', 'Crime de Nivel 3');

CREATE TABLE typeEvent (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT,
	description VARCHAR(50) NOT NULL,

	CONSTRAINT pk_type_event
		PRIMARY KEY (id),
	CONSTRAINT ck_type_event_description
		CHECK ( TRIM(description) <> '' )
);

INSERT INTO typeEvent(description) VALUES ('Furto'), ('Roubo');

CREATE TABLE audit (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT,
	boKey VARCHAR(20) NOT NULL,
	year VARCHAR(4) NOT NULL,
	idTypeEvent INT UNSIGNED NULL,
	idClassification INT UNSIGNED NOT NULL,
	idBrazilianCity INT UNSIGNED NULL,
	historic TEXT NOT NULL,

	CONSTRAINT pk_audit
		PRIMARY KEY (id),
	CONSTRAINT fk_type_event_audit
		FOREIGN KEY (idTypeEvent)
		REFERENCES typeEvent(id),
	CONSTRAINT fk_classification_audit
		FOREIGN KEY (idClassification)
		REFERENCES classification(id),
	CONSTRAINT ck_idclassification_audit
		CHECK (TRIM(idClassification) <> ''),
	CONSTRAINT uk_bokey_audit
		UNIQUE (boKey)
);

