-- nmig_test_db
CREATE TABLE IF NOT EXISTS `table_a`(
	`id_test_sequence` BIGINT NOT NULL AUTO_INCREMENT,
    `id_test_unique_index` INT NOT NULL,
    `id_test_composite_unique_index_1` INT NOT NULL,
    `id_test_composite_unique_index_2` INT NOT NULL,
    `id_test_index` INT NOT NULL,
    `id_test_composite_index_1` INT NOT NULL,
    `id_test_composite_index_2` INT NOT NULL,
    `json_test_comment` JSON COMMENT 'test column comment',
    `bit` BIT,
    `year` YEAR,
    `tinyint_test_default` TINYINT NOT NULL DEFAULT 20,
    `smallint` SMALLINT,
    `mediumint` MEDIUMINT,
    `int_test_not_null` INT NOT NULL,
    `bigint` BIGINT,
    `float` FLOAT,
    `double` DOUBLE,
    `double_precision` DOUBLE PRECISION,
    `numeric` NUMERIC,
    `decimal` DECIMAL(65,30),
    `decimal_19_2` DECIMAL(19,2),
    `char_5` CHAR(5),
    `varchar_5` VARCHAR(5),
    `date` DATE,
    `time` TIME,
    `datetime` DATETIME,
    `timestamp` TIMESTAMP,
    `enum` ENUM('e1', 'e2'),
    `set` SET('s1', 's2'),
    `tinytext` TINYTEXT,
    `mediumtext` MEDIUMTEXT,
    `longtext` LONGTEXT,
    `text` TEXT,
    `varbinary` VARBINARY(5),
    `binary` BINARY,
    `tinyblob` TINYBLOB,
    `mediumblob` MEDIUMBLOB,
    `longblob` LONGBLOB,
    `blob` BLOB,
    `null_char_in_varchar` VARCHAR(6) NOT NULL DEFAULT x'373300350035',
    PRIMARY KEY(`id_test_sequence`),
    UNIQUE KEY(`id_test_unique_index`),
    UNIQUE INDEX(`id_test_composite_unique_index_1`, `id_test_composite_unique_index_2`),
    INDEX (`id_test_index`),
    INDEX(`id_test_composite_index_1`, `id_test_composite_index_2`)
) ENGINE = innodb COMMENT = 'test table comment';

CREATE TABLE IF NOT EXISTS `table_b`(
	`id1` BIGINT NOT NULL,
	`id2` BIGINT NOT NULL,
    `word` VARCHAR(20),
    PRIMARY KEY(`id1`, `id2`)
) ENGINE = innodb;

CREATE TABLE IF NOT EXISTS `table_c`(
	`id` INT NOT NULL AUTO_INCREMENT,
    `word` VARCHAR(10),
    `table_a_id` BIGINT,
    `table_b_id1` BIGINT NOT NULL,
	`table_b_id2` BIGINT NOT NULL,
    PRIMARY KEY (`id`),
    KEY(`table_a_id`),
    KEY(`table_b_id1`, `table_b_id2`),
    CONSTRAINT `table_c_table_a_id_foreign` FOREIGN KEY(`table_a_id`) REFERENCES `table_a`(`id_test_sequence`) ON UPDATE RESTRICT ON DELETE CASCADE,
    CONSTRAINT `table_c_table_b_id_1_2_foreign` FOREIGN KEY(`table_b_id1`, `table_b_id2`) REFERENCES `table_b`(`id1`, `id2`)
) ENGINE = innodb;

CREATE TABLE IF NOT EXISTS `category_company`(
	`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	`company_id` INT(10) UNSIGNED NOT NULL,
	`category_id` INT(10) UNSIGNED NOT NULL,
	PRIMARY KEY (`id`)
)
CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci
ENGINE = innodb;

INSERT INTO `category_company`(`company_id`, `category_id`) VALUES(111, 2), (333, 2);
