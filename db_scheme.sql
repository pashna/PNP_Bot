-- MySQL Script generated by MySQL Workbench
-- Пн. 25 янв. 2016 00:15:27
-- Model: New Model    Version: 1.0
-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Schema telegram_db
-- -----------------------------------------------------
DROP SCHEMA IF EXISTS `telegram_db` ;

-- -----------------------------------------------------
-- Schema telegram_db
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `telegram_db` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ;
USE `telegram_db` ;

-- -----------------------------------------------------
-- Table `telegram_db`.`chats`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `telegram_db`.`chats` ;

CREATE TABLE IF NOT EXISTS `telegram_db`.`chats` (
  `id_chat` INT NOT NULL COMMENT '',
  `user_id` INT NULL COMMENT '',
  `user_name` VARCHAR(45) NULL COMMENT '',
  PRIMARY KEY (`id_chat`)  COMMENT '')
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `telegram_db`.`news`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `telegram_db`.`news` ;

CREATE TABLE IF NOT EXISTS `telegram_db`.`news` (
  `news_id` INT NOT NULL AUTO_INCREMENT COMMENT '',
  `news_url` VARCHAR(200) NULL COMMENT '',
  `news_predicted` DOUBLE NULL COMMENT '',
  `is_sended` INT NULL COMMENT '',
  `news_date` VARCHAR(45) NULL COMMENT '',
  `firsttime_tweets` INT NULL COMMENT '',
  `news_real` DOUBLE NULL DEFAULT -1 COMMENT '',
  PRIMARY KEY (`news_id`)  COMMENT '')
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
