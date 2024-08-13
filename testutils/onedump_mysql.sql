-- -------------------------------------------------------------
-- TablePlus 6.0.8(562)
--
-- https://tableplus.com/
--
-- Database: onedump
-- Generation Time: 2024-06-21 20:58:51.1800
-- -------------------------------------------------------------


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


DROP TABLE IF EXISTS `onedump`;
CREATE TABLE `onedump` (
  `char` char(20) DEFAULT NULL,
  `varchar` varchar(20) DEFAULT NULL,
  `binary` binary(20) DEFAULT NULL,
  `varbinary` varbinary(20) DEFAULT NULL,
  `tinyblob` tinyblob,
  `tinytext` tinytext,
  `text` text,
  `blob` blob,
  `mediumtext` mediumtext,
  `mediumblob` mediumblob,
  `longtext` longtext,
  `longblob` longblob,
  `enum` enum('1','2','3') DEFAULT NULL,
  `set` set('1','2','3') DEFAULT NULL,
  `bit` bit(1) DEFAULT NULL,
  `tinyint` tinyint DEFAULT NULL,
  `bool` tinyint(1) DEFAULT NULL,
  `boolean` tinyint(1) DEFAULT NULL,
  `smallint` smallint DEFAULT NULL,
  `mediumint` mediumint DEFAULT NULL,
  `int` int DEFAULT NULL,
  `bigint` bigint DEFAULT NULL,
  `float` float DEFAULT NULL,
  `double` double(10,2) DEFAULT NULL,
  `double_precision` double DEFAULT NULL,
  `decimal` decimal(10,2) DEFAULT NULL,
  `dec` decimal(10,2) DEFAULT NULL,
  `date` date default null,
  `datetime` datetime DEFAULT NULL,
  `timestamp` timestamp NULL DEFAULT NULL,
  `time` time DEFAULT NULL,
  `year` year DEFAULT NULL,
  `json` json DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
  `id` int DEFAULT NULL,
  `name` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `onedump` (`char`, `varchar`, `binary`, `varbinary`, `tinyblob`, `tinytext`, `text`, `blob`, `mediumtext`, `mediumblob`, `longtext`, `longblob`, `enum`, `set`, `bit`, `tinyint`, `bool`, `boolean`, `smallint`, `mediumint`, `int`, `bigint`, `float`, `double`, `double_precision`, `decimal`, `dec`, `date`, `datetime`, `timestamp`, `time`, `year`, `json`) VALUES
('a', 'abc', '7\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0', '7', 'tinyblob', 'tinytext', 'text', 'blob', 'mediumtext', 'mediumblob', 'longtext', 'longblob', '1', '2', b'1', 1, 1, 1, 1, 12, 12, 12, 12, 12.00, 12, 12.00, 12.00, '2024-06-17', '2024-06-17 16:50:54', '2024-06-17 16:50:58', '16:51:03', '2024', '{\"age\": 25, \"name\": \"Alice\", \"email\": \"alice@example.com\", \"isActive\": true}');

INSERT INTO `users` (`id`, `name`) VALUES (1, 'julian');



/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;