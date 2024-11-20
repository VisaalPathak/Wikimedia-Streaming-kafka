CREATE DATABASE IF NOT EXISTS rw_wikimedia;
USE rw_wikimedia;

DROP TABLE IF EXISTS wikimedia_data;

CREATE TABLE `wikimedia_data` (
  `schema` varchar(200) DEFAULT NULL,
  `uri` varchar(1000) DEFAULT NULL, 
  `request_id` varchar(200),
  `meta_id` varchar(200) DEFAULT NULL,
  `dt` varchar(50) DEFAULT NULL,
  `domain` varchar(30) DEFAULT NULL,
  `stream` varchar(30) DEFAULT NULL,
  `id` double DEFAULT NULL,
  `type` varchar(10) DEFAULT NULL,
  `namespace` int DEFAULT NULL,
  `title` text DEFAULT NULL,
  `user` varchar(150) DEFAULT NULL,
  `bot` varchar(20) DEFAULT NULL,
  `minor` tinyint(1) DEFAULT NULL,
  `pantrolled` varchar(20) DEFAULT NULL,
  `length_old` bigint DEFAULT NULL,
  `length_new` bigint DEFAULT NULL,
  `revision_old` bigint DEFAULT NULL,
  `revision_new` bigint DEFAULT NULL,
  `server_name` varchar(50) DEFAULT NULL,
  `wiki`  varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;