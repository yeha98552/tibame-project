CREATE TABLE `city` (
  `id` CHAR(32) NOT NULL PRIMARY KEY,
  `name` VARCHAR(255) NOT NULL,
  `name_en` VARCHAR(255) NOT NULL
) COMMENT = '縣市';

CREATE TABLE `type` (
  `id` CHAR(32) NOT NULL PRIMARY KEY,
  `name` VARCHAR(255) NOT NULL
) COMMENT = '種類';

CREATE TABLE `social_platform` (
  `id` CHAR(32) NOT NULL PRIMARY KEY,
  `name` VARCHAR(255) NOT NULL
) COMMENT = '社群平台';

CREATE TABLE `weather_station` (
  `id` CHAR(32) NOT NULL PRIMARY KEY,
  `station_orig_id` VARCHAR(255) NOT NULL COMMENT '氣象站原始ID',
  `name` VARCHAR(255) NOT NULL,
  `name_en` VARCHAR(255) NOT NULL,
  `lat` DECIMAL(8,6) NOT NULL COMMENT '緯度',
  `lon` DECIMAL(9,6) NOT NULL COMMENT '經度',
  `city_id` CHAR(32) NOT NULL,
  `address` VARCHAR(255) NOT NULL,
  FOREIGN KEY (`city_id`) REFERENCES `city` (`id`)
) COMMENT = '氣象站';

CREATE TABLE `attraction` (
  `attraction_id` CHAR(32) NOT NULL PRIMARY KEY,
  `name` VARCHAR(255) NOT NULL,
  `name_en` VARCHAR(255),
  `intro` VARCHAR(255) COMMENT '景點簡介',
  `googlemap_name` VARCHAR(255) NOT NULL COMMENT 'Google Map上的名稱',
  `googlemap_url` VARCHAR(255) NOT NULLCOMMENT 'Google Map的連結',
  `photo_url` VARCHAR(255) COMMENT 'tripadvisor景點照',
  `city_id` CHAR(32) NOT NULL,
  `lat` DECIMAL(8,6) NOT NULL COMMENT '緯度',
  `lon` DECIMAL(9,6) NOT NULL COMMENT '經度',
  FOREIGN KEY (`city_id`) REFERENCES `city` (`id`)
) COMMENT = '景點資訊';

CREATE TABLE `attraction_detail` (
  `attraction_id` CHAR(32) NOT NULL PRIMARY KEY,
  `info` VARCHAR(255) NOT NULL COMMENT '景點詳細介紹',
  FOREIGN KEY (`attraction_id`) REFERENCES `attraction` (`attraction_id`)
) COMMENT = '景點詳細資料';

CREATE TABLE `attraction_review` (
  `id` CHAR(32) NOT NULL PRIMARY KEY,
  `attraction_id` CHAR(32) NOT NULL,
  `author` VARCHAR(255) NOT NULL,
  `rating` FLOAT NOT NULL,
  `description` VARCHAR(255),
  `source_platform_id` CHAR(32) NOT NULL,
  `created_at` TIMESTAMP NOT NULL COMMENT '該評論撰寫的時間',
  `url` VARCHAR(255) NOT NULL,
  FOREIGN KEY (`attraction_id`) REFERENCES `attraction` (`attraction_id`),
  FOREIGN KEY (`source_platform_id`) REFERENCES `social_platform` (`id`)
) COMMENT = '景點在Google Map上的評論';

CREATE TABLE `attraction_social_article` (
  `id` CHAR(32) NOT NULL PRIMARY KEY,
  `attraction_id` CHAR(32) NOT NULL,
  `author` VARCHAR(255) NOT NULL,
  `description` VARCHAR(255),
  `url` VARCHAR(255) NOT NULL,
  `source_platform_id` CHAR(32) NOT NULL,
  `created_at` DATE NOT NULL,
  FOREIGN KEY (`attraction_id`) REFERENCES `attraction` (`attraction_id`),
  FOREIGN KEY (`source_platform_id`) REFERENCES `social_platform` (`id`)
) COMMENT = '景點社群媒體文章';

CREATE TABLE `attraction_history` (
  `attraction_id` CHAR(32) NOT NULL,
  `year` INTEGER NOT NULL,
  `month` INTEGER NOT NULL,
  `number_people` INTEGER NOT NULL,
  PRIMARY KEY (`attraction_id`, `year`, `month`),
  FOREIGN KEY (`attraction_id`) REFERENCES `attraction` (`attraction_id`)
) COMMENT = '景點過去每個月造訪人數(觀光局資料)';

CREATE TABLE `attraction_popularity` (
  `id` CHAR(32) NOT NULL PRIMARY KEY,
  `attraction_id` CHAR(32) NOT NULL,
  `value` INTEGER NOT NULL COMMENT '擁擠程度(0~100)',
  `hour` INTEGER NOT NULL,
  `created_at` TIMESTAMP NOT NULL,
  FOREIGN KEY (`attraction_id`) REFERENCES `attraction` (`attraction_id`)
) COMMENT = '景點每小時人潮(Live Google Maps popular times data)';

CREATE TABLE `attraction_type` (
  `attraction_id` CHAR(32) NOT NULL,
  `type_id` CHAR(32) NOT NULL,
  PRIMARY KEY (`attraction_id`, `type_id`),
  FOREIGN KEY (`attraction_id`) REFERENCES `attraction` (`attraction_id`),
  FOREIGN KEY (`type_id`) REFERENCES `type` (`id`)
) COMMENT = '景點種類(來自tripadvisor)';

CREATE TABLE `weather_history` (
  `station_id` CHAR(32) NOT NULL,
  `temperature_low` INTEGER NOT NULL,
  `temperature_high` INTEGER NOT NULL,
  `temperature_avg` INTEGER NOT NULL,
  `rainfall` INTEGER NOT NULL COMMENT '累積降雨量',
  `humidity_avg` INTEGER NOT NULL COMMENT '平均相對濕度',
  `total_sunshine_hrs` INTEGER NOT NULL COMMENT '總日照時數',
  `year` NOT NULL VARCHAR(255),
  `month` NOT NULLVARCHAR(255),
  PRIMARY KEY (`station_id`, `year`, `month`),
  FOREIGN KEY (`station_id`) REFERENCES `weather_station` (`id`)
) COMMENT = '縣市過去每月雨量和氣溫';

CREATE TABLE `weather_daily` (
  `id` CHAR(32) NOT NULL PRIMARY KEY,
  `station_id` CHAR(32) NOT NULL,
  `temperature_low` INTEGER NOT NULL,
  `temperature_high` INTEGER NOT NULL,
  `temperature_avg` INTEGER NOT NULL,
  `description` VARCHAR(255) NOT NULL,
  `current_date` DATE NOT NULL,
  FOREIGN KEY (`station_id`) REFERENCES `weather_station` (`id`)
) COMMENT = '縣市當天氣溫';

CREATE TABLE `weather_prediction` (
  `id` CHAR(32) NOT NULL PRIMARY KEY,
  `station_id` CHAR(32) NOT NULL,
  `temperature_low` INTEGER NOT NULL,
  `temperature_high` INTEGER NOT NULL,
  `temperature_avg` INTEGER NOT NULL,
  `rainfall` INTEGER NOT NULL COMMENT '降雨機率',
  `prediction_date` DATE NOT NULL COMMENT '預測的日期',
  `create_at` TIMESTAMP NOT NULL COMMENT '執行預測的時間/抓取日期',
  FOREIGN KEY (`station_id`) REFERENCES `weather_station` (`id`)
) COMMENT = '縣市未來7天的雨量和氣溫';

CREATE TABLE `attraction_hashtag` (
  `name` VARCHAR(255) NOT NULL PRIMARY KEY,
  `attraction_id` CHAR(32) NOT NULL,
  FOREIGN KEY (`attraction_id`) REFERENCES `attraction` (`attraction_id`)
) COMMENT = '景點同義詞';
