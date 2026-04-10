--Creación de las tablas en zona landing

-- landing de step trainer

CREATE EXTERNAL TABLE IF NOT EXISTS `db_test`.`raw_step_trainer` (
  `sensorReadingTime` string COMMENT 'Fecha de lectura',
  `serialNumber` string COMMENT 'Numero de serial',
  `distanceFromObject` bigint COMMENT 'Distancia al objeto'
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://jsedan-files/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json');




