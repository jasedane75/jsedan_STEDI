--landing de acelerometro

CREATE EXTERNAL TABLE IF NOT EXISTS `db_test`.`raw_accelerometer` (
  `timeStamp` string COMMENT 'fecha',
  `user` string COMMENT 'Correo del usuario',
  `x` float,
  `y` float,
  `z` float
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://jsedan-files/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');

