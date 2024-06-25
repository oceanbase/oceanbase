#package_name: external_table_alert_log
#author: liangjinrong.ljr

create database if not exists sys_external_tbs;
//

create external table if not exists sys_external_tbs.__all_external_alert_log_info (
  time varchar(30) as (metadata$filecol1),
  level varchar(10) as (metadata$filecol2),
  module varchar(30) as (metadata$filecol3),
  event varchar(100) as (metadata$filecol4),
  errcode int as (metadata$filecol5),
  ip varchar(100) as (substr(metadata$fileurl, 1, instr(metadata$fileurl, ':') - 1)),
  port varchar(100) as (substr(metadata$fileurl, instr(metadata$fileurl, ':') + 1, instr(metadata$fileurl, '%') - instr(metadata$fileurl, ':') - 1)),
  tenant_id int as (metadata$filecol6),
  thread_id int as (metadata$filecol7),
  thread_name varchar(50) as (metadata$filecol8),
  trace_id varchar(50) as (metadata$filecol9),
  func_name varchar(100) as (metadata$filecol10),
  code_location varchar(100) as (metadata$filecol11),
  message longtext as (metadata$filecol12)
)
location = 'log/alert'
format = (
  type = 'csv'
  field_delimiter = '|'
  field_optionally_enclosed_by = '"'
)
pattern = 'alert.log[.0-9]*'
;
//
