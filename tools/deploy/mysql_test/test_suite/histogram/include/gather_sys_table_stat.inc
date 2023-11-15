set ob_query_timeout=100000000;
if ($TENANT == 'mysql') {
  call dbms_stats.gather_table_stats('OCEANBASE','__ALL_VIRTUAL_CORE_ALL_TABLE');
  call dbms_stats.gather_table_stats('OCEANBASE','__ALL_VIRTUAL_CORE_COLUMN_TABLE');
  call dbms_stats.gather_table_stats('OCEANBASE','__ALL_TABLE');
  call dbms_stats.gather_table_stats('OCEANBASE','__ALL_COLUMN');
  call dbms_stats.gather_table_stats('OCEANBASE','__ALL_TABLE_STAT');
  call dbms_stats.gather_table_stats('OCEANBASE','__ALL_COLUMN_STAT');
}

if ($TENANT == 'oracle') {
  call dbms_stats.gather_table_stats('SYS','ALL_VIRTUAL_CORE_ALL_TABLE');
  call dbms_stats.gather_table_stats('SYS','ALL_VIRTUAL_CORE_COLUMN_TABLE');
  call dbms_stats.gather_table_stats('SYS','ALL_VIRTUAL_TABLE_REAL_AGENT');
  call dbms_stats.gather_table_stats('SYS','ALL_VIRTUAL_CORE_COLUMN_TABLE');
  call dbms_stats.gather_table_stats('SYS','ALL_VIRTUAL_COLUMN_REAL_AGENT');
}
set ob_query_timeout=default;