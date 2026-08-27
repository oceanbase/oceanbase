sql_unittest(test_parquet_bloom_filter)
sql_unittest(test_parquet_lazy_ranges)
sql_unittest(test_orc_bloom_filter)
sql_unittest(test_iceberg_delete_application)
sql_unittest(test_iceberg_partition_value)
target_sources(test_iceberg_partition_value PRIVATE
  ../engine/basic/utils/expr_maker.cpp)
file(COPY . DESTINATION . FILES_MATCHING PATTERN "*.parquet")
file(COPY . DESTINATION . FILES_MATCHING PATTERN "*.orc")
