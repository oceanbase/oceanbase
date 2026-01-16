sql_unittest(test_parquet_bloom_filter)
sql_unittest(test_orc_bloom_filter)
file(COPY . DESTINATION . FILES_MATCHING PATTERN "*.parquet")
file(COPY . DESTINATION . FILES_MATCHING PATTERN "*.orc")
