sql_unittest(test_ob_sha256_crypt_mysql_compat
  test_ob_sha256_crypt_mysql_compat.cpp
  ../../../deps/oblib/src/lib/encrypt/ob_sha256_crypt.cpp
)

sql_unittest(test_ob_caching_sha2_cache
  test_ob_caching_sha2_cache.cpp
  ../../../deps/oblib/src/lib/encrypt/ob_caching_sha2_cache.cpp
  ../../../deps/oblib/src/lib/encrypt/ob_sha256_crypt.cpp
)
