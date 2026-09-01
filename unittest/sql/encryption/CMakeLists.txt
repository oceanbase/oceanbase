sql_unittest(test_ob_sha256_crypt_mysql_compat
  test_ob_sha256_crypt_mysql_compat.cpp
  ../../../deps/oblib/src/lib/encrypt/ob_sha256_crypt.cpp
  ../../../deps/oblib/src/lib/encrypt/ob_crypt_common.cpp
)

sql_unittest(test_ob_caching_sha2_cache
  test_ob_caching_sha2_cache.cpp
  ../../../deps/oblib/src/lib/encrypt/ob_auth_digest_cache.cpp
  ../../../deps/oblib/src/lib/encrypt/ob_sha256_crypt.cpp
  ../../../deps/oblib/src/lib/encrypt/ob_crypt_common.cpp
)

sql_unittest(test_ob_sm3_crypt
  test_ob_sm3_crypt.cpp
  ../../../deps/oblib/src/lib/encrypt/ob_sm3_crypt.cpp
  ../../../deps/oblib/src/lib/encrypt/ob_sha256_crypt.cpp
  ../../../deps/oblib/src/lib/encrypt/ob_crypt_common.cpp
)
