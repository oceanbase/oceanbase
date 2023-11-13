#log_unittest(basic_archive)
#log_unittest(test_archive_task)
#log_unittest(test_archive_manage)
#log_unittest(test_archive_file_utils)

set(ARCHIVE_TEST_SRCS
    basic_archive.cpp
    fake_archive_piece_mgr.cpp)

add_library(archive_test ${ARCHIVE_TEST_SRCS})

target_include_directories(archive_test PUBLIC
  ${CMAKE_SOURCE_DIR}/unittest ${CMAKE_SOURCE_DIR}/mittest)
target_link_libraries(archive_test PUBLIC
  oceanbase
)

function(ob_unittest_archive case)
  ob_unittest(${ARGV})
  target_link_libraries(${case} PRIVATE gtest gmock archive_test observer_test oceanbase)
endfunction()

#ob_unittest_archive(test_archive_manage test_archive_manage.cpp)
ob_unittest_archive(test_archive_file_utils test_archive_file_utils.cpp)
ob_unittest_archive(test_restore_archive_log test_restore_archive_log.cpp)
#ob_unittest_archive(test_restore_mysql_proxy test_restore_mysql_proxy.cpp)
#ob_unittest_archive(test_restore_oracle_proxy test_restore_oracle_proxy.cpp)
