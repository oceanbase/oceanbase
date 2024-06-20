/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX LIB
#include <gtest/gtest.h>
#include <utime.h>
#include <string>
#include <fstream>
#include <dirent.h>
#include <regex.h>

#define protected public
#define private public

#include "lib/oblog/ob_log_compressor.h"
#include "lib/oblog/ob_log.h"
#include "lib/compress/ob_compressor_pool.h"
#include "common/ob_clock_generator.h"

using namespace oceanbase::lib;
namespace oceanbase {
namespace common {

const char *TEST_DIR = "testoblogcompressdir";
const char *TEST_ALERT_DIR = "testoblogcompressdiralert";
const char *RM_COMMAND = "rm -rf testoblogcompressdir testoblogcompressdiralert";

#define  CREATE_FILES(file_count, modify_time, timestamp) \
  for (int i = 0; i < file_count; i++) {  \
    snprintf(file_name, sizeof(file_name), "%s/%s.%ld", TEST_DIR, \
      OB_SYSLOG_FILE_PREFIX[i%OB_SYSLOG_COMPRESS_TYPE_COUNT], timestamp + i);  \
    FILE *file =fopen(file_name, "w");  \
    ASSERT_EQ(true, OB_NOT_NULL(file)); \
    ASSERT_EQ(data_size, fwrite(data, 1, data_size, file)); \
    fclose(file); \
    OB_LOG_COMPRESSOR.set_last_modify_time_(file_name, modify_time + i*60);  \
  }

int get_file_count_by_regex(const char *dirname, const char *pattern) {
  int count = 0;
  regex_t regex;
  struct dirent *entry;
  DIR *dir = opendir(dirname);

  if (OB_ISNULL(dir)) {
    count = -1;
  } else if (OB_NOT_NULL(pattern) && regcomp(&regex, pattern, REG_EXTENDED | REG_NOSUB) != 0) {
    closedir(dir);
    count = -1;
  } else {
    while ((entry = readdir(dir)) != NULL) {
      if (strncmp(entry->d_name, ".", 1) == 0 || strncmp(entry->d_name, "..", 2) == 0) {
        continue;
      }
      if (OB_ISNULL(pattern) || regexec(&regex, entry->d_name, 0, NULL, 0) == 0) {
        count++;
      }
    }

    closedir(dir);
    if (OB_NOT_NULL(pattern)) {
      regfree(&regex);
    }
  }

  return count;
}

int compress_file_by_zstd(ObCompressor *compressor, const char *file_name_in, const char *file_name_out)
{
  int ret = OB_SUCCESS;

  int src_size = OB_SYSLOG_COMPRESS_BLOCK_SIZE;
  int dest_size = OB_SYSLOG_COMPRESS_BUFFER_SIZE;
  char *src_buf = (char *)ob_malloc(src_size + dest_size, ObModIds::OB_COMPRESSOR);
  char *dest_buf = src_buf + src_size;
  size_t read_size = 0;
  int64_t w_size = 0;
  int sleep_10us = 10 * 1000;

  FILE *input_file = fopen(file_name_in, "r");
  FILE *output_file = fopen(file_name_out, "w");
  fwrite(dest_buf, 1, w_size, output_file);

  while (OB_SUCC(ret) && !feof(input_file)) {
    if ((read_size = fread(src_buf, 1, src_size, input_file)) > 0) {
      if (OB_FAIL(compressor->compress(src_buf, read_size, dest_buf, dest_size, w_size))) {
        printf("Failed to log_compress_block, err_code=%d.\n", ret);
      } else if (w_size != fwrite(dest_buf, 1, w_size, output_file)) {
        ret = OB_ERR_SYS;
        printf("Failed to fwrite, err_code=%d.\n", errno);
      }
    }
  }
  fclose(input_file);
  fclose(output_file);

  // clear environment
  ob_free(src_buf);

  return ret;
}

TEST(ObLogCompressor, log_priority_array_test)
{
  int ret = OB_SUCCESS;
  const int MAX_FILE_COUNT = OB_SYSLOG_DELETE_ARRAY_SIZE - 1;
  ObSyslogFile syslog;
  const ObSyslogFile *file_name_out;
  ObSyslogCompareFunctor cmp_;
  ObSyslogPriorityArray priority_array(cmp_);

  for (int i = 0; i < MAX_FILE_COUNT; i++) {
    syslog.mtime_ = 1500 + i;
    snprintf(syslog.file_name_, sizeof(syslog.file_name_), "priority_array_test_%d", 1500 + i);
    ASSERT_EQ(OB_SUCCESS, priority_array.push(syslog));
  }
  ASSERT_EQ(MAX_FILE_COUNT, priority_array.count());
  for (int i = 0; i < MAX_FILE_COUNT/2; i++) {
    syslog.mtime_ = 1200 + i;
    snprintf(syslog.file_name_, sizeof(syslog.file_name_), "priority_array_test_%d", 1200 + i);
    ASSERT_EQ(OB_SUCCESS, priority_array.push(syslog));
  }
  ASSERT_EQ(MAX_FILE_COUNT, priority_array.count());
  for (int i = 0; i < MAX_FILE_COUNT/2; i++) {
    file_name_out = NULL;
    snprintf(syslog.file_name_, sizeof(syslog.file_name_), "priority_array_test_%d", 1200 + i);
    ASSERT_EQ(OB_SUCCESS, priority_array.top(file_name_out));
    ASSERT_EQ(true, OB_NOT_NULL(file_name_out));
    ASSERT_EQ(0, strncmp(syslog.file_name_, file_name_out->file_name_, strlen(syslog.file_name_)));
    ASSERT_EQ(OB_SUCCESS, priority_array.pop());
  }
  for (int i = 0; i < MAX_FILE_COUNT/2; i++) {
    file_name_out = NULL;
    snprintf(syslog.file_name_, sizeof(syslog.file_name_), "priority_array_test_%d", 1500 + i);
    ASSERT_EQ(OB_SUCCESS, priority_array.top(file_name_out));
    ASSERT_EQ(true, OB_NOT_NULL(file_name_out));
    ASSERT_EQ(0, strncmp(syslog.file_name_, file_name_out->file_name_, strlen(syslog.file_name_)));
    ASSERT_EQ(OB_SUCCESS, priority_array.pop());
  }
}

TEST(ObLogCompressor, base_compressor_test)
{
  int ret = OB_SUCCESS;
  int test_count = 1000;
  int test_size = test_count * sizeof(int);
  ObCompressor *compressor_zstd;

  // get compressor
  ASSERT_EQ(OB_SUCCESS, ObCompressorPool::get_instance().get_compressor(ZSTD_1_3_8_COMPRESSOR, compressor_zstd));

  // prepare file
  const char *file_name = "test_ob_log_compressor_file";
  const char *file_name_zstd = "test_ob_log_compressor_file.zst";
  std::string file_name_str = std::string(file_name);
  std::string decompress_file_name_zstd = file_name_str + "_zstd";
  unlink(file_name_str.c_str());
  unlink(file_name_zstd);
  unlink(decompress_file_name_zstd.c_str());

  FILE *input_file = fopen(file_name, "w");
  ASSERT_EQ(true, NULL != input_file);
  int data[test_count];
  for (int i = 0; i < test_count; i++) {
    data[i] = i;
  }
  ASSERT_EQ(test_size, fwrite(data, 1, test_size, input_file));
  fclose(input_file);
  // sleep 1s
  sleep(1);
  struct stat st, st_zstd;
  ASSERT_EQ(0, stat(file_name, &st));

  // compress
  ASSERT_EQ(OB_SUCCESS, compress_file_by_zstd(compressor_zstd, file_name, file_name_zstd));
  ASSERT_EQ(0, stat(file_name_zstd, &st_zstd));
  ASSERT_EQ(true, st.st_mtime != st_zstd.st_mtime); // not equal

  // modify time
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.set_last_modify_time_(file_name_zstd, st.st_mtime));
  ASSERT_EQ(0, stat(file_name_zstd, &st_zstd));
  ASSERT_EQ(true, st.st_mtime == st_zstd.st_mtime); // equal

  // decompress, make sure your machine can exec zstd command
  std::string decompress_zstd_command = "zstd -d " + std::string(file_name_zstd) + " -o " + decompress_file_name_zstd;
  ASSERT_EQ(0, std::system(decompress_zstd_command.c_str()));

  // check
  std::string compare_command = "diff -q " + file_name_str + " " + decompress_file_name_zstd + " > /dev/null";
  ASSERT_EQ(0, system(compare_command.c_str()));

  unlink(file_name_str.c_str());
  unlink(file_name_zstd);
  unlink(decompress_file_name_zstd.c_str());
}

TEST(ObLogCompressor, syslog_compressor_base_test)
{
  int ret = OB_SUCCESS;

  // prepare
  int data_count = 1000;
  int loop_count = 2000;
  int data_size = data_count * sizeof(int);
  int data[data_count];
  const char *file_name = "testcompressfile.log.1234";
  const char *file_name_2 = "testcompressfile2.log.1234";
  const char *file_name_zstd = "testcompressfile.log.1234.zst";
  ASSERT_EQ(false, ObLogCompressor::is_compressed_file(file_name));
  ASSERT_EQ(true, ObLogCompressor::is_compressed_file(file_name_zstd));
  std::string file_name_str = std::string(file_name);
  std::string file_name_str_2 = std::string(file_name_2);
  std::string decompress_file_name_zstd = file_name_str + "_zstd";
  unlink(file_name_str.c_str());
  unlink(file_name_str_2.c_str());
  unlink(file_name_zstd);
  unlink(decompress_file_name_zstd.c_str());

  // function test
  ASSERT_EQ(0, system(RM_COMMAND));
  ASSERT_EQ(0, mkdir(TEST_DIR, 0777));
  strncpy(OB_LOG_COMPRESSOR.syslog_dir_, TEST_DIR, strlen(TEST_DIR));
  int64_t free_size = OB_LOG_COMPRESSOR.get_disk_remaining_size_();
  printf("free_size:%ld\n", free_size);
  ASSERT_GT(free_size, 0);
  ASSERT_EQ(0, system(RM_COMMAND));

  // prepare file data
  FILE *input_file = fopen(file_name, "w");
  ASSERT_EQ(true, NULL != input_file);
  for (int i = 0; i < data_count; i++) {
    data[i] = i;
  }
  for (int j = 0; j < loop_count; j++) {
    ASSERT_EQ(data_size, fwrite(data, 1, data_size, input_file));
  }
  fclose(input_file);
  std::string cp_file_command = "cp " + std::string(file_name) + " " + std::string(file_name_2);
  ASSERT_EQ(0, std::system(cp_file_command.c_str()));

  // record last modify time
  struct stat st, st_zstd;
  ASSERT_EQ(0, stat(file_name, &st));

  // compress
  ObCompressor *compressor_zstd;
  ASSERT_EQ(OB_SUCCESS, ObCompressorPool::get_instance().get_compressor(ZSTD_1_3_8_COMPRESSOR, compressor_zstd));
  LOG_INFO("start to init syslog compressor ");
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.init());
  strncpy(OB_LOG_COMPRESSOR.syslog_dir_, TEST_DIR, strlen(TEST_DIR));
  OB_LOG_COMPRESSOR.compressor_ = compressor_zstd;
  ASSERT_EQ(true, OB_LOG_COMPRESSOR.is_inited_);
  ASSERT_EQ(false, OB_LOG_COMPRESSOR.stopped_);
  usleep(100000); // wait register pm in farm

  int src_size = OB_SYSLOG_COMPRESS_BLOCK_SIZE;
  int dest_size = OB_SYSLOG_COMPRESS_BUFFER_SIZE;
  char *src_buf = (char *)ob_malloc(src_size + dest_size, ObModIds::OB_LOG);
  ASSERT_EQ(true, OB_NOT_NULL(src_buf));
  char *dest_buf = src_buf + src_size;
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.compress_single_file_(file_name, src_buf, dest_buf));
  ASSERT_EQ(0, stat(file_name_zstd, &st_zstd));
  ASSERT_EQ(true, st.st_mtime == st_zstd.st_mtime); // equal

  // decompress
  std::string decompress_zstd_command = "zstd -d " + std::string(file_name_zstd) + " -o " + decompress_file_name_zstd;
  ASSERT_EQ(0, std::system(decompress_zstd_command.c_str()));
  std::string compare_command = "diff -q " + file_name_str_2 + " " + decompress_file_name_zstd + " > /dev/null";
  ASSERT_EQ(0, std::system(compare_command.c_str()));

  // error test
  ASSERT_EQ(OB_INVALID_ARGUMENT, OB_LOG_COMPRESSOR.set_compress_func("zlib_1.0"));
  ASSERT_EQ(OB_INVALID_ARGUMENT, OB_LOG_COMPRESSOR.set_compress_func("lz4_1.9.1"));
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.set_compress_func("none"));

  // clear
  LOG_INFO("start to destroy syslog compressor ");
  OB_LOG_COMPRESSOR.destroy();
  ob_free(src_buf);
  unlink(file_name_str.c_str());
  unlink(file_name_str_2.c_str());
  unlink(file_name_zstd);
  unlink(decompress_file_name_zstd.c_str());
}

TEST(ObLogCompressor, syslog_compressor_thread_test)
{
  int ret = OB_SUCCESS;

  // init log compressor
  ASSERT_EQ(OB_SUCCESS, ObClockGenerator::get_instance().init());
  // loop faster
  OB_LOG_COMPRESSOR.loop_interval_ = 100000;
  LOG_INFO("start to init syslog compressor ");
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.init());
  ASSERT_EQ(true, OB_LOG_COMPRESSOR.is_inited_);
  strncpy(OB_LOG_COMPRESSOR.syslog_dir_, TEST_DIR, strlen(TEST_DIR));
  strncpy(OB_LOG_COMPRESSOR.alert_log_dir_, TEST_ALERT_DIR, strlen(TEST_ALERT_DIR));

  // prepare syslog file
  const int MAX_SYSLOG_COUNT = 8;
  const int min_uncompressed_count = 2;
  char file_name[OB_MAX_SYSLOG_FILE_NAME_SIZE];
  char * file_name_out;
  const char *data = "base_data.base_data.base_data.base_data.base_dat50base_data.base_data.base_data.base_data.base_da100"
                     "base_data.base_data.base_data.base_data.base_da150base_data.base_data.base_data.base_data.base_da200";
  int data_size = strlen(data);
  int file_size = 200;
  const char *pattern_uncompressed[OB_SYSLOG_COMPRESS_TYPE_COUNT] =
  {
    "^observer\\.log\\.[0-9]+$",
    "^rootservice\\.log\\.[0-9]+$",
    "^election\\.log\\.[0-9]+$",
    "^trace\\.log\\.[0-9]+$",
  };
  const char *pattern_compressed[OB_SYSLOG_COMPRESS_TYPE_COUNT] =
  {
    "^observer\\.log\\.[0-9]+\\.[a-z]+$",
    "^rootservice\\.log\\.[0-9]+\\.[a-z]+$",
    "^election\\.log\\.[0-9]+\\.[a-z]+$",
    "^trace\\.log\\.[0-9]+\\.[a-z]+$",
  };

  // get modify time
  time_t last_modified_time = time(NULL);

  last_modified_time -= 1000;
  ASSERT_EQ(0, system(RM_COMMAND));
  ASSERT_EQ(0, mkdir(TEST_DIR, 0777));
  ASSERT_EQ(0, mkdir(TEST_ALERT_DIR, 0777));
  for (int i = 0; i < OB_SYSLOG_COMPRESS_TYPE_COUNT; i++) {
    snprintf(file_name, sizeof(file_name), "%s/%s", TEST_DIR, OB_SYSLOG_FILE_PREFIX[i%OB_SYSLOG_COMPRESS_TYPE_COUNT]);
    FILE *file =fopen(file_name, "w");
    ASSERT_EQ(true, OB_NOT_NULL(file));
    ASSERT_EQ(data_size, fwrite(data, 1, data_size, file));
    fclose(file);
    OB_LOG_COMPRESSOR.set_last_modify_time_(file_name, last_modified_time + i*60);
  }
  last_modified_time -= 50000;
  CREATE_FILES(MAX_SYSLOG_COUNT * OB_SYSLOG_COMPRESS_TYPE_COUNT, last_modified_time, 202311241000L)

  // set parameter
  ASSERT_EQ(OB_SUCCESS, OB_LOGGER.set_max_file_index(MAX_SYSLOG_COUNT));
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.set_min_uncompressed_count(min_uncompressed_count));
  int total_count = get_file_count_by_regex(TEST_DIR, NULL);
  int last_total_count = 0;
  ASSERT_EQ((MAX_SYSLOG_COUNT + 1) * OB_SYSLOG_COMPRESS_TYPE_COUNT, total_count);
  for (int i = 0; i < OB_SYSLOG_COMPRESS_TYPE_COUNT; i++) {
    int uncompressed_count = get_file_count_by_regex(TEST_DIR, pattern_uncompressed[i]);
    ASSERT_EQ(MAX_SYSLOG_COUNT, uncompressed_count);
    int compressed_count = get_file_count_by_regex(TEST_DIR, pattern_compressed[i]);
    ASSERT_EQ(0, compressed_count);
  }

  // enable compress
  int64_t max_disk_size = OB_SYSLOG_COMPRESS_RESERVE_SIZE + file_size * ((MAX_SYSLOG_COUNT + 1 - 2)* OB_SYSLOG_COMPRESS_TYPE_COUNT);
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.set_max_disk_size(max_disk_size));
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.set_compress_func("zstd_1.3.8"));
  sleep(2);  // 2s
  total_count = get_file_count_by_regex(TEST_DIR, NULL);
  ASSERT_EQ((MAX_SYSLOG_COUNT + 1) * OB_SYSLOG_COMPRESS_TYPE_COUNT, total_count);
  for (int i = 0; i < OB_SYSLOG_COMPRESS_TYPE_COUNT; i++) {
    int uncompressed_count = get_file_count_by_regex(TEST_DIR, pattern_uncompressed[i]);
    ASSERT_LE(uncompressed_count, MAX_SYSLOG_COUNT - 2);
    ASSERT_GE(uncompressed_count, min_uncompressed_count);
    int compressed_count = get_file_count_by_regex(TEST_DIR, pattern_compressed[i]);
    ASSERT_GE(compressed_count, 2);
  }

  // add more syslog file
  last_modified_time += 10000;
  CREATE_FILES(2 * OB_SYSLOG_COMPRESS_TYPE_COUNT, last_modified_time, 202311242000L)
  sleep(1);
  total_count = get_file_count_by_regex(TEST_DIR, NULL);
  ASSERT_EQ((MAX_SYSLOG_COUNT + 3) * OB_SYSLOG_COMPRESS_TYPE_COUNT, total_count);
  for (int i = 0; i < OB_SYSLOG_COMPRESS_TYPE_COUNT; i++) {
    int uncompressed_count = get_file_count_by_regex(TEST_DIR, pattern_uncompressed[i]);
    ASSERT_LE(uncompressed_count, MAX_SYSLOG_COUNT - 2);
    ASSERT_GE(uncompressed_count, min_uncompressed_count);
    int compressed_count = get_file_count_by_regex(TEST_DIR, pattern_compressed[i]);
    ASSERT_GE(compressed_count, 4);
  }

  // make syslog_disk_size smaller, begin to delete file
  max_disk_size = OB_SYSLOG_DELETE_RESERVE_SIZE + file_size * (4 * OB_SYSLOG_COMPRESS_TYPE_COUNT);
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.set_max_disk_size(max_disk_size));
  sleep(1);
  total_count = get_file_count_by_regex(TEST_DIR, NULL);
  ASSERT_LT(total_count, (MAX_SYSLOG_COUNT + 3) * OB_SYSLOG_COMPRESS_TYPE_COUNT);
  ASSERT_GE(total_count, 4 * OB_SYSLOG_COMPRESS_TYPE_COUNT);
  for (int i = 0; i < OB_SYSLOG_COMPRESS_TYPE_COUNT; i++) {
    int uncompressed_count = get_file_count_by_regex(TEST_DIR, pattern_uncompressed[i]);
    ASSERT_GE(uncompressed_count, min_uncompressed_count);
  }
  last_total_count = total_count;

  // add more syslog file
  last_modified_time += 5000;
  CREATE_FILES(2 * OB_SYSLOG_COMPRESS_TYPE_COUNT, last_modified_time, 202311243000L)
  sleep(1);
  total_count = get_file_count_by_regex(TEST_DIR, NULL);
  ASSERT_LE(total_count, last_total_count);
  ASSERT_GE(total_count, 4 * OB_SYSLOG_COMPRESS_TYPE_COUNT);
  for (int i = 0; i < OB_SYSLOG_COMPRESS_TYPE_COUNT; i++) {
    int uncompressed_count = get_file_count_by_regex(TEST_DIR, pattern_uncompressed[i]);
    ASSERT_GE(uncompressed_count, min_uncompressed_count);
  }

  // disable compress
  ASSERT_EQ(OB_SUCCESS, OB_LOG_COMPRESSOR.set_compress_func("none"));
  last_modified_time += 5000;
  CREATE_FILES(2 * OB_SYSLOG_COMPRESS_TYPE_COUNT, last_modified_time, 202311244000L)
  sleep(1);
  total_count = get_file_count_by_regex(TEST_DIR, NULL);
  ASSERT_LT(total_count, 5 * OB_SYSLOG_COMPRESS_TYPE_COUNT);
  ASSERT_GE(total_count, 4 * OB_SYSLOG_COMPRESS_TYPE_COUNT);
  for (int i = 0; i < OB_SYSLOG_COMPRESS_TYPE_COUNT; i++) {
    int compressed_count = get_file_count_by_regex(TEST_DIR, pattern_compressed[i]);
    ASSERT_EQ(compressed_count, 0);
  }

  // clear
  ASSERT_EQ(0, system(RM_COMMAND));
  LOG_INFO("start to destroy syslog compressor ");
  OB_LOG_COMPRESSOR.destroy();
}

}  // namespace common
}  // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::ObLogger &logger = oceanbase::ObLogger::get_logger();
  logger.set_file_name("test_ob_log_compressor.log", true);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  OB_LOG_COMPRESSOR.destroy();
  return ret;
}
