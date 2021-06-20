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

#include "clog/ob_log_file_pool.h"
#include "lib/utility/ob_tracepoint.h"
#include "clog/ob_clog_mgr.h"

#include <gtest/gtest.h>
#include <gmock/gmock.h>

using namespace oceanbase::clog;
using namespace oceanbase::common;

namespace oceanbase {
namespace clog {
extern int64_t get_free_quota(const char* path, const int64_t percent, int64_t& limit_free_quota);
}

namespace unittest {

class ObFormatter {
public:
  ObFormatter(char* buf, int64_t len) : buf_(buf), len_(len), pos_(0)
  {}
  ~ObFormatter()
  {}
  void reuse()
  {
    pos_ = 0;
  }
  const char* format(const char* format, ...)
  {
    char* src = NULL;
    int64_t count = 0;
    va_list ap;
    va_start(ap, format);
    if (NULL == buf_ || len_ <= 0 || pos_ >= len_) {
    } else if ((count = vsnprintf(buf_ + pos_, len_ - pos_, format, ap)) + 1 <= len_ - pos_) {
      src = buf_ + pos_;
      pos_ += count + 1;
    }
    va_end(ap);
    return src;
  }

private:
  char* buf_;
  int64_t len_;
  int64_t pos_;
};

#define LOG_DIR "/tmp/log_test_dir"
#define BUFSIZE 1024
#define MAX_FILE_ID 10

class TestObLogFilePool : public ::testing::Test {
public:
  virtual void SetUp()
  {
    char buf[BUFSIZE];
    ObFormatter formatter(buf, sizeof(buf));
    EXPECT_EQ(0, system(formatter.format("%s %s", "mkdir", LOG_DIR)));
    formatter.reuse();
    for (int i = 1; i <= MAX_FILE_ID; i++) {
      EXPECT_EQ(0, system(formatter.format("%s %s/%d", "touch", LOG_DIR, i)));
    }
  }
  virtual void TearDown()
  {
    char buf[BUFSIZE];
    ObFormatter formatter(buf, sizeof(buf));
    EXPECT_EQ(0, system(formatter.format("%s %s", "rm -rf", LOG_DIR)));
  }
  int write_data(const int fd, const char* buf, const int64_t data_len)
  {
    int ret = OB_SUCCESS;
    if (0 == write(fd, buf, data_len)) {
      ret = OB_IO_ERROR;
    }
    return ret;
  }

protected:
};

class MyFunc {
public:
  MyFunc()
  {}
  ~MyFunc()
  {}
  bool operator()(const char* dir_name, const char* entry)
  {
    UNUSED(dir_name);
    UNUSED(entry);
    return true;
  }
};

TEST_F(TestObLogFilePool, test_ob_dir)
{
  ObDir mydir;
  MyFunc fn;
  EXPECT_EQ(*(mydir.get_dir_name()), '\0');
  EXPECT_EQ(OB_NOT_INIT, mydir.for_each(fn));

  EXPECT_EQ(OB_INVALID_ARGUMENT, mydir.init(NULL));
  EXPECT_EQ(OB_SUCCESS, mydir.init(LOG_DIR));
  EXPECT_EQ(OB_INIT_TWICE, mydir.init(LOG_DIR));

  EXPECT_EQ(OB_SUCCESS, mydir.for_each(fn));
  EXPECT_EQ(0, strcmp(mydir.get_dir_name(), LOG_DIR));

  mydir.destroy();
}

TEST_F(TestObLogFilePool, test_ob_log_dir)
{
  ObLogDir log_dir;
  file_id_t min_file_id = OB_INVALID_FILE_ID;
  file_id_t max_file_id = OB_INVALID_FILE_ID;
  EXPECT_EQ(OB_SUCCESS, log_dir.init(LOG_DIR));

  EXPECT_EQ(OB_SUCCESS, log_dir.get_file_id_range(min_file_id, max_file_id));
  EXPECT_EQ(1, min_file_id);
  EXPECT_EQ(MAX_FILE_ID, max_file_id);

  EXPECT_EQ(0, strncmp(LOG_DIR, log_dir.get_dir_name(), strlen(LOG_DIR)));
}

TEST_F(TestObLogFilePool, test_ob_write_file_pool)
{
  file_id_t min_file_id = OB_INVALID_FILE_ID;
  file_id_t max_file_id = OB_INVALID_FILE_ID;
  ObLogDir log_dir;
  ObLogWriteFilePool clog_file_pool;
  EXPECT_EQ(OB_SUCCESS, log_dir.init(LOG_DIR));
  EXPECT_EQ(OB_SUCCESS, clog_file_pool.init(&log_dir, CLOG_FILE_SIZE, CLOG_WRITE_POOL));
  for (int i = 1; i < MAX_FILE_ID; i++) {
    int fd = -1;
    EXPECT_EQ(OB_SUCCESS, clog_file_pool.get_fd(i, fd));
    EXPECT_GE(fd, 0);
    EXPECT_EQ(OB_SUCCESS, clog_file_pool.close_fd(i, fd));
  }
  EXPECT_EQ(OB_SUCCESS, clog_file_pool.get_file_id_range(min_file_id, max_file_id));
  EXPECT_EQ(min_file_id, 1);
  EXPECT_EQ(max_file_id, MAX_FILE_ID);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_ob_log_file_pool.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest::test_ob_log_file_pool");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
