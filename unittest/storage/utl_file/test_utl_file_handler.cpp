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

#define USING_LOG_PREFIX COMMON
#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "lib/string/ob_string.h"
#include "storage/utl_file/ob_utl_file_handler.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "share/ob_simple_mem_limit_getter.h"

namespace oceanbase
{
namespace common
{
static ObSimpleMemLimitGetter getter;
class TestUtlFileHandler : public blocksstable::TestDataFilePrepare
{
public:
  TestUtlFileHandler()
    : blocksstable::TestDataFilePrepare(&getter, "TestUtlFileHandler")
  {
  }
  virtual ~TestUtlFileHandler()
  {
  }
  virtual void SetUp();
  virtual void TearDown();
private:
  struct md5str
  {
    md5str()
    {
      MEMSET(str, 0, sizeof(str));
    }
    bool operator==(const md5str& rhs) const
    {
      return 0 == MEMCMP(str, rhs.str, sizeof(str));
    }
    ObString to_string() const
    {
      ObString s(sizeof(str), str);
      return s;
    }
    char str[32];
  };

  int md5sum(const char *dir, const char *filename, int64_t begin, int64_t end, md5str &result);
  int md5sum(const char *dir, const char *filename, md5str &result);
  int md5sum_impl(const char *cmd, md5str &result);
  int gen_src_log_file(const char *filename);
public:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestUtlFileHandler);
};

void TestUtlFileHandler::SetUp()
{
  system("rm -rf ./data_TestUtlFileHandler");
  TestDataFilePrepare::SetUp(); // init dir and io device
  FileDirectoryUtils::create_full_path("./data_TestUtlFileHandler");
}

void TestUtlFileHandler::TearDown()
{
  THE_IO_DEVICE->destroy();
  TestDataFilePrepare::TearDown();
}

int TestUtlFileHandler::md5sum(const char *dir, const char *filename, int64_t begin, int64_t end, md5str &result)
{
  int ret = OB_SUCCESS;
  char cmd[256] = { 0 };
  FILE *stream = NULL;
  int tmp_ret = snprintf(cmd, sizeof(cmd), "head -n %ld %s/%s | tail -n %ld | md5sum",
      end, dir, filename, end - begin + 1);
  if (tmp_ret <= 0 || tmp_ret >= sizeof(cmd)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to format cmd", K(ret), K(tmp_ret));
  }

  if (OB_SUCC(ret)) {
    ret = md5sum_impl(cmd, result);
  }
  return ret;
}

int TestUtlFileHandler::md5sum(const char *dir, const char *filename, md5str &result)
{
  int ret = OB_SUCCESS;
  char cmd[256] = { 0 };
  int tmp_ret = snprintf(cmd, sizeof(cmd), "md5sum %s/%s", dir, filename);
  if (tmp_ret <= 0 || tmp_ret >= sizeof(cmd)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to format cmd", K(ret), K(tmp_ret));
  }

  if (OB_SUCC(ret)) {
    ret = md5sum_impl(cmd, result);
  }
  return ret;
}

int TestUtlFileHandler::md5sum_impl(const char *cmd, md5str &result)
{
  int ret = OB_SUCCESS;
  FILE *stream = NULL;
  if (OB_UNLIKELY(NULL == (stream = popen(cmd, "r")))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to popen", K(ret), K(stream), K(cmd));
  } else if (OB_UNLIKELY(sizeof(result.str) != fread(result.str, 1, sizeof(result.str), stream))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to read", K(ret));
  }
  if (NULL != stream) {
    pclose(stream);
  }
  return ret;
}

int TestUtlFileHandler::gen_src_log_file(const char *filename)
{
  int ret = OB_SUCCESS;
  char cmd[256] = { 0 };
  int tmp_ret = snprintf(cmd, sizeof(cmd), "dd if=/usr/share/dict/words of=%s count=%d bs=1024", filename, 128*1024);
  if (tmp_ret <= 0 || tmp_ret >= sizeof(cmd)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to format cmd", K(ret), K(tmp_ret));
  }

  system(cmd);
  return ret;
}

TEST_F(TestUtlFileHandler, fcopy)
{
  const char *dir = ".";
  const char *src_filename = "observer.log";
  const char *dst_filename = "copy.log";

  ASSERT_EQ(OB_SUCCESS, gen_src_log_file(src_filename));

  // begin is bigger than end;
  {
    system("rm -f copy.log");
    int64_t begin = 10;
    int64_t end = 0;
    int ret = ObUtlFileHandler::fcopy(dir, src_filename, dir, dst_filename, begin, &end);
    ASSERT_NE(OB_SUCCESS, ret);
  }

  // begin is -1, negative
  {
    system("rm -f copy.log");
    int64_t begin = -1;
    int64_t end = 0;
    int ret = ObUtlFileHandler::fcopy(dir, src_filename, dir, dst_filename, begin, &end);
    ASSERT_NE(OB_SUCCESS, ret);
  }

  // begin is 0
  {
    system("rm -f copy.log");
    int64_t begin = 0;
    int64_t end = 100;
    int ret = ObUtlFileHandler::fcopy(dir, src_filename, dir, dst_filename, begin, &end);
    ASSERT_NE(OB_SUCCESS, ret);
  }

  // no end, whole file
  {
    system("rm -f copy.log");
    int64_t begin = 1;
    int ret = ObUtlFileHandler::fcopy(dir, src_filename, dir, dst_filename, begin);
    ASSERT_EQ(OB_SUCCESS, ret);

    md5str src_result;
    md5str dst_result;
    ASSERT_EQ(OB_SUCCESS, md5sum(dir, src_filename, src_result));
    ASSERT_EQ(OB_SUCCESS, md5sum(dir, dst_filename, dst_result));
    LOG_INFO("md5sum", "src_result", src_result.to_string(), "dst_result", dst_result.to_string());
    ASSERT_TRUE(src_result == dst_result);
  }

  // copy 10 lines
  {
    system("rm -f copy.log");
    int64_t begin = 1;
    int64_t end = 10;
    int ret = ObUtlFileHandler::fcopy(dir, src_filename, dir, dst_filename, begin, &end);
    ASSERT_EQ(OB_SUCCESS, ret);

    md5str src_result;
    md5str dst_result;
    ASSERT_EQ(OB_SUCCESS, md5sum(dir, src_filename, begin, end, src_result));
    ASSERT_EQ(OB_SUCCESS, md5sum(dir, dst_filename, dst_result));
    LOG_INFO("md5sum", "src_result", src_result.to_string(), "dst_result", dst_result.to_string());
    ASSERT_TRUE(src_result == dst_result);
  }

  // copy more lines
  // TODO: 4.0 unstable, examine later
  /*{
    system("rm -f copy.log");
    int64_t begin = 1234;
    int64_t end = 5678;
    int ret = ObUtlFileHandler::fcopy(dir, src_filename, dir, dst_filename, begin, &end);
    ASSERT_EQ(OB_SUCCESS, ret);

    md5str src_result;
    md5str dst_result;
    ASSERT_EQ(OB_SUCCESS, md5sum(dir, src_filename, begin, end, src_result));
    ASSERT_EQ(OB_SUCCESS, md5sum(dir, dst_filename, dst_result));
    LOG_INFO("md5sum", "src_result", src_result.to_string(), "dst_result", dst_result.to_string());
    ASSERT_TRUE(src_result == dst_result);
  }*/

  system("rm -f copy.log");
  system("rm -f observer.log");
}
} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_utl_file_handler.log");
  OB_LOGGER.set_file_name("test_utl_file_handler.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
