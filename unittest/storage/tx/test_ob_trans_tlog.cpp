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

#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_clock_generator.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace unittest
{

class TestObTransTlog : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestObTransTlog, analyse_tlog)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int ret = common::OB_ERR_UNEXPECTED;
  int fd = -1;
  char buf[32768];
  const char *tlog_file = "/tmp/tlog";
  int nread = 0;
  oceanbase::transaction::ObTransTraceLog tlog;
  fd = open(tlog_file, O_RDONLY);
  if (fd < 0) {
    TRANS_LOG(INFO, "open tlog file failed", K(tlog_file), K(fd));
  } else {
    TRANS_LOG(INFO, "open tlog file success", K(tlog_file), K(fd));
    nread = read(fd, buf, sizeof(buf));
    TRANS_LOG(INFO, "read tlog file", K(nread));
    if (nread != sizeof(tlog)) {
      TRANS_LOG(INFO, "tlog size not match", K(nread), K(sizeof(tlog)));
    } else {
      TRANS_LOG(INFO, "read tlog file success", K(nread));
      memcpy((void *)(&tlog), buf, sizeof(tlog));
      TRANS_LOG(INFO, "tlog", K(tlog));
    }
    close(fd);
    sleep(1);
  }
}


}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_trans_tlog.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);

  if (OB_SUCCESS != ObClockGenerator::init()) {
    TRANS_LOG(WARN, "init ob_clock_generator error!");
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  return ret;
}
