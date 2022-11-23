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
#include "rpc/obmysql/ob_mysql_packet.h"

using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::obmysql;

class ObFakeMySQLPacket : public ObMySQLPacket
{
public:
  ObFakeMySQLPacket() : content_len_(0) {}
  int64_t content_len_;
protected:
  virtual int serialize(char *start, const int64_t len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    if ((NULL == start) || (content_len_ < 0)) {
      ret = OB_INVALID_ARGUMENT;
    } else if ((len - pos) < content_len_) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      for (int i = 0; i < content_len_; i++) {
        start[pos + i] = 'a';
      }
      pos += content_len_;
    }
    return ret;
  }
};

class TestMySQLPacket : public ::testing::Test
{
public:
  TestMySQLPacket()
  {
  }

  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};


TEST_F(TestMySQLPacket, common)
{
  ObFakeMySQLPacket fake_pkt;
  fake_pkt.set_seq(255);
  fake_pkt.content_len_ = 1;
  int64_t pkt_count = 0;
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  int64_t len = 100;
  char *buf = new char[len];
  ret = fake_pkt.encode(buf, len, pos, pkt_count);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1, pkt_count);
  delete []buf;
  buf = NULL;

  len = 16 * 1024 *1024 - 1 + 4;
  buf = new char[len];
  fake_pkt.content_len_ = 16 * 1024 * 1024 - 1;
  pkt_count = 0;
  pos = 0;
  ret = fake_pkt.encode(buf, len, pos, pkt_count);
  EXPECT_EQ(OB_SIZE_OVERFLOW, ret);
  EXPECT_EQ(0, pkt_count);
  EXPECT_EQ(0, pos);
  delete []buf;
  buf = NULL;

  len = 16 * 1024 *1024 - 1 + 4 + 4;
  buf = new char[len];
  fake_pkt.content_len_ = 16 * 1024 * 1024 - 1;
  pkt_count = 0;
  pos = 0;
  ret = fake_pkt.encode(buf, len, pos, pkt_count);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(2, pkt_count);
  EXPECT_EQ(len, pos);
  delete []buf;
  buf = NULL;

  len = 16 * 1024 *1024 - 1 + 4 + 4;
  buf = new char[len];
  fake_pkt.content_len_ = 16 * 1024 * 1024 - 1;
  pkt_count = 0;
  pos = 0;
  ret = fake_pkt.encode(buf, len, pos, pkt_count);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(2, pkt_count);
  EXPECT_EQ(len, pos);
  delete []buf;
  buf = NULL;

  len = 16 * 1024 *1024 - 2 + 4;
  buf = new char[len];
  fake_pkt.content_len_ = 16 * 1024 * 1024 - 2;
  pkt_count = 0;
  pos = 0;
  ret = fake_pkt.encode(buf, len, pos, pkt_count);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1, pkt_count);
  EXPECT_EQ(len, pos);
  delete []buf;
  buf = NULL;

  len = 16 * 1024 *1024 - 1 + 1 + 4 + 4;
  buf = new char[len];
  fake_pkt.content_len_ = 16 * 1024 * 1024 - 1 + 1;
  pkt_count = 0;
  pos = 0;
  ret = fake_pkt.encode(buf, len, pos, pkt_count);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(2, pkt_count);
  EXPECT_EQ(len, pos);
  delete []buf;
  buf = NULL;

  len = (16 * 1024 * 1024 - 1 + 4) * 5 + 4;
  buf = new char[len];
  fake_pkt.content_len_ = (16 * 1024 * 1024 - 1) * 5;
  pkt_count = 0;
  pos = 0;
  ret = fake_pkt.encode(buf, len, pos, pkt_count);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(6, pkt_count);
  EXPECT_EQ(len, pos);
  delete []buf;
  buf = NULL;

  len = (16 * 1024 * 1024 - 1 + 4);
  buf = new char[len];
  fake_pkt.content_len_ = (16 * 1024 * 1024 - 1) * 5;
  pkt_count = 0;
  pos = 0;
  ret = fake_pkt.encode(buf, len, pos, pkt_count);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);
  EXPECT_EQ(0, pkt_count);
  EXPECT_EQ(0, pos);
  delete []buf;
  buf = NULL;
}


int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
