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

#define private public
#include "storage/tx/ob_id_service.h"
#undef private
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
using namespace common;
using namespace transaction;
namespace unittest
{

class TestObAllIDMeta : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
  static const int64_t type_count = ObIDService::MAX_SERVICE_TYPE + 2;
};

class MockObAllIDMetaUpper
{
public:
  MockObAllIDMetaUpper() : count_(TestObAllIDMeta::type_count) {}
  ~MockObAllIDMetaUpper() {}
  void init()
  {
    for (int i=0; i<count_; i++) {
      id_meta_[i].latest_log_ts_ = share::SCN::base_scn();
      id_meta_[i].limited_id_ = i + count_;
    }
  }
  bool is_invalid(const int64_t service_type)
  {
    return (id_meta_[service_type].limited_id_ == transaction::ObIDService::MIN_LAST_ID &&
            !id_meta_[service_type].latest_log_ts_.is_valid());
  }
  OB_UNIS_VERSION(1);
public: 
  int64_t count_;
  ObIDMeta id_meta_[TestObAllIDMeta::type_count];
};

class ObAllIDMetaLower : public transaction::ObAllIDMeta
{
public:
  void init()
  {
    for (int i=0; i<count_; i++) {
      id_meta_[i].latest_log_ts_ = share::SCN::base_scn();
      id_meta_[i].limited_id_ = i + TestObAllIDMeta::type_count;
    }
  }
  bool is_invalid(const int64_t service_type)
  {
    return (id_meta_[service_type].limited_id_ == transaction::ObIDService::MIN_LAST_ID &&
            !id_meta_[service_type].latest_log_ts_.is_valid());
  }
};

OB_DEF_SERIALIZE(MockObAllIDMetaUpper)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(id_meta_, count_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(MockObAllIDMetaUpper)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(id_meta_, count_);
  return len;
}

OB_DEF_DESERIALIZE(MockObAllIDMetaUpper)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;

  OB_UNIS_DECODE(count);
  if (OB_SUCC(ret)) {
    count_ = min(count_, count);
  }
  OB_UNIS_DECODE_ARRAY(id_meta_, count_);
  return ret;
}

TEST_F(TestObAllIDMeta, test_deserialize_from_upper_version)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockObAllIDMetaUpper mock_all_id_meta1;
  mock_all_id_meta1.init();
  MockObAllIDMetaUpper mock_all_id_meta2;
  mock_all_id_meta2.init();
  ObAllIDMetaLower all_id_meta1;
  ObAllIDMetaLower all_id_meta2;

  for(int i=0; i<ObIDService::MAX_SERVICE_TYPE; i++) {
    ASSERT_EQ(true, all_id_meta1.is_invalid(i));
    ASSERT_EQ(true, all_id_meta2.is_invalid(i));
  }

  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, mock_all_id_meta1.serialize(buffer, BUFFER_SIZE, pos));
  ASSERT_EQ(OB_SUCCESS, mock_all_id_meta2.serialize(buffer, BUFFER_SIZE, pos));

  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, all_id_meta1.deserialize(buffer, pos, start_index));
  ASSERT_EQ(OB_SUCCESS, all_id_meta2.deserialize(buffer, pos, start_index));

  for (int i=0; i<ObIDService::MAX_SERVICE_TYPE; i++) {
    EXPECT_EQ(mock_all_id_meta1.id_meta_[i].latest_log_ts_, all_id_meta1.id_meta_[i].latest_log_ts_);
    EXPECT_EQ(mock_all_id_meta1.id_meta_[i].limited_id_, all_id_meta1.id_meta_[i].limited_id_);
    EXPECT_EQ(mock_all_id_meta2.id_meta_[i].latest_log_ts_, all_id_meta2.id_meta_[i].latest_log_ts_);
    EXPECT_EQ(mock_all_id_meta2.id_meta_[i].limited_id_, all_id_meta2.id_meta_[i].limited_id_);
  }
}

TEST_F(TestObAllIDMeta, test_deserialize_from_lower_version)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockObAllIDMetaUpper mock_all_id_meta1;
  MockObAllIDMetaUpper mock_all_id_meta2;
  ObAllIDMetaLower all_id_meta1;
  all_id_meta1.init();
  ObAllIDMetaLower all_id_meta2;
  all_id_meta2.init();
  
  for(int i=0; i<TestObAllIDMeta::type_count; i++) {
    ASSERT_EQ(true, mock_all_id_meta1.is_invalid(i));
    ASSERT_EQ(true, mock_all_id_meta2.is_invalid(i));
  }

  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, all_id_meta1.serialize(buffer, BUFFER_SIZE, pos));
  ASSERT_EQ(OB_SUCCESS, all_id_meta2.serialize(buffer, BUFFER_SIZE, pos));

  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, mock_all_id_meta1.deserialize(buffer, pos, start_index));
  ASSERT_EQ(OB_SUCCESS, mock_all_id_meta2.deserialize(buffer, pos, start_index));

  for (int i=0; i<ObIDService::MAX_SERVICE_TYPE; i++) {
    EXPECT_EQ(mock_all_id_meta1.count_, all_id_meta1.count_);
    EXPECT_EQ(mock_all_id_meta1.id_meta_[i].latest_log_ts_, all_id_meta1.id_meta_[i].latest_log_ts_);
    EXPECT_EQ(mock_all_id_meta1.id_meta_[i].limited_id_, all_id_meta1.id_meta_[i].limited_id_);
    EXPECT_EQ(mock_all_id_meta2.count_, all_id_meta2.count_);
    EXPECT_EQ(mock_all_id_meta2.id_meta_[i].latest_log_ts_, all_id_meta2.id_meta_[i].latest_log_ts_);
    EXPECT_EQ(mock_all_id_meta2.id_meta_[i].limited_id_, all_id_meta2.id_meta_[i].limited_id_);
  }
  for (int i=ObIDService::MAX_SERVICE_TYPE; i<TestObAllIDMeta::type_count; i++) {
    ASSERT_EQ(true, mock_all_id_meta1.is_invalid(i));
    ASSERT_EQ(true, mock_all_id_meta2.is_invalid(i));
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
  logger.set_file_name("test_ob_id_meta.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
