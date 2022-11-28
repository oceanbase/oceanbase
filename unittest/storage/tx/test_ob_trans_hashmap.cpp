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

#include "storage/tx/ob_trans_hashmap.h"
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
using namespace common;
using namespace transaction;
using namespace storage;
namespace unittest
{
class TestObTrans : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
public :
  static const char *LOCAL_IP;
  static const int32_t PORT = 8080;
  static const ObAddr::VER IP_TYPE = ObAddr::IPV4;

  static const int64_t VALID_TABLE_ID = 1;
  static const int32_t VALID_PARTITION_ID = 1;
  static const int32_t VALID_PARTITION_COUNT = 100;
};
const char *TestObTrans::LOCAL_IP = "127.0.0.1";
const int64_t TIME_OUT = 1;

class ObTransTestValue : public ObTransHashLink<ObTransTestValue>
{
public:
  ObTransTestValue() {}
  int init(const ObTransID &trans_id)
  {
    int ret = OB_SUCCESS;
    if (!trans_id.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      trans_id_ = trans_id;
    }
    return ret;
  }
  bool contain(const ObTransID &trans_id) { return trans_id_ == trans_id; }
  const ObTransID &get_trans_id() const { return trans_id_; }
  TO_STRING_KV(K_(trans_id));
private:
  ObTransID trans_id_;
};

class ObTransTestValueAlloc
{
public:
  ObTransTestValue *alloc_value() {
    // for example
    return op_alloc(ObTransTestValue);
  }
  void free_value(ObTransTestValue * val) {
    // for example
    if (NULL != val) {
      op_free(val);
    }
  }
};

typedef ObTransHashMap<ObTransID, ObTransTestValue, ObTransTestValueAlloc, common::SpinRWLock> TestHashMap;

class ForeachFunctor
{
public:  
  ForeachFunctor(TestHashMap *map) : map_(map) {}
  bool operator() (ObTransTestValue *val)
  {
    TRANS_LOG(INFO, "currnet val info,", "trans_id", val->get_trans_id());
    if (NULL != map_) {
      map_->del(val->get_trans_id(), val);
    }
    return true;
  }
private:
  TestHashMap *map_;
};

class RemoveFunctor
{
public:  
  bool operator() (const ObTransTestValue *val)
  {
    TRANS_LOG(INFO, "currnet val info will be removed ", "trans_id", val->get_trans_id());
    return true;
  }
};

TEST_F(TestObTrans, hashmap_init_invalid)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  // init hashmap
  TestHashMap map;
  map.init(lib::ObMemAttr(OB_SERVER_TENANT_ID, "TestObTrans"));
  ObAddr observer(TestObTrans::IP_TYPE, TestObTrans::LOCAL_IP, TestObTrans::PORT);

  TRANS_LOG(INFO, "case1");
  // 1 invalid argument
  ObTransID trans_id0;
  ObTransTestValue *val0 = NULL;
  EXPECT_EQ(OB_SUCCESS, map.alloc_value(val0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, val0->init(trans_id0));
  map.free_value(val0);

  TRANS_LOG(INFO, "case2");
  // 2 insert and get
  ObTransID trans_id1 = ObTransID(TestObTrans::PORT);
  ObTransTestValue *val1 = NULL;
  EXPECT_EQ(OB_SUCCESS, map.alloc_value(val1));
  EXPECT_EQ(OB_SUCCESS, val1->init(trans_id1));
  // 2.1 insert
  ObTransTestValue *v = NULL;
  EXPECT_EQ(OB_SUCCESS, map.insert_and_get(trans_id1, val1, &v));
  map.revert(val1);
  // 2.2 check
  ObTransTestValue *tmp = NULL;
  EXPECT_EQ(OB_SUCCESS, map.get(trans_id1, tmp));
  EXPECT_EQ(tmp, val1);
  map.revert(tmp);

  TRANS_LOG(INFO, "case3");
  // 3 entry exist
  ObTransID trans_id2 = ObTransID(TestObTrans::PORT);
  ObTransTestValue *val2 = NULL;
  EXPECT_EQ(OB_SUCCESS, map.alloc_value(val2));
  EXPECT_EQ(OB_SUCCESS, val2->init(trans_id2));
   EXPECT_EQ(OB_ENTRY_EXIST, map.insert_and_get(trans_id2, val2, &v));
  map.free_value(val2);

  TRANS_LOG(INFO, "case4");
  ObTransID trans_id3 = ObTransID(100);
  ObTransTestValue *val3 = NULL;
  EXPECT_EQ(OB_SUCCESS, map.alloc_value(val3));
  EXPECT_EQ(OB_SUCCESS, val3->init(trans_id3));
  EXPECT_EQ(OB_SUCCESS, map.insert_and_get(trans_id3, val3, &v));
  map.revert(val3);

  TRANS_LOG(INFO, "case5");
  EXPECT_EQ(2, map.count());
  
  TRANS_LOG(INFO, "case6");
  // 4 foreach / remove if
  ForeachFunctor foreach_fn(&map);
  map.for_each(foreach_fn);
  RemoveFunctor remove_if_fn;
  map.remove_if(remove_if_fn);
  EXPECT_EQ(0, map.count());
}

}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_trans_hashmap.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
