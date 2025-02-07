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

#define UNITTEST_DEBUG
#include "storage/tablelock/ob_table_lock_rpc_struct.h"
#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace transaction;
using namespace transaction::tablelock;
#define CHECK_SERIALIZE_AND_DESERIALIZE(T)                                                                            \
  ret = replace_req.serialize(buf, LEN, pos);                                                                         \
  ASSERT_EQ(ret, OB_SUCCESS);                                                                                         \
  ASSERT_GT(pos, 0);                                                                                                  \
  TABLELOCK_LOG(INFO, "1. replace_req and unlock_req", K(replace_req), K(unlock_req), K(pos), KPHEX(buf, pos));       \
  ObReplaceLockRequest des_replace_req;                                                                               \
  T des_unlock_req;                                                                                                   \
  pos = 0;                                                                                                            \
  ret = des_replace_req.deserialize_and_check_header(buf, LEN, pos);                                                  \
  ASSERT_EQ(ret, OB_SUCCESS);                                                                                         \
  ASSERT_GT(pos, 0);                                                                                                  \
  TABLELOCK_LOG(INFO, "2. deserialize and check header", K(orig_pos), K(pos), KPHEX(buf + orig_pos, pos - orig_pos)); \
  orig_pos = pos;                                                                                                     \
  ret = des_replace_req.deserialize_new_lock_mode_and_owner(buf, LEN, pos);                                           \
  ASSERT_EQ(ret, OB_SUCCESS);                                                                                         \
  ASSERT_GT(pos, orig_pos);                                                                                           \
  TABLELOCK_LOG(INFO,                                                                                                 \
                "3. deserialize new lock_mode and new lock_owner",                                                    \
                K(orig_pos),                                                                                          \
                K(pos),                                                                                               \
                KPHEX(buf + orig_pos, pos - orig_pos));                                                               \
  orig_pos = pos;                                                                                                     \
  ret = des_unlock_req.deserialize(buf, LEN, pos);                                                                    \
  ASSERT_EQ(ret, OB_SUCCESS);                                                                                         \
  ASSERT_GT(pos, orig_pos);                                                                                           \
  des_replace_req.unlock_req_ = &des_unlock_req;                                                                      \
  TABLELOCK_LOG(INFO,                                                                                                 \
                "4. deserialized replace_req and unlock_req",                                                         \
                K(des_replace_req),                                                                                   \
                K(des_unlock_req),                                                                                    \
                K(orig_pos),                                                                                          \
                K(pos),                                                                                               \
                KPHEX(buf + orig_pos, pos - orig_pos));                                                               \
  ASSERT_TRUE(replace_req_is_equal(replace_req, des_replace_req));

template <typename T>
bool list_is_equal(const ObIArray<T> &list1, const ObIArray<T> &list2)
{
  bool is_equal = false;
  is_equal = list1.count() == list2.count();
  if (is_equal) {
    for (int64_t i = 0; i < list1.count(); i++) {
      is_equal = list1.at(i) == list2.at(i);
      if (!is_equal) {
        TABLELOCK_LOG(INFO, "meet not equal element", K(list1), K(list2), K(list1.at(i)), K(list2.at(i)));
        break;
      }
    }
  }
  return is_equal;
}

bool unlock_req_is_equal(const ObLockRequest &req1, const ObLockRequest &req2)
{
  bool is_equal = false;
  is_equal = req1.type_ == req2.type_ && req1.owner_id_ == req2.owner_id_ && req1.lock_mode_ == req2.lock_mode_
             && req1.op_type_ == req2.op_type_ && req1.timeout_us_ == req2.timeout_us_;
  if (is_equal) {
    switch (req1.type_) {
    case transaction::tablelock::ObLockRequest::ObLockMsgType::LOCK_TABLE_REQ:
    case transaction::tablelock::ObLockRequest::ObLockMsgType::UNLOCK_TABLE_REQ: {
      const ObUnLockTableRequest &lock_req1 = static_cast<const ObUnLockTableRequest &>(req1);
      const ObUnLockTableRequest &lock_req2 = static_cast<const ObUnLockTableRequest &>(req2);
      is_equal = lock_req1.table_id_ == lock_req2.table_id_;
      TABLELOCK_LOG(INFO, "compare unlock request", K(lock_req1), K(lock_req2), K(is_equal));
      break;
    }
    case transaction::tablelock::ObLockRequest::ObLockMsgType::LOCK_TABLET_REQ:
    case transaction::tablelock::ObLockRequest::ObLockMsgType::UNLOCK_TABLET_REQ: {
      const ObUnLockTabletsRequest &lock_req1 = static_cast<const ObUnLockTabletsRequest &>(req1);
      const ObUnLockTabletsRequest &lock_req2 = static_cast<const ObUnLockTabletsRequest &>(req2);
      is_equal =
        lock_req1.table_id_ == lock_req2.table_id_ && list_is_equal(lock_req1.tablet_ids_, lock_req1.tablet_ids_);
      TABLELOCK_LOG(INFO, "compare unlock request", K(lock_req1), K(lock_req2), K(is_equal));
      break;
    }
    case transaction::tablelock::ObLockRequest::ObLockMsgType::LOCK_PARTITION_REQ:
    case transaction::tablelock::ObLockRequest::ObLockMsgType::UNLOCK_PARTITION_REQ: {
      const ObUnLockPartitionRequest &lock_req1 = static_cast<const ObUnLockPartitionRequest &>(req1);
      const ObUnLockPartitionRequest &lock_req2 = static_cast<const ObUnLockPartitionRequest &>(req2);
      is_equal = lock_req1.table_id_ == lock_req2.table_id_ && lock_req1.part_object_id_ == lock_req2.part_object_id_;
      TABLELOCK_LOG(INFO, "compare unlock request", K(lock_req1), K(lock_req2), K(is_equal));
      break;
    }
    case transaction::tablelock::ObLockRequest::ObLockMsgType::LOCK_OBJ_REQ:
    case transaction::tablelock::ObLockRequest::ObLockMsgType::UNLOCK_OBJ_REQ: {
      const ObUnLockObjsRequest &lock_req1 = static_cast<const ObUnLockObjsRequest &>(req1);
      const ObUnLockObjsRequest &lock_req2 = static_cast<const ObUnLockObjsRequest &>(req2);
      is_equal = list_is_equal(lock_req1.objs_, lock_req1.objs_);
      TABLELOCK_LOG(INFO, "compare unlock request", K(lock_req1), K(lock_req2), K(is_equal));
      break;
    }
    case transaction::tablelock::ObLockRequest::ObLockMsgType::LOCK_ALONE_TABLET_REQ:
    case transaction::tablelock::ObLockRequest::ObLockMsgType::UNLOCK_ALONE_TABLET_REQ: {
      const ObUnLockAloneTabletRequest &lock_req1 = static_cast<const ObUnLockAloneTabletRequest &>(req1);
      const ObUnLockAloneTabletRequest &lock_req2 = static_cast<const ObUnLockAloneTabletRequest &>(req2);
      is_equal = lock_req1.table_id_ == lock_req2.table_id_ && lock_req1.ls_id_ == lock_req2.ls_id_
                 && list_is_equal(lock_req1.tablet_ids_, lock_req1.tablet_ids_);
      TABLELOCK_LOG(INFO, "compare unlock request", K(lock_req1), K(lock_req2), K(is_equal));
      break;
    }
    default: {
      is_equal = false;
      int ret = OB_INVALID_ARGUMENT;
      TABLELOCK_LOG(WARN, "meet unspport request type", K(req1), K(req2));
    }
    }
  }
  return is_equal;
}

bool replace_req_is_equal(const ObReplaceLockRequest &req1, const ObReplaceLockRequest &req2)
{
  bool is_equal = false;
  is_equal = req1.new_lock_owner_ == req2.new_lock_owner_ && req2.new_lock_mode_ == req2.new_lock_mode_;
  return is_equal && unlock_req_is_equal(*(req1.unlock_req_), *(req2.unlock_req_));
}

TEST(ObReplaceLockRequest, test_replace_table_lock)
{
  INIT_SUCC(ret);
  ObReplaceLockRequest replace_req;
  ObUnLockTableRequest unlock_req;
  const int LEN = 100;
  char buf[LEN];
  int64_t pos = 0;
  int64_t orig_pos = 0;

  unlock_req.owner_id_.convert_from_value(1001);
  unlock_req.lock_mode_ = EXCLUSIVE;
  unlock_req.op_type_ = OUT_TRANS_UNLOCK;
  unlock_req.timeout_us_ = 1000;
  unlock_req.table_id_ = 998;

  replace_req.new_lock_mode_ = SHARE;
  replace_req.new_lock_owner_.convert_from_value(1002);
  replace_req.unlock_req_ = &unlock_req;
  CHECK_SERIALIZE_AND_DESERIALIZE(ObUnLockTableRequest);
}

TEST(ObReplaceLockRequest, test_replace_alone_tablet)
{
  INIT_SUCC(ret);
  ObReplaceLockRequest replace_req;
  ObUnLockAloneTabletRequest unlock_req;
  const int LEN = 100;
  char buf[LEN];
  int64_t pos = 0;
  int64_t orig_pos = 0;

  unlock_req.owner_id_.convert_from_value(1001);
  unlock_req.lock_mode_ = EXCLUSIVE;
  unlock_req.op_type_ = OUT_TRANS_UNLOCK;
  unlock_req.timeout_us_ = 1000;
  unlock_req.table_id_ = 998;
  unlock_req.ls_id_ = 1;
  unlock_req.tablet_ids_.push_back(ObTabletID(123));
  unlock_req.tablet_ids_.push_back(ObTabletID(456));
  unlock_req.tablet_ids_.push_back(ObTabletID(789));

  replace_req.new_lock_mode_ = SHARE;
  replace_req.new_lock_owner_.convert_from_value(1002);
  replace_req.unlock_req_ = &unlock_req;
  CHECK_SERIALIZE_AND_DESERIALIZE(ObUnLockAloneTabletRequest);
}

TEST(ObReplaceLockRequest, test_replace_objs)
{
  INIT_SUCC(ret);
  ObReplaceLockRequest replace_req;
  ObUnLockObjsRequest unlock_req;
  const int LEN = 100;
  char buf[LEN];
  int64_t pos = 0;
  int64_t orig_pos = 0;

  unlock_req.owner_id_.convert_from_value(1001);
  unlock_req.lock_mode_ = EXCLUSIVE;
  unlock_req.op_type_ = OUT_TRANS_UNLOCK;
  unlock_req.timeout_us_ = 1000;
  ObLockID lock_id1;
  lock_id1.set(ObLockOBJType::OBJ_TYPE_TABLE, 123);
  unlock_req.objs_.push_back(lock_id1);
  ObLockID lock_id2;
  lock_id2.set(ObLockOBJType::OBJ_TYPE_TABLET, 456);
  unlock_req.objs_.push_back(lock_id2);
  ObLockID lock_id3;
  lock_id3.set(ObLockOBJType::OBJ_TYPE_COMMON_OBJ, 789);
  unlock_req.objs_.push_back(lock_id3);

  replace_req.new_lock_mode_ = SHARE;
  replace_req.new_lock_owner_.convert_from_value(1002);
  replace_req.unlock_req_ = &unlock_req;
  CHECK_SERIALIZE_AND_DESERIALIZE(ObUnLockTableRequest);
}

TEST(ObReplaceLockRequest, test_replace_all_locks)
{
  INIT_SUCC(ret);
  ObArenaAllocator allocator;
  ObReplaceAllLocksRequest replace_req(allocator);
  ObReplaceAllLocksRequest des_replace_req(allocator);
  ObUnLockTableRequest unlock_req1;
  ObUnLockTableRequest unlock_req2;
  ObLockTableRequest lock_req;
  const int LEN = 100;
  char buf[LEN];
  int64_t pos = 0;
  char tmp_buf[LEN];
  int64_t tmp_pos = 0;
  bool is_equal = false;

  unlock_req1.type_ = ObLockRequest::ObLockMsgType::UNLOCK_TABLE_REQ;
  unlock_req1.owner_id_.convert_from_value(1001);
  unlock_req1.lock_mode_ = ROW_SHARE;
  unlock_req1.op_type_ = OUT_TRANS_UNLOCK;
  unlock_req1.timeout_us_ = 1000;
  unlock_req1.table_id_ = 998;

  unlock_req2.type_ = ObLockRequest::ObLockMsgType::UNLOCK_TABLE_REQ;
  unlock_req2.owner_id_.convert_from_value(1002);
  unlock_req2.lock_mode_ = ROW_EXCLUSIVE;
  unlock_req2.op_type_ = OUT_TRANS_UNLOCK;
  unlock_req2.timeout_us_ = 1000;
  unlock_req2.table_id_ = 998;

  lock_req.owner_id_.convert_from_value(1003);
  lock_req.lock_mode_ = EXCLUSIVE;
  lock_req.op_type_ = OUT_TRANS_LOCK;
  lock_req.timeout_us_ = 1000;
  lock_req.table_id_ = 998;

  replace_req.unlock_req_list_.push_back(&unlock_req1);
  replace_req.unlock_req_list_.push_back(&unlock_req2);
  replace_req.lock_req_ = &lock_req;

  ret = replace_req.serialize(buf, LEN, pos);
  TABLELOCK_LOG(INFO,
                "1. serailize replace_req",
                K(ret),
                K(replace_req),
                K(unlock_req1),
                K(unlock_req2),
                K(lock_req),
                K(pos),
                KPHEX(buf, pos));
  ASSERT_EQ(ret, OB_SUCCESS);

  pos = 0;
  ret = des_replace_req.deserialize(buf, LEN, pos);

  TABLELOCK_LOG(INFO,
                "2. deserailize replace_req",
                K(ret),
                K(replace_req),
                K(des_replace_req),
                K(unlock_req1),
                K(unlock_req2),
                K(lock_req),
                K(pos),
                KPHEX(buf, pos));
  ASSERT_EQ(ret, OB_SUCCESS);

  is_equal = unlock_req_is_equal(*des_replace_req.lock_req_, *replace_req.lock_req_);
  ASSERT_TRUE(is_equal);
  for (int64_t i = 0; i < des_replace_req.unlock_req_list_.count() && is_equal; i++) {
    is_equal = unlock_req_is_equal(*des_replace_req.unlock_req_list_.at(i), *replace_req.unlock_req_list_.at(i));
  }
  ASSERT_TRUE(is_equal);
}
}  // namespace unittest
}  // namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -rf test_ob_replace_lock_request.log");

  oceanbase::ObLogger &logger = oceanbase::ObLogger::get_logger();
  logger.set_file_name("test_ob_replace_lock_request.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
  ;
}
