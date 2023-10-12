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
#include <thread>

#define protected public
#define private public
#include "storage/tx/wrs/ob_black_list.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace transaction;

namespace unittest
{
class TestObBlackList : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestObBlackList, black_list_init_invalid)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  // init
  ObBLService &bl_service = ObBLService::get_instance();
  EXPECT_EQ(OB_SUCCESS, bl_service.init());

  ObAddr addr(ObAddr::IPV4, "127.0.0.1", 2077);
  ObBLKey key;
  ObLSID ls_id(2);
  EXPECT_EQ(OB_SUCCESS, key.init(addr, 1006, ls_id));

  // add
  EXPECT_EQ(OB_SUCCESS, bl_service.add(key));

  // check
  bool check;
  EXPECT_EQ(OB_SUCCESS, bl_service.check_in_black_list(key, check));
  EXPECT_EQ(true, check);

  // update
  ObLsInfo ls_info;
  uint64_t curr_time = static_cast<uint64_t>(ObTimeUtility::current_time());
  EXPECT_EQ(OB_SUCCESS, ls_info.init(0, curr_time, OB_MIGRATION_STATUS_NONE, 0));
  EXPECT_EQ(OB_SUCCESS, bl_service.ls_bl_mgr_.update(key, ls_info));

  // check
  EXPECT_EQ(OB_SUCCESS, bl_service.check_in_black_list(key, check));
  EXPECT_EQ(true, check);

  // remove
  bl_service.remove(key);

  // check
  EXPECT_EQ(OB_SUCCESS, bl_service.check_in_black_list(key, check));
  EXPECT_EQ(false, check);

  // reset & init
  bl_service.reset();
  EXPECT_EQ(OB_SUCCESS, bl_service.init());

  // add 100 keys & check
  for (int i=1; i<=100; i++) {
    EXPECT_EQ(OB_SUCCESS, key.init(addr, 1006, ObLSID(i)));
    EXPECT_EQ(OB_SUCCESS, bl_service.add(key));
    EXPECT_EQ(OB_SUCCESS, bl_service.check_in_black_list(key, check));
    EXPECT_EQ(true, check);
  }

  // remove 100 keys & check
  for (int i=1; i<=100; i++) {
    EXPECT_EQ(OB_SUCCESS, key.init(addr, 1006, ObLSID(i)));
    bl_service.remove(key);
    EXPECT_EQ(OB_SUCCESS, bl_service.check_in_black_list(key, check));
    EXPECT_EQ(false, check);
  }

  // destroy
  bl_service.destroy();
}

TEST_F(TestObBlackList, black_list_error)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  // init key
  ObAddr addr(ObAddr::IPV4, "127.0.0.1", 2077);
  ObBLKey key;
  ObLSID ls_id(2);
  EXPECT_EQ(OB_SUCCESS, key.init(addr, 1006, ls_id));

  // error: insert before init
  ObBLService &bl_service = ObBLService::get_instance();
  EXPECT_EQ(OB_NOT_INIT, bl_service.add(key));

  // error: init twice
  EXPECT_EQ(OB_SUCCESS, bl_service.init());
  EXPECT_EQ(OB_INIT_TWICE, bl_service.init());

  // erroe: add twice
  EXPECT_EQ(OB_SUCCESS, bl_service.add(key));
  EXPECT_EQ(OB_ENTRY_EXIST, bl_service.add(key));

  // check
  bool check;
  EXPECT_EQ(OB_SUCCESS, bl_service.check_in_black_list(key, check));
  EXPECT_EQ(true, check);

  // remove twice
  bl_service.remove(key);
  bl_service.remove(key);

  // check
  EXPECT_EQ(OB_SUCCESS, bl_service.check_in_black_list(key, check));
  EXPECT_EQ(false, check);

  // destroy twice
  bl_service.destroy();
  bl_service.destroy();

  // error: access after destroy
  EXPECT_EQ(OB_NOT_INIT, bl_service.add(key));
  bl_service.remove(key);
  EXPECT_EQ(OB_NOT_INIT, bl_service.check_in_black_list(key, check));
}

TEST_F(TestObBlackList, black_list_inc_func)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  // init
  ObBLService &bl_service = ObBLService::get_instance();
  EXPECT_EQ(OB_SUCCESS, bl_service.init());

  ObAddr addr(ObAddr::IPV4, "127.0.0.1", 2077);
  ObBLKey key;
  bool check = false;

  // add 100 keys & check
  for (int i=1; i<=100; i++) {
    EXPECT_EQ(OB_SUCCESS, key.init(addr, 1006, ObLSID(i)));
    EXPECT_EQ(OB_SUCCESS, bl_service.add(key));
    EXPECT_EQ(OB_SUCCESS, bl_service.check_in_black_list(key, check));
    EXPECT_EQ(true, check);
  }

  // print
  bl_service.print_stat_();
  EXPECT_EQ(100, bl_service.ls_bl_mgr_.map_.size());

  // clean up
  bl_service.do_clean_up_();
  EXPECT_EQ(100, bl_service.ls_bl_mgr_.map_.size());

  // sleep 5s
  int64_t clean_ts = ObTimeUtility::current_time();
  usleep(BLACK_LIST_CLEAN_UP_INTERVAL);

  // do_thread_task_
  int64_t curr_ts = ObTimeUtility::current_time();
  int64_t print_ts = curr_ts - 1;
  bl_service.do_thread_task_(curr_ts, print_ts, clean_ts);
  EXPECT_EQ(curr_ts, clean_ts);
  EXPECT_EQ(0, bl_service.ls_bl_mgr_.map_.size());

  // destroy
  bl_service.destroy();
}

TEST_F(TestObBlackList, black_list_parallel_1)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  // init
  ObAddr addr(ObAddr::IPV4, "127.0.0.1", 2077);
  ObBLService &bl_service = ObBLService::get_instance();
  EXPECT_EQ(OB_SUCCESS, bl_service.init());

  // parallel test
  std::vector<std::thread> ths;
  uint64_t loop_cnt = 10000;
  uint64_t worker_cnt = 10;

  auto do_worker = [&] (uint64_t id) {
    ObBLKey key;
    uint64_t base_id = id * loop_cnt;
    // pre worker id, the first worker's pre worker is the last worker
    uint64_t pre_id = ((id + worker_cnt - 1) % worker_cnt) * loop_cnt;
    for (int i = 1; i <= loop_cnt; i++) {
      ObLSID ls_id(base_id + i);
      EXPECT_EQ(OB_SUCCESS, key.init(addr, 1006, ls_id));
      EXPECT_EQ(OB_SUCCESS, bl_service.add(key));
      ObLSID ls_id_2(pre_id + i);
      EXPECT_EQ(OB_SUCCESS, key.init(addr, 1006, ls_id_2));
      bl_service.remove(key);
    }
  };
  for (int i = 0; i < worker_cnt; i++) {
    std::thread th(do_worker, i);
    ths.push_back(std::move(th));
  }
  for (auto &th : ths) {
    th.join();
  }

  // destroy
  bl_service.destroy();
}

TEST_F(TestObBlackList, black_list_parallel_2)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  // init
  ObAddr addr(ObAddr::IPV4, "127.0.0.1", 2077);
  ObBLService &bl_service = ObBLService::get_instance();
  EXPECT_EQ(OB_SUCCESS, bl_service.init());

  // parallel test
  std::vector<std::thread> ths;
  uint64_t loop_cnt = 10000;
  uint64_t worker_cnt = 10;
  bool check = false;

  auto do_worker = [&] (uint64_t id) {
    int rand = 0;
    ObBLKey key;
    ObLsInfo ls_info;
    EXPECT_EQ(OB_SUCCESS, ls_info.init(0, 1, OB_MIGRATION_STATUS_NONE, 0));
    std::srand((unsigned)std::time(NULL));
    for (int i = 1; i <= loop_cnt; i++) {
      rand = std::rand() % 1000 + 1;
      ObLSID ls_id(std::rand() % loop_cnt + 1);
      EXPECT_EQ(OB_SUCCESS, key.init(addr, 1006, ls_id));

      if (rand <= 300) {
        bl_service.add(key);
      } else if (rand <= 600) {
        bl_service.ls_bl_mgr_.update(key, ls_info);
      } else if (rand <= 800) {
        bl_service.remove(key);
      } else {
        EXPECT_EQ(OB_SUCCESS, bl_service.check_in_black_list(key, check));
      }
    }
  };
  for (int i = 0; i < worker_cnt; i++) {
    std::thread th(do_worker, i);
    ths.push_back(std::move(th));
  }
  for (auto &th : ths) {
    th.join();
  }

  // destroy
  bl_service.destroy();
}

} // unittest
} // oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_black_list.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
