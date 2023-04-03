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
#include <vector>
#include <thread>

#include "lib/hash/ob_multi_mod_ref_mgr.h"

using namespace oceanbase;
using namespace common;

class TestLS
{
public:
  enum LSGetMod
  {
    TRANS_GET_STORE_CTX = 0,
    TRANS_PART_CTX_INIT = 1,
    STORAGE_MOD = 2,
    SQL_MOD = 3,
    TOTAL_MAX_MOD = 4,
  };

};

TEST(TestObRefMgr, basic)
{
  ObMultiModRefMgr<TestLS::LSGetMod> ref_mgr;
  ASSERT_EQ(0, ref_mgr.get_total_ref_cnt());
  ref_mgr.inc(TestLS::LSGetMod::TRANS_GET_STORE_CTX);
  ASSERT_EQ(1, ref_mgr.get_total_ref_cnt());
  ref_mgr.inc(TestLS::LSGetMod::TRANS_GET_STORE_CTX);
  ref_mgr.inc(TestLS::LSGetMod::TRANS_PART_CTX_INIT);
  ref_mgr.inc(TestLS::LSGetMod::STORAGE_MOD);
  ref_mgr.inc(TestLS::LSGetMod::SQL_MOD);
  ASSERT_EQ(5, ref_mgr.get_total_ref_cnt());

  ref_mgr.print();
  ref_mgr.dec(TestLS::LSGetMod::TRANS_GET_STORE_CTX);
  ASSERT_EQ(4, ref_mgr.get_total_ref_cnt());
  ref_mgr.dec(TestLS::LSGetMod::TRANS_GET_STORE_CTX);
  ref_mgr.dec(TestLS::LSGetMod::TRANS_PART_CTX_INIT);
  ref_mgr.dec(TestLS::LSGetMod::STORAGE_MOD);
  ref_mgr.dec(TestLS::LSGetMod::SQL_MOD);
  ASSERT_EQ(0, ref_mgr.get_total_ref_cnt());

  ref_mgr.print();
}

TEST(TestObRefMgr, bench)
{
  ObMultiModRefMgr<TestLS::LSGetMod> ref_mgr;
  std::vector<std::thread> ths;
  bool g_stop = false;
  for (int i = 0; i < 200; i++) {
    std::thread th([&, i] () {
      while (!g_stop) {
        ref_mgr.inc(TestLS::LSGetMod(i%TestLS::LSGetMod::TOTAL_MAX_MOD));
        ref_mgr.dec(TestLS::LSGetMod(i%TestLS::LSGetMod::TOTAL_MAX_MOD));
      }
      ::sleep(1);
    });
    ths.push_back(std::move(th));
  }
  for (int i = 0; i < 3; i++) {
    ref_mgr.print();
    ::sleep(1);
  }
  g_stop = true;
  for (auto &it : ths) {
    it.join();
  }
  ASSERT_EQ(0, ref_mgr.get_total_ref_cnt());
  ref_mgr.print();
}


TEST(TestObRefMgr, bench2)
{
  ObMultiModRefMgr<TestLS::LSGetMod> ref_mgr;
  std::vector<std::thread> ths;
  bool g_start = false;
  int release_cnt = 0;
  for (int i = 0; i < 200; i++) {
    ref_mgr.inc(TestLS::LSGetMod::STORAGE_MOD);
    std::thread th([&] () {
      while (true) {
        if (ATOMIC_LOAD(&g_start)) {
          bool can_release = ref_mgr.dec(TestLS::LSGetMod::STORAGE_MOD);
          if (can_release) {
            ATOMIC_INC(&release_cnt);
          }
          break;
        }
      }
      ::sleep(1);
    });
    ths.push_back(std::move(th));
  }
  ref_mgr.set_delete();
  ASSERT_EQ(200, ref_mgr.get_total_ref_cnt());
  ATOMIC_STORE(&g_start, true);
  for (auto &it : ths) {
    it.join();
  }
  ASSERT_EQ(0, ref_mgr.get_total_ref_cnt());
  ASSERT_EQ(1, release_cnt);
  ref_mgr.print();
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
