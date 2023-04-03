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
#include <random>
#include <string>
#include <pthread.h>

#include "share/scn.h"
#define private public
#include "logservice/palf/lsn_allocator.h"
#undef private

namespace oceanbase
{
using namespace common;
using namespace palf;

namespace unittest
{

class TestLSNAllocator : public ::testing::Test
{
public:
  TestLSNAllocator();
  virtual ~TestLSNAllocator();
  virtual void SetUp();
  virtual void TearDown();
protected:
  int64_t  palf_id_;
  LSNAllocator lsn_allocator_;
};

TestLSNAllocator::TestLSNAllocator()
    : palf_id_(1)
{
}

TestLSNAllocator::~TestLSNAllocator()
{
}

void TestLSNAllocator::SetUp()
{
}

void TestLSNAllocator::TearDown()
{
  PALF_LOG(INFO, "TestLSNAllocator has TearDown");
  PALF_LOG(INFO, "TearDown success");
}

constexpr int MAX_BUF_SIZE = 2 * 1024 * 1024;
const int64_t LOG_LOG_CNT = 1000;
int64_t log_size_array[LOG_LOG_CNT];

LSNAllocator global_lsn_allocator;

void init_size_array()
{
  for (int i = 0; i < LOG_LOG_CNT; i++) {
    log_size_array[i] = rand() % MAX_BUF_SIZE + 1;
  }
}

void init_offset_allocator()
{
  LSN start_lsn(0);
  EXPECT_EQ(OB_SUCCESS, global_lsn_allocator.init(0, share::SCN::base_scn(), start_lsn));
}

TEST_F(TestLSNAllocator, test_struct_field_value_upper_bound)
{
  // 测试struct中的int64_t是否会比uint64_t多消耗一位用于存储符号
  // 测试结论：使用int类型作为field，当最高位置为1时，直接读取它的value就会变成负数
  union TestMeta
  {
    uint64_t val64_;
    struct
    {
      uint8_t is_need_cut_ : 1;
      int64_t id_ : 2;
      int64_t ts_ : 61;
    };
  };
  TestMeta val;
  val.id_ = 1;
  // val.id_ = (1 << 2) - 1;  // 这行编译会报错，implicit truncation from 'int' to bit-field changes value from 3 to -1
  val.ts_ = 100;
  std::cout << val.id_ << ", sizeof(val):" << sizeof(val) << std::endl;

  val.id_ = 0;
  std::cout << "val.id_ is " << val.id_ << std::endl;  // 0
  val.id_ = 1;
  printf("val.id_ is 0x%x\n", val.id_); // 0x1
  val.id_++;
  std::cout << "val.id_ is " << val.id_ << std::endl;  // -2
  std::cout << "val.id_ & 0x11 is " << (val.id_ & 0x3) << std::endl;  // 2, correct!
  printf("val.id_ is 0x%x\n", val.id_); // 0xfffffffe
  val.id_++;
  std::cout << "val.id_ is " << val.id_ << std::endl;  // -1
  std::cout << "val.id_ & 0x11 is " << (val.id_ & 0x3) << std::endl;  // 3
  printf("val.id_ is 0x%x\n", val.id_); // 0xffffffff
  val.id_++;
  std::cout << "val.id_ is " << val.id_ << std::endl;  // 0
  std::cout << "val.id_ & 0x11 is " << (val.id_ & 0x3) << std::endl;  // 0, 加溢出了
  printf("val.id_ is 0x%x\n", val.id_); // 0x0

  union TestMeta2
  {
    uint64_t val64_;
    struct
    {
      uint8_t is_need_cut_ : 1;
      uint64_t id_ : 2;
      uint64_t ts_ : 61;
    };
  };
  TestMeta2 val2;
  val2.id_ = 1;
  // val2.id_ = (1 << 2) - 1;  // 这行编译会报错，implicit truncation from 'int' to bit-field changes value from 3 to -1
  std::cout << val2.id_ << ", sizeof(val2):" << sizeof(val2) << std::endl;

  val2.id_ = 0;
  std::cout << "val2.id_ is " << val2.id_ << std::endl;  // 0
  val2.id_ = 1;
  printf("val2.id_ is 0x%x\n", val2.id_); // 0x1
  val2.id_++;
  std::cout << "val2.id_ is " << val2.id_ << std::endl;  // 2
  std::cout << "val2.id_ & 0x11 is " << (val2.id_ & 0x3) << std::endl;  // 2
  printf("val2.id_ is 0x%x\n", val2.id_); // 0x2
  val2.id_++;
  std::cout << "val2.id_ is " << val2.id_ << std::endl;  // 3
  std::cout << "val2.id_ & 0x11 is " << (val2.id_ & 0x3) << std::endl;  // 3
  printf("val2.id_ is 0x%x\n", val2.id_); // 0x3
  val2.id_++;
  std::cout << "val2.id_ is " << val2.id_ << std::endl;  // 0
  std::cout << "val2.id_ & 0x11 is " << (val2.id_ & 0x3) << std::endl;  // 0, 加溢出了
  printf("val2.id_ is 0x%x\n", val2.id_); // 0x0

}

TEST_F(TestLSNAllocator, test_lsn_allocator_init)
{
  LSN start_lsn;
  int64_t initial_log_id = OB_INVALID_LOG_ID;
  share::SCN initial_scn;
  EXPECT_EQ(OB_INVALID_ARGUMENT, lsn_allocator_.init(initial_log_id, initial_scn, start_lsn));
  initial_log_id = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, lsn_allocator_.init(initial_log_id, initial_scn, start_lsn));
  initial_scn.set_base();
  EXPECT_EQ(OB_INVALID_ARGUMENT, lsn_allocator_.init(initial_log_id, initial_scn, start_lsn));
  start_lsn.val_ = 0;
  EXPECT_EQ(OB_SUCCESS, lsn_allocator_.init(initial_log_id, initial_scn, start_lsn));
  EXPECT_EQ(OB_INIT_TWICE, lsn_allocator_.init(initial_log_id, initial_scn, start_lsn));
}

TEST_F(TestLSNAllocator, test_lsn_allocator_alloc_lsn_scn)
{
  LSN start_lsn;
  int64_t initial_log_id = OB_INVALID_LOG_ID;
  share::SCN initial_scn = share::SCN::min_scn();
  initial_log_id = 0;
  start_lsn.val_ = 0;

  share::SCN b_scn;
  b_scn.convert_for_logservice(1000);
  int64_t size = 1000000;
  LSN lsn;
  int64_t log_id;
  share::SCN scn;
  bool is_new_log = false;
  bool need_gen_padding_entry = false;
  int64_t padding_len = 0;

  int64_t log_id_upper_bound = 99999;
  LSN lsn_upper_bound(9999999999);

  EXPECT_EQ(OB_NOT_INIT, lsn_allocator_.alloc_lsn_scn(b_scn, size, log_id_upper_bound, lsn_upper_bound, lsn, log_id, scn,
            is_new_log, need_gen_padding_entry, padding_len));
  EXPECT_EQ(OB_SUCCESS, lsn_allocator_.init(initial_log_id, initial_scn, start_lsn));
  int64_t invalid_size = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, lsn_allocator_.alloc_lsn_scn(b_scn, invalid_size, log_id_upper_bound, lsn_upper_bound, lsn, log_id, scn,
            is_new_log, need_gen_padding_entry, padding_len));
  EXPECT_EQ(OB_INVALID_ARGUMENT, lsn_allocator_.alloc_lsn_scn(b_scn, size, 0, lsn_upper_bound, lsn, log_id, scn, is_new_log, need_gen_padding_entry, padding_len));
  LSN invalid_lsn;
  EXPECT_EQ(OB_INVALID_ARGUMENT, lsn_allocator_.alloc_lsn_scn(b_scn, size, 1, invalid_lsn, lsn, log_id, scn, is_new_log, need_gen_padding_entry, padding_len));
  // test alloc_lsn_scn()
  EXPECT_EQ(OB_SUCCESS, lsn_allocator_.alloc_lsn_scn(b_scn, size, log_id_upper_bound, lsn_upper_bound, lsn, log_id, scn,
            is_new_log, need_gen_padding_entry, padding_len));
  EXPECT_EQ(initial_log_id + 1, log_id);
}

TEST_F(TestLSNAllocator, test_lsn_allocator_truncate)
{
  LSN start_lsn;
  int64_t initial_log_id = OB_INVALID_LOG_ID;
  initial_log_id = 0;
  share::SCN initial_scn = share::SCN::min_scn();
  start_lsn.val_ = 0;

  LSN tmp_lsn;
  int64_t tmp_log_id = 9999;
  share::SCN tmp_scn;
  tmp_scn.convert_for_logservice(55555);

  LSN end_lsn;
  int64_t end_log_id = OB_INVALID_LOG_ID;

  share::SCN b_scn;
  b_scn.convert_for_logservice(1000);
  int64_t size = 1000000;
  LSN lsn;
  int64_t log_id;
  share::SCN scn;
  bool is_new_log = false;
  bool need_gen_padding_entry = false;
  int64_t padding_len = 0;

  // test truncate()
  share::SCN new_scn;
  new_scn.convert_for_logservice(10);
  const int64_t truncate_log_id = 1024;
  EXPECT_EQ(OB_NOT_INIT, lsn_allocator_.truncate(tmp_lsn, truncate_log_id, new_scn));
  EXPECT_EQ(OB_NOT_INIT, lsn_allocator_.inc_update_last_log_info(tmp_lsn, tmp_log_id, tmp_scn));
  EXPECT_EQ(OB_NOT_INIT, lsn_allocator_.try_freeze_by_time(end_lsn, end_log_id));
  EXPECT_EQ(OB_NOT_INIT, lsn_allocator_.get_curr_end_lsn(end_lsn));
  EXPECT_EQ(OB_SUCCESS, lsn_allocator_.init(initial_log_id, initial_scn, start_lsn));
  EXPECT_EQ(OB_INVALID_ARGUMENT, lsn_allocator_.truncate(tmp_lsn, truncate_log_id, new_scn));
  tmp_lsn.val_ = 100;
  EXPECT_EQ(OB_SUCCESS, lsn_allocator_.truncate(tmp_lsn, truncate_log_id, new_scn));
  int64_t log_id_upper_bound = 99999;
  LSN lsn_upper_bound(9999999999);
  EXPECT_EQ(OB_SUCCESS, lsn_allocator_.alloc_lsn_scn(b_scn, size, log_id_upper_bound, lsn_upper_bound, lsn, log_id, scn,
            is_new_log, need_gen_padding_entry, padding_len));
  EXPECT_EQ(truncate_log_id + 1, log_id);
  // test truncate()
  tmp_lsn.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, lsn_allocator_.inc_update_last_log_info(tmp_lsn, tmp_log_id, tmp_scn));
  tmp_lsn.val_ = 10;  // no need update
  EXPECT_EQ(OB_SUCCESS, lsn_allocator_.inc_update_last_log_info(tmp_lsn, tmp_log_id, tmp_scn));
  EXPECT_EQ(OB_SUCCESS, lsn_allocator_.alloc_lsn_scn(b_scn, size, log_id_upper_bound, lsn_upper_bound, lsn, log_id, scn,
            is_new_log, need_gen_padding_entry, padding_len));
  EXPECT_EQ(truncate_log_id + 1, log_id);  // 聚合到上一条日志中
  // update success
  tmp_lsn.val_ = 10000000;
  EXPECT_EQ(OB_SUCCESS, lsn_allocator_.inc_update_last_log_info(tmp_lsn, tmp_log_id, tmp_scn));
  size = 2 * 1024 * 1024;
  EXPECT_EQ(OB_SUCCESS, lsn_allocator_.alloc_lsn_scn(b_scn, size, log_id_upper_bound, lsn_upper_bound, lsn, log_id, scn,
            is_new_log, need_gen_padding_entry, padding_len));
  EXPECT_EQ(tmp_log_id + 1, log_id);

  // 由于之前alloc的size比较大，故当前is_need_cut为true，这里会报-4109
  EXPECT_EQ(OB_STATE_NOT_MATCH, lsn_allocator_.try_freeze_by_time(end_lsn, end_log_id));
  // 生成一条新的小日志，预期is_need_cut会为false
  size = 10;
  EXPECT_EQ(OB_SUCCESS, lsn_allocator_.alloc_lsn_scn(b_scn, size, log_id_upper_bound, lsn_upper_bound, lsn, log_id, scn,
            is_new_log, need_gen_padding_entry, padding_len));

  EXPECT_EQ(log_id, lsn_allocator_.get_max_log_id());
  EXPECT_EQ(scn, lsn_allocator_.get_max_scn());
  EXPECT_EQ(OB_SUCCESS, lsn_allocator_.get_curr_end_lsn(end_lsn));
  LSN old_end_lsn = end_lsn;
  EXPECT_EQ(OB_SUCCESS, lsn_allocator_.try_freeze_by_time(end_lsn, end_log_id));
  EXPECT_EQ(old_end_lsn, end_lsn);
  EXPECT_EQ(log_id, end_log_id);
  EXPECT_EQ(OB_SUCCESS, lsn_allocator_.try_freeze(end_lsn, end_log_id));
  EXPECT_EQ(old_end_lsn, end_lsn);
  EXPECT_EQ(log_id, end_log_id);
}

TEST_F(TestLSNAllocator, test_alloc_offset_single_thread)
{
  init_size_array();
  LSNAllocator lsn_allocator;
  LSN start_lsn(0);
  EXPECT_EQ(OB_SUCCESS, lsn_allocator.init(0, share::SCN::base_scn(), start_lsn));

  int64_t avg_cost = 0;
  int64_t ROUND = 1;
  int64_t count = 1000000;
  int64_t log_id_upper_bound = 999999999;
  LSN lsn_upper_bound(ROUND * count * MAX_LOG_BUFFER_SIZE);
  for (int64_t j = 0; j < ROUND; j++) {
      int64_t idx = rand() % LOG_LOG_CNT;
    const int64_t begin_ts = ObTimeUtility::current_time_ns();
    for (int i = 0; i < count; i++) {
      share::SCN b_scn = share::SCN::base_scn();
      int64_t size = log_size_array[idx];
      LSN ret_offset;
      int64_t ret_log_id;
      share::SCN ret_scn;
      bool is_new_log = false;
      bool need_gen_padding_entry = false;
      int64_t padding_len = 0;

      EXPECT_EQ(OB_SUCCESS, lsn_allocator.alloc_lsn_scn(b_scn, size, log_id_upper_bound, lsn_upper_bound, ret_offset, ret_log_id, ret_scn,
            is_new_log, need_gen_padding_entry, padding_len));
    }
    int64_t cost = ObTimeUtility::current_time_ns() - begin_ts;
    // PALF_LOG(ERROR, "100w alloc cost time ns", K(cost));
    std::cout << "100w alloc cost time ns:" << cost << std::endl;
    avg_cost += cost;
  }
  std::cout << ROUND << " round 100w alloc avg cost time ns:" << avg_cost / ROUND << std::endl;
  std::cout << "finish test_alloc_offset_single_thread" << std::endl;
}

// 下面测试多线程alloc_lsn_scn的性能
class TestThread
{
public:
  TestThread() {}
  virtual ~TestThread() { }
public:
  void create_and_run(int64_t th_idx)
  {
    log_size_ = log_size_array[th_idx % LOG_LOG_CNT];
    th_idx_ = th_idx;
    if (0 != pthread_create(&thread_, NULL, routine, this)){
      PALF_LOG_RET(ERROR, OB_ERR_SYS, "create thread fail", K(thread_));
    } else {
      PALF_LOG(INFO, "create thread success", K(thread_), K(th_idx_), K(log_size_));
    }
  }

  void join()
  {
    pthread_join(thread_, NULL);
  }

  static void* routine(void *arg) {
    TestThread *test_thread = static_cast<TestThread*>(arg);
    share::SCN scn = share::SCN::base_scn();
    int64_t size = test_thread->log_size_;
    LSN ret_offset;
    int64_t ret_log_id;
    share::SCN ret_scn;
    bool is_new_log = false;
    bool need_gen_padding_entry = false;
    int64_t padding_len = 0;
    int64_t log_id_upper_bound = 9999999;
    LSN lsn_upper_bound(999999 * MAX_LOG_BUFFER_SIZE);

    for (int j = 0; j < 1; j++) {
    const int64_t begin_ts = ObTimeUtility::current_time_ns();
    for (int i = 0; i < 1000; i++) {
//      size = log_size_array[i % LOG_LOG_CNT];
      EXPECT_EQ(OB_SUCCESS, global_lsn_allocator.alloc_lsn_scn(scn, size, log_id_upper_bound, lsn_upper_bound, ret_offset, ret_log_id, ret_scn,
            is_new_log, need_gen_padding_entry, padding_len));
    }
    int64_t cost = ObTimeUtility::current_time_ns() - begin_ts;
//    std::cout << test_thread->th_idx_ << " finish 100w alloc cost time ns:" << cost << std::endl;
    }

    return NULL;
  }
public:
  pthread_t thread_;
  int64_t th_idx_;
  int64_t log_size_;
};

TEST_F(TestLSNAllocator, test_alloc_offset_multi_thread)
{
  init_size_array();
  init_offset_allocator();
  const int64_t THREAD_CNT = 128;
  TestThread threads[THREAD_CNT];
  for (int tidx = 0; tidx < THREAD_CNT; ++tidx) {
    threads[tidx].create_and_run(tidx);
    PALF_LOG(INFO, "create thread", K(tidx));
  }
  std::cout << " finish create all threads, count:" << THREAD_CNT << std::endl;
  for (int tidx = 0; tidx < THREAD_CNT; ++tidx) {
    threads[tidx].join();
  }
};


} // END of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  system("rm -rf ./test_lsn_allocator.log*");
  OB_LOGGER.set_file_name("test_lsn_allocator.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_lsn_allocator");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
