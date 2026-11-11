/**
 * Copyright (c) 2026 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <sys/mman.h>
#include <unistd.h>

#define private public
#define protected public
#include "sql/monitor/ob_sql_stat_record.h"
#include "sql/monitor/ob_sql_stat_manager.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#undef protected
#undef private

namespace oceanbase
{
namespace sql
{
using namespace common;

class TestSqlStatRecord : public ::testing::Test
{
public:
  enum { WRITER_COUNT = 8 };

  struct ObInitCandidate
  {
    ObSqlStatRecordKey key_;
    ObSQLSessionInfo session_;
    ObPhysicalPlan plan_;
    char sql_[64];
    char database_[32];

    int init(const int64_t index)
    {
      int ret = OB_SUCCESS;
      MEMSET(key_.sql_id_, 'A' + index, OB_MAX_SQL_ID_LENGTH);
      key_.sql_id_[OB_MAX_SQL_ID_LENGTH] = '\0';
      key_.set_plan_hash(1000 + index);
      key_.source_addr_.set_ipv4_addr("127.0.0.1", 4000 + index);
      snprintf(sql_, sizeof(sql_), "select %ld from sqlstat_candidate_%ld", index, index);
      snprintf(database_, sizeof(database_), "sqlstat_db_%ld", index);
      session_.set_stmt_type(0 == index % 2 ? stmt::T_SELECT : stmt::T_INSERT);
      session_.set_database_id(2000 + index);
      session_.sys_vars_cache_.set_collation_connection(
          0 == index % 2 ? CS_TYPE_UTF8MB4_GENERAL_CI : CS_TYPE_UTF8MB4_BIN);
      if (OB_FAIL(session_.set_default_database(ObString::make_string(database_)))) {
      } else if (OB_FAIL(session_.set_user("sqlstat_user", "localhost", 3000 + index))) {
      } else {
        plan_.object_id_ = 4000 + index;
        plan_.set_plan_type(0 == index % 2 ? OB_PHY_PLAN_LOCAL : OB_PHY_PLAN_DISTRIBUTED);
        plan_.set_need_param(false);
        plan_.stat_.gen_time_ = 5000 + index;
        plan_.stat_.raw_sql_ = ObString::make_string(sql_);
      }
      return ret;
    }
  };

  class ObProtectedKey
  {
  public:
    ObProtectedKey() : mapping_(MAP_FAILED), size_(0), key_(nullptr) {}
    ~ObProtectedKey()
    {
      if (MAP_FAILED != mapping_) {
        if (0 == mprotect(mapping_, size_, PROT_READ | PROT_WRITE)) {
          key_->~ObSqlStatRecordKey();
        }
        munmap(mapping_, size_);
      }
    }
    int init()
    {
      int ret = OB_SUCCESS;
      size_ = sysconf(_SC_PAGESIZE);
      mapping_ = mmap(nullptr, size_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
      if (MAP_FAILED == mapping_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        key_ = new (mapping_) ObSqlStatRecordKey();
        key_->set_sql_id("SQLSTAT_PROTECTED_KEY");
        if (0 != mprotect(mapping_, size_, PROT_NONE)) {
          ret = OB_ERR_UNEXPECTED;
        }
      }
      return ret;
    }
    ObSqlStatRecordKey &get_key() { return *key_; }
  private:
    void *mapping_;
    int64_t size_;
    ObSqlStatRecordKey *key_;
  };

  TestSqlStatRecord()
      : start_(false), entered_(0), reader_checks_(0), reader_errors_(0)
  {}
  void SetUp() override
  {
    for (int64_t i = 0; i < WRITER_COUNT; ++i) {
      ASSERT_EQ(OB_SUCCESS, candidates_[i].init(i));
      init_results_[i] = OB_ERR_UNEXPECTED;
      sum_results_[i] = OB_ERR_UNEXPECTED;
    }
  }
  int initialize_record(const int64_t index)
  {
    ObInitCandidate &candidate = candidates_[index];
    return record_.try_init_plan_stat_info(candidate.key_, candidate.session_,
        ObString::make_string(candidate.sql_), &candidate.plan_);
  }
  bool matches_candidate(const ObSqlStatInfo &info, const int64_t index) const
  {
    const ObInitCandidate &candidate = candidates_[index];
    return info.get_key() == candidate.key_
        && 0 == STRCMP(info.get_query_sql(), candidate.sql_)
        && info.get_sql_type() == candidate.session_.get_stmt_type()
        && info.get_sql_cs_type() == candidate.session_.get_local_collation_connection()
        && info.get_parsing_db_id() == candidate.session_.get_database_id()
        && 0 == STRCMP(info.get_parsing_db_name(), candidate.database_)
        && info.get_parsing_user_id() == candidate.session_.get_user_id()
        && info.get_plan_id() == candidate.plan_.get_plan_id()
        && info.get_plan_type() == candidate.plan_.get_plan_type()
        && info.get_first_load_time() == candidate.plan_.stat_.gen_time_;
  }
  bool matches_one_candidate() const
  {
    bool matched = false;
    for (int64_t i = 0; !matched && i < WRITER_COUNT; ++i) {
      matched = matches_candidate(record_.get_sql_stat_info(), i);
    }
    return matched;
  }
  void wait_for_start()
  {
    entered_.fetch_add(1, std::memory_order_release);
    while (!start_.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  }
  void start_threads(const int64_t count)
  {
    while (entered_.load(std::memory_order_acquire) < count) {
      std::this_thread::yield();
    }
    start_.store(true, std::memory_order_release);
  }
  static void run_initializer(TestSqlStatRecord *test, const int64_t index)
  {
    test->wait_for_start();
    test->init_results_[index] = test->initialize_record(index);
  }
  static void run_reader(TestSqlStatRecord *test)
  {
    test->wait_for_start();
    const std::chrono::steady_clock::time_point deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!test->record_.is_plan_stat_info_inited()
        && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::yield();
    }
    if (test->record_.is_plan_stat_info_inited()) {
      for (int64_t i = 0; i < 1024; ++i) {
        if (!test->record_.is_plan_stat_info_inited() || !test->matches_one_candidate()) {
          test->reader_errors_.fetch_add(1, std::memory_order_relaxed);
        }
        test->reader_checks_.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }
  static void run_initializing_accumulator(TestSqlStatRecord *test, const int64_t index)
  {
    ObExecutingSqlStatRecord executing;
    executing.elapsed_time_end_ = 7;
    executing.disk_reads_end_ = 1;
    executing.rows_processed_end_ = 3;
    executing.set_is_in_retry(0 != index % 2);
    executing.set_is_failed(0 != index % 2);
    test->wait_for_start();
    test->init_results_[index] = test->initialize_record(index);
    test->sum_results_[index] = test->record_.sum_stat_value(executing);
  }

protected:
  ObExecutedSqlStatRecord record_;
  ObInitCandidate candidates_[WRITER_COUNT];
  std::atomic<bool> start_;
  std::atomic<int64_t> entered_;
  std::atomic<int64_t> reader_checks_;
  std::atomic<int64_t> reader_errors_;
  int init_results_[WRITER_COUNT];
  int sum_results_[WRITER_COUNT];
};

TEST_F(TestSqlStatRecord, unready_transfer_skips_key_and_manager)
{
  ObProtectedKey protected_key;
  ObExecutingSqlStatRecord executing;
  executing.elapsed_time_end_ = 7;
  ASSERT_EQ(OB_SUCCESS, protected_key.init());
  ASSERT_FALSE(record_.is_plan_stat_info_inited());
  ASSERT_EQ(OB_SUCCESS, record_.sum_stat_value(executing));
  EXPECT_EQ(OB_SUCCESS, record_.move_to_sqlstat_cache(protected_key.get_key(), false));
  EXPECT_EQ(OB_SUCCESS, record_.move_to_sqlstat_cache(protected_key.get_key(), true));
  EXPECT_EQ(1, record_.get_executions_total());
  EXPECT_EQ(7, record_.get_elapsed_time_total());
  ATOMIC_STORE(&record_.plan_stat_init_state_, ObExecutedSqlStatRecord::PLAN_STAT_INITIALIZING);
  EXPECT_FALSE(record_.is_plan_stat_info_inited());
  EXPECT_EQ(OB_SUCCESS, record_.move_to_sqlstat_cache(protected_key.get_key(), false));
  EXPECT_EQ(OB_SUCCESS, record_.move_to_sqlstat_cache(protected_key.get_key(), true));
  EXPECT_EQ(1, record_.get_executions_total());
  EXPECT_EQ(7, record_.get_elapsed_time_total());
  EXPECT_FALSE(record_.is_plan_stat_info_inited());
}

TEST_F(TestSqlStatRecord, concurrent_initialization_publishes_one_complete_candidate)
{
  std::thread writers[WRITER_COUNT];
  for (int64_t i = 0; i < WRITER_COUNT; ++i) {
    writers[i] = std::thread(run_initializer, this, i);
  }
  std::thread reader(run_reader, this);
  start_threads(WRITER_COUNT + 1);
  for (int64_t i = 0; i < WRITER_COUNT; ++i) {
    writers[i].join();
    EXPECT_EQ(OB_SUCCESS, init_results_[i]);
  }
  reader.join();
  ASSERT_TRUE(record_.is_plan_stat_info_inited());
  EXPECT_EQ(1024, reader_checks_.load());
  EXPECT_EQ(0, reader_errors_.load());
  ASSERT_TRUE(matches_one_candidate());
  ObSqlStatInfo published;
  ASSERT_EQ(OB_SUCCESS, published.assign(record_.get_sql_stat_info()));
  for (int64_t i = 0; i < WRITER_COUNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, initialize_record(i));
    EXPECT_TRUE(record_.get_key() == published.get_key());
    EXPECT_STREQ(published.get_query_sql(), record_.get_sql_stat_info().get_query_sql());
    EXPECT_EQ(published.get_plan_id(), record_.get_sql_stat_info().get_plan_id());
    EXPECT_TRUE(matches_one_candidate());
  }
}

TEST_F(TestSqlStatRecord, initializing_keeps_execution_statistics)
{
  ATOMIC_STORE(&record_.plan_stat_init_state_, ObExecutedSqlStatRecord::PLAN_STAT_INITIALIZING);
  std::thread writers[WRITER_COUNT];
  for (int64_t i = 0; i < WRITER_COUNT; ++i) {
    writers[i] = std::thread(run_initializing_accumulator, this, i);
  }
  start_threads(WRITER_COUNT);
  for (int64_t i = 0; i < WRITER_COUNT; ++i) {
    writers[i].join();
    EXPECT_EQ(OB_SUCCESS, init_results_[i]);
    EXPECT_EQ(OB_SUCCESS, sum_results_[i]);
  }
  EXPECT_FALSE(record_.is_plan_stat_info_inited());
  EXPECT_EQ(4, record_.get_executions_total());
  EXPECT_EQ(4, record_.get_retry_total());
  EXPECT_EQ(56, record_.get_elapsed_time_total());
  EXPECT_EQ(8, record_.get_disk_reads_total());
  EXPECT_EQ(24, record_.get_rows_processed_total());
  EXPECT_EQ(4, record_.get_error_count_total());
  ATOMIC_STORE(&record_.plan_stat_init_state_, ObExecutedSqlStatRecord::PLAN_STAT_UNINITIALIZED);
  ASSERT_EQ(OB_SUCCESS, initialize_record(0));
  ASSERT_TRUE(record_.is_plan_stat_info_inited());
  EXPECT_TRUE(matches_candidate(record_.get_sql_stat_info(), 0));
  EXPECT_EQ(4, record_.get_executions_total());
  EXPECT_EQ(4, record_.get_retry_total());
  EXPECT_EQ(56, record_.get_elapsed_time_total());
  EXPECT_EQ(8, record_.get_disk_reads_total());
  EXPECT_EQ(24, record_.get_rows_processed_total());
  EXPECT_EQ(4, record_.get_error_count_total());
}

TEST_F(TestSqlStatRecord, initializing_execution_entry_keeps_first_and_batch_statistics)
{
  ObInitCandidate &candidate = candidates_[0];
  ObExecutedSqlStatRecord &plan_record = candidate.plan_.sql_stat_record_value_;
  ObExecutingSqlStatRecord executing;
  ObString sql = ObString::make_string(candidate.sql_);
  executing.elapsed_time_end_ = 7;
  executing.rows_processed_end_ = 3;
  candidate.session_.set_cur_sql_id(candidate.key_.sql_id_);
  candidate.plan_.set_plan_hash_value(candidate.key_.plan_hash_);
  ATOMIC_STORE(&plan_record.plan_stat_init_state_, ObExecutedSqlStatRecord::PLAN_STAT_INITIALIZING);
  const int64_t execution_count = ObExecutedSqlStatRecord::BATCH_COUNT + 2;
  for (int64_t i = 0; i < execution_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, executing.move_to_sqlstat_cache(candidate.session_, sql, &candidate.plan_));
  }
  EXPECT_FALSE(plan_record.is_plan_stat_info_inited());
  EXPECT_FALSE(plan_record.get_key().is_valid());
  EXPECT_EQ(execution_count, plan_record.get_executions_total());
  EXPECT_EQ(execution_count * 7, plan_record.get_elapsed_time_total());
  EXPECT_EQ(execution_count * 3, plan_record.get_rows_processed_total());
  ATOMIC_STORE(&plan_record.plan_stat_init_state_, ObExecutedSqlStatRecord::PLAN_STAT_UNINITIALIZED);
  ASSERT_EQ(OB_SUCCESS, plan_record.try_init_plan_stat_info(
      candidate.key_, candidate.session_, sql, &candidate.plan_));
  EXPECT_TRUE(plan_record.is_plan_stat_info_inited());
  EXPECT_TRUE(matches_candidate(plan_record.get_sql_stat_info(), 0));
  EXPECT_EQ(execution_count, plan_record.get_executions_total());
  EXPECT_EQ(execution_count * 7, plan_record.get_elapsed_time_total());
  EXPECT_EQ(execution_count * 3, plan_record.get_rows_processed_total());
  EXPECT_EQ(OB_SUCCESS, plan_record.reset());
}

TEST_F(TestSqlStatRecord, copy_assign_reset_and_reinitialize)
{
  ObExecutingSqlStatRecord executing;
  executing.elapsed_time_end_ = 7;
  ASSERT_EQ(OB_SUCCESS, initialize_record(0));
  ASSERT_EQ(OB_SUCCESS, record_.sum_stat_value(executing));
  ASSERT_EQ(OB_SUCCESS, record_.update_last_snap_record_value());
  ObExecutedSqlStatRecord copied(record_);
  ObExecutedSqlStatRecord assigned;
  ASSERT_EQ(OB_SUCCESS, assigned.assign(record_));
  EXPECT_TRUE(copied.is_plan_stat_info_inited());
  EXPECT_TRUE(assigned.is_plan_stat_info_inited());
  EXPECT_TRUE(matches_candidate(copied.get_sql_stat_info(), 0));
  EXPECT_TRUE(matches_candidate(assigned.get_sql_stat_info(), 0));
  EXPECT_EQ(1, copied.get_executions_total());
  EXPECT_EQ(1, assigned.get_executions_last_snap());
  ATOMIC_STORE(&record_.plan_stat_init_state_, ObExecutedSqlStatRecord::PLAN_STAT_INITIALIZING);
  ObExecutedSqlStatRecord pending_copy(record_);
  ASSERT_EQ(OB_SUCCESS, assigned.assign(record_));
  EXPECT_FALSE(pending_copy.is_plan_stat_info_inited());
  EXPECT_EQ(ObExecutedSqlStatRecord::PLAN_STAT_UNINITIALIZED, pending_copy.plan_stat_init_state_);
  EXPECT_FALSE(assigned.is_plan_stat_info_inited());
  EXPECT_EQ(ObExecutedSqlStatRecord::PLAN_STAT_UNINITIALIZED, assigned.plan_stat_init_state_);
  ASSERT_EQ(OB_SUCCESS, record_.reset());
  EXPECT_FALSE(record_.is_plan_stat_info_inited());
  EXPECT_FALSE(record_.get_key().is_valid());
  EXPECT_EQ(0, record_.get_executions_total());
  EXPECT_EQ(0, record_.get_executions_last_snap());
  EXPECT_EQ(0, record_.get_elapsed_time_total());
  ASSERT_EQ(OB_SUCCESS, initialize_record(1));
  EXPECT_TRUE(record_.is_plan_stat_info_inited());
  EXPECT_TRUE(matches_candidate(record_.get_sql_stat_info(), 1));
}

TEST_F(TestSqlStatRecord, real_record_hashmap_reclaims_after_last_reference)
{
  typedef ObSqlStatRecordNodeAlloc<ObExecutedSqlStatRecord, ObSqlStatRecordKey> ObRecordAlloc;
  typedef ObLinkHashMap<ObSqlStatRecordKey, ObExecutedSqlStatRecord, ObRecordAlloc> ObRecordMap;
  ObArenaAllocator allocator;
  ObRecordMap records(ObRecordAlloc(&allocator, 1), 16);
  ObExecutedSqlStatRecord *value = nullptr;
  ASSERT_EQ(OB_SUCCESS, records.init("SqlStatInitUT", OB_SERVER_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, records.create(candidates_[0].key_, value));
  ASSERT_NE(nullptr, value);
  ASSERT_EQ(OB_SUCCESS, value->try_init_plan_stat_info(candidates_[0].key_, candidates_[0].session_,
      ObString::make_string(candidates_[0].sql_), &candidates_[0].plan_));
  ASSERT_TRUE(value->is_plan_stat_info_inited());
  EXPECT_TRUE(matches_candidate(value->get_sql_stat_info(), 0));
  EXPECT_EQ(1, records.get_alloc_handle().alloc_count_);
  EXPECT_EQ(OB_SUCCESS, records.del(candidates_[0].key_));
  EXPECT_EQ(0, records.count());
  EXPECT_EQ(1, records.get_alloc_handle().alloc_count_);
  EXPECT_TRUE(matches_candidate(value->get_sql_stat_info(), 0));
  records.revert(value);
  value = nullptr;
  records.purge();
  EXPECT_EQ(0, records.get_alloc_handle().alloc_count_);
  ASSERT_EQ(OB_SUCCESS, records.create(candidates_[1].key_, value));
  ASSERT_NE(nullptr, value);
  EXPECT_FALSE(value->is_plan_stat_info_inited());
  records.revert(value);
  value = nullptr;
  EXPECT_EQ(OB_SUCCESS, records.del(candidates_[1].key_));
  records.purge();
  EXPECT_EQ(0, records.get_alloc_handle().alloc_count_);
}

}
}

int main(int argc, char *argv[])
{
  OB_LOGGER.set_log_level("WARN");
  OB_LOGGER.set_file_name("test_sql_stat_record.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
