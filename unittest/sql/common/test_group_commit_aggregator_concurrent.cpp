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

#define USING_LOG_PREFIX SQL
#include <gtest/gtest.h>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <mutex>
#ifdef OB_HOTSPOT_GROUP_COMMIT
#include "sql/ob_sql_group_commit_aggregator.h"
#include "sql/ob_sql_group_commit_struct.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/oblog/ob_log.h"
#include "share/rc/ob_tenant_base.h"
#include "rpc/ob_request.h"
#endif

#ifdef OB_HOTSPOT_GROUP_COMMIT
namespace oceanbase
{
using namespace oceanbase::sql;
using namespace oceanbase::common;
namespace test
{

// SQL: update accounts set c1=c1+?, c2=c2-? where id=? and c2 >?
// 参数: c1, c2, id, c2 均为Int类型
// id为key，值为100
// ps_stmt_id为12345
static const int64_t TEST_PS_STMT_ID = 12345;
static const int64_t TEST_KEY_PARAM_VALUE = 100;  // id的值
static const char* TEST_SQL = "update accounts set c1=c1+?, c2=c2-? where id=? and c2 >?";
static const int64_t TEST_PARAM_COUNT = 4;

class TestGroupCommitAggregatorConcurrent : public ::testing::Test
{
public:
  TestGroupCommitAggregatorConcurrent()
    : tenant_base_(OB_SYS_TENANT_ID),
      allocator_(common::ObModIds::TEST, common::OB_MALLOC_NORMAL_BLOCK_SIZE)
  {}
  virtual ~TestGroupCommitAggregatorConcurrent() {}

  virtual void SetUp() override
  {
    // 初始化租户环境
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init(nullptr));
    ObTenantEnv::set_tenant(&tenant_base_);
    // 初始化聚合器
    ASSERT_EQ(OB_SUCCESS, aggregator_.init());
    ObTenantBase *tenant_local = ObTenantEnv::get_tenant_local();
    tenant_local->set<ObSqlGroupCommitAggregator*>(&aggregator_);
  }

  virtual void TearDown() override
  {
    // mtl_wait 接受的是 ObSqlGroupCommitAggregator*& 类型
    // 需要创建一个临时指针变量
    ObSqlGroupCommitAggregator *aggregator_ptr = &aggregator_;
    ObSqlGroupCommitAggregator::mtl_wait(aggregator_ptr);
    aggregator_.destroy();
    tenant_base_.destroy();
    ObTenantEnv::set_tenant(nullptr);
  }

protected:
  // 创建测试用的ObGroupSqlKey
  void build_group_sql_key(ObGroupSqlKey &sql_key)
  {
    // 设置ps_sql_key
    ps_sql_key_ = ObPsSqlKey(1, ObString::make_string(TEST_SQL));

    // 设置参数类型 - 4个Int类型参数
    param_types_[0] = obmysql::EMySQLFieldType::MYSQL_TYPE_LONGLONG;  // c1 增量
    param_types_[1] = obmysql::EMySQLFieldType::MYSQL_TYPE_LONGLONG;  // c2 减量
    param_types_[2] = obmysql::EMySQLFieldType::MYSQL_TYPE_LONGLONG;  // id (key)
    param_types_[3] = obmysql::EMySQLFieldType::MYSQL_TYPE_LONGLONG;  // c2 条件

    sql_key.shallow_copy(ps_sql_key_, param_types_, TEST_PARAM_COUNT);
  }

  // 创建测试用的ObGroupKey - 使用独立的参数存储
  void build_group_key(ObGroupKey &group_key, ObObjParam *key_param, int64_t key_value)
  {
    key_param->set_int(key_value);
    group_key.shallow_copy(key_param, 1);
  }

  // 创建测试用的ObRequest
  rpc::ObRequest* create_request()
  {
    void *ptr = allocator_.alloc(sizeof(rpc::ObRequest));
    if (OB_ISNULL(ptr)) {
      int ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc ObRequest", K(ret));
      return nullptr;
    }
    rpc::ObRequest *req = new (ptr) rpc::ObRequest(rpc::ObRequest::OB_MYSQL);
    req->set_receive_timestamp(ObTimeUtility::current_time());
    return req;
  }

  // 模拟 obmp_stmt_execute.cpp 中的 alloc_params 和 process_group_commit_sub_request
  // 创建完整的子请求
  ObGroupCommitSubRequest* create_sub_request(int64_t c1_delta, int64_t c2_delta,
                                               int64_t id_value, int64_t c2_condition)
  {
    ObGroupCommitSubRequest *sub_req = nullptr;
    int ret = aggregator_.alloc_sub_request(sub_req);
    if (OB_SUCCESS != ret || sub_req == nullptr) {
      LOG_WARN("fail to alloc sub request", K(ret));
      return nullptr;
    }

    sub_req->ps_client_stmt_id_ = TEST_PS_STMT_ID;
    // 模拟 process_group_commit_sub_request 中 sub_req->req_ = req_
    sub_req->req_ = create_request();
    if (OB_ISNULL(sub_req->req_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create request", K(ret));
      aggregator_.free_sub_request(sub_req);
      return nullptr;
    }
    ObSQLSessionInfo *default_session = static_cast<ObSQLSessionInfo*>(allocator_.alloc(sizeof(ObSQLSessionInfo)));
    if (OB_ISNULL(default_session)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc default session", K(ret));
      aggregator_.free_sub_request(sub_req);
      return nullptr;
    }
    default_session = new (default_session) ObSQLSessionInfo();
    // 批量聚合时会调用 sess_->is_in_transaction() 选主请求，session 需要是有效对象。
    sub_req->sess_ = default_session;

    // 设置参数
    ObObjParam param1, param2, param3, param4;
    param1.set_int(c1_delta);
    param2.set_int(c2_delta);
    param3.set_int(id_value);
    param4.set_int(c2_condition);

    if (OB_FAIL(sub_req->params_.push_back(param1)) ||
        OB_FAIL(sub_req->params_.push_back(param2)) ||
        OB_FAIL(sub_req->params_.push_back(param3)) ||
        OB_FAIL(sub_req->params_.push_back(param4))) {
      LOG_WARN("fail to push back params", K(ret));
      aggregator_.free_sub_request(sub_req);
      return nullptr;
    }

    sub_req->enqueue_ts_ = ObTimeUtility::current_time();
    return sub_req;
  }

protected:
  share::ObTenantBase tenant_base_;
  ObSqlGroupCommitAggregator aggregator_;
  ObArenaAllocator allocator_;

  // 用于存储 sql_key 数据
  ObPsSqlKey ps_sql_key_;
  obmysql::EMySQLFieldType param_types_[TEST_PARAM_COUNT];
};

/**
 * 测试67个请求的串行聚合和处理（每个请求使用不同的 group_key）
 *
 * 模拟 obmp_stmt_execute.cpp 中的处理流程:
 * 1. alloc_sub_request 分配子请求
 * 2. add_sub_request 添加到聚合队列
 * 3. 根据返回结果判断执行模式 (GROUP_COMMIT_SUB_EXEC 或 GROUP_COMMIT_SUB_WAIT)
 * 4. 处理完成后调用 free_agg_request 释放资源
 *
 * 此测试中每个请求使用不同的 group_key (id=100, 101, 102, ...)
 * 因此每个请求都会立即获得执行权，不会有等待队列
 */
TEST_F(TestGroupCommitAggregatorConcurrent, test_serial_67_requests_diff_keys)
{
  const int64_t TEST_REQUEST_COUNT = 67;
  ObGroupSqlKey sql_key;
  build_group_sql_key(sql_key);

  int64_t success_count = 0;
  int64_t sub_exec_count = 0;  // GROUP_COMMIT_SUB_EXEC 模式计数
  int64_t total_sub_req_processed = 0;

  LOG_INFO("Starting test_serial_67_requests_diff_keys",
           K(TEST_REQUEST_COUNT),
           "sql", TEST_SQL);

  // 为每个请求创建独立的 group_key 参数存储
  ObObjParam key_params[TEST_REQUEST_COUNT];

  for (int64_t i = 0; i < TEST_REQUEST_COUNT; ++i) {
    int ret = OB_SUCCESS;
    bool need_response_error = false;

    // 1. 模拟 before_process 中的 alloc_sub_request
    int64_t c1_delta = i + 1;
    int64_t c2_delta = i * 2;
    int64_t id_value = TEST_KEY_PARAM_VALUE + i;  // 每个请求使用不同的 id 作为 group_key
    int64_t c2_condition = i;

    ObGroupCommitSubRequest *sub_req = create_sub_request(c1_delta, c2_delta, id_value, c2_condition);
    ASSERT_NE(nullptr, sub_req) << "Failed to create sub_request for request " << i;

    // 2. 构造 group_key
    ObGroupKey group_key;
    build_group_key(group_key, &key_params[i], id_value);

    bool direct_execute = false;
    // 3. 模拟 process_group_commit_sub_request 中调用 add_sub_request
    ret = aggregator_.add_sub_request(sql_key, group_key, sub_req, direct_execute, need_response_error);

    if (OB_FAIL(ret)) {
      LOG_WARN("add_sub_request failed", K(ret), K(i), K(need_response_error));
      continue;
    }

    success_count++;

    // 4. 模拟 process 中的处理逻辑
    // 检查是否获得了执行权 (类似 obmp_stmt_execute.cpp 中的判断)
    if (sub_req->req_->has_group_commit_agg_info()) {
      ObGroupCommitAggInfo *agg_info = sub_req->req_->get_group_commit_agg_info();
      int64_t sub_req_cnt = agg_info->get_sub_req_cnt();

      if (sub_req_cnt == 1) {
        // GROUP_COMMIT_SUB_EXEC 模式: 只有一个子请求，在当前线程执行
        sub_exec_count++;

        LOG_INFO("Processing request in SUB_EXEC mode",
                 K(i), K(sub_req_cnt), KPC(agg_info));

        // 模拟 SQL 执行完成...

        // 5. 模拟执行完成后调用 free_agg_request
        // 由于每个请求使用不同的 group_key，等待队列为空
        // free_agg_request 中的 dec_lock_concurrency 不会触发新的聚合请求
        ret = ObSqlGroupCommitAggregator::free_agg_request(*sub_req->req_);
        if (OB_FAIL(ret)) {
          LOG_ERROR("free_agg_request failed", K(ret), K(i));
        } else {
          total_sub_req_processed++;
        }
      } else {
        // 不应该发生
        LOG_INFO("Unexpected: aggregated batch execution", K(i), K(sub_req_cnt));
        ret = ObSqlGroupCommitAggregator::free_agg_request(*sub_req->req_);
        total_sub_req_processed += sub_req_cnt;
      }
    } else {
      // 不应该发生
      LOG_INFO("Unexpected: request in WAIT mode", K(i));
    }
  }

  LOG_INFO("test_serial_67_requests_diff_keys completed",
           K(TEST_REQUEST_COUNT),
           K(success_count),
           K(sub_exec_count),
           K(total_sub_req_processed));

  ASSERT_EQ(success_count, TEST_REQUEST_COUNT) << "All requests should be added successfully";
  ASSERT_EQ(sub_exec_count, TEST_REQUEST_COUNT) << "All requests should be in SUB_EXEC mode";
  ASSERT_EQ(total_sub_req_processed, TEST_REQUEST_COUNT) << "All requests should be processed";
}

/**
 * 测试并发场景：多线程同时添加请求到不同的 group_key
 * 模拟67个并发请求，每个请求更新不同的行
 */
TEST_F(TestGroupCommitAggregatorConcurrent, test_concurrent_67_requests_diff_keys)
{
  const int64_t TEST_THREAD_COUNT = 67;
  ObGroupSqlKey sql_key;
  build_group_sql_key(sql_key);

  // 启动信号
  std::atomic<bool> start_flag(false);
  std::atomic<int64_t> success_count(0);
  std::atomic<int64_t> sub_exec_count(0);
  std::atomic<int64_t> processed_count(0);

  // 为每个线程创建独立的 group_key 参数存储
  std::vector<ObObjParam> key_params(TEST_THREAD_COUNT);
  std::vector<ObGroupKey> group_keys(TEST_THREAD_COUNT);
  std::vector<ObGroupCommitSubRequest*> sub_reqs(TEST_THREAD_COUNT);

  // 预先创建所有子请求
  for (int64_t i = 0; i < TEST_THREAD_COUNT; ++i) {
    int64_t id_value = TEST_KEY_PARAM_VALUE + i;  // 每个线程使用不同的 id
    sub_reqs[i] = create_sub_request(i + 1, i * 2, id_value, i);
    ASSERT_NE(nullptr, sub_reqs[i]) << "Failed to create sub_request for thread " << i;
    build_group_key(group_keys[i], &key_params[i], id_value);
  }

  // 线程函数
  auto thread_func = [&](int64_t thread_id) {
    // 等待启动信号
    while (!start_flag.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }

    ObTenantSwitchGuard guard(&tenant_base_);
    ObTenantBase *tenant_local = ObTenantEnv::get_tenant_local();
    tenant_local->set<ObSqlGroupCommitAggregator*>(&aggregator_);

    int ret = OB_SUCCESS;
    bool need_response_error = false;
    bool direct_execute = false;
    ret = aggregator_.add_sub_request(sql_key, group_keys[thread_id],
                                       sub_reqs[thread_id], direct_execute, need_response_error);

    if (OB_SUCC(ret)) {
      success_count.fetch_add(1);

      if (sub_reqs[thread_id]->req_->has_group_commit_agg_info()) {
        ObGroupCommitAggInfo *agg_info = sub_reqs[thread_id]->req_->get_group_commit_agg_info();
        if (agg_info->get_sub_req_cnt() == 1) {
          sub_exec_count.fetch_add(1);

          // 由于每个线程使用不同的 group_key，等待队列为空
          // free_agg_request 不会触发新的聚合请求
          int free_ret = ObSqlGroupCommitAggregator::free_agg_request(*sub_reqs[thread_id]->req_);
          if (OB_SUCCESS == free_ret) {
            processed_count.fetch_add(1);
          }
        }
      }
    }
  };

  // 创建所有线程
  std::vector<std::thread> threads;
  for (int64_t i = 0; i < TEST_THREAD_COUNT; ++i) {
    threads.emplace_back(thread_func, i);
  }

  LOG_INFO("All 67 threads created, starting...");

  // 短暂等待确保所有线程就绪
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // 设置启动信号，所有线程同时开始
  start_flag.store(true, std::memory_order_release);

  // 等待所有线程完成
  for (auto &t : threads) {
    t.join();
  }

  LOG_INFO("test_concurrent_67_requests_diff_keys completed",
           K(TEST_THREAD_COUNT),
           "success_count", success_count.load(),
           "sub_exec_count", sub_exec_count.load(),
           "processed_count", processed_count.load());

  // 验证结果
  ASSERT_EQ(success_count.load(), TEST_THREAD_COUNT) << "All 67 threads should succeed";
  ASSERT_EQ(sub_exec_count.load(), TEST_THREAD_COUNT) << "All 67 requests should be in SUB_EXEC mode";
  ASSERT_EQ(processed_count.load(), TEST_THREAD_COUNT) << "All 67 requests should be processed";
}

/**
 * 测试基本的 alloc/free sub_request 功能
 * 这是最基础的内存分配测试
 */
TEST_F(TestGroupCommitAggregatorConcurrent, test_alloc_free_sub_request)
{
  const int64_t TEST_COUNT = 67;
  std::vector<ObGroupCommitSubRequest*> sub_reqs;

  LOG_INFO("Starting test_alloc_free_sub_request", K(TEST_COUNT));

  // 分配67个子请求
  for (int64_t i = 0; i < TEST_COUNT; ++i) {
    ObGroupCommitSubRequest *sub_req = nullptr;
    int ret = aggregator_.alloc_sub_request(sub_req);
    ASSERT_EQ(OB_SUCCESS, ret) << "alloc_sub_request should succeed";
    ASSERT_NE(nullptr, sub_req) << "sub_req should not be null";
    sub_reqs.push_back(sub_req);
  }

  LOG_INFO("Allocated all sub requests", K(TEST_COUNT));

  // 释放67个子请求
  for (auto sub_req : sub_reqs) {
    aggregator_.free_sub_request(sub_req);
  }

  LOG_INFO("test_alloc_free_sub_request completed successfully", K(TEST_COUNT));
}

/**
 * 测试 ObGroupSqlKey 和 ObGroupKey 的创建和比较
 */
TEST_F(TestGroupCommitAggregatorConcurrent, test_group_key_operations)
{
  ObGroupSqlKey sql_key1, sql_key2;
  build_group_sql_key(sql_key1);
  build_group_sql_key(sql_key2);

  // 验证相同的 sql_key 相等
  ASSERT_TRUE(sql_key1 == sql_key2) << "Same sql_key should be equal";
  uint64_t hash1 = 0, hash2 = 0;
  sql_key1.hash(hash1);
  sql_key2.hash(hash2);
  ASSERT_EQ(hash1, hash2) << "Same sql_key should have same hash";

  // 测试 group_key
  ObObjParam key_param1, key_param2, key_param3;
  ObGroupKey group_key1, group_key2, group_key3;

  build_group_key(group_key1, &key_param1, 100);
  build_group_key(group_key2, &key_param2, 100);
  build_group_key(group_key3, &key_param3, 101);

  // 验证相同值的 group_key 相等
  ASSERT_TRUE(group_key1 == group_key2) << "Same group_key value should be equal";
  uint64_t gk_hash1 = 0, gk_hash2 = 0;
  group_key1.hash(gk_hash1);
  group_key2.hash(gk_hash2);
  ASSERT_EQ(gk_hash1, gk_hash2) << "Same group_key should have same hash";

  // 验证不同值的 group_key 不相等
  ASSERT_FALSE(group_key1 == group_key3) << "Different group_key value should not be equal";

  LOG_INFO("test_group_key_operations completed successfully");
}

/**
 * 测试 ObGroupValue 的 add_request, dec_inflight_req_count, dec_lock_concurrency
 *
 * 场景：100个线程并发向同一个 ObGroupValue 添加请求
 * - 第一个请求直接获得执行权 (row_lock_concurrency_ == 0)
 * - 后续请求先进入 wait_queue_
 * - 去掉 max_lock_concurrency 限制后，只要 wait_queue_ 达到 agg_batch_size_(10)，就批量取出
 * - 执行完成后调用 dec_lock_concurrency 会从等待队列取出下一批请求
 */
TEST_F(TestGroupCommitAggregatorConcurrent, test_group_value_add_request_100_threads)
{
  const int64_t TEST_THREAD_COUNT = 100;
  auto finish_inflight_req = [](ObGroupValue &group_value) {
    ObSpinLockGuard guard(group_value.lock_);
    ASSERT_GT(group_value.inflight_req_count_, 0);
    group_value.inflight_req_count_--;
  };

  // 1. 构建 ObGroupSqlKey 和 ObGroupSqlValue
  ObGroupSqlKey sql_key;
  build_group_sql_key(sql_key);

  ObGroupSqlValue sql_value;
  ASSERT_EQ(OB_SUCCESS, sql_value.init(sql_key, &aggregator_));

  // 2. 构建 ObGroupKey 和 ObGroupValue
  // 所有100个线程使用同一个 group_key (模拟更新同一行)
  ObObjParam group_key_param;
  group_key_param.set_int(TEST_KEY_PARAM_VALUE);  // id = 100
  ObGroupKey group_key;
  group_key.shallow_copy(&group_key_param, 1);

  ObGroupValue group_value;
  ASSERT_EQ(OB_SUCCESS, group_value.init(&sql_value, group_key));

  // 设置聚合参数
  group_value.agg_batch_size_ = 10;        // 批量聚合大小

  LOG_INFO("Starting test_group_value_add_request_100_threads",
           K(TEST_THREAD_COUNT),
           "agg_batch_size", group_value.agg_batch_size_);

  // 3. 创建100个子请求
  std::vector<ObGroupCommitSubRequest*> sub_reqs(TEST_THREAD_COUNT);
  for (int64_t i = 0; i < TEST_THREAD_COUNT; ++i) {
    sub_reqs[i] = create_sub_request(i + 1, i * 2, TEST_KEY_PARAM_VALUE, i);
    ASSERT_NE(nullptr, sub_reqs[i]) << "Failed to create sub_request " << i;
  }

  // 4. 并发控制
  std::atomic<bool> start_flag(false);
  std::atomic<int64_t> add_request_count(0);
  std::atomic<int64_t> got_exec_right_count(0);  // 获得执行权的计数
  std::atomic<int64_t> wait_count(0);            // 进入等待队列的计数

  // 存储获得执行权的 handle 索引
  std::mutex handles_mutex;
  std::vector<int64_t> exec_thread_ids;  // 记录获得执行权的线程ID
  std::vector<ObInflightReqHandle> inflight_handles(TEST_THREAD_COUNT);  // 每个线程一个 handle

  // 5. 线程函数：调用 add_request
  auto add_request_func = [&](int64_t thread_id) {
    // 等待启动信号
    while (!start_flag.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }

    ObTenantSwitchGuard guard(&tenant_base_);

    group_value.add_request(*sub_reqs[thread_id], inflight_handles[thread_id], false/*force_agg_single*/);

    add_request_count.fetch_add(1);

    if (inflight_handles[thread_id].is_valid()) {
      got_exec_right_count.fetch_add(1);

      // 记录获得执行权的线程ID
      {
        std::lock_guard<std::mutex> lock(handles_mutex);
        exec_thread_ids.push_back(thread_id);
      }

      LOG_INFO("Thread got execution right",
               K(thread_id),
               "sub_req_cnt", inflight_handles[thread_id].sub_req_cnt_);
    } else {
      wait_count.fetch_add(1);
      LOG_DEBUG("Thread added to wait queue", K(thread_id));
    }
  };

  // 6. 创建并启动100个线程
  std::vector<std::thread> threads;
  for (int64_t i = 0; i < TEST_THREAD_COUNT; ++i) {
    threads.emplace_back(add_request_func, i);
  }

  // 短暂等待确保所有线程就绪
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // 设置启动信号
  start_flag.store(true, std::memory_order_release);

  // 等待所有线程完成
  for (auto &t : threads) {
    t.join();
  }

  LOG_INFO("Phase 1: All add_request completed",
           "add_request_count", add_request_count.load(),
           "got_exec_right_count", got_exec_right_count.load(),
           "wait_count", wait_count.load(),
           "inflight_req_count", group_value.inflight_req_count_,
           "row_lock_concurrency", group_value.row_lock_concurrency_,
           "wait_queue_size", group_value.wait_queue_.size());

  // 7. 验证 Phase 1 结果
  ASSERT_EQ(add_request_count.load(), TEST_THREAD_COUNT);

  // 8. 模拟执行完成：依次处理获得执行权的请求
  // 调用 dec_lock_concurrency 会从等待队列取出下一批请求
  int64_t processed_count = 0;
  int64_t total_sub_req_processed = 0;

  // 用于存储从等待队列中取出的下一批请求的 handle
  std::vector<ObInflightReqHandle> next_handles_storage;
  next_handles_storage.reserve(100);

  // 首先处理初始获得执行权的 handles
  std::vector<int64_t> current_thread_ids;
  {
    std::lock_guard<std::mutex> lock(handles_mutex);
    current_thread_ids = std::move(exec_thread_ids);
  }

  while (!current_thread_ids.empty() || !next_handles_storage.empty() || group_value.wait_queue_.size() > 0) {
    // 处理通过 exec_thread_ids 记录的 handles
    for (int64_t tid : current_thread_ids) {
      ObInflightReqHandle &handle = inflight_handles[tid];
      if (!handle.is_valid()) {
        continue;
      }

      int64_t sub_req_cnt = handle.sub_req_cnt_;
      total_sub_req_processed += sub_req_cnt;
      processed_count++;

      LOG_INFO("Processing batch from thread",
               K(processed_count),
               K(tid),
               K(sub_req_cnt),
               "inflight_req_count", group_value.inflight_req_count_,
               "row_lock_concurrency", group_value.row_lock_concurrency_,
               "wait_queue_size", group_value.wait_queue_.size());

      // 先标记 handle 的 row_lock_concurrency 已处理
      handle.is_row_lock_concurrency_occupied_ = false;

      // 模拟执行完成，释放锁并发度
      ObInflightReqHandle next_handle;
      int ret = group_value.dec_lock_concurrency(next_handle);
      ASSERT_EQ(OB_SUCCESS, ret);

      // 如果返回了 next_handle，说明从等待队列取出了新的批次
      if (next_handle.is_valid()) {
        next_handles_storage.push_back(ObInflightReqHandle());
        next_handles_storage.back().swap(next_handle);
        LOG_INFO("Got next batch from wait queue",
                 "next_sub_req_cnt", next_handles_storage.back().sub_req_cnt_);
      }

      // This unittest builds a standalone ObGroupValue instead of putting it
      // into sql_value.key_map_, so it needs to simulate request completion by
      // releasing inflight_req_count_ directly.
      finish_inflight_req(group_value);

      // 标记 handle 为无效，避免重复处理
      handle.is_valid_ = false;

      LOG_INFO("After dec_inflight_req_count",
               //K(need_remove),
               "inflight_req_count", group_value.inflight_req_count_,
               "row_lock_concurrency", group_value.row_lock_concurrency_);
    }
    current_thread_ids.clear();

    // 处理从等待队列取出的 next_handles
    if (!next_handles_storage.empty()) {
      std::vector<ObInflightReqHandle> current_next_handles;
      current_next_handles.swap(next_handles_storage);

      for (auto &handle : current_next_handles) {
        if (!handle.is_valid()) {
          continue;
        }

        int64_t sub_req_cnt = handle.sub_req_cnt_;
        total_sub_req_processed += sub_req_cnt;
        processed_count++;

        LOG_INFO("Processing batch from next_handles",
                 K(processed_count),
                 K(sub_req_cnt),
                 "inflight_req_count", group_value.inflight_req_count_,
                 "row_lock_concurrency", group_value.row_lock_concurrency_,
                 "wait_queue_size", group_value.wait_queue_.size());

        // 先标记 handle 的 row_lock_concurrency 已处理
        handle.is_row_lock_concurrency_occupied_ = false;

        // 模拟执行完成，释放锁并发度
        ObInflightReqHandle next_handle;
        int ret = group_value.dec_lock_concurrency(next_handle);
        ASSERT_EQ(OB_SUCCESS, ret);

        if (next_handle.is_valid()) {
          next_handles_storage.push_back(ObInflightReqHandle());
          next_handles_storage.back().swap(next_handle);
          LOG_INFO("Got another next batch",
                   "next_sub_req_cnt", next_handles_storage.back().sub_req_cnt_);
        }

        finish_inflight_req(group_value);
        // 标记 handle 为无效
        handle.is_valid_ = false;

        LOG_INFO("After dec_inflight_req_count (next)",
                 //K(need_remove),
                 "inflight_req_count", group_value.inflight_req_count_,
                 "row_lock_concurrency", group_value.row_lock_concurrency_);
      }
    }

    // 检查是否还有等待的请求但没有 handle 可以处理
    if (current_thread_ids.empty() && next_handles_storage.empty() && group_value.wait_queue_.size() > 0) {
      int ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Wait queue not empty but no execution handle",
                K(ret), "wait_queue_size", group_value.wait_queue_.size());
      break;
    }
  }

  LOG_INFO("Phase 2: All batches processed",
           K(processed_count),
           K(total_sub_req_processed),
           "final_inflight_req_count", group_value.inflight_req_count_,
           "final_row_lock_concurrency", group_value.row_lock_concurrency_,
           "final_wait_queue_size", group_value.wait_queue_.size());

  // 9. 最终验证
  ASSERT_EQ(total_sub_req_processed, TEST_THREAD_COUNT)
      << "All 100 sub requests should be processed";
  ASSERT_EQ(group_value.inflight_req_count_, 0)
      << "No inflight requests should remain";
  ASSERT_EQ(group_value.row_lock_concurrency_, 0)
      << "No lock concurrency should remain";
  ASSERT_EQ(0, group_value.wait_queue_.size())
      << "Wait queue should be empty";

  // 10. 清理 - 释放子请求（不通过 aggregator 因为没有真正加入聚合器）
  for (auto sub_req : sub_reqs) {
    if (sub_req != nullptr) {
      aggregator_.free_sub_request(sub_req);
    }
  }

  LOG_INFO("test_group_value_add_request_100_threads completed successfully");
}

/**
 * 测试 ObGroupValue 的并发 dec_lock_concurrency
 *
 * 场景：
 * 先添加50个请求，其中首个请求和满批次等待请求获得执行权，其余进入等待队列
 * 然后并发调用 dec_lock_concurrency
 */
TEST_F(TestGroupCommitAggregatorConcurrent, test_group_value_concurrent_dec_lock_concurrency)
{
  const int64_t TEST_REQUEST_COUNT = 50;
  auto finish_inflight_req = [](ObGroupValue &group_value) {
    ObSpinLockGuard guard(group_value.lock_);
    ASSERT_GT(group_value.inflight_req_count_, 0);
    group_value.inflight_req_count_--;
  };

  // 1. 构建 ObGroupSqlKey 和 ObGroupSqlValue
  ObGroupSqlKey sql_key;
  build_group_sql_key(sql_key);

  ObGroupSqlValue sql_value;
  ASSERT_EQ(OB_SUCCESS, sql_value.init(sql_key, &aggregator_));

  // 2. 构建 ObGroupKey 和 ObGroupValue
  ObObjParam group_key_param;
  group_key_param.set_int(TEST_KEY_PARAM_VALUE);
  ObGroupKey group_key;
  group_key.shallow_copy(&group_key_param, 1);

  ObGroupValue group_value;
  ASSERT_EQ(OB_SUCCESS, group_value.init(&sql_value, group_key));

  // 设置聚合参数：批量大小为10
  group_value.agg_batch_size_ = 10;

  LOG_INFO("Starting test_group_value_concurrent_dec_lock_concurrency",
           K(TEST_REQUEST_COUNT));

  // 3. 创建50个子请求和对应的 handles
  std::vector<ObGroupCommitSubRequest*> sub_reqs(TEST_REQUEST_COUNT);
  std::vector<ObInflightReqHandle> handles(TEST_REQUEST_COUNT);

  for (int64_t i = 0; i < TEST_REQUEST_COUNT; ++i) {
    sub_reqs[i] = create_sub_request(i + 1, i * 2, TEST_KEY_PARAM_VALUE, i);
    ASSERT_NE(nullptr, sub_reqs[i]);
  }

  // 4. 串行添加请求，收集获得执行权的 handle 索引
  std::vector<int64_t> exec_indices;
  int64_t wait_count = 0;

  for (int64_t i = 0; i < TEST_REQUEST_COUNT; ++i) {
    group_value.add_request(*sub_reqs[i], handles[i], false);

    if (handles[i].is_valid()) {
      exec_indices.push_back(i);
      LOG_INFO("Request got execution right", K(i), "sub_req_cnt", handles[i].sub_req_cnt_);
    } else {
      wait_count++;
    }
  }

  LOG_INFO("After adding all requests",
           "exec_count", exec_indices.size(),
           K(wait_count),
           "inflight_req_count", group_value.inflight_req_count_,
           "row_lock_concurrency", group_value.row_lock_concurrency_,
           "wait_queue_size", group_value.wait_queue_.size());

  // 5. 并发调用 dec_lock_concurrency
  std::atomic<bool> start_flag(false);
  std::atomic<int64_t> dec_count(0);
  std::atomic<int64_t> next_batch_count(0);
  std::mutex next_handles_mutex;
  std::vector<ObInflightReqHandle> next_handles_storage;
  next_handles_storage.reserve(100);

  auto dec_func = [&](int64_t exec_idx) {
    while (!start_flag.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }

    ObTenantSwitchGuard guard(&tenant_base_);

    int64_t handle_idx = exec_indices[exec_idx];
    ObInflightReqHandle &handle = handles[handle_idx];

    if (handle.is_valid()) {
      // 先标记 row_lock_concurrency 已处理
      handle.is_row_lock_concurrency_occupied_ = false;

      ObInflightReqHandle next_handle;
      int ret = group_value.dec_lock_concurrency(next_handle);
      if (OB_SUCCESS == ret) {
        dec_count.fetch_add(1);

        if (next_handle.is_valid()) {
          next_batch_count.fetch_add(1);
          const int64_t next_sub_req_cnt = next_handle.sub_req_cnt_;
          {
            std::lock_guard<std::mutex> lock(next_handles_mutex);
            next_handles_storage.push_back(ObInflightReqHandle());
            next_handles_storage.back().swap(next_handle);
          }
          LOG_INFO("Got next batch in concurrent dec",
                   K(exec_idx), K(handle_idx),
                   K(next_sub_req_cnt));
        }
      }
    }
  };

  // 创建线程
  std::vector<std::thread> dec_threads;
  for (size_t i = 0; i < exec_indices.size(); ++i) {
    dec_threads.emplace_back(dec_func, i);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  start_flag.store(true, std::memory_order_release);

  for (auto &t : dec_threads) {
    t.join();
  }

  LOG_INFO("After concurrent dec_lock_concurrency",
           "dec_count", dec_count.load(),
           "next_batch_count", next_batch_count.load(),
           "row_lock_concurrency", group_value.row_lock_concurrency_,
           "wait_queue_size", group_value.wait_queue_.size());

  // 6. 统计处理的请求数
  int64_t total_sub_req_processed = 0;

  // 统计初始获得执行权的 handles
  for (int64_t idx : exec_indices) {
    total_sub_req_processed += handles[idx].sub_req_cnt_;
    finish_inflight_req(group_value);
    handles[idx].is_valid_ = false;
  }

  // 处理从等待队列取出的 next_handles
  while (!next_handles_storage.empty()) {
    std::vector<ObInflightReqHandle> current;
    current.swap(next_handles_storage);

    for (auto &handle : current) {
      if (handle.is_valid()) {
        total_sub_req_processed += handle.sub_req_cnt_;

        // 标记 row_lock_concurrency 已处理
        handle.is_row_lock_concurrency_occupied_ = false;

        ObInflightReqHandle next;
        group_value.dec_lock_concurrency(next);

        finish_inflight_req(group_value);
        handle.is_valid_ = false;

        if (next.is_valid()) {
          next_handles_storage.push_back(ObInflightReqHandle());
          next_handles_storage.back().swap(next);
        }
      }
    }
  }

  LOG_INFO("test_group_value_concurrent_dec_lock_concurrency completed",
           K(total_sub_req_processed),
           "final_inflight_req_count", group_value.inflight_req_count_,
           "final_row_lock_concurrency", group_value.row_lock_concurrency_,
           "final_wait_queue_size", group_value.wait_queue_.size());

  // 验证
  ASSERT_EQ(total_sub_req_processed, TEST_REQUEST_COUNT)
      << "All 50 requests should be processed";
  ASSERT_EQ(group_value.inflight_req_count_, 0);
  ASSERT_EQ(group_value.row_lock_concurrency_, 0);
  ASSERT_EQ(0, group_value.wait_queue_.size());

  // 清理
  for (auto sub_req : sub_reqs) {
    if (sub_req != nullptr) {
      aggregator_.free_sub_request(sub_req);
    }
  }
}

} // namespace test
} // namespace oceanbase
#endif

int main(int argc, char **argv)
{
#ifdef OB_HOTSPOT_GROUP_COMMIT
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
#endif
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
