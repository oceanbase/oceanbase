/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#define USING_LOG_PREFIX STORAGE
#include "lib/container/ob_se_array.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/utility.h"
#include "share/ob_errno.h"
#include "storage/high_availability/ob_migration_sliding_window_controller.h"
#include "storage/high_availability/ob_migration_tenant_window_mgr.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;

namespace oceanbase {
namespace unittest {

class TestWindowSlidCallback : public ObIMigrationWindowSlidCallback {
public:
  std::mutex mu_;
  std::vector<int64_t> granted_start_seqs_;
  std::vector<int64_t> granted_slot_counts_;

  void on_window_slid(const int64_t granted_start_seq,
                      const int64_t granted_slot_count) override {
    std::lock_guard<std::mutex> guard(mu_);
    granted_start_seqs_.push_back(granted_start_seq);
    granted_slot_counts_.push_back(granted_slot_count);
  }
};

static int init_test_window_mgr(ObMigrationTenantWindowMgr &mgr,
                                const int64_t max_slots,
                                const int64_t slot_buf_size) {
  return mgr.init(OB_SERVER_TENANT_ID, max_slots, slot_buf_size, max_slots);
}

// Mini in-test "fetch driver" mirroring ObVecIdxSegFetchDriver's contract on
// the dest side: under the new fill_data contract (non-blocking, seq must be
// in window), callers MUST only fill seqs that the dest controller has
// granted. Production code learns this via on_window_slid; tests learn it the
// same way. Workers pop seqs from this queue instead of choosing seqs
// themselves, so fill_data is always called for in-window seqs.
//
// seed_initial() is needed because the controller's init() grants the first
// window without firing on_window_slid (callback only fires on later slides).
class GrantDispatchQueue : public ObIMigrationWindowSlidCallback {
public:
  GrantDispatchQueue() : closed_(false) {}

  void seed_initial(const int64_t start, const int64_t count) {
    push_range_(start, count);
  }

  void on_window_slid(const int64_t granted_start_seq,
                      const int64_t granted_slot_count) override {
    if (granted_slot_count > 0) {
      push_range_(granted_start_seq, granted_slot_count);
    }
  }

  // Block until a seq is available, or the queue is closed and empty.
  // Returns true with out_seq populated, or false on close.
  bool pop_seq(int64_t &out_seq) {
    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait(lk, [this] { return !queue_.empty() || closed_; });
    if (queue_.empty()) {
      return false;
    }
    out_seq = queue_.front();
    queue_.pop_front();
    return true;
  }

  void close() {
    std::lock_guard<std::mutex> lk(mu_);
    closed_ = true;
    cv_.notify_all();
  }

private:
  void push_range_(const int64_t start, const int64_t count) {
    std::lock_guard<std::mutex> lk(mu_);
    for (int64_t i = 0; i < count; ++i) {
      queue_.push_back(start + i);
    }
    cv_.notify_all();
  }

  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<int64_t> queue_;
  bool closed_;
};

// Source-side controller is non-blocking only (try_get_data returns OB_EAGAIN
// when data is not yet produced). Some tests still want a blocking "wait until
// data shows up" semantic for ergonomics — this local helper gives them that
// by polling try_get_data with a deadline.
static int source_wait_get_data(
    ObMigrationSlidingWindowSourceController &ctrl,
    const int64_t seq_idx, char *buf, const int64_t buf_len,
    int64_t &data_len, const int64_t wait_timeout_us) {
  const int64_t deadline = ObTimeUtility::current_time()
      + (0 != wait_timeout_us
          ? wait_timeout_us
          : ObMigrationSlidingWindowController::DEFAULT_OP_TIMEOUT_US);
  int ret = OB_EAGAIN;
  while (OB_EAGAIN == (ret = ctrl.try_get_data(seq_idx, buf, buf_len, data_len))) {
    if (ObTimeUtility::current_time() >= deadline) {
      return OB_TIMEOUT;
    }
    ob_usleep(static_cast<uint32_t>(
        ObMigrationSlidingWindowController::SLOT_POLL_INTERVAL_US));
  }
  return ret;
}

// Dest-side: fill only in-window seq then consume (slides head, grants next slot).
// Matches production: RPC fill happens per granted seq, not batch pre-fill.
static int dest_fill_and_consume(
    ObMigrationSlidingWindowDestController &ctrl,
    const int64_t seq_idx, const char *data, const int64_t data_len,
    char *out_buf, const int64_t out_buf_len, int64_t &out_data_len,
    const int64_t wait_timeout_us = 0) {
  int ret = ctrl.fill_data(seq_idx, data, data_len);
  if (OB_SUCC(ret)) {
    ret = ctrl.get_next_consume_data(out_buf, out_buf_len, out_data_len, wait_timeout_us);
  }
  return ret;
}

// =================================================================================
// [Dest basic test] Simulate normal workflow in single-threaded mode
// =================================================================================
TEST(TestMigrationSlidingWindowDest, dest_basic_fill_get_slide) {
  TestWindowSlidCallback cb_ctx;
  const int64_t win = 3;
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 6, 32));

  ObMigrationSlidingWindowDestHandle dest_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowDestController::create(
      &wmgr, &cb_ctx, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dest_handle));

  char buf[64] = {0};
  int64_t len = 0;
  ASSERT_EQ(OB_SUCCESS, dest_fill_and_consume(*dest_handle, 0, "A", 1, buf, sizeof(buf), len));
  ASSERT_EQ(1, len);
  ASSERT_EQ('A', buf[0]);

  {
    std::lock_guard<std::mutex> guard(cb_ctx.mu_);
    ASSERT_GE(cb_ctx.granted_slot_counts_.size(), 1U);
    ASSERT_GT(cb_ctx.granted_slot_counts_[0], 0);
  }

  ASSERT_EQ(OB_SUCCESS, dest_fill_and_consume(*dest_handle, 1, "B", 1, buf, sizeof(buf), len));
  ASSERT_EQ(1, len);
  ASSERT_EQ('B', buf[0]);

  // seq 2 is in window after consuming seq 1; fill but do not consume yet.
  ASSERT_EQ(OB_SUCCESS, dest_handle->fill_data(2, "C", 1));

  int64_t head = 0, wsz = 0;
  ASSERT_EQ(OB_SUCCESS, dest_handle->get_runtime_snapshot(head, wsz));
  ASSERT_EQ(2, head);

  dest_handle.reset();
}

// =================================================================================
// [Source basic test] Simulate DAG slot filling and out-of-order RPC completion with batch sliding
// =================================================================================
TEST(TestMigrationSlidingWindowSource, source_complete_out_of_order) {
  TestWindowSlidCallback cb_ctx;
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 6, 16));

  ObMigrationSlidingWindowSourceHandle src_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowSourceController::create(
      &wmgr, &cb_ctx, ObDagPrio::DAG_PRIO_VECTOR_INDEX, src_handle));

  // 1. DAG fills data in order (Thread B)
  ASSERT_EQ(OB_SUCCESS, src_handle->generate_next_data("0", 1)); // seq 0
  ASSERT_EQ(OB_SUCCESS, src_handle->generate_next_data("1", 1)); // seq 1

  char buf[64] = {0};
  int64_t len = 0;

  // 2. RPC thread fetches and consumes out of order (Thread A)
  ASSERT_EQ(OB_SUCCESS, source_wait_get_data(*src_handle, 1, buf, sizeof(buf), len, 0));

  {
    std::lock_guard<std::mutex> guard(cb_ctx.mu_);
    ASSERT_TRUE(cb_ctx.granted_slot_counts_.empty());
  }

  // 3. Head completes
  ASSERT_EQ(OB_SUCCESS, source_wait_get_data(*src_handle, 0, buf, sizeof(buf), len, 0));

  // Source side intentionally does NOT fire on_window_slid after a slide:
  // window growth on source is pull-based via acquire_window_slot_ inside
  // generate_next_data, so the slide-head path skips the refill+callback.
  {
    std::lock_guard<std::mutex> guard(cb_ctx.mu_);
    ASSERT_TRUE(cb_ctx.granted_slot_counts_.empty());
  }

  src_handle.reset();
}

// =================================================================================
// [Dest concurrency stress test] Thread D (N writers, out-of-order) vs Thread C (1 reader, strict order)
// =================================================================================
TEST(TestMigrationSlidingWindowDest, concurrent_network_io_and_dag) {
  const int64_t win = 16;
  const int64_t total_tasks = 1000;
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 32, 16));

  // Workers pull seqs from this queue (mirroring driver behavior); only seqs
  // granted by the dest controller appear, so fill_data is always in-window.
  GrantDispatchQueue dispatch;
  ObMigrationSlidingWindowDestHandle dest_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowDestController::create(
      &wmgr, &dispatch, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dest_handle));

  // init() does not fire on_window_slid for the first batch of slots; seed it.
  int64_t init_head = 0;
  int64_t init_window = 0;
  ASSERT_EQ(OB_SUCCESS, dest_handle->get_runtime_snapshot(init_head, init_window));
  dispatch.seed_initial(init_head, init_window);

  std::atomic<int> fill_err{OB_SUCCESS};
  std::atomic<int> consume_err{OB_SUCCESS};
  std::atomic<int64_t> highest_consume_seq{-1};

  auto net_io_thread = [&]() {
    int64_t seq = 0;
    while (dispatch.pop_seq(seq)) {
      if (seq >= total_tasks) {
        // dest may keep granting beyond the test's logical end; ignore.
        continue;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(rand() % 100));
      std::string payload = std::to_string(seq);
      const int ret = dest_handle->fill_data(seq, payload.c_str(), payload.size());
      if (OB_SUCCESS != ret) {
        fill_err.store(ret);
        return;
      }
    }
  };

  auto dag_consumer_thread = [&]() {
    for (int seq = 0; seq < total_tasks; ++seq) {
      char buf[64] = {0};
      int64_t len = 0;
      const int ret = dest_handle->get_next_consume_data(buf, sizeof(buf), len, 5000000);
      if (OB_SUCCESS != ret) {
        consume_err.store(ret);
        return;
      }

      std::string read_payload(buf, len);
      if (read_payload != std::to_string(seq)) {
        consume_err.store(OB_ERR_UNEXPECTED);
        return;
      }

      highest_consume_seq.store(seq);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < 8; ++i) {
    threads.emplace_back(net_io_thread);
  }
  std::thread consumer(dag_consumer_thread);

  consumer.join();
  // All required seqs are filled by the time the consumer finishes; remaining
  // dispatch entries (if any) are seqs >= total_tasks and can be discarded.
  dispatch.close();
  for (std::thread &t : threads) {
    t.join();
  }

  EXPECT_EQ(OB_SUCCESS, fill_err.load());
  EXPECT_EQ(OB_SUCCESS, consume_err.load());
  EXPECT_EQ(total_tasks - 1, highest_consume_seq.load());

  dest_handle.reset();
}

// =================================================================================
// [Source concurrency stress test] Thread B (1 writer, sequential) vs Thread A (N readers, out-of-order)
// =================================================================================
TEST(TestMigrationSlidingWindowSource, concurrent_dag_gen_and_rpc_ack) {
  const int64_t win = 16;
  const int64_t total_tasks = 1000;
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 32, 16));

  ObMigrationSlidingWindowSourceHandle src_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowSourceController::create(
      &wmgr, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, src_handle));

  std::atomic<int> gen_err{OB_SUCCESS};
  std::atomic<int> rpc_err{OB_SUCCESS};

  auto dag_producer_thread = [&]() {
    for (int seq = 0; seq < total_tasks; ++seq) {
      std::string payload = std::to_string(seq);
      int ret = src_handle->generate_next_data(payload.c_str(), payload.size(), 5000000);
      if (OB_SUCCESS != ret) {
        gen_err.store(ret);
        return;
      }
    }
  };

  auto rpc_network_thread = [&](int thread_idx) {
    const int num_rpc_threads = 8;
    for (int seq = thread_idx; seq < total_tasks; seq += num_rpc_threads) {
      char buf[64] = {0};
      int64_t len = 0;

      int ret = source_wait_get_data(*src_handle, seq, buf, sizeof(buf), len, 5000000);
      if (OB_SUCCESS != ret) {
        LOG_WARN("source_wait_get_data failed", K(ret), K(seq));
        rpc_err.store(ret);
        return;
      }

      std::string read_payload(buf, len);
      if (read_payload != std::to_string(seq)) {
        rpc_err.store(OB_ERR_UNEXPECTED);
        return;
      }

      std::this_thread::sleep_for(std::chrono::microseconds(rand() % 100));
    }
  };

  std::thread producer(dag_producer_thread);
  std::vector<std::thread> rpc_threads;
  for (int i = 0; i < 8; ++i) {
    rpc_threads.emplace_back(rpc_network_thread, i);
  }

  producer.join();
  for (auto &t : rpc_threads)
    t.join();

  EXPECT_EQ(OB_SUCCESS, gen_err.load());
  EXPECT_EQ(OB_SUCCESS, rpc_err.load());

  int64_t h = 0, wsz = 0;
  ASSERT_EQ(OB_SUCCESS, src_handle->get_runtime_snapshot(h, wsz));
  ASSERT_EQ(total_tasks, h);

  src_handle.reset();
}

// =================================================================================
// [Full dual-side integration test] Source(ThreadA+B) linked with Dest(ThreadC+D) end-to-end pipeline stress test
// =================================================================================
TEST(TestMigrationSlidingWindowIntegration, full_pipeline_source_to_dest) {
  const int total_tasks = 2000;
  const int win = 16;
  const int num_network_bridges = 12;

  ObMigrationTenantWindowMgr mgr_src;
  ObMigrationTenantWindowMgr mgr_dst;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(mgr_src, 64, 32));
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(mgr_dst, 64, 32));

  GrantDispatchQueue dispatch;
  ObMigrationSlidingWindowSourceHandle src_handle;
  ObMigrationSlidingWindowDestHandle dst_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowSourceController::create(
      &mgr_src, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, src_handle));
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowDestController::create(
      &mgr_dst, &dispatch, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dst_handle));

  int64_t init_head = 0;
  int64_t init_window = 0;
  ASSERT_EQ(OB_SUCCESS, dst_handle->get_runtime_snapshot(init_head, init_window));
  dispatch.seed_initial(init_head, init_window);

  std::atomic<int> global_err{OB_SUCCESS};

  // --- [1] Source Thread B (DAG Writer) ---
  std::thread src_dag_producer([&]() {
    for (int seq = 0; seq < total_tasks; ++seq) {
      if (OB_SUCCESS != global_err.load()) {
        return;
      }
      std::string payload = "BLOCK_" + std::to_string(seq);
      const int r = src_handle->generate_next_data(payload.c_str(), payload.size(), 1000000);
      if (OB_SUCCESS != r) {
        global_err = r;
        return;
      }
    }
  });

  // --- [2] Simulated network layer; bridges only ever fill seqs the dest has
  //         granted (delivered through the dispatcher), satisfying fill_data's
  //         in-window contract.
  auto network_bridge = [&]() {
    int64_t seq = 0;
    while (dispatch.pop_seq(seq)) {
      if (OB_SUCCESS != global_err.load()) {
        return;
      }
      if (seq >= total_tasks) {
        continue;
      }
      char buf[64] = {0};
      int64_t len = 0;
      int r = source_wait_get_data(*src_handle, seq, buf, sizeof(buf), len, 10000000);
      if (OB_SUCCESS != r) {
        global_err = r;
        return;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(rand() % 100));
      r = dst_handle->fill_data(seq, buf, len);
      if (OB_SUCCESS != r) {
        global_err = r;
        return;
      }
    }
  };

  std::vector<std::thread> network_threads;
  for (int i = 0; i < num_network_bridges; ++i) {
    network_threads.emplace_back(network_bridge);
  }

  // --- [3] Dest Thread C (DAG Consumer) ---
  std::thread dst_dag_consumer([&]() {
    for (int seq = 0; seq < total_tasks; ++seq) {
      if (OB_SUCCESS != global_err.load()) {
        return;
      }
      char buf[64] = {0};
      int64_t len = 0;
      const int r = dst_handle->get_next_consume_data(buf, sizeof(buf), len, 10000000);
      if (OB_SUCCESS != r) {
        global_err = r;
        return;
      }
      std::string payload(buf, len);
      if (payload != "BLOCK_" + std::to_string(seq)) {
        global_err = OB_ERR_UNEXPECTED;
        return;
      }
    }
  });

  // --- Wait for completion ---
  src_dag_producer.join();
  dst_dag_consumer.join();
  // After the consumer is done, every seq < total_tasks has been filled.
  // Closing the dispatcher releases bridges still parked on pop_seq().
  dispatch.close();
  for (std::thread &t : network_threads) {
    t.join();
  }

  EXPECT_EQ(OB_SUCCESS, global_err.load());

  int64_t h, w;
  ASSERT_EQ(OB_SUCCESS, src_handle->get_runtime_snapshot(h, w));
  ASSERT_EQ(total_tasks, h);
  ASSERT_EQ(OB_SUCCESS, dst_handle->get_runtime_snapshot(h, w));
  ASSERT_EQ(total_tasks, h);

  src_handle.reset();
  dst_handle.reset();
  mgr_src.destroy();
  mgr_dst.destroy();
}

// =================================================================================
// [Extreme chaos test] Simulate very high concurrency, severe out-of-order, variable-length packets, extreme network jitter
// =================================================================================
TEST(TestMigrationSlidingWindowIntegration, extreme_chaos_pipeline) {
  const int64_t total_tasks = 50000;
  const int64_t win = 16;
  const int num_rpc_workers = 32;
  const int64_t slot_buf_size = 512;

  ObMigrationTenantWindowMgr mgr_src;
  ObMigrationTenantWindowMgr mgr_dst;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(mgr_src, 128, slot_buf_size));
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(mgr_dst, 128, slot_buf_size));

  GrantDispatchQueue dispatch;
  ObMigrationSlidingWindowSourceHandle src_handle;
  ObMigrationSlidingWindowDestHandle dst_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowSourceController::create(
      &mgr_src, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, src_handle));
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowDestController::create(
      &mgr_dst, &dispatch, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dst_handle));

  int64_t init_head = 0;
  int64_t init_window = 0;
  ASSERT_EQ(OB_SUCCESS, dst_handle->get_runtime_snapshot(init_head, init_window));
  dispatch.seed_initial(init_head, init_window);

  std::atomic<int> global_err{OB_SUCCESS};

  auto make_payload = [](int seq) {
    std::string s = "SEQ_" + std::to_string(seq) + "_DATA_";
    int extra_len = (seq * 31) % 256;
    s.append(extra_len, static_cast<char>('A' + (seq % 26)));
    return s;
  };

  // --- [1] Source Thread B (DAG Writer) ---
  std::thread src_dag_producer([&]() {
    for (int seq = 0; seq < total_tasks; ++seq) {
      if (OB_SUCCESS != global_err.load()) {
        return;
      }
      std::string payload = make_payload(seq);
      int r;
      while ((r = src_handle->generate_next_data(payload.c_str(), payload.size(), 1000)) == OB_TIMEOUT) {
        if (OB_SUCCESS != global_err.load()) {
          return;
        }
        std::this_thread::yield();
      }
      if (OB_SUCCESS != r) {
        global_err = r;
        return;
      }
    }
  });

  // --- [2] Simulated chaos network layer; workers only fill seqs the dest has
  //         granted (via the dispatcher), respecting fill_data's contract.
  auto chaos_network_worker = [&]() {
    std::mt19937 gen(std::hash<std::thread::id>{}(std::this_thread::get_id()));
    std::uniform_int_distribution<> delay_us(0, 2000);

    int64_t seq = 0;
    while (dispatch.pop_seq(seq)) {
      if (OB_SUCCESS != global_err.load()) {
        return;
      }
      if (seq >= total_tasks) {
        continue;
      }

      char buf[512] = {0};
      int64_t len = 0;
      int r;
      while ((r = source_wait_get_data(*src_handle, seq, buf, sizeof(buf), len, 2000)) == OB_TIMEOUT) {
        if (OB_SUCCESS != global_err.load()) {
          return;
        }
      }
      if (OB_SUCCESS != r) {
        global_err = r;
        return;
      }

      std::this_thread::sleep_for(std::chrono::microseconds(delay_us(gen)));

      r = dst_handle->fill_data(seq, buf, len);
      if (OB_SUCCESS != r) {
        global_err = r;
        return;
      }
    }
  };

  std::vector<std::thread> network_threads;
  for (int i = 0; i < num_rpc_workers; ++i) {
    network_threads.emplace_back(chaos_network_worker);
  }

  // --- [3] Dest Thread C (DAG Consumer) ---
  std::thread dst_dag_consumer([&]() {
    for (int seq = 0; seq < total_tasks; ++seq) {
      if (OB_SUCCESS != global_err.load()) {
        return;
      }

      char buf[512] = {0};
      int64_t len = 0;
      int r;
      while ((r = dst_handle->get_next_consume_data(buf, sizeof(buf), len, 1500)) == OB_TIMEOUT) {
        if (OB_SUCCESS != global_err.load()) {
          return;
        }
      }
      if (OB_SUCCESS != r) {
        global_err = r;
        return;
      }

      std::string payload(buf, len);
      std::string expected_payload = make_payload(seq);
      if (payload != expected_payload) {
        global_err = OB_ERR_UNEXPECTED;
        return;
      }
    }
  });

  // --- Wait for full pipeline to finish ---
  src_dag_producer.join();
  dst_dag_consumer.join();
  dispatch.close();
  for (std::thread &t : network_threads) {
    t.join();
  }

  EXPECT_EQ(OB_SUCCESS, global_err.load());

  int64_t final_h, final_w;
  ASSERT_EQ(OB_SUCCESS, src_handle->get_runtime_snapshot(final_h, final_w));
  EXPECT_EQ(total_tasks, final_h);
  ASSERT_EQ(OB_SUCCESS, dst_handle->get_runtime_snapshot(final_h, final_w));
  EXPECT_EQ(total_tasks, final_h);

  src_handle.reset();
  dst_handle.reset();
  mgr_src.destroy();
  mgr_dst.destroy();
}

// =================================================================================
// Single-slot tenant pool: max_window=1, init gets only 1 mgr slot, advances one-by-one via sliding, full pipeline must complete
// =================================================================================
TEST(TestMigrationSlidingWindowIntegration, single_slot_pool_serial_pipeline) {
  const int64_t total_tasks = 400;
  const int64_t win = 8;
  const int64_t mgr_slots = 1;
  const int num_network_bridges = 4;
  const int64_t slot_buf_size = 64;

  ObMigrationTenantWindowMgr mgr_src;
  ObMigrationTenantWindowMgr mgr_dst;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(mgr_src, mgr_slots, slot_buf_size));
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(mgr_dst, mgr_slots, slot_buf_size));

  GrantDispatchQueue dispatch;
  ObMigrationSlidingWindowSourceHandle src_handle;
  ObMigrationSlidingWindowDestHandle dst_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowSourceController::create(
      &mgr_src, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, src_handle));
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowDestController::create(
      &mgr_dst, &dispatch, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dst_handle));

  int64_t hs = 0;
  int64_t ws = 0;
  ASSERT_EQ(OB_SUCCESS, src_handle->get_runtime_snapshot(hs, ws));
  EXPECT_EQ(1, ws);
  ASSERT_EQ(OB_SUCCESS, dst_handle->get_runtime_snapshot(hs, ws));
  EXPECT_EQ(1, ws);
  dispatch.seed_initial(hs, ws);

  std::atomic<int> global_err{OB_SUCCESS};

  std::thread producer([&]() {
    for (int seq = 0; seq < total_tasks; ++seq) {
      if (OB_SUCCESS != global_err.load()) {
        return;
      }
      std::string payload = "S1_BLK_" + std::to_string(seq);
      int r;
      while ((r = src_handle->generate_next_data(payload.c_str(), payload.size(), 0)) == OB_TIMEOUT) {
        if (OB_SUCCESS != global_err.load()) {
          return;
        }
        std::this_thread::yield();
      }
      if (OB_SUCCESS != r) {
        global_err = r;
        return;
      }
    }
  });

  auto bridge_fn = [&]() {
    int64_t seq = 0;
    while (dispatch.pop_seq(seq)) {
      if (OB_SUCCESS != global_err.load()) {
        return;
      }
      if (seq >= total_tasks) {
        continue;
      }
      char buf[64] = {0};
      int64_t len = 0;
      int r;
      while ((r = source_wait_get_data(*src_handle, seq, buf, sizeof(buf), len, 0)) == OB_TIMEOUT) {
        if (OB_SUCCESS != global_err.load()) {
          return;
        }
      }
      if (OB_SUCCESS != r) {
        global_err = r;
        return;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(rand() % 20));
      r = dst_handle->fill_data(seq, buf, len);
      if (OB_SUCCESS != r) {
        global_err = r;
        return;
      }
    }
  };

  std::vector<std::thread> bridges;
  for (int i = 0; i < num_network_bridges; ++i) {
    bridges.emplace_back(bridge_fn);
  }

  std::thread consumer([&]() {
    for (int seq = 0; seq < total_tasks; ++seq) {
      if (OB_SUCCESS != global_err.load()) {
        return;
      }
      char buf[64] = {0};
      int64_t len = 0;
      int r;
      while ((r = dst_handle->get_next_consume_data(buf, sizeof(buf), len, 0)) == OB_TIMEOUT) {
        if (OB_SUCCESS != global_err.load()) {
          return;
        }
      }
      if (OB_SUCCESS != r) {
        global_err = r;
        return;
      }
      std::string payload(buf, len);
      if (payload != "S1_BLK_" + std::to_string(seq)) {
        global_err = OB_ERR_UNEXPECTED;
        return;
      }
    }
  });

  producer.join();
  consumer.join();
  dispatch.close();
  for (std::thread &t : bridges) {
    t.join();
  }

  EXPECT_EQ(OB_SUCCESS, global_err.load());
  int64_t fh = 0;
  int64_t fw = 0;
  ASSERT_EQ(OB_SUCCESS, src_handle->get_runtime_snapshot(fh, fw));
  EXPECT_EQ(total_tasks, fh);
  ASSERT_EQ(OB_SUCCESS, dst_handle->get_runtime_snapshot(fh, fw));
  EXPECT_EQ(total_tasks, fh);

  src_handle.reset();
  dst_handle.reset();
  mgr_src.destroy();
  mgr_dst.destroy();
}

// =================================================================================
// [Multi-group shared WindowMgr test] Multiple (src+dst) pipelines sharing the same pair of WindowMgrs
// =================================================================================
TEST(TestMigrationSlidingWindowIntegration, multi_pipeline_shared_window_mgr) {
  // Scale chosen so that the shared mgr is contended but not over-subscribed:
  // under the new fill_data contract, dest grants are only attempted on slide,
  // so persistent over-subscription can starve a group out indefinitely. With
  // 4 groups (8 controllers in total) sharing 32 slots and win=8, fair-share
  // and dag_prio quotas leave enough headroom for every group to keep growing.
  const int64_t total_tasks_per_group = 200;
  const int64_t num_groups = 4;
  const int64_t win = 8;
  const int64_t num_network_bridges = 4;
  const int64_t slot_buf_size = 64;
  const int64_t mgr_total_slots = 32;

  ObMigrationTenantWindowMgr mgr_src;
  ObMigrationTenantWindowMgr mgr_dst;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(mgr_src, mgr_total_slots, slot_buf_size));
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(mgr_dst, mgr_total_slots, slot_buf_size));

  ObMigrationSlidingWindowSourceHandle src_handles[num_groups];
  ObMigrationSlidingWindowDestHandle dst_handles[num_groups];
  // Per-group dispatcher; each is bound to its own dst controller as the
  // on_window_slid callback.
  std::vector<std::unique_ptr<GrantDispatchQueue>> dispatchers;
  for (int64_t g = 0; g < num_groups; ++g) {
    dispatchers.emplace_back(new GrantDispatchQueue());
  }

  std::atomic<int> global_err{OB_SUCCESS};

  auto make_payload = [](int group, int seq) {
    return "G" + std::to_string(group) + "_BLK_" + std::to_string(seq);
  };

  auto run_pipeline = [&](int group_id) {
    ObMigrationSlidingWindowSourceHandle &src_h = src_handles[group_id];
    ObMigrationSlidingWindowDestHandle &dst_h = dst_handles[group_id];
    GrantDispatchQueue &dispatch = *dispatchers[group_id];

    {
      int r = ObMigrationSlidingWindowSourceController::create(
          &mgr_src, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, src_h);
      if (OB_SUCCESS != r) {
        global_err = r;
        return;
      }
      r = ObMigrationSlidingWindowDestController::create(
          &mgr_dst, &dispatch, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dst_h);
      if (OB_SUCCESS != r) {
        global_err = r;
        return;
      }
      src_h->set_total_task_count(total_tasks_per_group);
      dst_h->set_total_task_count(total_tasks_per_group);

      int64_t init_head = 0;
      int64_t init_window = 0;
      r = dst_h->get_runtime_snapshot(init_head, init_window);
      if (OB_SUCCESS != r) {
        global_err = r;
        return;
      }
      dispatch.seed_initial(init_head, init_window);
    }

    // [1] Source DAG producer
    std::thread producer([&, group_id]() {
      for (int seq = 0; seq < total_tasks_per_group; ++seq) {
        if (OB_SUCCESS != global_err.load()) {
          return;
        }
        std::string payload = make_payload(group_id, seq);
        int r;
        while ((r = src_h->generate_next_data(payload.c_str(), payload.size(), 1000)) == OB_TIMEOUT) {
          if (OB_SUCCESS != global_err.load()) {
            return;
          }
          std::this_thread::yield();
        }
        if (OB_SUCCESS != r) {
          global_err = r;
          return;
        }
      }
    });

    // [2] Network bridge threads — pull seqs from this group's dispatcher.
    auto bridge_fn = [&]() {
      int64_t seq = 0;
      while (dispatch.pop_seq(seq)) {
        if (OB_SUCCESS != global_err.load()) {
          return;
        }
        if (seq >= total_tasks_per_group) {
          continue;
        }
        char buf[64] = {0};
        int64_t len = 0;
        int r;
        while ((r = source_wait_get_data(*src_h, seq, buf, sizeof(buf), len, 1000)) == OB_TIMEOUT) {
          if (OB_SUCCESS != global_err.load()) {
            return;
          }
        }
        if (OB_SUCCESS != r) {
          global_err = r;
          return;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(rand() % 50));
        r = dst_h->fill_data(seq, buf, len);
        if (OB_SUCCESS != r) {
          global_err = r;
          return;
        }
      }
    };

    std::vector<std::thread> bridges;
    for (int i = 0; i < num_network_bridges; ++i) {
      bridges.emplace_back(bridge_fn);
    }

    // [3] Dest DAG consumer
    std::thread consumer([&, group_id]() {
      for (int seq = 0; seq < total_tasks_per_group; ++seq) {
        if (OB_SUCCESS != global_err.load()) {
          return;
        }
        char buf[64] = {0};
        int64_t len = 0;
        int r;
        while ((r = dst_h->get_next_consume_data(buf, sizeof(buf), len, 1000)) == OB_TIMEOUT) {
          if (OB_SUCCESS != global_err.load()) {
            return;
          }
        }
        if (OB_SUCCESS != r) {
          global_err = r;
          return;
        }
        std::string payload(buf, len);
        std::string expected = make_payload(group_id, seq);
        if (payload != expected) {
          global_err = OB_ERR_UNEXPECTED;
          return;
        }
      }
    });

    producer.join();
    consumer.join();
    dispatch.close();
    for (std::thread &t : bridges) {
      t.join();
    }

    {
      int64_t h, w;
      ASSERT_EQ(OB_SUCCESS, src_h->get_runtime_snapshot(h, w));
      EXPECT_EQ(total_tasks_per_group, h)
          << "src group " << group_id << " head_seq mismatch (in pipeline)";
      ASSERT_EQ(OB_SUCCESS, dst_h->get_runtime_snapshot(h, w));
      EXPECT_EQ(total_tasks_per_group, h)
          << "dst group " << group_id << " head_seq mismatch (in pipeline)";
    }

    src_h.reset();
    dst_h.reset();
  };

  std::vector<std::thread> group_threads;
  for (int64_t g = 0; g < num_groups; ++g) {
    group_threads.emplace_back(run_pipeline, static_cast<int>(g));
  }
  for (auto &t : group_threads)
    t.join();

  EXPECT_EQ(OB_SUCCESS, global_err.load());

  // reset is idempotent, repeated calls are safe
  for (int64_t g = 0; g < num_groups; ++g) {
    src_handles[g].reset();
    dst_handles[g].reset();
  }
  mgr_src.destroy();
  mgr_dst.destroy();
}

// =================================================================================
// [Init error handling] Invalid arguments -> OB_INVALID_ARGUMENT
// =================================================================================
TEST(TestMigrationSlidingWindowErrorHandling, init_invalid_arguments) {
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 8, 32));

  // null window_mgr
  {
    ObMigrationSlidingWindowDestHandle dest_handle;
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObMigrationSlidingWindowDestController::create(
        nullptr, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dest_handle));
  }

  wmgr.destroy();
}

// =================================================================================
// [Init error handling] Duplicate initialization -> OB_INIT_TWICE
// =================================================================================
TEST(TestMigrationSlidingWindowErrorHandling, init_twice) {
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 8, 32));

  ObMigrationSlidingWindowDestHandle dest_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowDestController::create(
      &wmgr, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dest_handle));
  // ctrl->init() called again directly should return OB_INIT_TWICE
  EXPECT_EQ(OB_INIT_TWICE, dest_handle->init(&wmgr, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX));

  dest_handle.reset();
  wmgr.destroy();
}

// =================================================================================
// [Not initialized] Operations called before init -> OB_NOT_INIT
// =================================================================================
TEST(TestMigrationSlidingWindowErrorHandling, operations_before_init) {
  // Dest controller
  {
    ObMigrationSlidingWindowDestHandle dest_handle;
    int64_t h = 0, w = 0;
    EXPECT_FALSE(dest_handle.is_valid());
  }
  // Source controller
  {
    ObMigrationSlidingWindowSourceHandle src_handle;
    EXPECT_FALSE(src_handle.is_valid());
  }
}

// =================================================================================
// [Destroy idempotency] Multiple handle.reset() calls should not crash
// =================================================================================
TEST(TestMigrationSlidingWindowErrorHandling, destroy_idempotent) {
  // reset without init (no-op)
  {
    ObMigrationSlidingWindowDestHandle dest_handle;
    dest_handle.reset();
    dest_handle.reset();
  }
  // reset after init, called twice
  {
    ObMigrationTenantWindowMgr wmgr;
    ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 8, 32));

    ObMigrationSlidingWindowDestHandle dest_handle;
    ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowDestController::create(
        &wmgr, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dest_handle));
    dest_handle.reset();
    dest_handle.reset(); // second reset should be safe no-op

    wmgr.destroy();
  }
  // source controller same pattern
  {
    ObMigrationTenantWindowMgr wmgr;
    ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 8, 32));

    ObMigrationSlidingWindowSourceHandle src_handle;
    ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowSourceController::create(
        &wmgr, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, src_handle));
    src_handle.reset();
    src_handle.reset();

    wmgr.destroy();
  }
}

// =================================================================================
// [is_inited lifecycle] false before init, true after init, false after reset
// =================================================================================
TEST(TestMigrationSlidingWindowErrorHandling, is_inited_lifecycle) {
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 8, 32));

  ObMigrationSlidingWindowDestHandle dest_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowDestController::create(
      &wmgr, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dest_handle));
  EXPECT_TRUE(dest_handle->is_inited());

  dest_handle.reset();
  // After reset the controller is destroyed, is_inited() not accessible
  // (handle no longer valid)
  EXPECT_FALSE(dest_handle.is_valid());

  wmgr.destroy();
}

// =================================================================================
// [set_total_task_count + OB_ITER_END] Returns ITER_END after consuming all tasks
// =================================================================================
TEST(TestMigrationSlidingWindowDest, total_task_count_iter_end) {
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 8, 32));

  ObMigrationSlidingWindowDestHandle dest_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowDestController::create(
      &wmgr, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dest_handle));

  const int64_t total = 3;
  dest_handle->set_total_task_count(total);

  char buf[32] = {0};
  int64_t len = 0;
  ASSERT_EQ(OB_SUCCESS, dest_fill_and_consume(*dest_handle, 0, "X", 1, buf, sizeof(buf), len));
  ASSERT_EQ('X', buf[0]);
  ASSERT_EQ(OB_SUCCESS, dest_fill_and_consume(*dest_handle, 1, "Y", 1, buf, sizeof(buf), len));
  ASSERT_EQ('Y', buf[0]);
  ASSERT_EQ(OB_SUCCESS, dest_fill_and_consume(*dest_handle, 2, "Z", 1, buf, sizeof(buf), len));
  ASSERT_EQ('Z', buf[0]);

  EXPECT_EQ(OB_ITER_END, dest_handle->get_next_consume_data(buf, sizeof(buf), len, 1000));

  dest_handle.reset();
  wmgr.destroy();
}

// =================================================================================
// [set_total_task_count(0)] Immediate ITER_END
// =================================================================================
TEST(TestMigrationSlidingWindowDest, total_task_count_zero_immediate_iter_end) {
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 8, 32));

  ObMigrationSlidingWindowDestHandle dest_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowDestController::create(
      &wmgr, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dest_handle));

  dest_handle->set_total_task_count(0);

  char buf[32] = {0};
  int64_t len = 0;
  EXPECT_EQ(OB_ITER_END, dest_handle->get_next_consume_data(buf, sizeof(buf), len, 1000));

  dest_handle.reset();
  wmgr.destroy();
}

// =================================================================================
// [Source] set_total_task_count + ITER_END
// =================================================================================
TEST(TestMigrationSlidingWindowSource, total_task_count_iter_end) {
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 8, 32));

  ObMigrationSlidingWindowSourceHandle src_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowSourceController::create(
      &wmgr, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, src_handle));

  const int64_t total = 2;
  src_handle->set_total_task_count(total);

  ASSERT_EQ(OB_SUCCESS, src_handle->generate_next_data("A", 1));
  ASSERT_EQ(OB_SUCCESS, src_handle->generate_next_data("B", 1));

  char buf[32] = {0};
  int64_t len = 0;
  ASSERT_EQ(OB_SUCCESS, source_wait_get_data(*src_handle, 0, buf, sizeof(buf), len, 0));
  ASSERT_EQ(OB_SUCCESS, source_wait_get_data(*src_handle, 1, buf, sizeof(buf), len, 0));

  EXPECT_EQ(OB_ITER_END, source_wait_get_data(*src_handle, 2, buf, sizeof(buf), len, 1000));

  src_handle.reset();
  wmgr.destroy();
}

// =================================================================================
// [OB_BUF_NOT_ENOUGH] Output buffer smaller than data length
// =================================================================================
TEST(TestMigrationSlidingWindowSource, get_data_buf_not_enough) {
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 8, 64));

  ObMigrationSlidingWindowSourceHandle src_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowSourceController::create(
      &wmgr, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, src_handle));

  const char *data = "0123456789";
  ASSERT_EQ(OB_SUCCESS, src_handle->generate_next_data(data, 10));

  char small_buf[5] = {0};
  int64_t len = 0;
  EXPECT_EQ(OB_BUF_NOT_ENOUGH,
            source_wait_get_data(*src_handle, 0, small_buf, sizeof(small_buf), len, 1000));

  src_handle.reset();
  wmgr.destroy();
}

// =================================================================================
// [Invalid arguments] fill_data / get_data with null pointer or invalid length
// =================================================================================
TEST(TestMigrationSlidingWindowErrorHandling, fill_data_invalid_args) {
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 8, 32));

  ObMigrationSlidingWindowDestHandle dest_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowDestController::create(
      &wmgr, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dest_handle));

  EXPECT_EQ(OB_INVALID_ARGUMENT, dest_handle->fill_data(0, nullptr, 1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, dest_handle->fill_data(0, "A", -1));

  dest_handle.reset();
  wmgr.destroy();
}

TEST(TestMigrationSlidingWindowErrorHandling, get_data_invalid_args) {
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 8, 32));

  ObMigrationSlidingWindowSourceHandle src_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowSourceController::create(
      &wmgr, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, src_handle));

  ASSERT_EQ(OB_SUCCESS, src_handle->generate_next_data("A", 1));

  int64_t len = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, src_handle->try_get_data(0, nullptr, 16, len));
  char buf[16] = {0};
  EXPECT_EQ(OB_INVALID_ARGUMENT, src_handle->try_get_data(0, buf, 0, len));
  EXPECT_EQ(OB_INVALID_ARGUMENT, src_handle->try_get_data(0, buf, -1, len));

  src_handle.reset();
  wmgr.destroy();
}

// =================================================================================
// fill_data for seq already slid past head: check_seq_in_window_ -> OB_ERR_UNEXPECTED
// =================================================================================
TEST(TestMigrationSlidingWindowDest, fill_past_head_seq) {
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 8, 32));

  ObMigrationSlidingWindowDestHandle dest_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowDestController::create(
      &wmgr, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dest_handle));

  ASSERT_EQ(OB_SUCCESS, dest_handle->fill_data(0, "A", 1));
  char buf[32] = {0};
  int64_t len = 0;
  ASSERT_EQ(OB_SUCCESS, dest_handle->get_next_consume_data(buf, sizeof(buf), len));

  int64_t head = 0, wsz = 0;
  ASSERT_EQ(OB_SUCCESS, dest_handle->get_runtime_snapshot(head, wsz));
  ASSERT_EQ(1, head);

  EXPECT_EQ(OB_ERR_UNEXPECTED, dest_handle->fill_data(0, "B", 1));

  dest_handle.reset();
  wmgr.destroy();
}

// =================================================================================
// [Callback verification] Run to completion after set_total_task_count, verify callback parameters
// =================================================================================
TEST(TestMigrationSlidingWindowDest, slide_callback_params_verified) {
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 8, 32));

  TestWindowSlidCallback cb_ctx;
  ObMigrationSlidingWindowDestHandle dest_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowDestController::create(
      &wmgr, &cb_ctx, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dest_handle));

  const int64_t total = 4;
  dest_handle->set_total_task_count(total);

  char buf[32] = {0};
  int64_t len = 0;
  for (int64_t i = 0; i < total; ++i) {
    char c = static_cast<char>('A' + i);
    ASSERT_EQ(OB_SUCCESS, dest_fill_and_consume(*dest_handle, i, &c, 1, buf, sizeof(buf), len));
    ASSERT_EQ(1, len);
    ASSERT_EQ(static_cast<char>('A' + i), buf[0]);
  }

  {
    std::lock_guard<std::mutex> guard(cb_ctx.mu_);
    ASSERT_EQ(static_cast<size_t>(total), cb_ctx.granted_slot_counts_.size());
  }

  EXPECT_EQ(OB_ITER_END, dest_handle->get_next_consume_data(buf, sizeof(buf), len, 1000));

  dest_handle.reset();
  wmgr.destroy();
}

// Retry-driven consume is covered by driver outer-loop tests; on_consume_poll hook removed.

// =================================================================================
// [Destroy concurrency] stop + wakeup wakes up blocked consumer threads
//
// The controller heap allocation + handle ref-count means the controller stays
// alive until all consuming threads exit. stop() flips stopped_=true so the
// blocked consumer sees OB_CANCELED on its next try_consume_slot_ call.
// =================================================================================
TEST(TestMigrationSlidingWindowDest, destroy_wakes_blocked_consumer) {
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 8, 32));

  ObMigrationSlidingWindowDestHandle dest_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowDestController::create(
      &wmgr, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dest_handle));

  std::atomic<int> consumer_ret{OB_SUCCESS};

  std::thread consumer([&]() {
    char buf[32] = {0};
    int64_t len = 0;
    consumer_ret.store(
        dest_handle->get_next_consume_data(buf, sizeof(buf), len, 0 /*wait forever*/));
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // stop + wakeup unblocks the consumer; handle.reset() cleans up after consumer exits
  dest_handle->stop();
  dest_handle->wakeup_waiters();
  consumer.join();

  EXPECT_EQ(OB_CANCELED, consumer_ret.load());

  dest_handle.reset();
  wmgr.destroy();
}

// =================================================================================
// [Dest EAGAIN retry scenario] Simulate async RPCs getting EAGAIN at source,
// window sliding past unfilled slots, and the retry mechanism eventually filling them.
//
// Timeline:
//   1. dest start_initial_window sends 4 async RPCs (seq 0..3)
//   2. seq 1/2/3 RPCs get OB_EAGAIN at source (slot still RESERVED)
//      → queued into pending_retry_seqs_ by ObFetchVecIdxSegDataCB::process
//   3. seq 0 RPC succeeds → fill_data(0) → consumer consumes seq 0
//      → on_window_slid sends seq 4 only (does NOT re-send 1/2/3)
//   4. consumer enters wait_and_get_data(seq=1):
//      → do_retry_() re-sends 1/2/3 and drains pending_retry_seqs_
//      → inner polling loop in get_next_consume_data retries try_consume_slot_
//   5. If the re-send RPCs get EAGAIN again (source still not READY),
//      consumer must keep polling and eventually receive data after
//      the retry filler succeeds — must not get stuck until 60s timeout.
// =================================================================================
TEST(TestMigrationSlidingWindowDest, eagain_retry_after_window_slid) {
  TestWindowSlidCallback cb_ctx;
  const int64_t win = 4;
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 8, 32));

  ObMigrationSlidingWindowDestHandle dest_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowDestController::create(
      &wmgr, &cb_ctx, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dest_handle));
  dest_handle->set_total_task_count(5);

  // Step 1: Only seq 0 is filled (simulating seq 1/2/3 RPCs got EAGAIN at source).
  ASSERT_EQ(OB_SUCCESS, dest_handle->fill_data(0, "A", 1));

  // Step 2: Consumer consumes seq 0 → window slides → on_window_slid sends seq 4.
  {
    char buf[32] = {0};
    int64_t len = 0;
    ASSERT_EQ(OB_SUCCESS, dest_handle->get_next_consume_data(buf, sizeof(buf), len, 1000));
    ASSERT_EQ(1, len);
    ASSERT_EQ('A', buf[0]);
  }

  // Verify on_window_slid was called (seq 0 consumed, new slot seq 4 granted).
  {
    std::lock_guard<std::mutex> guard(cb_ctx.mu_);
    ASSERT_GE(cb_ctx.granted_slot_counts_.size(), 1U);
    ASSERT_GT(cb_ctx.granted_slot_counts_[0], 0);
  }

  // Step 3: Consumer thread blocks on seq 1 (not yet filled).
  // Simultaneously, a "retry filler" thread gradually fills seq 1..4 with
  // increasing delays, simulating multiple EAGAIN cycles before success.
  std::atomic<int> consume_ret{OB_SUCCESS};
  std::atomic<int64_t> last_consumed_seq{-1};

  std::thread consumer([&]() {
    for (int seq = 1; seq < 5; ++seq) {
      char buf[32] = {0};
      int64_t len = 0;
      const char expected = static_cast<char>('A' + seq);
      // Use a per-seq timeout long enough to tolerate retry delays.
      int ret = dest_handle->get_next_consume_data(buf, sizeof(buf), len, 5000000);
      if (OB_SUCCESS != ret) {
        consume_ret.store(ret);
        return;
      }
      if (len != 1 || buf[0] != expected) {
        consume_ret.store(OB_ERR_UNEXPECTED);
        return;
      }
      last_consumed_seq.store(seq);
    }
    consume_ret.store(OB_SUCCESS);
  });

  // Retry filler: simulates the driver's do_retry_() re-sending RPCs after
  // EAGAIN responses. Delays model source-side serialization lag.
  std::thread retry_filler([&]() {
    // seq 1: first retry succeeds after short delay
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_EQ(OB_SUCCESS, dest_handle->fill_data(1, "B", 1));

    // seq 2: multiple EAGAIN cycles before success (longer delay)
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    ASSERT_EQ(OB_SUCCESS, dest_handle->fill_data(2, "C", 1));

    // seq 3: another EAGAIN cycle
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    ASSERT_EQ(OB_SUCCESS, dest_handle->fill_data(3, "D", 1));

    // seq 4: filled by on_window_slid's send_range_ (delayed arrival)
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_EQ(OB_SUCCESS, dest_handle->fill_data(4, "E", 1));
  });

  consumer.join();
  retry_filler.join();

  EXPECT_EQ(OB_SUCCESS, consume_ret.load());
  EXPECT_EQ(4, last_consumed_seq.load());

  // Verify final state: all 5 seqs consumed.
  int64_t head = 0, wsz = 0;
  ASSERT_EQ(OB_SUCCESS, dest_handle->get_runtime_snapshot(head, wsz));
  EXPECT_EQ(5, head);

  dest_handle.reset();
  wmgr.destroy();
}

// =================================================================================
// [Operations after reset] Handle invalid after reset
// =================================================================================
TEST(TestMigrationSlidingWindowErrorHandling, operations_after_destroy) {
  ObMigrationTenantWindowMgr wmgr;
  ASSERT_EQ(OB_SUCCESS, init_test_window_mgr(wmgr, 8, 32));

  ObMigrationSlidingWindowDestHandle dest_handle;
  ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowDestController::create(
      &wmgr, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, dest_handle));
  dest_handle.reset();

  EXPECT_FALSE(dest_handle.is_valid());

  wmgr.destroy();
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv) {
  OB_LOGGER.set_log_level("DEBUG");
  system("rm -f test_migration_sliding_window_controller.log*");
  OB_LOGGER.set_file_name("test_migration_sliding_window_controller.log");
  STORAGE_LOG(INFO, "begin unittest: test_migration_sliding_window_controller");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
