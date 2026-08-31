/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#define USING_LOG_PREFIX SQL_ENG
#define private public
#define protected public
#include "src/sql/engine/px/ob_px_bloom_filter.h"
#include "src/sql/engine/join/ob_join_filter_op.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace std;

class ObPxBloomFilterTest : public ::testing::Test
{};

// Verify cooperative MEMSET path: a single "leader" calls ObPxBloomFilter::init
// with a standalone ObPxBfMemsetHelper; multiple "follower" threads concurrently
// call helper->follower_help() to mimic PX workers spinning in wait_constructed.
// The bits buffer must be fully zeroed and the helper must retract its task
// after leader_memset returns.
TEST_F(ObPxBloomFilterTest, test_px_bloom_filter_cooperative_memset)
{
    int ret = OB_SUCCESS;
    ObArenaAllocator arena_alloc;

    oceanbase::sql::ObPxBfMemsetHelper helper;

    constexpr int64_t kFollowerCount = 7;
    std::atomic<bool> stop_followers{false};
    std::atomic<int64_t> followers_saw_task{0};
    std::atomic<int64_t> help_rounds{0};

    std::vector<std::thread> followers;
    followers.reserve(kFollowerCount);
    for (int i = 0; i < kFollowerCount; ++i) {
        followers.emplace_back([&]() {
        while (!stop_followers.load(std::memory_order_acquire)) {
            if (nullptr != ATOMIC_LOAD(&helper.buf_)) {
            followers_saw_task.fetch_add(1, std::memory_order_relaxed);
            }
            helper.follower_help();
            help_rounds.fetch_add(1, std::memory_order_relaxed);
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
        });
    }

    // Hit max_filter_size so bits_array_length_ * 8 = 64MB > 16MB threshold.
    oceanbase::sql::ObPxBloomFilter bloom_filter;
    const int64_t kMaxFilterSize = 64L * 1024 * 1024;
    const int64_t kDataLength = 100000000;
    ret = bloom_filter.init(kDataLength, arena_alloc, 1002, 0.01, kMaxFilterSize, &helper);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(bloom_filter.is_inited());

    const int64_t bytes_memset = bloom_filter.get_bits_array_length() * (int64_t)sizeof(int64_t);
    ASSERT_GE(bytes_memset, 16L << 20)
        << "need >= 16MB to trigger cooperative path, got " << bytes_memset;

    // Leader completes cooperative memset and retracts the published task.
    helper.leader_memset();
    ASSERT_EQ(nullptr, ATOMIC_LOAD(&helper.buf_));

    // bits_array_ is fully zeroed after cooperative memset.
    int64_t *bits = bloom_filter.get_bits_array();
    ASSERT_NE(nullptr, bits);
    const int64_t bits_len = bloom_filter.get_bits_array_length();
    for (int64_t i = 0; i < bits_len; ++i) {
        ASSERT_EQ(0, bits[i]) << "non-zero bits at idx " << i;
    }

    // Release followers.
    stop_followers.store(true, std::memory_order_release);
    for (auto &t : followers) {
        t.join();
    }

    cout << "cooperative memset: size=" << bytes_memset
            << " followers_saw_task=" << followers_saw_task.load()
            << " help_rounds=" << help_rounds.load()
            << endl;
}

 // Size below PARALLEL_MEMSET_THRESHOLD (16MB): leader must fall back to plain
 // single-threaded MEMSET without ever publishing a task; helper stays idle.
TEST_F(ObPxBloomFilterTest, test_px_bloom_filter_cooperative_memset_below_threshold)
{
    int ret = OB_SUCCESS;
    ObArenaAllocator arena_alloc;

    oceanbase::sql::ObPxBfMemsetHelper helper;

    oceanbase::sql::ObPxBloomFilter bloom_filter;
    const int64_t kMaxFilterSize = 8L * 1024 * 1024; // 8MB < 16MB threshold
    const int64_t kDataLength = 1000000;
    ret = bloom_filter.init(kDataLength, arena_alloc, 1002, 0.01, kMaxFilterSize, &helper);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(bloom_filter.is_inited());

    const int64_t bytes_memset = bloom_filter.get_bits_array_length() * (int64_t)sizeof(int64_t);
    ASSERT_LT(bytes_memset, 16L << 20) << "expected sub-threshold size, got " << bytes_memset;

    // Task never published on the small path.
    ASSERT_EQ(nullptr, ATOMIC_LOAD(&helper.buf_));

    int64_t *bits = bloom_filter.get_bits_array();
    ASSERT_NE(nullptr, bits);
    const int64_t bits_len = bloom_filter.get_bits_array_length();
    for (int64_t i = 0; i < bits_len; ++i) {
        ASSERT_EQ(0, bits[i]);
    }
}

// Nullptr helper: legacy single-thread MEMSET path must still work end-to-end.
TEST_F(ObPxBloomFilterTest, test_px_bloom_filter_null_memset_helper)
{
    int ret = OB_SUCCESS;
    ObArenaAllocator arena_alloc;
    oceanbase::sql::ObPxBloomFilter bloom_filter;
    const int64_t kMaxFilterSize = 64L * 1024 * 1024;
    const int64_t kDataLength = 100000000;
    ret = bloom_filter.init(kDataLength, arena_alloc, 1002, 0.01, kMaxFilterSize, nullptr);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(bloom_filter.is_inited());

    int64_t *bits = bloom_filter.get_bits_array();
    ASSERT_NE(nullptr, bits);
    const int64_t bits_len = bloom_filter.get_bits_array_length();
    for (int64_t i = 0; i < bits_len; ++i) {
        ASSERT_EQ(0, bits[i]);
    }
}

int main(int argc, char **argv)
{
    int ret = OB_SUCCESS;
    system("rm -f test_px_bloom_filter.log*");
    OB_LOGGER.set_file_name("test_px_bloom_filter.log", true, true);
    OB_LOGGER.set_log_level("TRACE", "TRACE");
    ::testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
    return ret;
}
