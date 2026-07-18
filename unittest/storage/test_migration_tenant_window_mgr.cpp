/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

#define USING_LOG_PREFIX STORAGE
#include "lib/container/ob_se_array.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_errno.h"
#include "storage/high_availability/ob_migration_tenant_window_mgr.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;

namespace oceanbase {
namespace unittest {

static constexpr uint64_t kTenantId = OB_SERVER_TENANT_ID;
static constexpr int64_t kSlotBufSize = 64;

static int init_mgr(ObMigrationTenantWindowMgr &mgr,
                    const int64_t max_slots,
                    const int64_t capacity = -1)
{
  // Default capacity equals max_slots when caller doesn't pass an explicit
  // upper bound. Tests that resize beyond max_slots must pass `capacity`.
  const int64_t actual_capacity = capacity > 0 ? capacity : max_slots;
  return mgr.init(kTenantId, max_slots, kSlotBufSize, actual_capacity);
}

// Tests register controllers as DEST to avoid the SOURCE-side cap check.
static int register_test_ctrl(ObMigrationTenantWindowMgr &mgr, int64_t &ctrl_id)
{
  return mgr.register_controller(ObMigrationControllerInfo::Role::DEST,
                                 share::ObDagPrio::DAG_PRIO_VECTOR_INDEX,
                                 ctrl_id);
}

// =================================================================================
// init / destroy / lifecycle
// =================================================================================
TEST(TestMigrationTenantWindowMgr, init_basic)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 4));
  EXPECT_TRUE(mgr.is_inited());
  EXPECT_FALSE(mgr.is_stopped());

  uint64_t tid = OB_INVALID_TENANT_ID;
  int64_t slot_buf = 0;
  int64_t total = 0;
  int64_t free_cnt = 0;
  EXPECT_EQ(OB_SUCCESS, mgr.get_tenant_id(tid));
  EXPECT_EQ(kTenantId, tid);
  EXPECT_EQ(OB_SUCCESS, mgr.get_slot_buf_size(slot_buf));
  EXPECT_EQ(kSlotBufSize, slot_buf);
  EXPECT_EQ(OB_SUCCESS, mgr.get_total_slot_count(total));
  EXPECT_EQ(4, total);
  EXPECT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(4, free_cnt);
  EXPECT_EQ(4, mgr.get_internal_capacity_for_test());

  mgr.destroy();
  EXPECT_FALSE(mgr.is_inited());
}

TEST(TestMigrationTenantWindowMgr, init_invalid_args)
{
  ObMigrationTenantWindowMgr mgr;
  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.init(OB_INVALID_TENANT_ID, 4, 32, 4));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.init(kTenantId, 0, 32, 4));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.init(kTenantId, 4, 0, 4));
  // capacity must be > 0 and >= max_slots.
  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.init(kTenantId, 4, 32, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.init(kTenantId, 4, 32, 3));
  EXPECT_FALSE(mgr.is_inited());
}

TEST(TestMigrationTenantWindowMgr, init_twice_rejected)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 2));
  EXPECT_EQ(OB_INIT_TWICE, init_mgr(mgr, 2));
  mgr.destroy();
}

// =================================================================================
// apply / free / register
// =================================================================================
TEST(TestMigrationTenantWindowMgr, apply_and_free_basic)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 4));

  int64_t ctrl_id = 1;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, ctrl_id));

  ObSEArray<ObMigrationTenantWindowSlot, 8> slots;
  int64_t granted = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(2, ctrl_id, slots, granted));
  EXPECT_EQ(2, granted);
  ASSERT_EQ(2, slots.count());
  for (int64_t i = 0; i < slots.count(); ++i) {
    EXPECT_TRUE(slots.at(i).is_valid());
    EXPECT_EQ(kSlotBufSize, slots.at(i).buf_cap());
  }

  int64_t free_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(2, free_cnt);

  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(slots, ctrl_id));

  ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(4, free_cnt);

  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(ctrl_id));
  mgr.destroy();
}

TEST(TestMigrationTenantWindowMgr, apply_without_register_fails)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 4));

  ObSEArray<ObMigrationTenantWindowSlot, 8> slots;
  int64_t granted = 0;
  // No controller registered: ctrl_count == 0 path -> OB_ERR_UNEXPECTED.
  EXPECT_EQ(OB_ERR_UNEXPECTED, mgr.apply_slots(1, 1, slots, granted));

  int64_t rid7 = 7;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, rid7));
  // Apply with a different (non-registered) controller id -> OB_ERR_UNEXPECTED.
  EXPECT_EQ(OB_ERR_UNEXPECTED, mgr.apply_slots(1, 8, slots, granted));

  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(rid7));
  mgr.destroy();
}

TEST(TestMigrationTenantWindowMgr, fair_share_with_two_controllers)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 4));

  int64_t a = 1;
  int64_t b = 2;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, a));
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, b));

  // fair_share = ceil(4 / 2) = 2 per controller.
  ObSEArray<ObMigrationTenantWindowSlot, 8> slots_a;
  ObSEArray<ObMigrationTenantWindowSlot, 8> slots_b;
  int64_t granted_a = 0;
  int64_t granted_b = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(4, a, slots_a, granted_a));
  EXPECT_EQ(2, granted_a);
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(4, b, slots_b, granted_b));
  EXPECT_EQ(2, granted_b);

  // Both at fair share, no further grant possible.
  ObSEArray<ObMigrationTenantWindowSlot, 8> more;
  int64_t granted_more = 0;
  EXPECT_EQ(OB_EAGAIN, mgr.apply_slots(1, a, more, granted_more));
  EXPECT_EQ(0, granted_more);

  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(slots_a, a));
  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(slots_b, b));

  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(a));
  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(b));
  mgr.destroy();
}

TEST(TestMigrationTenantWindowMgr, free_with_wrong_owner_rejected)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 2));

  int64_t a = 1;
  int64_t b = 2;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, a));
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, b));

  ObSEArray<ObMigrationTenantWindowSlot, 4> slots;
  int64_t granted = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(1, a, slots, granted));
  ASSERT_EQ(1, granted);

  // B tries to free A's slot.
  EXPECT_EQ(OB_ERR_UNEXPECTED, mgr.free_slots(slots, b));
  // A frees successfully.
  EXPECT_EQ(OB_SUCCESS, mgr.free_slots(slots, a));

  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(a));
  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(b));
  mgr.destroy();
}

// =================================================================================
// resize: expand basic
// =================================================================================
TEST(TestMigrationTenantWindowMgr, resize_expand_basic)
{
  ObMigrationTenantWindowMgr mgr;
  // capacity = 5 so resize(5) is in-range; max_slots_ starts at 2.
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 2, 5 /* capacity */));

  int64_t actual = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.resize(5, actual));
  EXPECT_EQ(5, actual);

  int64_t total = 0;
  int64_t free_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.get_total_slot_count(total));
  EXPECT_EQ(5, total);
  ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(5, free_cnt);
  EXPECT_EQ(5, mgr.get_internal_capacity_for_test());

  mgr.destroy();
}

// =================================================================================
// resize: shrink with all slots idle -> drained off the free stack, capacity_
// itself is fixed at init.
// =================================================================================
TEST(TestMigrationTenantWindowMgr, resize_shrink_idle_drains_stack)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 6));

  int64_t actual = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.resize(3, actual));
  EXPECT_EQ(3, actual);

  int64_t total = 0;
  int64_t free_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.get_total_slot_count(total));
  EXPECT_EQ(3, total);
  ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(3, free_cnt);
  // capacity_ is the resize upper bound, fixed at init.
  EXPECT_EQ(6, mgr.get_internal_capacity_for_test());

  mgr.destroy();
}

// =================================================================================
// resize: shrink with a held slot, then expand back; pool converges to new max.
// =================================================================================
TEST(TestMigrationTenantWindowMgr, shrink_with_held_then_expand_converges)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 6));

  int64_t ctrl_id = 1;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, ctrl_id));

  ObSEArray<ObMigrationTenantWindowSlot, 4> held;
  int64_t granted = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(1, ctrl_id, held, granted));
  ASSERT_EQ(1, granted);

  // Shrink to 3 while one slot is in use.
  int64_t actual = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.resize(3, actual));
  EXPECT_EQ(3, actual);

  int64_t total = 0;
  int64_t free_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.get_total_slot_count(total));
  EXPECT_EQ(3, total);
  ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(2, free_cnt);  // max_slots_(3) - in_use(1)
  EXPECT_EQ(6, mgr.get_internal_capacity_for_test());

  // Expand back to 6.
  ASSERT_EQ(OB_SUCCESS, mgr.resize(6, actual));
  EXPECT_EQ(6, actual);
  ASSERT_EQ(OB_SUCCESS, mgr.get_total_slot_count(total));
  EXPECT_EQ(6, total);
  ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(5, free_cnt);  // max_slots_(6) - in_use(1)
  EXPECT_EQ(6, mgr.get_internal_capacity_for_test());

  // Free the held slot; mgr should be back to all-idle.
  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(held, ctrl_id));
  ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(6, free_cnt);

  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(ctrl_id));
  mgr.destroy();
}

// =================================================================================
// resize: shrink then expand to a larger logical cap; subsequent applies
// grow the pool to the new max.
// =================================================================================
TEST(TestMigrationTenantWindowMgr, expand_grows_pool_for_apply)
{
  ObMigrationTenantWindowMgr mgr;
  // capacity = 10 so we can expand up to 10 later.
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 6, 10 /* capacity */));

  int64_t ctrl_id = 1;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, ctrl_id));

  ObSEArray<ObMigrationTenantWindowSlot, 4> held;
  int64_t granted = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(1, ctrl_id, held, granted));
  ASSERT_EQ(1, granted);

  int64_t actual = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.resize(3, actual));
  EXPECT_EQ(10, mgr.get_internal_capacity_for_test());

  // Expand to 10: max_slots_ grows; apply will lazily materialize new nodes.
  ASSERT_EQ(OB_SUCCESS, mgr.resize(10, actual));
  EXPECT_EQ(10, actual);

  int64_t total = 0;
  int64_t free_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.get_total_slot_count(total));
  EXPECT_EQ(10, total);
  ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(9, free_cnt);  // max_slots_(10) - in_use(1)
  EXPECT_EQ(10, mgr.get_internal_capacity_for_test());

  // Sanity: full apply should work after resize.
  ObSEArray<ObMigrationTenantWindowSlot, 16> all;
  int64_t granted_all = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(9, ctrl_id, all, granted_all));
  EXPECT_EQ(9, granted_all);

  // Free everything before destroy.
  for (int64_t i = 0; i < held.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, all.push_back(held.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(all, ctrl_id));

  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(ctrl_id));
  mgr.destroy();
}

// =================================================================================
// Repeated shrink/expand cycles must converge: alive count tracks max_slots_,
// capacity_ stays fixed.
// =================================================================================
TEST(TestMigrationTenantWindowMgr, repeated_resize_converges)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 8));

  for (int round = 0; round < 50; ++round) {
    int64_t actual = 0;
    int64_t free_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, mgr.resize(2, actual));
    EXPECT_EQ(2, actual);
    ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
    EXPECT_EQ(2, free_cnt) << "round " << round;
    EXPECT_EQ(8, mgr.get_internal_capacity_for_test()) << "round " << round;

    ASSERT_EQ(OB_SUCCESS, mgr.resize(8, actual));
    EXPECT_EQ(8, actual);
    ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
    EXPECT_EQ(8, free_cnt) << "round " << round;
    EXPECT_EQ(8, mgr.get_internal_capacity_for_test()) << "round " << round;
  }

  // After many cycles mgr should still serve a full apply/free round.
  int64_t ctrl_id = 1;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, ctrl_id));
  ObSEArray<ObMigrationTenantWindowSlot, 16> slots;
  int64_t granted = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(8, ctrl_id, slots, granted));
  EXPECT_EQ(8, granted);
  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(slots, ctrl_id));
  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(ctrl_id));
  mgr.destroy();
}

// =================================================================================
// resize: invalid args / no-op
// =================================================================================
TEST(TestMigrationTenantWindowMgr, resize_invalid_args)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 4));

  int64_t actual = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.resize(0, actual));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.resize(-1, actual));
  // Resize beyond capacity_ (== 4) is rejected.
  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.resize(5, actual));
  mgr.destroy();
}

TEST(TestMigrationTenantWindowMgr, resize_to_same_value_is_noop)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 4));

  int64_t actual = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.resize(4, actual));
  EXPECT_EQ(4, actual);
  EXPECT_EQ(4, mgr.get_internal_capacity_for_test());
  mgr.destroy();
}

// =================================================================================
// stopped state: rejects new apply, still allows free/unregister to drain
// =================================================================================
TEST(TestMigrationTenantWindowMgr, mark_stopped_rejects_apply_allows_free)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 4));

  int64_t ctrl_id = 1;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, ctrl_id));

  ObSEArray<ObMigrationTenantWindowSlot, 4> slots;
  int64_t granted = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(2, ctrl_id, slots, granted));
  ASSERT_EQ(2, granted);

  ASSERT_EQ(OB_SUCCESS, mgr.mark_stopped());
  EXPECT_TRUE(mgr.is_stopped());

  // New apply rejected after stop.
  ObSEArray<ObMigrationTenantWindowSlot, 4> more;
  int64_t granted2 = 0;
  EXPECT_EQ(OB_NOT_RUNNING, mgr.apply_slots(1, ctrl_id, more, granted2));

  // free and unregister still succeed so controllers can drain.
  EXPECT_EQ(OB_SUCCESS, mgr.free_slots(slots, ctrl_id));
  EXPECT_EQ(OB_SUCCESS, mgr.unregister_controller(ctrl_id));

  // mark_stopped is idempotent.
  EXPECT_EQ(OB_SUCCESS, mgr.mark_stopped());
  mgr.destroy();
}

// =================================================================================
// wait_for_free_slot wakes when a held slot is freed
// =================================================================================
TEST(TestMigrationTenantWindowMgr, wait_for_free_slot_wakes_on_free)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 1));

  int64_t ctrl_id = 1;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, ctrl_id));

  ObSEArray<ObMigrationTenantWindowSlot, 2> slots;
  int64_t granted = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(1, ctrl_id, slots, granted));
  ASSERT_EQ(1, granted);

  std::atomic<int> waiter_ret{OB_SUCCESS};
  std::atomic<bool> waiter_done{false};
  std::thread waiter([&]() {
    int r = mgr.wait_for_free_slot(2L * 1000L * 1000L);  // 2s
    waiter_ret.store(r);
    waiter_done.store(true);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_FALSE(waiter_done.load());

  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(slots, ctrl_id));

  waiter.join();
  EXPECT_EQ(OB_SUCCESS, waiter_ret.load());

  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(ctrl_id));
  mgr.destroy();
}

// =================================================================================
// wait_for_free_slot wakes when resize expands (broadcast on grow path)
// =================================================================================
TEST(TestMigrationTenantWindowMgr, wait_for_free_slot_wakes_on_resize_expand)
{
  ObMigrationTenantWindowMgr mgr;
  // Start with one alive slot but reserve capacity for a second.
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 1, 2 /* capacity */));

  int64_t ctrl_id = 1;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, ctrl_id));

  ObSEArray<ObMigrationTenantWindowSlot, 2> slots;
  int64_t granted = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(1, ctrl_id, slots, granted));
  ASSERT_EQ(1, granted);

  std::atomic<int> waiter_ret{OB_SUCCESS};
  std::atomic<bool> waiter_done{false};
  std::thread waiter([&]() {
    int r = mgr.wait_for_free_slot(2L * 1000L * 1000L);  // 2s
    waiter_ret.store(r);
    waiter_done.store(true);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_FALSE(waiter_done.load());

  int64_t actual = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.resize(2, actual));
  EXPECT_EQ(2, actual);

  waiter.join();
  EXPECT_EQ(OB_SUCCESS, waiter_ret.load());

  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(slots, ctrl_id));
  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(ctrl_id));
  mgr.destroy();
}

// =================================================================================
// Shrink with all slots in-use cannot drain; free_slots over-capacity branch
// drops buffers until alive count matches max_slots_.
// =================================================================================
TEST(TestMigrationTenantWindowMgr, over_capacity_drains_via_free_slots)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 4, 4));

  int64_t ctrl_id = 1;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, ctrl_id));

  ObSEArray<ObMigrationTenantWindowSlot, 8> held;
  int64_t granted = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(4, ctrl_id, held, granted));
  ASSERT_EQ(4, granted);

  int64_t free_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(0, free_cnt);

  int64_t actual = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.resize(2, actual));
  EXPECT_EQ(2, actual);
  // shrink_ cannot pop idle buffers when the free stack is empty.
  ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(0, free_cnt);

  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(held, ctrl_id));

  ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(2, free_cnt);
  int64_t total = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.get_total_slot_count(total));
  EXPECT_EQ(2, total);
  EXPECT_EQ(4, mgr.get_internal_capacity_for_test());

  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(ctrl_id));
  mgr.destroy();
}

// =================================================================================
// apply_slots: fair-share caps granted count even when request is huge
// =================================================================================
TEST(TestMigrationTenantWindowMgr, apply_large_request_capped_by_fair_share)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 3, 3));

  int64_t ctrl_id = 1;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, ctrl_id));

  ObSEArray<ObMigrationTenantWindowSlot, 16> slots;
  int64_t granted = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(10, ctrl_id, slots, granted));
  EXPECT_EQ(3, granted);
  EXPECT_EQ(3, slots.count());

  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(slots, ctrl_id));

  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(ctrl_id));
  mgr.destroy();
}

// =================================================================================
// Over-capacity frees destroy excess nodes; later expand allows the pool to
// regrow under apply.
// =================================================================================
TEST(TestMigrationTenantWindowMgr, over_capacity_free_then_expand_converges)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 8, 8));

  int64_t ctrl_id = 1;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, ctrl_id));

  ObSEArray<ObMigrationTenantWindowSlot, 16> held;
  int64_t granted = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(8, ctrl_id, held, granted));
  ASSERT_EQ(8, granted);

  int64_t actual = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.resize(2, actual));
  EXPECT_EQ(2, actual);

  // Free three arbitrary held slots while still over max_slots_; the over
  // capacity branch destroys their nodes outright.
  ObSEArray<ObMigrationTenantWindowSlot, 8> first_three;
  ASSERT_EQ(OB_SUCCESS, first_three.push_back(held.at(0)));
  ASSERT_EQ(OB_SUCCESS, first_three.push_back(held.at(1)));
  ASSERT_EQ(OB_SUCCESS, first_three.push_back(held.at(2)));
  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(first_three, ctrl_id));

  int64_t free_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(0, free_cnt);  // max_slots_(2) - in_use(5), clamped to 0

  ASSERT_EQ(OB_SUCCESS, mgr.resize(8, actual));
  EXPECT_EQ(8, actual);
  ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(3, free_cnt);  // max_slots_(8) - in_use(5)

  // Drain the remaining 5 holds.
  ObSEArray<ObMigrationTenantWindowSlot, 16> rest;
  for (int64_t i = 3; i < held.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, rest.push_back(held.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(rest, ctrl_id));

  ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(8, free_cnt);

  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(ctrl_id));
  mgr.destroy();
}

// =================================================================================
// shrink + over-capacity free: excess buffers are destroyed; unregister is clean.
// =================================================================================
TEST(TestMigrationTenantWindowMgr, shrink_over_capacity_free_then_unregister)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 8, 8));

  int64_t ctrl_id = 0;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, ctrl_id));

  ObSEArray<ObMigrationTenantWindowSlot, 16> held;
  int64_t granted = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(6, ctrl_id, held, granted));
  ASSERT_EQ(6, granted);

  int64_t actual = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.resize(2, actual));
  EXPECT_EQ(2, actual);

  // Free all held slots: over-capacity branch destroys excess nodes.
  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(held, ctrl_id));

  int64_t free_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(2, free_cnt);

  // Unregister: all slots returned, no leak.
  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(ctrl_id));
  mgr.destroy();
}

// =================================================================================
// Concurrent apply/free correctness: invariant used + free == total
// =================================================================================
TEST(TestMigrationTenantWindowMgr, concurrent_apply_free_invariant)
{
  ObMigrationTenantWindowMgr mgr;
  const int64_t total_slots = 32;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, total_slots));

  const int num_ctrls = 4;
  const int rounds_per_ctrl = 500;
  for (int i = 0; i < num_ctrls; ++i) {
    int64_t loop_cid = i + 1;
    ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, loop_cid));
  }

  std::atomic<int> err{OB_SUCCESS};
  auto worker = [&](int ctrl_id) {
    for (int r = 0; r < rounds_per_ctrl && err.load() == OB_SUCCESS; ++r) {
      ObSEArray<ObMigrationTenantWindowSlot, 8> slots;
      int64_t granted = 0;
      const int64_t want = (r % 3) + 1;
      const int rc = mgr.apply_slots(want, ctrl_id, slots, granted);
      if (OB_SUCCESS == rc) {
        std::this_thread::sleep_for(std::chrono::microseconds(20));
        if (OB_SUCCESS != mgr.free_slots(slots, ctrl_id)) {
          err.store(OB_ERR_UNEXPECTED);
          return;
        }
      } else if (OB_EAGAIN != rc) {
        // OB_EAGAIN is the legitimate "no slot / over fair share" signal.
        err.store(rc);
        return;
      }
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < num_ctrls; ++i) {
    threads.emplace_back(worker, i + 1);
  }
  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(OB_SUCCESS, err.load());

  int64_t free_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(total_slots, free_cnt);
  EXPECT_EQ(total_slots, mgr.get_internal_capacity_for_test());

  for (int i = 0; i < num_ctrls; ++i) {
    ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(i + 1));
  }
  mgr.destroy();
}

// =================================================================================
// Concurrent apply/free overlapping with resize cycles.
// =================================================================================
TEST(TestMigrationTenantWindowMgr, concurrent_resize_with_apply_free)
{
  ObMigrationTenantWindowMgr mgr;
  // capacity must be >= max target of the resize cycle below (16).
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 8, 16 /* capacity */));

  const int num_ctrls = 4;
  const int rounds_per_ctrl = 300;
  for (int i = 0; i < num_ctrls; ++i) {
    int64_t loop_cid = i + 1;
    ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, loop_cid));
  }

  std::atomic<int> err{OB_SUCCESS};
  std::atomic<bool> stop_resizer{false};

  auto worker = [&](int ctrl_id) {
    for (int r = 0; r < rounds_per_ctrl && err.load() == OB_SUCCESS; ++r) {
      ObSEArray<ObMigrationTenantWindowSlot, 8> slots;
      int64_t granted = 0;
      const int rc = mgr.apply_slots((r % 2) + 1, ctrl_id, slots, granted);
      if (OB_SUCCESS == rc) {
        std::this_thread::sleep_for(std::chrono::microseconds(10));
        if (OB_SUCCESS != mgr.free_slots(slots, ctrl_id)) {
          err.store(OB_ERR_UNEXPECTED);
          return;
        }
      } else if (OB_EAGAIN != rc) {
        err.store(rc);
        return;
      }
    }
  };

  std::thread resizer([&]() {
    int64_t actual = 0;
    const int64_t targets[] = {2, 4, 8, 16, 4, 12, 1, 8};
    for (int round = 0; round < 30 && !stop_resizer.load(); ++round) {
      for (int64_t t : targets) {
        if (stop_resizer.load()) {
          break;
        }
        if (OB_SUCCESS != mgr.resize(t, actual)) {
          err.store(OB_ERR_UNEXPECTED);
          return;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(50));
      }
    }
  });

  std::vector<std::thread> threads;
  for (int i = 0; i < num_ctrls; ++i) {
    threads.emplace_back(worker, i + 1);
  }
  for (auto &t : threads) {
    t.join();
  }
  stop_resizer.store(true);
  resizer.join();

  EXPECT_EQ(OB_SUCCESS, err.load());

  // Settle to a known capacity and check post-conditions.
  int64_t actual = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.resize(8, actual));
  EXPECT_EQ(8, actual);
  int64_t total = 0;
  int64_t free_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.get_total_slot_count(total));
  EXPECT_EQ(8, total);
  ASSERT_EQ(OB_SUCCESS, mgr.get_free_slot_count(free_cnt));
  EXPECT_EQ(8, free_cnt);
  EXPECT_EQ(16, mgr.get_internal_capacity_for_test());

  for (int i = 0; i < num_ctrls; ++i) {
    ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(i + 1));
  }
  mgr.destroy();
}

// =================================================================================
// register / unregister edge cases
// =================================================================================
TEST(TestMigrationTenantWindowMgr, register_unregister_edges)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 4));

  int64_t auto_id = OB_INVALID_INDEX_INT64;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, auto_id));
  EXPECT_GE(auto_id, 1);
  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(auto_id));

  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.unregister_controller(OB_INVALID_INDEX_INT64));

  int64_t first_id = OB_INVALID_INDEX_INT64;
  int64_t second_id = OB_INVALID_INDEX_INT64;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, first_id));
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, second_id));
  EXPECT_NE(first_id, second_id);
  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(first_id));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, mgr.unregister_controller(first_id));
  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(second_id));

  mgr.destroy();
}

// =================================================================================
// Lazy allocation: init creates no nodes; apply_slots grows the pool on demand.
// =================================================================================
TEST(TestMigrationTenantWindowMgr, init_zero_nodes_lazy_growth)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 4));
  EXPECT_EQ(0, mgr.get_node_count_for_test());

  int64_t ctrl_id = 0;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, ctrl_id));
  EXPECT_EQ(0, mgr.get_node_count_for_test());

  ObSEArray<ObMigrationTenantWindowSlot, 8> slots;
  int64_t granted = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(2, ctrl_id, slots, granted));
  EXPECT_EQ(2, granted);
  EXPECT_EQ(2, mgr.get_node_count_for_test());

  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(slots, ctrl_id));
  // Free goes to free_list_, nodes still cached.
  EXPECT_EQ(2, mgr.get_node_count_for_test());

  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(ctrl_id));
  mgr.destroy();
}

// =================================================================================
// Last unregister releases all cached idle nodes so an idle tenant pays zero.
// =================================================================================
TEST(TestMigrationTenantWindowMgr, last_unregister_releases_cached_buffers)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 4));

  // First migration round.
  int64_t ctrl_a = 0;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, ctrl_a));
  ObSEArray<ObMigrationTenantWindowSlot, 8> slots_a;
  int64_t granted_a = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(2, ctrl_a, slots_a, granted_a));
  EXPECT_EQ(2, granted_a);
  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(slots_a, ctrl_a));
  EXPECT_EQ(2, mgr.get_node_count_for_test());

  // Last unregister drops every cached node.
  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(ctrl_a));
  EXPECT_EQ(0, mgr.get_node_count_for_test());

  // Next migration round starts cold; nodes get created lazily.
  int64_t ctrl_b = 0;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, ctrl_b));
  ObSEArray<ObMigrationTenantWindowSlot, 8> slots_b;
  int64_t granted_b = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(3, ctrl_b, slots_b, granted_b));
  EXPECT_EQ(3, granted_b);
  EXPECT_EQ(3, mgr.get_node_count_for_test());
  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(slots_b, ctrl_b));
  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(ctrl_b));
  EXPECT_EQ(0, mgr.get_node_count_for_test());

  mgr.destroy();
}

// =================================================================================
// Concurrent migrations: cached nodes are kept until the last controller leaves.
// =================================================================================
TEST(TestMigrationTenantWindowMgr, concurrent_unregister_keeps_cache_until_last)
{
  ObMigrationTenantWindowMgr mgr;
  ASSERT_EQ(OB_SUCCESS, init_mgr(mgr, 4));

  int64_t a = 0;
  int64_t b = 0;
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, a));
  ASSERT_EQ(OB_SUCCESS, register_test_ctrl(mgr, b));

  ObSEArray<ObMigrationTenantWindowSlot, 4> slots_a;
  ObSEArray<ObMigrationTenantWindowSlot, 4> slots_b;
  int64_t granted = 0;
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(2, a, slots_a, granted));
  ASSERT_EQ(OB_SUCCESS, mgr.apply_slots(2, b, slots_b, granted));
  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(slots_a, a));
  ASSERT_EQ(OB_SUCCESS, mgr.free_slots(slots_b, b));
  EXPECT_EQ(4, mgr.get_node_count_for_test());

  // First unregister should NOT release the cache (b still active).
  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(a));
  EXPECT_EQ(4, mgr.get_node_count_for_test());

  // Last unregister releases everything.
  ASSERT_EQ(OB_SUCCESS, mgr.unregister_controller(b));
  EXPECT_EQ(0, mgr.get_node_count_for_test());

  mgr.destroy();
}

// =================================================================================
// calc_max_slots_by_memory: pure helper
// =================================================================================
TEST(TestMigrationTenantWindowMgr, calc_max_slots_by_memory)
{
  EXPECT_EQ(0, ObMigrationTenantWindowMgr::calc_max_slots_by_memory(0, 1024));
  EXPECT_EQ(0, ObMigrationTenantWindowMgr::calc_max_slots_by_memory(1024, 0));
  EXPECT_EQ(0, ObMigrationTenantWindowMgr::calc_max_slots_by_memory(-1, 1024));
  EXPECT_EQ(4, ObMigrationTenantWindowMgr::calc_max_slots_by_memory(4096, 1024));
  EXPECT_EQ(4, ObMigrationTenantWindowMgr::calc_max_slots_by_memory(4500, 1024));  // truncates
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  system("rm -f test_migration_tenant_window_mgr.log*");
  OB_LOGGER.set_file_name("test_migration_tenant_window_mgr.log");
  STORAGE_LOG(INFO, "begin unittest: test_migration_tenant_window_mgr");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
