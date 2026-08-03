/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include "storage/high_availability/ob_storage_ha_service.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace storage
{

class TestMigrationSrcBook : public ::testing::Test {};

static ObAddr addr(const char *ip, int32_t port)
{
  ObAddr a;
  EXPECT_TRUE(a.set_ip_addr(ip, port));
  return a;
}

// 帮手:让 ls_id 占住指定 addr —— 单候选 pick 必中
static void occupy(ObMigrationSrcBook &book, const ObLSID &ls_id, const ObAddr &src)
{
  ObArray<ObAddr> only;
  ASSERT_EQ(OB_SUCCESS, only.push_back(src));
  ObAddr chosen;
  ASSERT_EQ(OB_SUCCESS, book.pick_coolest(ls_id, only, chosen));
  ASSERT_EQ(src, chosen);
}

// (a) 一个 addr 被占后,pick_coolest 优先挑没被占的
TEST_F(TestMigrationSrcBook, prefer_unreserved)
{
  ObMigrationSrcBook book;
  ObAddr a = addr("1.1.1.1", 1);
  ObAddr b = addr("2.2.2.2", 2);
  occupy(book, ObLSID(1001), a);
  ObArray<ObAddr> in;
  ASSERT_EQ(OB_SUCCESS, in.push_back(a));
  ASSERT_EQ(OB_SUCCESS, in.push_back(b));
  ObAddr chosen;
  ObLSID picker(2001);
  // pick 会记账,每轮 release 还原后重试:b 是唯一 cool 的,必须每次都是 b
  for (int i = 0; i < 20; ++i) {
    ASSERT_EQ(OB_SUCCESS, book.pick_coolest(picker, in, chosen));
    ASSERT_EQ(b, chosen);
    ASSERT_EQ(OB_SUCCESS, book.release(picker));
  }
}

// (b) release 后热度归零
TEST_F(TestMigrationSrcBook, release_drops_heat)
{
  ObMigrationSrcBook book;
  ObAddr a = addr("1.1.1.1", 1);
  ObLSID ls1(1001);
  occupy(book, ls1, a);
  ASSERT_EQ(OB_SUCCESS, book.release(ls1));
  ObArray<ObAddr> in;
  ASSERT_EQ(OB_SUCCESS, in.push_back(a));
  ObAddr chosen;
  ASSERT_EQ(OB_SUCCESS, book.pick_coolest(ObLSID(2001), in, chosen));
  ASSERT_EQ(a, chosen);
}

// (c) 同 ls 重复 pick 只按一份计数 —— 旧选择被覆盖,不双倍累计
TEST_F(TestMigrationSrcBook, repick_overwrites_no_double_count)
{
  ObMigrationSrcBook book;
  ObAddr a = addr("1.1.1.1", 1);
  ObAddr b = addr("2.2.2.2", 2);
  ObLSID ls1(1001);
  occupy(book, ls1, a);
  occupy(book, ls1, b);  // 同 LS 重选,切到 b,a 的热度应回到 0
  ObArray<ObAddr> in;
  ASSERT_EQ(OB_SUCCESS, in.push_back(a));
  ASSERT_EQ(OB_SUCCESS, in.push_back(b));
  ObAddr chosen;
  ObLSID picker(2001);
  for (int i = 0; i < 20; ++i) {
    ASSERT_EQ(OB_SUCCESS, book.pick_coolest(picker, in, chosen));
    ASSERT_EQ(a, chosen);  // a 不再被占用
    ASSERT_EQ(OB_SUCCESS, book.release(picker));
  }
}

// (d) 多 LS 占同一 addr,热度累加;pick_coolest 选 cooler 的
TEST_F(TestMigrationSrcBook, heat_accumulates)
{
  ObMigrationSrcBook book;
  ObAddr hot = addr("1.1.1.1", 1);
  ObAddr cool = addr("2.2.2.2", 2);
  occupy(book, ObLSID(1001), hot);
  occupy(book, ObLSID(1002), hot);
  occupy(book, ObLSID(1003), cool);
  ObArray<ObAddr> in;
  ASSERT_EQ(OB_SUCCESS, in.push_back(hot));
  ASSERT_EQ(OB_SUCCESS, in.push_back(cool));
  ObAddr chosen;
  ObLSID picker(2001);
  for (int i = 0; i < 20; ++i) {
    ASSERT_EQ(OB_SUCCESS, book.pick_coolest(picker, in, chosen));
    ASSERT_EQ(cool, chosen);
    ASSERT_EQ(OB_SUCCESS, book.release(picker));
  }
}

// (e) 全占场景:仍然挑热度最低的;同热度时随机平局
TEST_F(TestMigrationSrcBook, all_occupied_picks_min_heat)
{
  ObMigrationSrcBook book;
  ObAddr a = addr("1.1.1.1", 1);
  ObAddr b = addr("2.2.2.2", 2);
  ObAddr c = addr("3.3.3.3", 3);
  // a 被 2 个 LS 占,b 被 1 个,c 被 1 个 → 应在 b/c 中选
  occupy(book, ObLSID(1001), a);
  occupy(book, ObLSID(1002), a);
  occupy(book, ObLSID(1003), b);
  occupy(book, ObLSID(1004), c);
  ObArray<ObAddr> in;
  ASSERT_EQ(OB_SUCCESS, in.push_back(a));
  ASSERT_EQ(OB_SUCCESS, in.push_back(b));
  ASSERT_EQ(OB_SUCCESS, in.push_back(c));
  ObAddr chosen;
  ObLSID picker(2001);
  bool saw_b = false, saw_c = false;
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(OB_SUCCESS, book.pick_coolest(picker, in, chosen));
    ASSERT_NE(a, chosen);  // a 永不被选
    if (chosen == b) saw_b = true;
    if (chosen == c) saw_c = true;
    ASSERT_EQ(OB_SUCCESS, book.release(picker));
  }
  // 100 次轮询,b 和 c 各以 50% 概率出现的话至少各看到一次 (容忍 rand 偏置极小)
  ASSERT_TRUE(saw_b);
  ASSERT_TRUE(saw_c);
}

// (f) 空候选返回 OB_ENTRY_NOT_EXIST;非法 ls_id 返回 OB_INVALID_ARGUMENT
TEST_F(TestMigrationSrcBook, invalid_inputs)
{
  ObMigrationSrcBook book;
  ObArray<ObAddr> in;
  ObAddr chosen;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, book.pick_coolest(ObLSID(2001), in, chosen));
  ASSERT_EQ(OB_SUCCESS, in.push_back(addr("1.1.1.1", 1)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, book.pick_coolest(ObLSID(), in, chosen));
  ASSERT_EQ(OB_INVALID_ARGUMENT, book.reserve(ObLSID(), in.at(0)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, book.reserve(ObLSID(2001), ObAddr()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, book.release(ObLSID()));
}

// (g) pick 即记账:并发选源场景下,第二个 pick 必须看到第一个 pick 的选择
TEST_F(TestMigrationSrcBook, pick_records_choice_immediately)
{
  ObMigrationSrcBook book;
  ObAddr a = addr("1.1.1.1", 1);
  ObAddr b = addr("2.2.2.2", 2);
  ObArray<ObAddr> in;
  ASSERT_EQ(OB_SUCCESS, in.push_back(a));
  ASSERT_EQ(OB_SUCCESS, in.push_back(b));
  ObAddr first;
  ObAddr second;
  ASSERT_EQ(OB_SUCCESS, book.pick_coolest(ObLSID(3001), in, first));
  ASSERT_EQ(OB_SUCCESS, book.pick_coolest(ObLSID(3002), in, second));
  // 没有任何显式 reserve,两次 pick 也必须互相错开
  ASSERT_NE(first, second);
}

// A fixed source (the leader-only path) is recorded without constructing a
// single-element candidate pool, and affects subsequent heat decisions.
TEST_F(TestMigrationSrcBook, reserve_records_fixed_source)
{
  ObMigrationSrcBook book;
  const ObAddr a = addr("1.1.1.1", 1);
  const ObAddr b = addr("2.2.2.2", 2);
  const ObLSID reserved_ls(3001);
  const ObLSID picker(3002);
  ASSERT_EQ(OB_SUCCESS, book.reserve(reserved_ls, a));

  ObArray<ObAddr> in;
  ASSERT_EQ(OB_SUCCESS, in.push_back(a));
  ASSERT_EQ(OB_SUCCESS, in.push_back(b));
  ObAddr chosen;
  ASSERT_EQ(OB_SUCCESS, book.pick_coolest(picker, in, chosen));
  ASSERT_EQ(b, chosen);
  ASSERT_EQ(OB_SUCCESS, book.release(picker));

  // Reserving the same LS again replaces its old source instead of adding a
  // second reference.
  ASSERT_EQ(OB_SUCCESS, book.reserve(reserved_ls, b));
  ASSERT_EQ(OB_SUCCESS, book.pick_coolest(picker, in, chosen));
  ASSERT_EQ(a, chosen);
}

// (h) release 不存在的 ls 是幂等 no-op(迁移失败清理路径会无条件调用)
TEST_F(TestMigrationSrcBook, release_absent_is_noop)
{
  ObMigrationSrcBook book;
  ASSERT_EQ(OB_SUCCESS, book.release(ObLSID(9999)));
  ObAddr a = addr("1.1.1.1", 1);
  ObLSID ls1(1001);
  occupy(book, ls1, a);
  ASSERT_EQ(OB_SUCCESS, book.release(ls1));
  ASSERT_EQ(OB_SUCCESS, book.release(ls1));  // 二次释放也 OK
}

// (i) list_reservations 快照出当前所有持有预约的 ls;release 后不再出现
// (后台自愈用它列出 key,再逐个问 migration handler 是否还在跑 —— 存活判定
//  在 service 层依赖 LS/handler,这里只覆盖 book 暴露的 key 快照原语)
TEST_F(TestMigrationSrcBook, list_reservations_snapshots_keys)
{
  ObMigrationSrcBook book;
  ObAddr a = addr("1.1.1.1", 1);
  ObAddr b = addr("2.2.2.2", 2);
  // 空簿:列出为空
  ObArray<ObMigrationSrcBook::ReservationSnapshot> reservations;
  ASSERT_EQ(OB_SUCCESS, book.list_reservations(reservations));
  ASSERT_EQ(0, reservations.count());

  occupy(book, ObLSID(1001), a);
  occupy(book, ObLSID(1002), b);
  ASSERT_EQ(OB_SUCCESS, book.list_reservations(reservations));
  ASSERT_EQ(2, reservations.count());
  bool saw_1001 = false, saw_1002 = false;
  for (int64_t i = 0; i < reservations.count(); ++i) {
    if (reservations.at(i).ls_id_ == ObLSID(1001)) saw_1001 = true;
    if (reservations.at(i).ls_id_ == ObLSID(1002)) saw_1002 = true;
  }
  ASSERT_TRUE(saw_1001);
  ASSERT_TRUE(saw_1002);

  // release 1001 后,只剩 1002
  ASSERT_EQ(OB_SUCCESS, book.release(ObLSID(1001)));
  ASSERT_EQ(OB_SUCCESS, book.list_reservations(reservations));
  ASSERT_EQ(1, reservations.count());
  ASSERT_EQ(ObLSID(1002), reservations.at(0).ls_id_);
}

TEST_F(TestMigrationSrcBook, stale_snapshot_does_not_release_new_reservation)
{
  ObMigrationSrcBook book;
  const ObAddr hot = addr("1.1.1.1", 1);
  const ObAddr cool = addr("2.2.2.2", 2);
  const ObLSID migrated_ls(1001);
  occupy(book, migrated_ls, hot);

  ObArray<ObMigrationSrcBook::ReservationSnapshot> reservations;
  ASSERT_EQ(OB_SUCCESS, book.list_reservations(reservations));
  ASSERT_EQ(1, reservations.count());
  const ObMigrationSrcBook::ReservationSnapshot old_snapshot = reservations.at(0);

  // A new migration for the same LS replaces the old reservation while the
  // sweeper is checking the old migration's handler state.
  occupy(book, migrated_ls, hot);
  bool released = true;
  ASSERT_EQ(OB_SUCCESS, book.release_if_version_matches(old_snapshot, released));
  ASSERT_FALSE(released);

  // The new reservation still contributes heat.
  ObArray<ObAddr> candidates;
  ASSERT_EQ(OB_SUCCESS, candidates.push_back(hot));
  ASSERT_EQ(OB_SUCCESS, candidates.push_back(cool));
  ObAddr chosen;
  const ObLSID picker(2001);
  ASSERT_EQ(OB_SUCCESS, book.pick_coolest(picker, candidates, chosen));
  ASSERT_EQ(cool, chosen);
  ASSERT_EQ(OB_SUCCESS, book.release(picker));

  ASSERT_EQ(OB_SUCCESS, book.list_reservations(reservations));
  ASSERT_EQ(1, reservations.count());
  ASSERT_NE(old_snapshot.version_, reservations.at(0).version_);
  ASSERT_EQ(OB_SUCCESS,
      book.release_if_version_matches(reservations.at(0), released));
  ASSERT_TRUE(released);
}

// repick 时当前 LS 的旧 reservation 会被替换,不应参与本次 heat。
// ls1->a、ls2->b 后,排除 ls1 自身则 a=0/b=1,结果必须仍是 a。
TEST_F(TestMigrationSrcBook, repick_excludes_own_old_reservation)
{
  ObMigrationSrcBook book;
  ObAddr a = addr("1.1.1.1", 1);
  ObAddr b = addr("2.2.2.2", 2);
  ObLSID ls1(1001);
  occupy(book, ls1, a);
  occupy(book, ObLSID(1002), b);

  ObArray<ObAddr> in;
  ASSERT_EQ(OB_SUCCESS, in.push_back(a));
  ASSERT_EQ(OB_SUCCESS, in.push_back(b));
  ObAddr chosen;
  ASSERT_EQ(OB_SUCCESS, book.pick_coolest(ls1, in, chosen));
  ASSERT_EQ(a, chosen);
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_migration_src_book.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_migration_src_book.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
