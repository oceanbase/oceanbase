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

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_tracepoint.h"

#define private public
#include "logservice/restoreservice/ob_remote_log_source.h"
#undef private

namespace oceanbase
{
namespace logservice
{
namespace unittest
{

using namespace common;
using namespace palf;

class NamedErrsimGuard
{
public:
  explicit NamedErrsimGuard(const char *name) : name_(name) {}
  ~NamedErrsimGuard() { reset(); }

  int set_once(const int error_code)
  {
    common::EventItem item;
    item.error_code_ = error_code;
    item.occur_ = 1;
    item.trigger_freq_ = 0;
    return common::EventTable::set_event(name_, item);
  }

  int reset()
  {
    common::EventItem item;
    return common::EventTable::set_event(name_, item);
  }

private:
  const char *name_;
};

static share::SCN build_scn()
{
  share::SCN scn = share::SCN::base_scn();
  if (!scn.is_valid()) {
    scn.set_base();
  }
  return scn;
}

static void set_valid_location_progress(ObLogArchivePieceContext &context,
                                        const int64_t file_offset)
{
  const share::SCN base_scn = build_scn();
  context.locate_round_ = true;
  context.dest_id_ = 1;
  context.min_round_id_ = 1;
  context.max_round_id_ = 1;

  context.round_context_.state_ = ObLogArchivePieceContext::RoundContext::State::ACTIVE;
  context.round_context_.round_id_ = 1;
  context.round_context_.start_scn_ = base_scn;
  context.round_context_.end_scn_ = share::SCN::max_scn();
  context.round_context_.min_piece_id_ = 1;
  context.round_context_.max_piece_id_ = 1;
  context.round_context_.base_piece_id_ = 1;
  context.round_context_.piece_switch_interval_ = 60 * 1000 * 1000;
  context.round_context_.base_piece_scn_ = base_scn;

  context.inner_piece_context_.state_ =
      ObLogArchivePieceContext::InnerPieceContext::State::ACTIVE;
  context.inner_piece_context_.piece_id_ = 1;
  context.inner_piece_context_.round_id_ = 1;
  context.inner_piece_context_.min_lsn_in_piece_ = LSN(0);
  context.inner_piece_context_.max_lsn_in_piece_ = LSN(1024);
  context.inner_piece_context_.min_file_id_ = 1;
  context.inner_piece_context_.max_file_id_ = 1;
  context.inner_piece_context_.file_id_ = 1;
  context.inner_piece_context_.file_offset_ = file_offset;
  context.inner_piece_context_.max_lsn_ = LSN(file_offset);
}

TEST(TestRemoteLogSource, location_backwrite_success_updates_parent)
{
  const share::ObLSID ls_id(1001);
  const int64_t old_file_offset = 128;
  const int64_t new_file_offset = 256;
  share::ObBackupDest dest;
  ObRemoteLocationParent parent(ls_id);
  ObRemoteLocationParent task_source(ls_id);
  ObRemoteLocationParent next_task_source(ls_id);

  ASSERT_EQ(OB_SUCCESS, dest.set("file:///log_restore_source"));
  ASSERT_EQ(OB_SUCCESS, parent.set(dest, share::SCN::max_scn()));
  set_valid_location_progress(parent.piece_context_, old_file_offset);
  ASSERT_TRUE(parent.piece_context_.is_valid());

  ASSERT_EQ(OB_SUCCESS, parent.deep_copy_to(task_source));
  set_valid_location_progress(task_source.piece_context_, new_file_offset);
  ASSERT_TRUE(task_source.piece_context_.is_valid());

  ASSERT_EQ(OB_SUCCESS, parent.update_locate_info(task_source));
  EXPECT_TRUE(parent.piece_context_.is_valid());
  EXPECT_TRUE(parent.piece_context_.archive_dest_.is_valid());
  EXPECT_TRUE(parent.piece_context_.archive_dest_ == task_source.piece_context_.archive_dest_);
  EXPECT_EQ(new_file_offset, parent.piece_context_.inner_piece_context_.file_offset_);

  ASSERT_EQ(OB_SUCCESS, parent.deep_copy_to(next_task_source));
  EXPECT_TRUE(next_task_source.piece_context_.is_valid());
  EXPECT_EQ(new_file_offset, next_task_source.piece_context_.inner_piece_context_.file_offset_);
}

TEST(TestRemoteLogSource, location_backwrite_deep_copy_failure_preserves_parent)
{
  static const char *const ERRSIM_NAME =
      "ERRSIM_LOG_RESTORE_LOCATION_BACKWRITE_DEEP_COPY_FAIL";
  const share::ObLSID ls_id(1001);
  const int64_t old_file_offset = 128;
  const int64_t new_file_offset = 256;
  share::ObBackupDest dest;
  ObRemoteLocationParent parent(ls_id);
  ObRemoteLocationParent task_source(ls_id);
  ObRemoteLocationParent next_task_source(ls_id);
  NamedErrsimGuard errsim_guard(ERRSIM_NAME);

  ASSERT_EQ(OB_SUCCESS, dest.set("file:///log_restore_source"));
  ASSERT_EQ(OB_SUCCESS, parent.set(dest, share::SCN::max_scn()));
  set_valid_location_progress(parent.piece_context_, old_file_offset);
  ASSERT_TRUE(parent.piece_context_.is_valid());

  ASSERT_EQ(OB_SUCCESS, parent.deep_copy_to(task_source));
  set_valid_location_progress(task_source.piece_context_, new_file_offset);
  ASSERT_TRUE(task_source.piece_context_.is_valid());

  ASSERT_EQ(OB_SUCCESS, errsim_guard.set_once(OB_ALLOCATE_MEMORY_FAILED));
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, parent.update_locate_info(task_source));
  EXPECT_TRUE(parent.piece_context_.is_valid());
  EXPECT_TRUE(parent.piece_context_.archive_dest_.is_valid());
  EXPECT_TRUE(parent.piece_context_.archive_dest_ == task_source.piece_context_.archive_dest_);
  EXPECT_EQ(old_file_offset, parent.piece_context_.inner_piece_context_.file_offset_);

  ASSERT_EQ(OB_SUCCESS, parent.deep_copy_to(next_task_source));
  EXPECT_TRUE(next_task_source.piece_context_.is_valid());
  EXPECT_EQ(old_file_offset, next_task_source.piece_context_.inner_piece_context_.file_offset_);
}

} // namespace unittest
} // namespace logservice
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ob_remote_log_source.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
