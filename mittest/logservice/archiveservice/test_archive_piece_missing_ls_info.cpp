// owner: anli.hx
// owner group: log
/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <atomic>
#include <cstdlib>
#include <limits.h>
#include <gtest/gtest.h>
#include "lib/file/file_directory_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/backup/ob_archive_path.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_archive_store.h"
#include "share/io/ob_io_manager.h"
#include "share/ob_device_manager.h"
#include "share/ob_thread_pool.h"
#define private public
#include "logservice/restoreservice/ob_log_archive_piece_mgr.h"
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace logservice;
using namespace palf;
using namespace share;

static const int64_t DEST_ID = 1;
static const int64_t SOURCE_DEST_ID = DEST_ID + 1;
static const int64_t ROUND_ID = 1;
static const int64_t OLD_PIECE_ID = 34;
static const int64_t NEW_PIECE_ID = 35;
static const int64_t LS_ID = 1001;
static const int64_t BASE_TS = 1000 * 1000 * 1000L;
static const int64_t PIECE_INTERVAL = 10 * 1000 * 1000L;
static const char *const MISSING_LS_INFO_EVENT = "EN_ARCHIVE_READ_SINGLE_LS_INFO_NOT_EXIST";

// 仅模拟 round 查询；piece 元数据加载、LOW_BOUND 判定和 piece 切换循环均执行真实实现。
class PieceLookupContext : public ObLogArchivePieceContext
{
public:
  PieceLookupContext() : old_low_bound_loads_(0), new_frozen_loads_(0) {}

  std::atomic<int64_t> old_low_bound_loads_;
  std::atomic<int64_t> new_frozen_loads_;

private:
  int get_round_range_() override
  {
    dest_id_ = DEST_ID;
    min_round_id_ = ROUND_ID;
    max_round_id_ = ROUND_ID;
    return OB_SUCCESS;
  }

  int get_round_(const SCN &) override
  {
    round_context_.reset();
    round_context_.round_id_ = ROUND_ID;
    locate_round_ = true;
    return OB_SUCCESS;
  }

  int load_round_(const int64_t round_id, RoundContext &round, bool &exist) override
  {
    exist = ROUND_ID == round_id;
    round.reset();
    round.round_id_ = round_id;
    round.state_ = RoundContext::State::STOP;
    round.base_piece_id_ = OLD_PIECE_ID;
    round.piece_switch_interval_ = PIECE_INTERVAL;
    round.start_scn_.convert_from_ts(BASE_TS);
    round.base_piece_scn_ = round.start_scn_;
    round.end_scn_.convert_from_ts(BASE_TS + 2 * PIECE_INTERVAL);
    return OB_SUCCESS;
  }

  int get_round_piece_range_(const int64_t, int64_t &min_piece_id, int64_t &max_piece_id) override
  {
    min_piece_id = OLD_PIECE_ID;
    max_piece_id = NEW_PIECE_ID;
    return OB_SUCCESS;
  }

  int get_piece_meta_info_(const int64_t piece_id) override
  {
    const int ret = ObLogArchivePieceContext::get_piece_meta_info_(piece_id);
    if (OB_SUCCESS == ret) {
      if (OLD_PIECE_ID == piece_id && inner_piece_context_.is_low_bound_()) {
        old_low_bound_loads_.fetch_add(1);
      } else if (NEW_PIECE_ID == piece_id && inner_piece_context_.is_frozen_()) {
        new_frozen_loads_.fetch_add(1);
      }
    }
    return ret;
  }
};

class LocateWorker : public ObThreadPool
{
public:
  explicit LocateWorker(PieceLookupContext &context,
      const LSN &lsn = LSN(PALF_BLOCK_SIZE + 1024 * 1024L))
      : context_(context), lsn_(lsn), finished_(false), result_(OB_ERR_UNEXPECTED), piece_id_(0) {}
  ~LocateWorker() { stop(); wait(); }

  void run1() override
  {
    SCN scn;
    scn.convert_from_ts(BASE_TS + 1000 * 1000L);
    int64_t dest_id = 0;
    int64_t round_id = 0;
    int64_t file_id = 0;
    int64_t offset = 0;
    LSN max_lsn;
    bool to_newest = false;
    result_ = context_.get_piece(scn, lsn_,
        dest_id, round_id, piece_id_, file_id, offset, max_lsn, to_newest);
    finished_.store(true);
  }

  PieceLookupContext &context_;
  LSN lsn_;
  std::atomic<bool> finished_;
  int result_;
  int64_t piece_id_;
};

class ArchivePieceMissingLSInfo : public ::testing::Test
{
public:
  ArchivePieceMissingLSInfo() : directory_created_(false) { directory_[0] = '\0'; }

  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().init_devices_env());
    ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().init(128 * 1024 * 1024L, 32, 1));
    ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().start());
  }

  static void TearDownTestCase()
  {
    ObIOManager::get_instance().stop();
    ObIOManager::get_instance().wait();
    ObDeviceManager::get_instance().destroy();
    ObIOManager::get_instance().destroy();
  }

  void SetUp() override
  {
    // 测试需要指定临时目录时，通过此环境变量设置。
    const char *tmp_root = std::getenv("OB_ARCHIVE_PIECE_TEST_TMP_DIR");
    if (nullptr == tmp_root) {
      tmp_root = ".";
    }
    ASSERT_EQ(OB_SUCCESS, databuff_printf(directory_, sizeof(directory_),
        "%s/archive_piece_missing_ls_info_XXXXXX", tmp_root));
    ASSERT_NE(nullptr, mkdtemp(directory_));
    directory_created_ = true;
    char uri[PATH_MAX + 16];
    char absolute_path[PATH_MAX];
    ASSERT_NE(nullptr, realpath(directory_, absolute_path));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "file://%s", absolute_path));
    ASSERT_EQ(OB_SUCCESS, dest_.set(uri));
    ASSERT_EQ(OB_SUCCESS, store_.init(dest_));
    ASSERT_EQ(OB_SUCCESS, write_piece_(OLD_PIECE_ID, PALF_BLOCK_SIZE, 2 * PALF_BLOCK_SIZE));
    ASSERT_EQ(OB_SUCCESS, write_piece_(NEW_PIECE_ID, 2 * PALF_BLOCK_SIZE, 3 * PALF_BLOCK_SIZE));
  }

  void TearDown() override
  {
    store_.reset();
    dest_.reset();
    if (directory_created_) {
      EXPECT_EQ(OB_SUCCESS, FileDirectoryUtils::delete_directory_rec(directory_));
    }
  }

  int locate_(PieceLookupContext &context, const LSN &lsn, int64_t &piece_id)
  {
    LocateWorker worker(context, lsn);
    int ret = worker.start();
    if (OB_SUCCESS == ret) {
      const int64_t deadline = ObTimeUtility::current_time() + 5 * 1000 * 1000L;
      while (!worker.finished_.load() && ObTimeUtility::current_time() < deadline) {
        ob_usleep(1000);
      }
      const bool finished = worker.finished_.load();
      worker.stop();
      worker.wait();
      ret = finished ? worker.result_ : OB_TIMEOUT;
      piece_id = worker.piece_id_;
    }
    return ret;
  }

protected:
  int write_piece_(const int64_t piece_id, const uint64_t min_lsn, const uint64_t max_lsn)
  {
    int ret = OB_SUCCESS;
    ObSingleLSInfoDesc ls_info;
    ls_info.dest_id_ = DEST_ID;
    ls_info.round_id_ = ROUND_ID;
    ls_info.piece_id_ = piece_id;
    ls_info.ls_id_ = ObLSID(LS_ID);
    ls_info.min_lsn_ = min_lsn;
    ls_info.max_lsn_ = max_lsn;
    ls_info.start_scn_.convert_from_ts(BASE_TS + (piece_id - OLD_PIECE_ID) * PIECE_INTERVAL);
    ls_info.checkpoint_scn_.convert_from_ts(BASE_TS + (piece_id - OLD_PIECE_ID + 1) * PIECE_INTERVAL);

    ObPieceInfoDesc piece_info;
    piece_info.dest_id_ = DEST_ID;
    piece_info.round_id_ = ROUND_ID;
    piece_info.piece_id_ = piece_id;
    ObSinglePieceDesc frozen_info;
    frozen_info.piece_.key_ = ObTenantArchivePieceAttr::Key(1002, DEST_ID, ROUND_ID, piece_id);
    frozen_info.piece_.dest_no_ = 0;
    frozen_info.piece_.start_scn_ = ls_info.start_scn_;
    frozen_info.piece_.checkpoint_scn_ = ls_info.checkpoint_scn_;
    frozen_info.piece_.max_scn_ = ls_info.checkpoint_scn_;
    frozen_info.piece_.end_scn_ = ls_info.checkpoint_scn_;
    frozen_info.piece_.status_.status_ = ObArchivePieceStatus::Status::FROZEN;
    frozen_info.piece_.file_status_ = ObBackupFileStatus::BACKUP_FILE_AVAILABLE;
    frozen_info.piece_.path_.assign(dest_.get_root_path());
    if (OB_FAIL(piece_info.filelist_.push_back(ls_info))) {
    } else if (OB_FAIL(store_.write_single_ls_info(DEST_ID, ROUND_ID, piece_id, ObLSID(LS_ID), ls_info))) {
    } else if (OB_FAIL(store_.write_piece_info(DEST_ID, ROUND_ID, piece_id, piece_info))) {
    } else if (OB_FAIL(store_.write_single_piece(DEST_ID, ROUND_ID, piece_id, frozen_info))) {
    }
    return ret;
  }

  char directory_[PATH_MAX];
  bool directory_created_;
  ObBackupDest dest_;
  ObArchiveStore store_;
};

TEST_F(ArchivePieceMissingLSInfo, locate_normal_frozen_pieces)
{
  PieceLookupContext context;
  ASSERT_EQ(OB_SUCCESS, context.init(ObLSID(LS_ID), dest_));
  int64_t piece_id = 0;
  ASSERT_EQ(OB_SUCCESS, locate_(context, LSN(PALF_BLOCK_SIZE + 1024 * 1024L), piece_id));
  EXPECT_EQ(OLD_PIECE_ID, piece_id);
  ASSERT_EQ(OB_SUCCESS, locate_(context, LSN(2 * PALF_BLOCK_SIZE + 1024 * 1024L), piece_id));
  EXPECT_EQ(NEW_PIECE_ID, piece_id);
}

class MissingLSInfoGuard
{
public:
  ~MissingLSInfoGuard() { reset(); }

  int enable()
  {
    EventItem item;
    item.error_code_ = OB_OBJECT_NOT_EXIST;
    item.occur_ = 0;
    item.trigger_freq_ = 1;
    item.cond_ = OLD_PIECE_ID;
    return EventTable::set_event(MISSING_LS_INFO_EVENT, item);
  }

  int reset()
  {
    EventItem item;
    return EventTable::set_event(MISSING_LS_INFO_EVENT, item);
  }
};

TEST_F(ArchivePieceMissingLSInfo, injection_only_hides_old_piece_ls_info)
{
  MissingLSInfoGuard injection;
  ASSERT_EQ(OB_SUCCESS, injection.enable());
  bool exist = false;
  ASSERT_EQ(OB_SUCCESS, store_.is_single_piece_file_exist(DEST_ID, ROUND_ID, OLD_PIECE_ID, exist));
  ASSERT_TRUE(exist);
  ASSERT_EQ(OB_SUCCESS, store_.is_single_ls_info_file_exist(DEST_ID, ROUND_ID, OLD_PIECE_ID, ObLSID(LS_ID), exist));
  ASSERT_TRUE(exist);
  ObSingleLSInfoDesc desc;
  EXPECT_EQ(OB_OBJECT_NOT_EXIST, store_.read_single_ls_info(DEST_ID, ROUND_ID, OLD_PIECE_ID, ObLSID(LS_ID), desc));
  EXPECT_EQ(OB_SUCCESS, store_.read_single_ls_info(DEST_ID, ROUND_ID, NEW_PIECE_ID, ObLSID(LS_ID), desc));
  EXPECT_EQ(2 * PALF_BLOCK_SIZE, desc.min_lsn_);
  ASSERT_EQ(OB_SUCCESS, injection.reset());
  EXPECT_EQ(OB_SUCCESS, store_.read_single_ls_info(DEST_ID, ROUND_ID, OLD_PIECE_ID, ObLSID(LS_ID), desc));
}

TEST_F(ArchivePieceMissingLSInfo, locate_half_deleted_piece_without_cycle)
{
  MissingLSInfoGuard injection;
  ASSERT_EQ(OB_SUCCESS, injection.enable());
  PieceLookupContext context;
  ASSERT_EQ(OB_SUCCESS, context.init(ObLSID(LS_ID), dest_));
  int64_t piece_id = 0;
  ASSERT_EQ(OB_SUCCESS, locate_(context, LSN(PALF_BLOCK_SIZE + 1024 * 1024L), piece_id));
  EXPECT_EQ(OLD_PIECE_ID, piece_id);
  EXPECT_EQ(0, context.old_low_bound_loads_.load());
  EXPECT_EQ(0, context.new_frozen_loads_.load());
  // 旧 piece 缺少 LS 元数据时，仍应能定位下一 piece 中的有效 LSN。
  ASSERT_EQ(OB_SUCCESS, locate_(context, LSN(2 * PALF_BLOCK_SIZE + 1024 * 1024L), piece_id));
  EXPECT_EQ(NEW_PIECE_ID, piece_id);
}

TEST_F(ArchivePieceMissingLSInfo, ls_absent_from_manifest_remains_low_bound)
{
  MissingLSInfoGuard injection;
  ASSERT_EQ(OB_SUCCESS, injection.enable());
  ObPieceInfoDesc desc;
  ASSERT_EQ(OB_SUCCESS, store_.read_piece_info(DEST_ID, ROUND_ID, OLD_PIECE_ID, desc));
  desc.filelist_.at(0).ls_id_ = ObLSID(LS_ID + 1);
  ASSERT_EQ(OB_SUCCESS, store_.write_piece_info(DEST_ID, ROUND_ID, OLD_PIECE_ID, desc));
  PieceLookupContext context;
  ASSERT_EQ(OB_SUCCESS, context.init(ObLSID(LS_ID), dest_));
  int64_t piece_id = 0;
  ASSERT_EQ(OB_SUCCESS, locate_(context, LSN(2 * PALF_BLOCK_SIZE + 1024 * 1024L), piece_id));
  EXPECT_EQ(NEW_PIECE_ID, piece_id);
  EXPECT_GE(context.old_low_bound_loads_.load(), 1);
}

TEST_F(ArchivePieceMissingLSInfo, fallback_accepts_manifest_with_source_dest_id)
{
  MissingLSInfoGuard injection;
  ASSERT_EQ(OB_SUCCESS, injection.enable());
  ObPieceInfoDesc desc;
  ASSERT_EQ(OB_SUCCESS, store_.read_piece_info(DEST_ID, ROUND_ID, OLD_PIECE_ID, desc));
  // 补日志复制后，目标路径下的元数据仍保留源 dest_id。
  desc.dest_id_ = SOURCE_DEST_ID;
  desc.filelist_.at(0).dest_id_ = SOURCE_DEST_ID;
  ASSERT_EQ(OB_SUCCESS, store_.write_piece_info(DEST_ID, ROUND_ID, OLD_PIECE_ID, desc));
  PieceLookupContext context;
  ASSERT_EQ(OB_SUCCESS, context.init(ObLSID(LS_ID), dest_));
  int64_t piece_id = 0;
  ASSERT_EQ(OB_SUCCESS, locate_(context, LSN(PALF_BLOCK_SIZE + 1024 * 1024L), piece_id));
  EXPECT_EQ(OLD_PIECE_ID, piece_id);
  EXPECT_TRUE(context.inner_piece_context_.is_frozen_());
  EXPECT_EQ(PALF_BLOCK_SIZE, context.inner_piece_context_.min_lsn_in_piece_.val_);
  EXPECT_EQ(2 * PALF_BLOCK_SIZE, context.inner_piece_context_.max_lsn_in_piece_.val_);
  EXPECT_EQ(0, context.old_low_bound_loads_.load());
}

TEST_F(ArchivePieceMissingLSInfo, ls_absent_from_relocated_manifest_remains_low_bound)
{
  MissingLSInfoGuard injection;
  ASSERT_EQ(OB_SUCCESS, injection.enable());
  ObPieceInfoDesc desc;
  ASSERT_EQ(OB_SUCCESS, store_.read_piece_info(DEST_ID, ROUND_ID, OLD_PIECE_ID, desc));
  desc.dest_id_ = SOURCE_DEST_ID;
  desc.filelist_.at(0).dest_id_ = SOURCE_DEST_ID;
  desc.filelist_.at(0).ls_id_ = ObLSID(LS_ID + 1);
  ASSERT_EQ(OB_SUCCESS, store_.write_piece_info(DEST_ID, ROUND_ID, OLD_PIECE_ID, desc));
  PieceLookupContext context;
  ASSERT_EQ(OB_SUCCESS, context.init(ObLSID(LS_ID), dest_));
  int64_t piece_id = 0;
  ASSERT_EQ(OB_SUCCESS, locate_(context, LSN(2 * PALF_BLOCK_SIZE + 1024 * 1024L), piece_id));
  EXPECT_EQ(NEW_PIECE_ID, piece_id);
  EXPECT_GE(context.old_low_bound_loads_.load(), 1);
}

TEST_F(ArchivePieceMissingLSInfo, mismatched_ls_manifest_dest_id_returns_error)
{
  MissingLSInfoGuard injection;
  ASSERT_EQ(OB_SUCCESS, injection.enable());
  ObPieceInfoDesc desc;
  ASSERT_EQ(OB_SUCCESS, store_.read_piece_info(DEST_ID, ROUND_ID, OLD_PIECE_ID, desc));
  desc.dest_id_ = SOURCE_DEST_ID;
  // 即使 LS 记录的 dest_id 与路径一致，与总清单的源 dest_id 不一致时也必须报错。
  desc.filelist_.at(0).dest_id_ = DEST_ID;
  ASSERT_EQ(OB_SUCCESS, store_.write_piece_info(DEST_ID, ROUND_ID, OLD_PIECE_ID, desc));
  PieceLookupContext context;
  ASSERT_EQ(OB_SUCCESS, context.init(ObLSID(LS_ID), dest_));
  int64_t piece_id = 0;
  EXPECT_EQ(OB_INVALID_DATA, locate_(context, LSN(PALF_BLOCK_SIZE + 1024 * 1024L), piece_id));
}

TEST_F(ArchivePieceMissingLSInfo, missing_manifest_returns_error)
{
  MissingLSInfoGuard injection;
  ASSERT_EQ(OB_SUCCESS, injection.enable());
  ObBackupPath path;
  ASSERT_EQ(OB_SUCCESS, ObArchivePathUtil::get_piece_info_file_path(
      dest_, DEST_ID, ROUND_ID, OLD_PIECE_ID, path));
  ASSERT_EQ(OB_SUCCESS, ObBackupIoAdapter::del_file(path.get_obstr(), dest_.get_storage_info()));
  PieceLookupContext context;
  ASSERT_EQ(OB_SUCCESS, context.init(ObLSID(LS_ID), dest_));
  int64_t piece_id = 0;
  EXPECT_EQ(OB_OBJECT_NOT_EXIST, locate_(context, LSN(PALF_BLOCK_SIZE + 1024 * 1024L), piece_id));
  EXPECT_EQ(0, context.old_low_bound_loads_.load());
}

TEST_F(ArchivePieceMissingLSInfo, mismatched_manifest_returns_error)
{
  MissingLSInfoGuard injection;
  ASSERT_EQ(OB_SUCCESS, injection.enable());
  ObPieceInfoDesc desc;
  ASSERT_EQ(OB_SUCCESS, store_.read_piece_info(DEST_ID, ROUND_ID, OLD_PIECE_ID, desc));
  desc.piece_id_ = NEW_PIECE_ID;
  ASSERT_EQ(OB_SUCCESS, store_.write_piece_info(DEST_ID, ROUND_ID, OLD_PIECE_ID, desc));
  PieceLookupContext context;
  ASSERT_EQ(OB_SUCCESS, context.init(ObLSID(LS_ID), dest_));
  int64_t piece_id = 0;
  EXPECT_EQ(OB_INVALID_DATA, locate_(context, LSN(PALF_BLOCK_SIZE + 1024 * 1024L), piece_id));
}

TEST_F(ArchivePieceMissingLSInfo, mismatched_ls_record_returns_error)
{
  MissingLSInfoGuard injection;
  ASSERT_EQ(OB_SUCCESS, injection.enable());
  ObPieceInfoDesc desc;
  ASSERT_EQ(OB_SUCCESS, store_.read_piece_info(DEST_ID, ROUND_ID, OLD_PIECE_ID, desc));
  desc.filelist_.at(0).piece_id_ = NEW_PIECE_ID;
  ASSERT_EQ(OB_SUCCESS, store_.write_piece_info(DEST_ID, ROUND_ID, OLD_PIECE_ID, desc));
  PieceLookupContext context;
  ASSERT_EQ(OB_SUCCESS, context.init(ObLSID(LS_ID), dest_));
  int64_t piece_id = 0;
  EXPECT_EQ(OB_INVALID_DATA, locate_(context, LSN(PALF_BLOCK_SIZE + 1024 * 1024L), piece_id));
}

TEST_F(ArchivePieceMissingLSInfo, manifest_preserves_ls_deleted_state)
{
  MissingLSInfoGuard injection;
  ASSERT_EQ(OB_SUCCESS, injection.enable());
  ObPieceInfoDesc desc;
  ASSERT_EQ(OB_SUCCESS, store_.read_piece_info(DEST_ID, ROUND_ID, OLD_PIECE_ID, desc));
  desc.filelist_.at(0).deleted_ = true;
  ASSERT_EQ(OB_SUCCESS, store_.write_piece_info(DEST_ID, ROUND_ID, OLD_PIECE_ID, desc));
  PieceLookupContext context;
  ASSERT_EQ(OB_SUCCESS, context.init(ObLSID(LS_ID), dest_));
  int64_t piece_id = 0;
  EXPECT_EQ(OB_ITER_END, locate_(context, LSN(2 * PALF_BLOCK_SIZE + 1024 * 1024L), piece_id));
  EXPECT_TRUE(context.inner_piece_context_.is_gc_());
}

TEST_F(ArchivePieceMissingLSInfo, fallback_uses_selected_backup_destination)
{
  MissingLSInfoGuard injection;
  ASSERT_EQ(OB_SUCCESS, injection.enable());
  char uri[PATH_MAX + 16];
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/empty_primary", dest_.get_root_path().ptr()));
  ObBackupDest primary_dest;
  ASSERT_EQ(OB_SUCCESS, primary_dest.set(uri));
  PieceLookupContext context;
  ASSERT_EQ(OB_SUCCESS, context.init(ObLSID(LS_ID), primary_dest, dest_));
  int64_t piece_id = 0;
  ASSERT_EQ(OB_SUCCESS, locate_(context, LSN(PALF_BLOCK_SIZE + 1024 * 1024L), piece_id));
  EXPECT_EQ(OLD_PIECE_ID, piece_id);
  EXPECT_TRUE(context.inner_piece_context_.on_backup_archive_dest_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_archive_piece_missing_ls_info.log", true, false);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
