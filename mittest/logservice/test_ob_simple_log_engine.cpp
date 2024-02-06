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

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "logservice/palf/log_group_entry.h"
#include <cstdio>
#include <gtest/gtest.h>
#include <signal.h>
#include <stdexcept>
#define private public
#include "env/ob_simple_log_cluster_env.h"
#include "logservice/palf/log_reader_utils.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_group_entry_header.h"
#include "logservice/palf/log_io_worker.h"
#include "logservice/palf/lsn.h"
#include "share/scn.h"
#include "logservice/palf/log_io_task.h"
#include "logservice/palf/log_writer_utils.h"
#include "logservice/palf_handle_guard.h"
#undef private

const std::string TEST_NAME = "log_engine";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
using namespace palf;
namespace unittest
{
class TestObSimpleLogClusterLogEngine : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterLogEngine() : ObSimpleLogClusterTestEnv()
  {
    palf_epoch_ = 0;
  }
  ~TestObSimpleLogClusterLogEngine() { destroy(); }
  int init()
  {
    int ret = OB_SUCCESS;
    int64_t leader_idx = 0;
    id_ = ATOMIC_AAF(&palf_id_, 1);
    if (OB_FAIL(create_paxos_group(id_, leader_idx, leader_))) {
      PALF_LOG(ERROR, "create_paxos_group failed", K(ret));
    } else {
      log_engine_ = &leader_.palf_handle_impl_->log_engine_;
    }
    return ret;
  }
  int reload(const LSN &log_tail_redo, const LSN &log_tail_meta, const LSN &base_lsn)
  {
    int ret = OB_SUCCESS;
    palf_epoch_ = ATOMIC_AAF(&palf_epoch_, 1);
    LogGroupEntryHeader entry_header;
    bool is_integrity = true;
    ObILogAllocator *alloc_mgr = log_engine_->alloc_mgr_;
    LogRpc *log_rpc = log_engine_->log_net_service_.log_rpc_;
    LogIOWorker *log_io_worker = log_engine_->log_io_worker_;
    LogPlugins *plugins = log_engine_->plugins_;
    LogEngine log_engine;
    ILogBlockPool *log_block_pool = log_engine_->log_storage_.block_mgr_.log_block_pool_;
    if (OB_FAIL(log_engine.load(leader_.palf_handle_impl_->palf_id_,
                                leader_.palf_handle_impl_->log_dir_,
                                alloc_mgr,
                                log_block_pool,
                                &(leader_.palf_handle_impl_->hot_cache_),
                                log_rpc,
                                log_io_worker,
                                plugins,
                                entry_header,
                                palf_epoch_,
                                is_integrity,
                                PALF_BLOCK_SIZE,
                                PALF_META_BLOCK_SIZE))) {
      PALF_LOG(WARN, "load failed", K(ret));
    } else if (log_tail_redo != log_engine.log_storage_.log_tail_
        || log_tail_meta != log_engine.log_meta_storage_.log_tail_
        || base_lsn != log_engine.log_meta_.log_snapshot_meta_.base_lsn_) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "reload failed", K(ret), K(log_engine), KPC(log_engine_), K(log_tail_redo), K(log_tail_meta), K(base_lsn));
    } else {
      PALF_LOG(INFO, "reload success", K(log_engine), KPC(log_engine_));
    }
    return ret;
  }

  int delete_block_by_human(const block_id_t block_id)
  {
    int ret = OB_SUCCESS;
    char file_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    const char *log_dir = log_engine_->log_storage_.block_mgr_.log_dir_;
    if (OB_FAIL(convert_to_normal_block(log_dir, block_id, file_path, OB_MAX_FILE_NAME_LENGTH))) {
      PALF_LOG(WARN, "convert_to_normal_block failed", K(ret), K(log_dir), K(block_id));
    } else if (0 != unlink(file_path)){
      ret = convert_sys_errno();
      PALF_LOG(WARN, "unlink failed", K(ret), K(block_id), K(file_path));
    }
    return ret;
  }
  int write_several_blocks(const block_id_t base_block_id, const int block_count)
  {
    int64_t long_buf_len = 16383 * 128;
    LogWriteBuf write_buf;
    char *long_buf = reinterpret_cast<char *>(ob_malloc(long_buf_len, "test_log_engine"));
    LogGroupEntryHeader header;
    int64_t log_checksum;
    const block_id_t donot_delete_block_before_this = 3;
    write_buf.reset();
    memset(long_buf, 0, long_buf_len);
    EXPECT_EQ(OB_SUCCESS, write_buf.push_back(long_buf, long_buf_len));
    // EXPECT_EQ(32, write_buf.write_buf_.count());
    EXPECT_EQ(OB_SUCCESS,
              header.generate(false,
                              true,
                              write_buf,
                              long_buf_len - sizeof(LogGroupEntryHeader),
                              share::SCN::base_scn(),
                              1,
                              LSN(donot_delete_block_before_this * PALF_BLOCK_SIZE),
                              1,
                              log_checksum));
    header.update_header_checksum();
    int64_t pos = 0;
    EXPECT_EQ(OB_SUCCESS, header.serialize(long_buf, long_buf_len, pos));
    int ret = OB_SUCCESS;
    LogStorage &log_storage = leader_.palf_handle_impl_->log_engine_.log_storage_;
    block_id_t min_block_id = LOG_INVALID_BLOCK_ID, max_block_id = LOG_INVALID_BLOCK_ID;
    if (block_count == 0) {
      ret = OB_INVALID_ARGUMENT;
      return ret;
    }
    bool need_submit_log = true;
    if (OB_FAIL(log_storage.get_block_id_range(min_block_id, max_block_id)) && OB_ENTRY_NOT_EXIST != ret) {
      PALF_LOG(ERROR, "get_block_id_range failed", K(ret));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      min_block_id = base_block_id;
      max_block_id = base_block_id;
      ret = OB_SUCCESS;
    }
    block_id_t end_block_id = max_block_id + block_count;
    PALF_LOG(INFO, "runlin trace before", K(end_block_id), K(max_block_id));
    do {
      if (max_block_id < end_block_id) {
        need_submit_log = true;
        ret = OB_SUCCESS;
      } else {
        need_submit_log = false;
      }
      share::SCN tmp_scn;
      tmp_scn.convert_for_logservice(max_block_id);
      if (true == need_submit_log && OB_FAIL(log_storage.writev(log_storage.log_tail_, write_buf, tmp_scn))) {
        PALF_LOG(ERROR, "submit_log failed", K(ret));
      } else {
      }
      if (OB_FAIL(log_storage.get_block_id_range(min_block_id, max_block_id))) {
        PALF_LOG(ERROR, "get_block_id_range failed", K(ret));
      }
    } while (OB_SUCC(ret) && true == need_submit_log);
    PALF_LOG(INFO, "runlin trace after", K(end_block_id), K(max_block_id));
    return ret;
  }
  void destroy() {}
  int64_t id_;
  int64_t palf_epoch_;
  LogEngine *log_engine_;
  PalfHandleImplGuard leader_;
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 1;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 1;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;

// 验证flashback过程中宕机重启
TEST_F(TestObSimpleLogClusterLogEngine, flashback_restart)
{
  SET_CASE_LOG_FILE(TEST_NAME, "flashback_restart");
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin flashback_restart");
  PalfHandleImplGuard leader;
  int64_t id_1 = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx_1 = 0;
  PalfEnv *palf_env = NULL;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_1, leader_idx_1, leader));
  EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx_1, palf_env));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 66, leader_idx_1, MAX_LOG_BODY_SIZE));
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
  SCN scn;
  LogStorage *log_storage = &leader.get_palf_handle_impl()->log_engine_.log_storage_;
  LSN log_tail = log_storage->log_tail_;
  scn = leader.get_palf_handle_impl()->get_end_scn();
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 33, leader_idx_1, MAX_LOG_BODY_SIZE));
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
  int64_t mode_version;
  AccessMode mode;
  EXPECT_EQ(OB_SUCCESS, leader.get_palf_handle_impl()->get_access_mode(mode_version, mode));
  LSN flashback_lsn(PALF_BLOCK_SIZE*lsn_2_block(log_tail, PALF_BLOCK_SIZE));
  EXPECT_EQ(OB_SUCCESS, log_storage->begin_flashback(flashback_lsn));
  leader.reset();
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());

  {
    PalfHandleImplGuard leader1;
    EXPECT_EQ(OB_SUCCESS, get_leader(id_1, leader1, leader_idx_1));
    LogStorage *log_storage = &leader1.get_palf_handle_impl()->log_engine_.log_storage_;
    EXPECT_LE(2, log_storage->block_mgr_.max_block_id_);
    EXPECT_EQ(OB_SUCCESS, log_storage->block_mgr_.create_tmp_block_handler(2));
    EXPECT_EQ(OB_SUCCESS, log_storage->update_manifest_(3));
    EXPECT_EQ(OB_SUCCESS, log_storage->block_mgr_.delete_block_from_back_to_front_until(2));
    {
      LogBlockMgr *block_mgr = &log_storage->block_mgr_;
      int block_id = 2;
      int ret = OB_SUCCESS;
      // 1. rename "block_id.tmp" to "block_id.flashback"
      // 2. delete "block_id", make sure each block has returned into BlockPool
      // 3. rename "block_id.flashback" to "block_id"
      // NB: for restart, the block which named 'block_id.flashback' must be renamed to 'block_id'
      char tmp_block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
      char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
      char flashback_block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
      if (block_id != block_mgr->curr_writable_block_id_) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "block_id is not same as curr_writable_handler_, unexpected error",
            K(ret), K(block_id), KPC(block_mgr));
      } else if (OB_FAIL(block_id_to_string(block_id, block_path, OB_MAX_FILE_NAME_LENGTH))) {
	PALF_LOG(ERROR, "block_id_to_string failed", K(ret), K(block_id));
      } else if (OB_FAIL(block_id_to_tmp_string(block_id, tmp_block_path, OB_MAX_FILE_NAME_LENGTH))) {
	PALF_LOG(ERROR, "block_id_to_tmp_string failed", K(ret), K(block_id));
      } else if (OB_FAIL(block_id_to_flashback_string(block_id, flashback_block_path, OB_MAX_FILE_NAME_LENGTH))) {
	PALF_LOG(ERROR, "block_id_to_flashback_string failed", K(ret), K(block_id));
      } else if (OB_FAIL(block_mgr->do_rename_and_fsync_(tmp_block_path, flashback_block_path))) {
        PALF_LOG(ERROR, "do_rename_and_fsync_ failed", K(ret), KPC(block_mgr));
      } else {
        PALF_LOG(INFO, "rename_tmp_block_handler_to_normal success", K(ret), KPC(block_mgr));
      }
    }
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
}

TEST_F(TestObSimpleLogClusterLogEngine, exception_path)
{
  SET_CASE_LOG_FILE(TEST_NAME, "exception_path");
  EXPECT_EQ(OB_SUCCESS, init());
  OB_LOGGER.set_log_level("TRACE");
  // TODO: to be reopened by runlin.
  ObTenantMutilAllocator *allocator =
      dynamic_cast<ObTenantMutilAllocator *>(log_engine_->alloc_mgr_);
  OB_ASSERT(NULL != allocator);
  allocator->set_limit(32);
  FlushLogCbCtx flush_ctx;
  LogWriteBuf write_buf;
  const char *buf = "hello";
  EXPECT_FALSE(flush_ctx.is_valid());
  EXPECT_FALSE(write_buf.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_engine_->submit_flush_log_task(flush_ctx, write_buf));
  flush_ctx.lsn_ = LSN(1);
  flush_ctx.scn_ = share::SCN::base_scn();
  EXPECT_EQ(OB_INVALID_ARGUMENT, write_buf.push_back(NULL, strlen(buf)));
  EXPECT_EQ(OB_SUCCESS, write_buf.push_back(buf, strlen(buf)));
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, log_engine_->submit_flush_log_task(flush_ctx, write_buf));
  write_buf.reset();
  const int64_t long_buf_len = MAX_LOG_BODY_SIZE;
  char *long_buf = reinterpret_cast<char *>(ob_malloc(long_buf_len, "test_log_engine"));
  LogGroupEntryHeader header;
  int64_t log_checksum;
  const block_id_t donot_delete_block_before_this = 3;
  write_buf.reset();
  memset(long_buf, 0, long_buf_len);

  // Test LogStorage
  LogStorage *log_storage = &log_engine_->log_storage_;
  LogStorage *meta_storage = &log_engine_->log_meta_storage_;
  block_id_t min_block_id, max_block_id;
  share::SCN tmp_scn;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            log_engine_->append_log(LSN(LOG_INVALID_LSN_VAL), write_buf, tmp_scn));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_storage->writev(LSN(LOG_INVALID_LSN_VAL), write_buf, tmp_scn));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, log_engine_->get_block_id_range(min_block_id, max_block_id));
  EXPECT_EQ(LSN(0), log_engine_->get_begin_lsn());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, log_storage->get_block_id_range(min_block_id, max_block_id));
  EXPECT_EQ(LSN(0), log_storage->get_begin_lsn());
  EXPECT_EQ(OB_SUCCESS, log_storage->truncate_prefix_blocks(LSN(0)));
  EXPECT_EQ(true, log_storage->need_append_block_header_);
  EXPECT_EQ(true, log_storage->need_switch_block_());
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_storage->truncate(LSN(100000000)));
  // no block id 1
  EXPECT_EQ(OB_ERR_UNEXPECTED, log_storage->delete_block(1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, meta_storage->append_meta(buf, 10000000));

  int64_t log_id = 1;
  share::SCN scn = share::SCN::base_scn();
  LSN truncate_lsn;
  allocator->set_limit(1*1024*1024*1024);

  EXPECT_EQ(OB_SUCCESS, write_several_blocks(0, 11));
  PALF_LOG(INFO, "after write_several_blocks 11");

  EXPECT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
  EXPECT_EQ(0, min_block_id);
  EXPECT_EQ(11, max_block_id);

  // 测试truncate场景
  block_id_t truncate_block_id = max_block_id - 2;
  EXPECT_EQ(OB_SUCCESS, log_storage->truncate(LSN(truncate_block_id * PALF_BLOCK_SIZE)));
  EXPECT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
  // 此时最后一个block是空的
  EXPECT_EQ(log_storage->log_tail_, LSN(truncate_block_id * PALF_BLOCK_SIZE));
  EXPECT_EQ(truncate_block_id, max_block_id);
  EXPECT_EQ(lsn_2_block(log_engine_->log_meta_storage_.log_block_header_.min_lsn_, PALF_BLOCK_SIZE), truncate_block_id + 1);

  LogSnapshotMeta snapshot_meta;
  EXPECT_EQ(OB_SUCCESS, snapshot_meta.generate(LSN(1 * PALF_BLOCK_SIZE)));
  EXPECT_EQ(OB_SUCCESS, log_engine_->log_meta_.update_log_snapshot_meta(snapshot_meta));
  EXPECT_EQ(OB_SUCCESS, log_engine_->append_log_meta_(log_engine_->log_meta_));
  EXPECT_EQ(OB_SUCCESS, log_storage->delete_block(0));
  EXPECT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
  EXPECT_EQ(1, min_block_id);
  EXPECT_EQ(LSN(max_block_id * PALF_BLOCK_SIZE), log_storage->log_tail_);

  log_storage = log_engine_->get_log_storage();
  LogBlockHeader block_header;
  share::SCN scn_0;
  share::SCN scn_11;
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_storage->get_block_min_scn(0, scn_0));
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, log_storage->read_block_header_(0, block_header));
  EXPECT_EQ(OB_ERR_OUT_OF_UPPER_BOUND,
            log_storage->get_block_min_scn(truncate_block_id, scn_11));
  LSN log_tail = log_engine_->log_storage_.log_tail_;
  share::SCN ts_origin = scn_11;
  PALF_LOG(INFO, "after second write_several_blocks 1", K(truncate_block_id), K(max_block_id));
  // 由于truncate之后，最后一个文件是空的，因此max_block_id = truncate_block_id
  EXPECT_EQ(OB_SUCCESS, write_several_blocks(0, 1));
  EXPECT_EQ(OB_SUCCESS, log_storage->get_block_min_scn(truncate_block_id, scn_11));
  EXPECT_NE(scn_11, ts_origin);

  // 测试重启场景
  EXPECT_EQ(OB_SUCCESS, reload(log_engine_->log_storage_.log_tail_, log_engine_->log_meta_storage_.log_tail_, log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));
  PALF_LOG(INFO, "after reload1");

  //测试truncate_prefix 场景
  block_id_t truncate_prefix_block_id = 4;
  LogInfo prev_log_info;
  prev_log_info.lsn_ = LSN(truncate_prefix_block_id*PALF_BLOCK_SIZE);
  prev_log_info.log_id_ = 0;
  prev_log_info.log_proposal_id_ = 0;
  prev_log_info.scn_ = share::SCN::min_scn();
  prev_log_info.accum_checksum_ = 0;
  EXPECT_EQ(OB_SUCCESS, snapshot_meta.generate(prev_log_info.lsn_, prev_log_info));
  EXPECT_EQ(OB_SUCCESS, log_engine_->log_meta_.update_log_snapshot_meta(snapshot_meta));
  EXPECT_EQ(OB_SUCCESS, log_engine_->append_log_meta_(log_engine_->log_meta_));
  EXPECT_EQ(OB_SUCCESS,
            log_storage->truncate_prefix_blocks(LSN(truncate_prefix_block_id * PALF_BLOCK_SIZE)));
  // 测试truncate_prefix后,继续写一个block
  write_several_blocks(0, 1);
  EXPECT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
  EXPECT_EQ(truncate_prefix_block_id, min_block_id);
  EXPECT_EQ(truncate_block_id+2, max_block_id);

  // 测试目录清空场景，此时log_tail应该为truncate_prefix_block_id
  // 目录清空之后，会重置log_tail
  truncate_prefix_block_id = max_block_id + 2;
  prev_log_info.lsn_ = LSN(truncate_prefix_block_id*PALF_BLOCK_SIZE);
  prev_log_info.log_id_ = 0;
  prev_log_info.log_proposal_id_ = 0;
  prev_log_info.scn_ =SCN::min_scn();
  prev_log_info.accum_checksum_ = 0;
  EXPECT_EQ(OB_SUCCESS, snapshot_meta.generate(prev_log_info.lsn_, prev_log_info));
  EXPECT_EQ(OB_SUCCESS, log_engine_->log_meta_.update_log_snapshot_meta(snapshot_meta));
  EXPECT_EQ(OB_SUCCESS, log_engine_->append_log_meta_(log_engine_->log_meta_));
  const LSN old_log_tail = log_engine_->log_storage_.log_tail_;
  EXPECT_EQ(OB_SUCCESS, log_engine_->truncate_prefix_blocks(prev_log_info.lsn_));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, log_storage->get_block_id_range(min_block_id, max_block_id));
  // truncate_prefix_block_id 和 prev_lsn对应的block_id一样
  EXPECT_EQ(log_storage->log_tail_, LSN(truncate_prefix_block_id * PALF_BLOCK_SIZE));

  // 测试目录清空后，读数据是否正常报错
  ReadBufGuard buf_guard("dummy", 100);
  int64_t out_read_size;
  EXPECT_EQ(OB_ERR_OUT_OF_UPPER_BOUND,
            log_storage->pread(LSN((truncate_prefix_block_id + 1) * PALF_BLOCK_SIZE),
                               100,
                               buf_guard.read_buf_,
                               out_read_size));
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND,
            log_storage->pread(LSN((truncate_prefix_block_id - 1) * PALF_BLOCK_SIZE),
                               100,
                               buf_guard.read_buf_,
                               out_read_size));
  // 测试目录清空后，重启是否正常
  EXPECT_EQ(OB_SUCCESS, reload(log_engine_->log_storage_.log_tail_, log_engine_->log_meta_storage_.log_tail_, log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));

  PALF_LOG(INFO, "directory is empty");
  // 测试目录清空后，写数据是否正常
  // 此时log_tail为truncate_prefix_block_id的头部
  const block_id_t expected_min_block_id = lsn_2_block(log_storage->log_tail_, log_storage->logical_block_size_);
  EXPECT_EQ(OB_SUCCESS, write_several_blocks(expected_min_block_id, 3));
  EXPECT_EQ(OB_SUCCESS, log_storage->get_block_id_range(min_block_id, max_block_id));
  EXPECT_EQ(expected_min_block_id, min_block_id);
  EXPECT_EQ(expected_min_block_id+3, max_block_id);
  share::SCN scn_cur;
  EXPECT_EQ(OB_SUCCESS, log_engine_->get_block_min_scn(max_block_id, scn_cur));

  // 测试人为删除文件的重启场景
  EXPECT_EQ(OB_SUCCESS, log_engine_->get_block_id_range(min_block_id, max_block_id));
  EXPECT_EQ(OB_SUCCESS, delete_block_by_human(max_block_id));
  EXPECT_EQ(OB_ERR_UNEXPECTED, reload(log_engine_->log_storage_.log_tail_, log_engine_->log_meta_storage_.log_tail_, log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));
  EXPECT_EQ(OB_SUCCESS, delete_block_by_human(min_block_id));
  EXPECT_EQ(OB_ERR_UNEXPECTED, reload(log_engine_->log_storage_.log_tail_, log_engine_->log_meta_storage_.log_tail_, log_engine_->log_meta_.log_snapshot_meta_.base_lsn_));

  if (OB_NOT_NULL(long_buf)) {
    ob_free(long_buf);
  }
  leader_.reset();
  PALF_LOG(INFO, "end exception_path");
}


class IOTaskVerify : public LogIOTask {
public:
  IOTaskVerify(const int64_t palf_id, const int64_t palf_epoch) : LogIOTask(palf_id, palf_epoch), count_(0), after_consume_count_(0) {}
  virtual int do_task_(int tg_id, IPalfEnvImpl *palf_env_impl)
  {
    count_ ++;
    return OB_SUCCESS;
  };
  virtual int after_consume_(IPalfEnvImpl *palf_env_impl) { return OB_SUCCESS; }
  virtual LogIOTaskType get_io_task_type_() const { return LogIOTaskType::FLUSH_META_TYPE; }
  virtual void free_this_(IPalfEnvImpl *impl) {UNUSED(impl);}
  int64_t get_io_size_() const {return 0;}
  bool need_purge_throttling_() const {return true;}
  int init(int64_t palf_id)
  {
    palf_id_ = palf_id;
    return OB_SUCCESS;
  };
  int64_t count_;
  int64_t after_consume_count_;
};

TEST_F(TestObSimpleLogClusterLogEngine, io_reducer_basic_func)
{
  SET_CASE_LOG_FILE(TEST_NAME, "io_reducer_func");
  update_server_log_disk(4*1024*1024*1024ul);
  update_disk_options(4*1024*1024*1024ul/palf::PALF_PHY_BLOCK_SIZE);
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin io_reducer_basic_func");
  PalfHandleImplGuard leader_1;
  int64_t id_1 = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx_1 = 0;
  PalfEnv *palf_env = NULL;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_1, leader_idx_1, leader_1));
  EXPECT_EQ(OB_SUCCESS, get_palf_env(leader_idx_1, palf_env));

  LogIOWorker *log_io_worker = leader_1.palf_handle_impl_->log_engine_.log_io_worker_;

  int64_t prev_log_id_1 = 0;
  int64_t prev_has_batched_size = 0;
	LogEngine *log_engine = &leader_1.palf_handle_impl_->log_engine_;
	IOTaskCond io_task_cond_1(id_1, log_engine->palf_epoch_);
  IOTaskVerify io_task_verify_1(id_1, log_engine->palf_epoch_);
  // 单日志流场景
  // 卡住log_io_worker的处理
  {
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_1));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1024, id_1, 110));
    const int64_t log_id = leader_1.palf_handle_impl_->sw_.get_max_log_id();
    LSN max_lsn = leader_1.palf_handle_impl_->sw_.get_max_lsn();
    io_task_cond_1.cond_.signal();
    wait_lsn_until_flushed(max_lsn, leader_1);
    EXPECT_EQ(OB_ITER_END, read_log(leader_1));
    // sw内部做了自适应freeze之后这个等式可能不成立, 因为上层可能基于写盘反馈触发提交下一个io_task
//    EXPECT_EQ(log_id, log_io_worker->batch_io_task_mgr_.has_batched_size_);
    prev_log_id_1 = log_id;
    prev_has_batched_size = log_io_worker->batch_io_task_mgr_.has_batched_size_;
  }
  // 单日志流场景
  // 当聚合度为1的时候，应该走正常的提交流程，目前暂未实现，先通过has_batched_size不计算绕过
  {
    // 聚合度为1的忽略
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_1));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1, id_1, 110));
    sleep(1);
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_verify_1));
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_verify_1));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1024, id_1, 110));
    const int64_t log_id = leader_1.palf_handle_impl_->sw_.get_max_log_id();
    LSN max_lsn = leader_1.palf_handle_impl_->sw_.get_max_lsn();
    io_task_cond_1.cond_.signal();
    wait_lsn_until_flushed(max_lsn, leader_1);
//    EXPECT_EQ(log_id - 1, log_io_worker->batch_io_task_mgr_.has_batched_size_);
    EXPECT_EQ(2, io_task_verify_1.count_);
//    EXPECT_EQ(log_io_worker->batch_io_task_mgr_.has_batched_size_ - prev_has_batched_size,
//              log_id - prev_log_id_1 - 1);
    prev_log_id_1 = log_id;
    prev_has_batched_size = log_io_worker->batch_io_task_mgr_.has_batched_size_;
  }

  // 多日志流场景
  int64_t id_2 = ATOMIC_AAF(&palf_id_, 1);
  int64_t prev_log_id_2 = 0;
  int64_t leader_idx_2 = 0;
  PalfHandleImplGuard leader_2;
	IOTaskCond io_task_cond_2(id_2, log_engine->palf_epoch_);
  IOTaskVerify io_task_verify_2(id_2, log_engine->palf_epoch_);
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_2, leader_idx_2, leader_2));
  {
    LogIOWorker *log_io_worker = leader_2.palf_handle_impl_->log_engine_.log_io_worker_;
    // 聚合度为1的忽略
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_2));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1, id_1, 110));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_2, 1, id_2, 110));
    sleep(1);
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_verify_2));
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_verify_1));

    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1024, id_1, 110));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_2, 1024, id_2, 110));

    const int64_t log_id_1 = leader_1.palf_handle_impl_->sw_.get_max_log_id();
    LSN max_lsn_1 = leader_1.palf_handle_impl_->sw_.get_max_lsn();
    const int64_t log_id_2 = leader_2.palf_handle_impl_->sw_.get_max_log_id();
    LSN max_lsn_2 = leader_2.palf_handle_impl_->sw_.get_max_lsn();
    io_task_cond_2.cond_.signal();
    wait_lsn_until_flushed(max_lsn_1, leader_1);
    wait_lsn_until_flushed(max_lsn_2, leader_2);
    EXPECT_EQ(3, io_task_verify_1.count_);
    EXPECT_EQ(1, io_task_verify_2.count_);

    // ls1已经有个一个log_id被忽略聚合了
//    EXPECT_EQ(log_io_worker->batch_io_task_mgr_.has_batched_size_ - prev_has_batched_size,
//              log_id_1 - 1 + log_id_2 -1 - prev_log_id_1);
    prev_has_batched_size = log_io_worker->batch_io_task_mgr_.has_batched_size_;
    prev_log_id_2 = log_id_2;
    prev_log_id_1 = log_id_1;
  }

  // 三个日志流，stripe为2
  // 目前不支持可配的LogIOWorkerConfig，此测试暂时不打开，但结果是对的
  // int64_t id_3 = ATOMIC_AAF(&palf_id_, 1);
  // int64_t leader_idx_3 = 0;
  // int64_t prev_log_id_3 = 0;
  // PalfHandleImplGuard leader_3;
  // IOTaskCond io_task_cond_3;
  // IOTaskVerify io_task_verify_3;
  // io_task_cond_3.init(id_3);
  // io_task_verify_3.init(id_3);
  // EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_3, leader_idx_3, leader_3));
  // {
  //   EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_3));
  //   EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1, id_1, 110));
  //   EXPECT_EQ(OB_SUCCESS, submit_log(leader_2, 1, id_2, 110));
  //   EXPECT_EQ(OB_SUCCESS, submit_log(leader_3, 1, id_3, 110));
  //   EXPECT_EQ(OB_SUCCESS, submit_log(leader_2, 1, id_2, 110));
  //   sleep(1);
  //   io_task_cond_3.cond_.signal();
  //   const int64_t log_id_1 = leader_1.palf_handle_impl_->sw_.get_max_log_id();
  //   LSN max_lsn_1 = leader_1.palf_handle_impl_->sw_.get_max_lsn();
  //   const int64_t log_id_2 = leader_2.palf_handle_impl_->sw_.get_max_log_id();
  //   LSN max_lsn_2 = leader_2.palf_handle_impl_->sw_.get_max_lsn();
  //   const int64_t log_id_3 = leader_3.palf_handle_impl_->sw_.get_max_log_id();
  //   LSN max_lsn_3 = leader_3.palf_handle_impl_->sw_.get_max_lsn();
  //   wait_lsn_until_flushed(max_lsn_1, leader_1);
  //   wait_lsn_until_flushed(max_lsn_2, leader_2);
  //   wait_lsn_until_flushed(max_lsn_3, leader_3);
  //   EXPECT_EQ(log_io_worker->batch_io_task_mgr_.has_batched_size_ - prev_has_batched_size, 0);
  // }
  // 验证切文件场景
  int64_t id_3 = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx_3 = 0;
  int64_t prev_log_id_3 = 0;
  PalfHandleImplGuard leader_3;
	IOTaskCond io_task_cond_3(id_3, log_engine->palf_epoch_);
  IOTaskVerify io_task_verify_3(id_3, log_engine->palf_epoch_);
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_3, leader_idx_3, leader_3));
  {
    LogIOWorker *log_io_worker = leader_3.palf_handle_impl_->log_engine_.log_io_worker_;
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_3));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1, id_1, 110));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_2, 1, id_2, 110));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_3, 1, id_3, 110));
    sleep(1);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_2, 1, id_2, 110));
    sleep(1);
    io_task_cond_3.cond_.signal();
    const int64_t log_id_1 = leader_1.palf_handle_impl_->sw_.get_max_log_id();
    LSN max_lsn_1 = leader_1.palf_handle_impl_->sw_.get_max_lsn();
    const int64_t log_id_2 = leader_2.palf_handle_impl_->sw_.get_max_log_id();
    LSN max_lsn_2 = leader_2.palf_handle_impl_->sw_.get_max_lsn();
    const int64_t log_id_3 = leader_3.palf_handle_impl_->sw_.get_max_log_id();
    LSN max_lsn_3 = leader_3.palf_handle_impl_->sw_.get_max_lsn();
    wait_lsn_until_flushed(max_lsn_1, leader_1);
    wait_lsn_until_flushed(max_lsn_2, leader_2);
    wait_lsn_until_flushed(max_lsn_3, leader_3);
//  EXPECT_EQ(log_io_worker->batch_io_task_mgr_.has_batched_size_ - prev_has_batched_size, 2);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 31, leader_idx_1, MAX_LOG_BODY_SIZE));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 2, leader_idx_1, 900 *1024));
    max_lsn_1 = leader_1.palf_handle_impl_->get_max_lsn();
    wait_lsn_until_flushed(max_lsn_1, leader_1);

    PALF_LOG(INFO, "current log_tail", K(leader_1.palf_handle_impl_->get_max_lsn()));
    EXPECT_EQ(0, leader_1.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.min_block_id_);

    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1024, leader_idx_1, 300));
    max_lsn_1 = leader_1.palf_handle_impl_->get_max_lsn();
    wait_lsn_until_flushed(max_lsn_1, leader_1);
    EXPECT_EQ(2, leader_1.palf_handle_impl_->log_engine_.log_storage_.block_mgr_.max_block_id_);

    EXPECT_EQ(OB_SUCCESS, submit_log(leader_1, 1024, leader_idx_1, 300));
    max_lsn_1 = leader_1.palf_handle_impl_->get_max_lsn();
    wait_lsn_until_flushed(max_lsn_1, leader_1);
    EXPECT_EQ(OB_ITER_END, read_log(leader_1));
  }

  // 测试epoch change
  PALF_LOG(INFO, "begin test epoch change");
  int64_t id_4 = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx_4 = 0;
  int64_t prev_log_id_4 = 0;
  PalfHandleImplGuard leader_4;
	IOTaskCond io_task_cond_4(id_4, log_engine->palf_epoch_);
  IOTaskVerify io_task_verify_4(id_4, log_engine->palf_epoch_);
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_4, leader_idx_4, leader_4));
  {
    LogIOWorker *log_io_worker = leader_4.palf_handle_impl_->log_engine_.log_io_worker_;
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_4));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_4, 10, id_4, 110));
    sleep(1);
    LSN max_lsn = leader_4.palf_handle_impl_->sw_.get_max_lsn();
    io_task_cond_4.cond_.signal();
    PALF_LOG(INFO, "after signal");
    // signal之后需要sleep一会等前面的日志都提交给io_worker,
    // 否则在反馈模式下, 这批日志可能会延迟submit, 排在下一个cond task后面
    sleep(1);
    wait_lsn_until_flushed(max_lsn, leader_4);
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_4));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_4, 10, id_4, 110));
    sleep(1);
    leader_4.palf_handle_impl_->log_engine_.palf_epoch_++;
    io_task_cond_4.cond_.signal();
    LSN log_tail = leader_4.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
    PALF_LOG(INFO, "after signal", K(max_lsn), K(log_tail));
    wait_lsn_until_flushed(max_lsn, leader_4);
    sleep(1);
    log_tail = leader_4.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
    PALF_LOG(INFO, "after flused case 4", K(max_lsn), K(log_tail));
    EXPECT_EQ(max_lsn, log_tail);
  }

  // 测试truncate
  PALF_LOG(INFO, "begin test truncate");
  int64_t id_5 = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx_5 = 0;
  int64_t prev_log_id_5 = 0;
  PalfHandleImplGuard leader_5;
  IOTaskCond io_task_cond_5(id_5, log_engine->palf_epoch_);
  IOTaskVerify io_task_verify_5(id_5, log_engine->palf_epoch_);
  TruncateLogCbCtx ctx(LSN(0));
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_5, leader_idx_5, leader_5));
  {
    LogIOWorker *log_io_worker = leader_5.palf_handle_impl_->log_engine_.log_io_worker_;
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_5));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader_5, 10, id_5, 110));
    LSN max_lsn = leader_5.palf_handle_impl_->sw_.get_max_lsn();
    sleep(2);
    // 在提交truncate log task之前需先等待之前的日志提交写盘
    io_task_cond_5.cond_.signal();
    wait_lsn_until_flushed(max_lsn, leader_5);
    EXPECT_EQ(OB_SUCCESS, leader_5.palf_handle_impl_->log_engine_.submit_truncate_log_task(ctx));
    sleep(1);
    EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_5));
    sleep(1);
    io_task_cond_5.cond_.signal();
    // wait_lsn_until_flushed(max_lsn, leader_5);
    EXPECT_EQ(0, leader_5.palf_handle_impl_->log_engine_.log_storage_.log_tail_);
  }

  PALF_LOG(INFO, "begin test sw full case");
   // 测试滑动窗口满场景
   // 聚合的两条日志分别在头尾部
   int64_t id_6 = ATOMIC_AAF(&palf_id_, 1);
   int64_t leader_idx_6 = 0;
   int64_t prev_log_id_6 = 0;
   PalfHandleImplGuard leader_6;
   IOTaskCond io_task_cond_6(id_6, log_engine->palf_epoch_);
   IOTaskVerify io_task_verify_6(id_6, log_engine->palf_epoch_);
   EXPECT_EQ(OB_SUCCESS, create_paxos_group(id_6, leader_idx_6, leader_6));
   {
      LogIOWorker *log_io_worker = leader_6.palf_handle_impl_->log_engine_.log_io_worker_;
     {
       EXPECT_EQ(OB_SUCCESS, submit_log(leader_6, 15, id_6, MAX_LOG_BODY_SIZE));
       sleep(2);
       LSN max_lsn = leader_6.palf_handle_impl_->sw_.get_max_lsn();
       wait_lsn_until_flushed(max_lsn, leader_6);
       EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_6));
       EXPECT_EQ(OB_SUCCESS, submit_log(leader_6, 1, id_6, 10*1024));
       sleep(1);
       LSN max_lsn1 = leader_6.palf_handle_impl_->sw_.get_max_lsn();
       int64_t remain_size = LEADER_DEFAULT_GROUP_BUFFER_SIZE - max_lsn1.val_ - LogGroupEntryHeader::HEADER_SER_SIZE - LogEntryHeader::HEADER_SER_SIZE;
       EXPECT_EQ(OB_SUCCESS, submit_log(leader_6, 1, id_6, remain_size));
       sleep(1);
       LSN max_lsn2 = leader_6.palf_handle_impl_->sw_.get_max_lsn();
       PALF_LOG_RET(ERROR, OB_SUCCESS, "runlin trace", K(max_lsn2), K(max_lsn1), K(remain_size), K(max_lsn));
       EXPECT_EQ(max_lsn2, LSN(LEADER_DEFAULT_GROUP_BUFFER_SIZE));
       io_task_cond_6.cond_.signal();
       wait_lsn_until_flushed(max_lsn2, leader_6);
     }
     EXPECT_EQ(OB_SUCCESS, submit_log(leader_6, 3, id_6, MAX_LOG_BODY_SIZE));
     sleep(2);
     LSN max_lsn = leader_6.palf_handle_impl_->sw_.get_max_lsn();
     wait_lsn_until_flushed(max_lsn, leader_6);
     EXPECT_EQ(OB_SUCCESS, log_io_worker->submit_io_task(&io_task_cond_6));
     EXPECT_EQ(OB_SUCCESS, submit_log(leader_6, 1, id_6, 10*1024));
     sleep(1);
     LSN max_lsn1 = leader_6.palf_handle_impl_->sw_.get_max_lsn();
     int64_t remain_size = FOLLOWER_DEFAULT_GROUP_BUFFER_SIZE - max_lsn1.val_ - LogGroupEntryHeader::HEADER_SER_SIZE - LogEntryHeader::HEADER_SER_SIZE;
     EXPECT_EQ(OB_SUCCESS, submit_log(leader_6, 1, id_6, remain_size));
     sleep(1);
     LSN max_lsn2 = leader_6.palf_handle_impl_->sw_.get_max_lsn();
     PALF_LOG_RET(ERROR, OB_SUCCESS, "runlin trace", K(max_lsn2), K(max_lsn1), K(remain_size), K(max_lsn));
     EXPECT_EQ(max_lsn2, LSN(FOLLOWER_DEFAULT_GROUP_BUFFER_SIZE));
     EXPECT_EQ(OB_SUCCESS, submit_log(leader_6, 1, id_6, 100));
     sleep(1);
     LSN max_lsn3 = leader_6.palf_handle_impl_->sw_.get_max_lsn();
     io_task_cond_6.cond_.signal();
     //EXPECT_EQ(max_lsn, leader_6.palf_handle_.palf_handle_impl_->log_engine_.log_storage_.log_tail_);
     wait_lsn_until_flushed(max_lsn3, leader_6);
     LSN log_tail = leader_6.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
     EXPECT_EQ(max_lsn3, log_tail);
   }

  PALF_LOG(INFO, "end io_reducer_basic_func");
}

//TEST_F(TestObSimpleLogClusterLogEngine, io_reducer_performance)
//{
//  SET_CASE_LOG_FILE(TEST_NAME, "io_reducer_performance");
//
//  OB_LOGGER.set_log_level("ERROR");
//  int64_t id = ATOMIC_AAF(&palf_id_, 1);
//  int64_t leader_idx = 0;
//  PalfHandleImplGuard leader;
//  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
//  leader.palf_env_impl_->log_io_worker_.batch_io_task_mgr_.has_batched_size_ = 0;
//  leader.palf_env_impl_->log_io_worker_.batch_io_task_mgr_.handle_count_ = 0;
//  int64_t start_ts = ObTimeUtility::current_time();
//  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 40 * 10000, leader_idx, 100));
//  const LSN max_lsn = leader.palf_handle_impl_->get_max_lsn();
//  wait_lsn_until_flushed(max_lsn, leader);
//  const int64_t has_batched_size = leader.palf_env_impl_->log_io_worker_.batch_io_task_mgr_.has_batched_size_;
//  const int64_t handle_count = leader.palf_env_impl_->log_io_worker_.batch_io_task_mgr_.handle_count_;
//  const int64_t log_id = leader.palf_handle_impl_->sw_.get_max_log_id();
//  int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
//  PALF_LOG(ERROR, "runlin trace performance", K(cost_ts), K(log_id), K(max_lsn), K(has_batched_size), K(handle_count));
//}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv) { RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME); }
