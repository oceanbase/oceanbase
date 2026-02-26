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

#define USING_LOG_PREFIX STORAGE

#include "ob_tenant_checkpoint_slog_handler.h"
#include "storage/slog_ckpt/ob_tablet_replay_create_handler.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "storage/meta_store/ob_storage_meta_io_util.h"
#include "storage/slog/ob_storage_log_reader.h"
#include "observer/omt/ob_tenant.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/slog_ckpt/ob_tenant_meta_snapshot_handler.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace blocksstable;
using namespace observer;
namespace storage
{

ObLSCkptMember::ObLSCkptMember()
  : version_(LS_CKPT_MEM_VERSION), length_(0),
    ls_meta_(), dup_ls_meta_(), tablet_meta_entry_()
{
}

ObLSCkptMember::~ObLSCkptMember()
{
  reset();
}

void ObLSCkptMember::reset()
{
  version_ = LS_CKPT_MEM_VERSION;
  length_ = 0;
  ls_meta_.reset();
  tablet_meta_entry_.reset();
  dup_ls_meta_.reset();
}

bool ObLSCkptMember::is_valid() const
{
  return ls_meta_.is_valid()
         && dup_ls_meta_.is_valid()
         && tablet_meta_entry_.is_valid();
}

int ObLSCkptMember::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  const int64_t length = get_serialize_size();

  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (LS_CKPT_MEM_VERSION != version_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid version", K(ret), K_(version));
  } else if (OB_UNLIKELY(length_ > buf_len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer's length is not enough", K(ret), K_(length), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, new_pos, version_))) {
    LOG_WARN("fail to serialize ObLSCkptMember's version", K(ret), K(buf_len), K(new_pos), K(version_));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i32(buf, buf_len, new_pos, length))) {
    LOG_WARN("fail to serialize ObLSCkptMember's length", K(ret), K(buf_len), K(new_pos), K(length));
  } else if (new_pos - pos < length && OB_FAIL(ls_meta_.serialize(buf, buf_len, new_pos))) {
    LOG_WARN("fail to serialize ls meta", K(ret), K(buf_len), K(new_pos), K(ls_meta_));
  } else if (new_pos - pos < length && OB_FAIL(dup_ls_meta_.serialize(buf, buf_len, new_pos))) {
    LOG_WARN("fail to serialize dup ls meta", K(ret), K(buf_len), K(new_pos), K(dup_ls_meta_));
  } else if (new_pos - pos < length && OB_FAIL(tablet_meta_entry_.serialize(buf, buf_len, new_pos))) {
    LOG_WARN("fail to serialize tablet meta entry", K(ret), K(buf_len), K(new_pos), K(tablet_meta_entry_));
  } else if (OB_UNLIKELY(length != new_pos - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("length doesn't match", K(ret), K(length), K(new_pos), K(pos));
  } else {
    pos = new_pos;
  }

  return ret;
}

int ObLSCkptMember::deserialize(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(pos < 0) || OB_UNLIKELY(buf_len <= pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, buf_len, new_pos, (int32_t *)&version_))) {
    LOG_WARN("fail to deserialize ObLSCkptMember's version", K(ret), K(buf_len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, buf_len, new_pos, (int32_t *)&length_))) {
    LOG_WARN("fail to deserialize ObLSCkptMember's length", K(ret), K(buf_len), K(new_pos));
  } else if (OB_UNLIKELY(length_ > buf_len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer's length is not enough", K(ret), K(length_), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(LS_CKPT_MEM_VERSION != version_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ObLSCkptMember's version is invalid", K(ret), K(version_));
  } else if (new_pos - pos < length_ && OB_FAIL(ls_meta_.deserialize(buf, buf_len, new_pos))) {
    LOG_WARN("fail to deserialize ls meta", K(ret), K(buf_len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(dup_ls_meta_.deserialize(buf, buf_len, new_pos))) {
    LOG_WARN("fail to deserialize dup ls meta", K(ret), K(buf_len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(tablet_meta_entry_.deserialize(buf, buf_len, new_pos))) {
    LOG_WARN("fail to deserialize tablet meta entry", K(ret), K(buf_len), K(new_pos));
  } else if (OB_UNLIKELY(length_ != new_pos - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("length doesn't match", K(ret), K(buf_len), K(new_pos));
  } else {
    pos = new_pos;
  }

  return ret;
}

int64_t ObLSCkptMember::get_serialize_size() const
{
  int64_t size = 0;
  size += serialization::encoded_length_i32(version_);
  size += serialization::encoded_length_i32(length_);
  size += ls_meta_.get_serialize_size();
  size += dup_ls_meta_.get_serialize_size();
  size += tablet_meta_entry_.get_serialize_size();
  return size;
}

void ObTenantCheckpointSlogHandler::ObWriteCheckpointTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  if (SERVER_STORAGE_META_SERVICE.is_started()) {
    if (OB_FAIL(handler_->write_checkpoint(ObTenantSlogCheckpointWorkflow::normal_type()))) {
      LOG_WARN("fail to write checkpoint", K(ret));
    }
  } else {
    // 必须等待所有的slog回放完成才能做ckpt,否则有些macro block可能没有被mark
    LOG_INFO("slog replay not finish, do not write checkpoint");
  }
}

ObTenantCheckpointSlogHandler::ObTenantCheckpointSlogHandler()
  : is_inited_(false),
    is_writing_checkpoint_(false),
    slogger_(nullptr),
    lock_(common::ObLatchIds::SLOG_CKPT_LOCK),
    ls_block_handle_(),
    tablet_block_handle_(),
    wait_gc_tablet_block_handle_(),
    tg_id_(-1),
    write_ckpt_task_(this),
    replay_tablet_disk_addr_map_(),
    replay_wait_gc_tablet_set_(),
    replay_gc_tablet_set_(),
    super_block_mutex_(),
    ckpt_info_()
{
}

int ObTenantCheckpointSlogHandler::init(ObStorageLogger &slogger)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantCheckpointSlogHandler has inited", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::WriteCkpt, tg_id_))) {
    LOG_WARN("fail to tg create tenant", K(ret));
  } else {
    slogger_ = &slogger;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::start()
{
  int ret = OB_SUCCESS;
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  const ObTenantSuperBlock super_block = tenant->get_super_block();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!super_block.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant super block invalid", K(ret), K(super_block));
  } else if (OB_FAIL(replay_checkpoint_and_slog(super_block))) {
    LOG_WARN("fail to read_checkpoint_and_replay_slog", K(ret), K(super_block));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("ObTenantCheckpointSlogHandler TG_START failed", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, write_ckpt_task_,
               ObWriteCheckpointTask::WRITE_CHECKPOINT_INTERVAL_US, true))) {
    LOG_WARN("WriteCheckpointTask TG_SCHEDULE failed", K(ret));
  }
  return ret;
}

void ObTenantCheckpointSlogHandler::stop()
{
  if (IS_INIT) {
    TG_STOP(tg_id_);
  }
  // since the ls service deletes tablets in the stop interface when observer stop,
  // it must ensure that no checkpoint is in progress after stop, otherwise the tablet may be lost
  wait();
}

void ObTenantCheckpointSlogHandler::wait()
{
  if (IS_INIT) {
    TG_WAIT(tg_id_);
  }
}

void ObTenantCheckpointSlogHandler::destroy()
{
  if (IS_INIT) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    TG_DESTROY(tg_id_);
    slogger_ = nullptr;
    ls_block_handle_.reset();
    tablet_block_handle_.reset();
    wait_gc_tablet_block_handle_.reset();
    tg_id_ = -1;
    replay_tablet_disk_addr_map_.destroy();
    replay_wait_gc_tablet_set_.destroy();
    replay_gc_tablet_set_.destroy();
    is_inited_ = false;
  }
}

int ObTenantCheckpointSlogHandler::replay_checkpoint_and_slog(const ObTenantSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
  const ObMemAttr mem_attr(MTL_ID(), "TenantReplay");
  const int64_t replay_tablet_cnt = 10003;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCheckpointSlogHandler not init", K(ret));
  } else if (OB_FAIL(replay_tablet_disk_addr_map_.create(replay_tablet_cnt, mem_attr, mem_attr))) {
    LOG_WARN("fail to create replay map", K(ret));
  } else if (OB_FAIL(replay_wait_gc_tablet_set_.create(replay_tablet_cnt, mem_attr, mem_attr))) {
    LOG_WARN("fail to create replay wait gc tablet set", K(ret));
  } else if (OB_FAIL(replay_gc_tablet_set_.create(replay_tablet_cnt, mem_attr, mem_attr))) {
    LOG_WARN("fail to create replay gc tablet set", K(ret));
  } else if (OB_FAIL(replay_snapshot(super_block))) {
    LOG_WARN("fail to replay snapshot", K(ret), K(super_block));
  } else if (OB_FAIL(replay_checkpoint(super_block))) {
    LOG_WARN("fail to read_ls_checkpoint", K(ret), K(super_block));
  } else if (OB_FAIL(replay_tenant_slog(super_block.replay_start_point_))) {
    LOG_WARN("fail to replay_tenant_slog", K(ret), K(super_block));
  } else {
    replay_tablet_disk_addr_map_.destroy();
    replay_wait_gc_tablet_set_.destroy();
    replay_gc_tablet_set_.destroy();
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::replay_checkpoint(const ObTenantSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
  const bool is_replay_old = super_block.is_trivial_version();
  replay_tablet_disk_addr_map_.reuse();

  if (OB_UNLIKELY(is_replay_old)) {
    if (OB_FAIL(replay_old_checkpoint(super_block))) {
      LOG_WARN("fail to replay old version checkpoint", K(ret), K(super_block));
    }
  } else {
    if (OB_FAIL(replay_new_checkpoint(super_block))) {
      LOG_WARN("fail to replay new version checkpoint", K(ret), K(super_block));
    }
  }

  LOG_INFO("finish replay tenant checkpoint", K(ret), K(super_block));

  return ret;
}

int ObTenantCheckpointSlogHandler::replay_old_checkpoint(const ObTenantSuperBlock &super_block)
{
  int ret = OB_SUCCESS;

  ObTenantStorageCheckpointReader tenant_storage_ckpt_reader;
  ObArray<MacroBlockId> meta_block_list;

  ObTenantStorageCheckpointReader::ObStorageMetaOp replay_ls_op =
      std::bind(&ObTenantCheckpointSlogHandler::replay_ls_meta,
      this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

  ObTenantStorageCheckpointReader::ObStorageMetaOp replay_tablet_op =
      std::bind(&ObTenantCheckpointSlogHandler::replay_tablet,
      this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

  if (!replay_ls_op.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replay_ls_op invalid", K(ret));
  } else if (!replay_tablet_op.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replay_tablet_op invalid", K(ret));
  } else if (OB_FAIL(tenant_storage_ckpt_reader.iter_read_meta_item(
      super_block.ls_meta_entry_, replay_ls_op, meta_block_list))) {
    LOG_WARN("fail to replay ls meta checkpoint", K(ret));
  } else if (OB_FAIL(ls_block_handle_.add_macro_blocks(meta_block_list))) {
    LOG_WARN("fail to add_macro_blocks", K(ret));
  } else if (OB_FAIL(tenant_storage_ckpt_reader.iter_read_meta_item(
      super_block.tablet_meta_entry_, replay_tablet_op, meta_block_list))) {
    LOG_WARN("fail to replay tablet checkpoint", K(ret));
  } else if (OB_FAIL(tablet_block_handle_.add_macro_blocks(meta_block_list))) {
    LOG_WARN("fail to add_macro_blocks", K(ret));
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::replay_ls_meta(
  const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  UNUSEDx(addr);
  ObLSMeta ls_meta;
  int64_t pos = 0;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(ls_meta.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize", K(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->replay_create_ls(ls_meta.get_ls_epoch(), ls_meta))) {
    LOG_WARN("fail to replay_put_ls", K(ret));
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::replay_tablet(
    const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!addr.is_valid() || nullptr == buf || buf_len <= 0 || !addr.is_block())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(addr));
  } else if (OB_FAIL(inner_replay_old_deserialize(
      addr,
      buf,
      buf_len,
      false /* allow to overwrite the map's element or not */))) {
    LOG_WARN("fail to replay old tablet", K(ret), K(addr), KP(buf), K(buf_len));
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::replay_snapshot(const ObTenantSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < super_block.snapshot_cnt_; i++) {
    const ObTenantSnapshotMeta &snapshot = super_block.tenant_snapshots_[i];
    replay_tablet_disk_addr_map_.reuse();
    if (OB_FAIL(do_replay_single_snapshot(snapshot.ls_meta_entry_))) {
      LOG_WARN("fail to replay single snapshot", K(ret), K(snapshot));
    }
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::do_replay_single_snapshot(const blocksstable::MacroBlockId &ls_meta_entry)
{
  int ret = OB_SUCCESS;
  ObTenantStorageCheckpointReader ls_ckpt_reader;
  ObSArray<MacroBlockId> ls_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("ReplaySnap", MTL_ID()));
  ObTenantStorageCheckpointReader::ObStorageMetaOp replay_snapshot_ls_op = std::bind(
      &ObTenantCheckpointSlogHandler::replay_snapshot_ls,
      this,
      std::placeholders::_1,
      std::placeholders::_2,
      std::placeholders::_3);
  bool inc_ls_blocks_ref_succ = false;

  if (OB_FAIL(ls_ckpt_reader.iter_read_meta_item(
      ls_meta_entry, replay_snapshot_ls_op, ls_block_list))) {
    LOG_WARN("fail to iter replay ls", K(ret), K(ls_meta_entry));
  } else if (OB_FAIL(ObTenantMetaSnapshotHandler::inc_linked_block_ref(
      ls_block_list, inc_ls_blocks_ref_succ))) {
    LOG_WARN("fail to increase ls linked blocks' ref cnt", K(ret));
  } else {
    ObTabletReplayCreateHandler handler;
    if (OB_FAIL(handler.init(replay_tablet_disk_addr_map_, ObTabletRepalyOperationType::REPLAY_INC_MACRO_REF))) {
      LOG_WARN("fail to init ObTabletReplayCreateHandler", K(ret));
    } else if (OB_FAIL(handler.concurrent_replay(GCTX.startup_accel_handler_))) {
      LOG_WARN("fail to concurrent replay tablets", K(ret));
    }
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::replay_snapshot_ls(
    const ObMetaDiskAddr &addr,
    const char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObLSCkptMember ls_ckpt_member;
  int64_t pos = 0;
  ObTenantStorageCheckpointReader tablet_ckpt_reader;
  ObSArray<MacroBlockId> tablet_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("ReplaySnap", MTL_ID()));
  ObTenantStorageCheckpointReader::ObStorageMetaOp replay_tablet_op =
      std::bind(&ObTenantCheckpointSlogHandler::replay_new_tablet,
      this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
  bool inc_tablet_blocks_ref_succ = false;

  if (OB_FAIL(ls_ckpt_member.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize ls_ckpt_member", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(tablet_ckpt_reader.iter_read_meta_item(
      ls_ckpt_member.tablet_meta_entry_, replay_tablet_op, tablet_block_list))) {
    LOG_WARN("fail to iter replay tablet", K(ret), K(ls_ckpt_member));
  } else if (OB_FAIL(ObTenantMetaSnapshotHandler::inc_linked_block_ref(
      tablet_block_list, inc_tablet_blocks_ref_succ))) {
    LOG_WARN("fail to increase tablet linked blocks' ref cnt", K(ret));
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::replay_new_checkpoint(const ObTenantSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
#define UPDATE_MIN_MAX_FILE_ID(block_list)                                \
  for (int64_t i = 0; OB_SUCC(ret) && i < block_list.count(); ++i) {      \
    const MacroBlockId &macro_id = block_list.at(i);                      \
    if (macro_id.fourth_id() > max_file_id) {                             \
      max_file_id = macro_id.fourth_id();                                 \
    }                                                                     \
    if (macro_id.fourth_id() < min_file_id) {                             \
      min_file_id = macro_id.fourth_id();                                 \
    }                                                                     \
  }
  ObSArray<MacroBlockId> wait_gc_tablet_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("ReplayCKPT", MTL_ID()));
  ObSArray<MacroBlockId> tablet_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("ReplayCKPT", MTL_ID()));
  ObSArray<MacroBlockId> ls_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("ReplayCKPT", MTL_ID()));
  ObTenantStorageCheckpointReader::ObStorageMetaOp replay_ls_op = std::bind(
      &ObTenantCheckpointSlogHandler::replay_new_ls,
      this,
      std::placeholders::_1,
      std::placeholders::_2,
      std::placeholders::_3,
      std::ref(tablet_block_list));
  ObTenantStorageCheckpointReader::ObStorageMetaOp replay_wait_gc_tablet_op = std::bind(
      &ObTenantStorageMetaService::replay_wait_gc_tablet_array,
      &TENANT_STORAGE_META_SERVICE,
      std::placeholders::_1,
      std::placeholders::_2,
      std::placeholders::_3);

  if (OB_FAIL(ObTenantStorageCheckpointReader::iter_read_meta_item(
      super_block.ls_meta_entry_, replay_ls_op, ls_block_list))) {
    LOG_WARN("fail to iter replay ls", K(ret), K(super_block));
  } else if (OB_FAIL(ls_block_handle_.add_macro_blocks(ls_block_list))) {
    LOG_WARN("fail to add ls linked blocks", K(ret), K(ls_block_list));
  } else if (OB_FAIL(tablet_block_handle_.add_macro_blocks(tablet_block_list))) {
    LOG_WARN("fail to add tablet linked blocks", K(ret), K(tablet_block_list));
  } else if (GCTX.is_shared_storage_mode() && OB_FAIL(ObTenantStorageCheckpointReader::iter_read_meta_item(
          super_block.wait_gc_tablet_entry_, replay_wait_gc_tablet_op, wait_gc_tablet_block_list))) {
    LOG_WARN("fail to iter replay wait gc tablet array", K(ret));
  } else {
    omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
    const int64_t blk_cnt = ls_block_list.count() + tablet_block_list.count() + wait_gc_tablet_block_list.count();
    int64_t min_file_id = INT64_MAX;
    int64_t max_file_id = INT64_MIN;
    UPDATE_MIN_MAX_FILE_ID(ls_block_list);
    UPDATE_MIN_MAX_FILE_ID(tablet_block_list);
    UPDATE_MIN_MAX_FILE_ID(wait_gc_tablet_block_list);
    if (OB_SUCC(ret)) {
      if (0 == blk_cnt) { // nothing to do.
      } else if (OB_UNLIKELY(max_file_id < min_file_id || min_file_id < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, min/max file id is invalid", K(ret), K(min_file_id), K(max_file_id));
      } else if (GCTX.is_shared_storage_mode()) {
        HEAP_VAR(ObTenantSuperBlock, super_block) {
          lib::ObMutexGuard guard(super_block_mutex_);
          super_block = tenant->get_super_block();
          super_block.min_file_id_ = min_file_id;
          super_block.max_file_id_ = max_file_id;
          tenant->set_tenant_super_block(super_block);
          FLOG_INFO("update min max file id in super block", K(min_file_id), K(max_file_id), K(super_block));
        }
      }
    }
  }
#undef UPDATE_MIN_MAX_FILE_ID
  return ret;
}

int ObTenantCheckpointSlogHandler::replay_new_ls(
    const ObMetaDiskAddr &addr,
    const char *buf,
    const int64_t buf_len,
    ObIArray<MacroBlockId> &tablet_block_list)
{
  int ret = OB_SUCCESS;
  ObLSCkptMember ls_ckpt_member;
  int64_t pos = 0;
  ObSArray<MacroBlockId> tmp_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("ReplayCKPT", MTL_ID()));
  ObTenantStorageCheckpointReader tablet_ckpt_reader;
  ObTenantStorageCheckpointReader::ObStorageMetaOp replay_tablet_op =
      std::bind(&ObTenantCheckpointSlogHandler::replay_new_tablet,
      this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

  if (OB_FAIL(ls_ckpt_member.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize ls_ckpt_member", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(MTL(ObLSService *)->replay_create_ls(ls_ckpt_member.ls_meta_.get_ls_epoch(), ls_ckpt_member.ls_meta_))) {
    LOG_WARN("fail to replay put ls", K(ret), K(ls_ckpt_member));
  } else if (OB_FAIL(replay_dup_table_ls_meta(ls_ckpt_member.dup_ls_meta_))) {
    LOG_WARN("fail to replay set dup table ls meta", K(ret), K(ls_ckpt_member));
  } else if (OB_FAIL(tablet_ckpt_reader.iter_read_meta_item(
      ls_ckpt_member.tablet_meta_entry_, replay_tablet_op, tmp_block_list))) {
    LOG_WARN("fail to iter replay tablet", K(ret), K(ls_ckpt_member));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_block_list.count(); i++) {
      if (OB_FAIL(tablet_block_list.push_back(tmp_block_list.at(i)))) {
        LOG_WARN("fail to push back macro block id", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::replay_new_tablet(
    const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_replay_deserialize(
      buf, buf_len, false /* allow to overwrite the map's element or not */))) {
    LOG_WARN("fail to replay tablet", K(ret), KP(buf), K(buf_len));
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::replay_dup_table_ls_meta(
    const transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta &dup_ls_meta)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (OB_FAIL(MTL(ObLSService *)
                         ->get_ls(dup_ls_meta.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    if (OB_LS_NOT_EXIST == ret) {
      LOG_INFO("this is possible when writing ls checkpoint but ls is removing", K(dup_ls_meta));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get ls", K(ret), K(dup_ls_meta));
    }
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(dup_ls_meta));
  } else if (OB_FAIL(ls->set_dup_table_ls_meta(dup_ls_meta))) {
    LOG_WARN("set dup ls meta failed", K(ret), K(dup_ls_meta));
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::replay_tenant_slog(const common::ObLogCursor &start_point)
{
  int ret = OB_SUCCESS;
  ObLogCursor replay_finish_point;
  ObStorageLogReplayer replayer;
  blocksstable::ObLogFileSpec log_file_spec;
  log_file_spec.retry_write_policy_ = "normal";
  log_file_spec.log_create_policy_ = "normal";
  log_file_spec.log_write_policy_ = "truncate";

  if (OB_FAIL(replayer.init(slogger_->get_dir(), log_file_spec))) {
    LOG_WARN("fail to init slog replayer", K(ret));
  } else if (OB_FAIL(replayer.register_redo_module(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE, this))) {
    LOG_WARN("fail to register redo module", K(ret));
  } else if (OB_FAIL(replayer.replay(start_point, replay_finish_point, MTL_ID()))) {
    LOG_WARN("fail to replay tenant slog", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode() && OB_FAIL(TENANT_STORAGE_META_SERVICE.replay_apply_wait_gc_tablet_items(
          replay_wait_gc_tablet_set_, replay_gc_tablet_set_))) {
    LOG_WARN("fail to replay apply wait gc tablet items", K(ret));
#endif
  } else {
    ObTabletReplayCreateHandler handler;
    if (OB_FAIL(handler.init(replay_tablet_disk_addr_map_, ObTabletRepalyOperationType::REPLAY_CREATE_TABLET))) {
      LOG_WARN("fail to init ObTabletReplayCreateHandler", K(ret));
    } else if (OB_FAIL(handler.concurrent_replay(GCTX.startup_accel_handler_))) {
      LOG_WARN("fail to concurrent replay tablets", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(replayer.replay_over())) {
    LOG_WARN("fail to replay over", K(ret));
  } else if (OB_FAIL(slogger_->start_log(replay_finish_point))) {
    LOG_WARN("fail to start_slog", K(ret), K(replay_finish_point));
  }

  LOG_INFO("finish replay tenant slog", K(ret), K(start_point), K(replay_finish_point));

  return ret;
}

int ObTenantCheckpointSlogHandler::clone_ls(ObStartupAccelTaskHandler* startup_accel_handler,
                                            const blocksstable::MacroBlockId &tablet_meta_entry)
{
  int ret = OB_SUCCESS;
  const ObMemAttr mem_attr(MTL_ID(), "SnapCreate");
  const int64_t replay_tablet_cnt = 10003;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCheckpointSlogHandler not init", K(ret));
  } else if (OB_ISNULL(startup_accel_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("startup_accel_handler is unexpected nullptr", K(ret));
  } else if (OB_FAIL(replay_tablet_disk_addr_map_.create(replay_tablet_cnt, mem_attr, mem_attr))) {
    LOG_WARN("fail to create replay map", K(ret));
  } else {
    ObTenantStorageCheckpointReader tablet_snapshot_reader;
    ObSArray<MacroBlockId> meta_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("SnapCreate", MTL_ID()));
    ObArenaAllocator arena_allocator("SnapRecovery", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTenantStorageCheckpointReader::ObStorageMetaOp clone_tablet_op = std::bind(
        &ObTenantCheckpointSlogHandler::clone_tablet,
        this,
        std::placeholders::_1,
        std::placeholders::_2,
        std::placeholders::_3);

    if (OB_UNLIKELY(!tablet_meta_entry.is_valid() || IS_EMPTY_BLOCK_LIST(tablet_meta_entry))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg", K(ret), K(tablet_meta_entry));
    } else if (OB_FAIL(tablet_snapshot_reader.iter_read_meta_item(
        tablet_meta_entry, clone_tablet_op, meta_block_list))) {
      LOG_WARN("fail to iter create tablet", K(ret), K(tablet_meta_entry));
    } else {
      ObTabletReplayCreateHandler handler;
      if (OB_FAIL(handler.init(replay_tablet_disk_addr_map_, ObTabletRepalyOperationType::REPLAY_CLONE_TABLET))) {
        LOG_WARN("fail to init ObTabletReplayCreateHandler", K(ret));
      } else if (OB_FAIL(handler.concurrent_replay(startup_accel_handler))) {
        LOG_WARN("fail to concurrent replay tablets", K(ret));
      }
    }
    replay_tablet_disk_addr_map_.destroy();
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::clone_tablet(
    const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObUpdateTabletLog slog;
  ObTabletMapKey tablet_key;
  int64_t pos = 0;
  if (OB_FAIL(slog.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize create tablet slog", K(ret), K(pos), K(buf_len), K(slog));
  } else if (OB_UNLIKELY(!slog.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("slog is invalid", K(ret), K(slog));
  } else {
    tablet_key.ls_id_ = slog.ls_id_;
    tablet_key.tablet_id_ = slog.tablet_id_;
    const ObReplayTabletValue value(slog.disk_addr_,
                                    slog.ls_epoch_,
                                    slog.tablet_attr_,
                                    slog.accelerate_info_);
    if (OB_FAIL(replay_tablet_disk_addr_map_.set_refactored(tablet_key, value, /*allow_override*/ 0))) {
      LOG_WARN("fail to update tablet meta addr", K(ret), K(slog));
    }
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::write_checkpoint(const ObTenantSlogCheckpointWorkflow::Type ckpt_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  omt::ObTenant *tenant = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCheckpointSlogHandler not init", K(ret));
  } else if (OB_ISNULL(tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null tenant", K(ret), K(tenant));
  } else if (tenant->is_hidden()) {
    // skip hidden tenant
    LOG_INFO("tenant is hidden while trying to process tenant slog checkpoint, skip this time");
  } else {
    // wait until another ckpt task finished...
    while (!ATOMIC_BCAS(&is_writing_checkpoint_, false, true)) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) { // 10s
        LOG_INFO("wait until last checkpoint finished");
      }
      ob_usleep(100 * 1000); // 100ms
    }

    if (OB_FAIL(gc_checkpoint_file())) {
      LOG_WARN("fail to gc checkpoint file before checkpoint", K(ret));
    } else if (OB_FAIL(ObTenantSlogCheckpointWorkflow::execute(ckpt_type, *this))) {
      LOG_WARN("failed to execute tenant slog checkpoint workflow", K(ret), K(ckpt_type));
    }

    // Regardless of success or failure, gc checkpoint file
    if (OB_TMP_FAIL(gc_checkpoint_file())) {
      LOG_WARN("fail to gc checkpoint file after checkpoint", K(ret), K(tmp_ret));
    }

    ATOMIC_STORE(&is_writing_checkpoint_, false);  // end up checkpoint
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::gc_checkpoint_file()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCheckpointSlogHandler not init", K(ret));
  } else if (!GCTX.is_shared_storage_mode()) {
    // nothing to do
  } else {
    omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
    const ObTenantSuperBlock &super_block = tenant->get_super_block();
    if (OB_FAIL(gc_min_checkpoint_file(super_block.min_file_id_))) {
      LOG_WARN("fail to gc min checkpoint file", K(ret), K(super_block));
    } else if (OB_FAIL(gc_max_checkpoint_file(super_block.max_file_id_))) {
      LOG_WARN("fail to gc max checkpoint file", K(ret), K(super_block));
    }
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::gc_min_checkpoint_file(const int64_t min_file_id)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  int64_t min_exist_file_id = min_file_id - 1;
  bool is_exist = true;
  for (; OB_SUCC(ret) && min_exist_file_id >= 0; --min_exist_file_id) {
    ObStorageObjectOpt opt;
    opt.set_private_ckpt_opt(MTL_ID(), MTL_EPOCH_ID(), min_exist_file_id);
    if (OB_FAIL(ObStorageMetaIOUtil::check_meta_existence(opt, 0/*do not need ls_epoch*/, is_exist))) {
      LOG_WARN("fail to check slog checkpoint file exist", K(ret), K(opt));
    } else if (!is_exist) {
      ++min_exist_file_id;
      break;
    }
  }
  for (; OB_SUCC(ret) && min_exist_file_id >= 0 && min_exist_file_id < min_file_id; ++min_exist_file_id) {
    MacroBlockId macro_id;
    ObStorageObjectOpt opt;
    opt.set_private_ckpt_opt(MTL_ID(), MTL_EPOCH_ID(), min_exist_file_id);
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, macro_id))) {
      LOG_WARN("fail to get object id", K(ret), K(opt), K(macro_id));
    } else if (OB_FAIL(MTL(ObTenantFileManager *)->delete_file(macro_id, 0/*do not need ls_epoch*/))) {
      LOG_WARN("fail to delete file", K(ret), K(macro_id));
    }
  }
#endif
  return ret;
}

int ObTenantCheckpointSlogHandler::gc_max_checkpoint_file(const int64_t max_file_id)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  int64_t max_exist_file_id = max_file_id + 1;
  bool is_exist = true;
  for (; OB_SUCC(ret) && max_exist_file_id < INT64_MAX; ++max_exist_file_id) {
    ObStorageObjectOpt opt;
    opt.set_private_ckpt_opt(MTL_ID(), MTL_EPOCH_ID(), max_exist_file_id);
    if (OB_FAIL(ObStorageMetaIOUtil::check_meta_existence(opt, 0/*do not need ls_epoch*/, is_exist))) {
      LOG_WARN("fail to check slog checkpoint file exist", K(ret), K(opt));
    } else if (!is_exist) {
      --max_exist_file_id;
      break;
    }
  }
  for (; OB_SUCC(ret) && max_exist_file_id < INT64_MAX && max_exist_file_id > max_file_id; --max_exist_file_id) {
    MacroBlockId macro_id;
    ObStorageObjectOpt opt;
    opt.set_private_ckpt_opt(MTL_ID(), MTL_EPOCH_ID(), max_exist_file_id);
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, macro_id))) {
      LOG_WARN("fail to get object id", K(ret), K(opt), K(macro_id));
    } else if (OB_FAIL(MTL(ObTenantFileManager *)->delete_file(macro_id, 0/*do not need ls_epoch*/))) {
      LOG_WARN("fail to delete file", K(ret), K(macro_id));
    }
  }
#endif
  return ret;
}

int ObTenantCheckpointSlogHandler::add_snapshot(const ObTenantSnapshotMeta &tenant_snapshot)
{
  int ret = OB_SUCCESS;
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  lib::ObMutexGuard guard(super_block_mutex_);
  ObTenantSuperBlock super_block = tenant->get_super_block();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCheckpointSlogHandler hasn't been inited", K(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_snapshot));
  } else if (OB_FAIL(super_block.add_snapshot(tenant_snapshot))) {
    LOG_WARN("fail to add snapshot to super block", K(ret), K(tenant_snapshot));
  } else if (OB_FAIL(SERVER_STORAGE_META_SERVICE.update_tenant_super_block(0, super_block))) {
    LOG_WARN("fail to update tenant super block", K(ret), K(super_block));
  } else {
    tenant->set_tenant_super_block(super_block);
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::delete_snapshot(const ObTenantSnapshotID &snapshot_id)
{
  int ret = OB_SUCCESS;
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  lib::ObMutexGuard guard(super_block_mutex_);
  ObTenantSuperBlock super_block = tenant->get_super_block();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCheckpointSlogHandler hasn't been inited", K(ret));
  } else if (OB_UNLIKELY(!snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(snapshot_id));
  } else if (OB_FAIL(super_block.delete_snapshot(snapshot_id))) {
    LOG_WARN("fail to delete target snapshot", K(ret), K(snapshot_id));
  } else if (OB_FAIL(SERVER_STORAGE_META_SERVICE.update_tenant_super_block(0, super_block))) {
    LOG_WARN("fail to update tenant super block", K(ret), K(super_block));
  } else {
    tenant->set_tenant_super_block(super_block);
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::swap_snapshot(const ObTenantSnapshotMeta &tenant_snapshot)
{
  int ret = OB_SUCCESS;
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  lib::ObMutexGuard guard(super_block_mutex_);
  ObTenantSuperBlock super_block = tenant->get_super_block();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCheckpointSlogHandler hasn't been inited", K(ret));
  } else if (OB_UNLIKELY(!tenant_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_snapshot));
  } else if (OB_FAIL(super_block.delete_snapshot(tenant_snapshot.snapshot_id_))) {
    LOG_WARN("fail to delete target snapshot", K(ret), K(tenant_snapshot));
  } else if (OB_FAIL(super_block.add_snapshot(tenant_snapshot))) {
    LOG_WARN("fail to add snapshot", K(ret), K(tenant_snapshot));
  } else if (OB_FAIL(SERVER_STORAGE_META_SERVICE.update_tenant_super_block(0, super_block))) {
    LOG_WARN("fail to update tenant super block", K(ret), K(super_block));
  } else {
    tenant->set_tenant_super_block(super_block);
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::read_tablet_checkpoint_by_addr(
  const ObMetaDiskAddr &addr, char *item_buf, int64_t &item_buf_len)
{
  // refer to the comments in update_tablet_meta_addr_and_block_list,
  // this func may return OB_SEARCH_NOT_FOUND, and the caller need retry
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);
  if (OB_FAIL(ObTenantStorageCheckpointReader::read_tablet_checkpoint_by_addr(
        tablet_block_handle_.get_meta_block_list(), addr, item_buf, item_buf_len))) {
    LOG_WARN("fail to read_tablet_checkpoint_by_addr", K(ret));
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::get_meta_block_list(ObIArray<MacroBlockId> &meta_block_list)
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);
  meta_block_list.reset();
  const ObIArray<blocksstable::MacroBlockId> &ls_block_list =
    ls_block_handle_.get_meta_block_list();
  const ObIArray<blocksstable::MacroBlockId> &tablet_block_list =
    tablet_block_handle_.get_meta_block_list();
  const ObIArray<blocksstable::MacroBlockId> &wait_gc_tablet_block_list =
    wait_gc_tablet_block_handle_.get_meta_block_list();

  for (int64_t i = 0; OB_SUCC(ret) && i < ls_block_list.count(); ++i) {
    if (OB_FAIL(meta_block_list.push_back(ls_block_list.at(i)))) {
      LOG_WARN("fail to push back meta block", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_block_list.count(); ++i) {
    if (OB_FAIL(meta_block_list.push_back(tablet_block_list.at(i)))) {
      LOG_WARN("fail to push back meta block", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < wait_gc_tablet_block_list.count(); ++i) {
    if (OB_FAIL(meta_block_list.push_back(wait_gc_tablet_block_list.at(i)))) {
      LOG_WARN("fail to push back meta block", K(ret));
    }
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::replay(const ObRedoModuleReplayParam &param)
{
  int ret = OB_SUCCESS;
  ObRedoLogMainType main_type = ObRedoLogMainType::OB_REDO_LOG_MAX;
  enum ObRedoLogSubType sub_type;
  ObIRedoModule::parse_cmd(param.cmd_, main_type, sub_type);

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE != main_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong redo log main type.", K(ret), K(main_type), K(sub_type));
  } else {
    switch (sub_type) {
    case ObRedoLogSubType::OB_REDO_LOG_CREATE_LS_COMMIT: {
      if (OB_FAIL(inner_replay_create_ls_commit_slog(param))) {
        LOG_WARN("fail to replay create ls commit slog", K(ret), K(param));
      }
      break;
    }
    case ObRedoLogSubType::OB_REDO_LOG_CREATE_LS: {
      if (OB_FAIL(inner_replay_create_ls_slog(param))) {
        LOG_WARN("fail to replay create ls slog", K(ret), K(param));
      }
      break;
    }
    case ObRedoLogSubType::OB_REDO_LOG_UPDATE_LS: {
      if (OB_FAIL(inner_replay_update_ls_slog(param))) {
        LOG_WARN("fail to replay update ls slog", K(ret), K(param));
      }
      break;
    }
    case ObRedoLogSubType::OB_REDO_LOG_CREATE_LS_ABORT: {
      if (OB_FAIL(inner_replay_delete_ls(param))) {
        LOG_WARN("fail to replay create ls abort slog", K(ret), K(param));
      }
      break;
    }
    case ObRedoLogSubType::OB_REDO_LOG_DELETE_LS: {
      if (OB_FAIL(inner_replay_delete_ls(param))) {
        LOG_WARN("fail to replay remove ls slog", K(param));
      }
      break;
    }
    case ObRedoLogSubType::OB_REDO_LOG_UPDATE_TABLET: {
      if (OB_FAIL(inner_replay_update_tablet(param))) {
        LOG_WARN("fail to replay update tablet slog", K(ret), K(param));
      }
      break;
    }
    case ObRedoLogSubType::OB_REDO_LOG_DELETE_TABLET: {
      if (OB_FAIL(inner_replay_delete_tablet(param))) {
        LOG_WARN("fail to replay delete tablet slog", K(param));
      }
      break;
    }
    case ObRedoLogSubType::OB_REDO_LOG_GC_TABLET: {
      if (OB_FAIL(inner_replay_gc_tablet(param))) {
        LOG_WARN("fail to replay gc tablet slog", K(param));
      }
      break;
    }
    case ObRedoLogSubType::OB_REDO_LOG_PUT_OLD_TABLET: {
      if (OB_FAIL(inner_replay_put_old_tablet(param))) {
        LOG_WARN("fail to replay put old tablet slog", K(param));
      }
      break;
    }
    case ObRedoLogSubType::OB_REDO_LOG_EMPTY_SHELL_TABLET: {
      if (OB_FAIL(inner_replay_empty_shell_tablet(param))) {
        LOG_WARN("fail to replay put old tablet slog", K(param));
      }
      break;
    }
    case ObRedoLogSubType::OB_REDO_LOG_UPDATE_DUP_TABLE_LS: {
      if (OB_FAIL(inner_replay_dup_table_ls_slog(param))) {
        LOG_WARN("fail to replay dup_table ls slog", K(param));
      }
      break;
    }

    default: {
      ret = OB_ERR_SYS;
      LOG_ERROR("wrong redo log subtype", K(ret), K(sub_type), K(param));
    }
    }
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::inner_replay_create_ls_slog(const ObRedoModuleReplayParam &param)
{
  int ret = OB_SUCCESS;

  ObLSMetaLog slog_entry;
  int64_t pos = 0;
  if (OB_FAIL(slog_entry.deserialize(param.buf_, param.disk_addr_.size(), pos))) {
    LOG_WARN("fail to deserialize slog", K(ret), K(param), K(pos));
  } else if (OB_FAIL(MTL(ObLSService *)->replay_create_ls(slog_entry.get_ls_meta().get_ls_epoch(), slog_entry.get_ls_meta()))) {
    LOG_WARN("fail to replay ls meta slog", K(ret), K(param), K(pos));
  } else {
    LOG_INFO("successfully replay create ls slog", K(param), K(pos));
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::inner_replay_update_ls_slog(const ObRedoModuleReplayParam &param)
{
  int ret = OB_SUCCESS;

  ObLSMetaLog slog_entry;
  int64_t pos = 0;
  if (OB_FAIL(slog_entry.deserialize(param.buf_, param.disk_addr_.size(), pos))) {
    LOG_WARN("fail to deserialize slog", K(ret), K(param), K(pos));
  } else if (OB_FAIL(MTL(ObLSService *)->replay_update_ls(slog_entry.get_ls_meta()))) {
    LOG_WARN("fail to replay ls meta slog", K(ret), K(param), K(pos));
  } else {
    LOG_INFO("successfully replay update ls slog", K(param), K(pos));
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::inner_replay_dup_table_ls_slog(
    const ObRedoModuleReplayParam &param)
{
  int ret = OB_SUCCESS;

  ObDupTableCkptLog slog_entry;
  int64_t pos = 0;
  ObLSHandle ls_handle;
  ObLS *ls_ptr = nullptr;

  if (OB_FAIL(slog_entry.deserialize(param.buf_, param.disk_addr_.size(), pos))) {
    LOG_WARN("fail to deserialize slog", K(ret), K(param), K(pos));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(slog_entry.get_dup_ls_meta().ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    if (OB_LS_NOT_EXIST == ret) {
      LOG_INFO("this is possible when writing ls checkpoint but ls is removing", K(slog_entry));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get ls", K(ret), K(slog_entry));
    }
  } else if (OB_ISNULL(ls_ptr = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls_ptr", K(ret), K(param), K(pos));
  } else if (OB_FAIL(ls_ptr->set_dup_table_ls_meta(slog_entry.get_dup_ls_meta()))) {
    LOG_WARN("fail to replay dup table ls meta slog", K(ret), K(param), K(pos));
  } else {
    LOG_INFO("successfully replay dup table ls meta slog", K(param), K(pos));
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::inner_replay_create_ls_commit_slog(
    const ObRedoModuleReplayParam &param)
{
  int ret = OB_SUCCESS;

  ObLSID ls_id;
  ObCreateLSCommitSLog slog_entry(ls_id);
  int64_t pos = 0;
  const bool is_replay = true;
  if (OB_FAIL(slog_entry.deserialize(param.buf_, param.disk_addr_.size(), pos))) {
    LOG_WARN("fail to deserialize slog", K(ret), K(param), K(pos));
  } else if (OB_FAIL(MTL(ObLSService *)->replay_create_ls_commit(ls_id))) {
    LOG_WARN("fail to replay create ls commit slog", K(ret), K(param), K(pos));
  } else {
    LOG_INFO("successfully replay create ls commit slog");
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::inner_replay_delete_ls(const ObRedoModuleReplayParam &param)
{
  int ret = OB_SUCCESS;

  ObLSID ls_id;
  ObLSIDLog slog_entry(ls_id);
  int64_t pos = 0;
  const bool is_replay = true;
  if (OB_FAIL(slog_entry.deserialize(param.buf_, param.disk_addr_.size(), pos))) {
    LOG_WARN("fail to deserialize remove log stream slog", K(param), K(pos));
  } else if (OB_FAIL(MTL(ObLSService *)->replay_remove_ls(ls_id))) {
    LOG_WARN("fail to remove log stream", K(param), K(pos));
  } else if (OB_FAIL(remove_tablets_from_replay_map_(ls_id))) {
    LOG_WARN("fail to remove tablets", K(ret), K(ls_id));
  } else {
    LOG_INFO("successfully replay remove log stream", K(ret), K(ls_id));
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::remove_tablets_from_replay_map_(const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;

  ObArray<ObTabletMapKey> need_removed_tablets;
  ReplayTabletDiskAddrMap::iterator iter = replay_tablet_disk_addr_map_.begin();
  for (; OB_SUCC(ret) && iter != replay_tablet_disk_addr_map_.end(); ++iter) {
    const ObTabletMapKey &map_key = iter->first;
    if (ls_id == map_key.ls_id_) {
      if (OB_FAIL(need_removed_tablets.push_back(map_key))) {
        LOG_WARN("fail to push back", K(ret), K(map_key));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < need_removed_tablets.count(); i++) {
    if (OB_FAIL(replay_tablet_disk_addr_map_.erase_refactored(need_removed_tablets.at(i)))) {
      LOG_WARN("fail to erase tablet", K(ret), K(need_removed_tablets.at(i)));
    }
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::inner_replay_update_tablet(const ObRedoModuleReplayParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_replay_deserialize(param.buf_, param.data_size_, true /*overwrite if tablet exists*/))) {
    LOG_WARN("fail to deserialize slog and set disk addr map", K(ret));
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::inner_replay_deserialize(
    const char *buf,
    const int64_t buf_len,
    bool allow_override /* allow to overwrite the map's element or not */)
{
  int ret = OB_SUCCESS;
  ObUpdateTabletLog slog;
  ObTabletMapKey tablet_key;
  int64_t pos = 0;
  if (OB_FAIL(slog.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize create tablet slog", K(ret), K(pos), K(buf_len), K(slog));
  } else if (OB_UNLIKELY(!slog.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("slog is invalid", K(ret), K(slog));
  } else {
    tablet_key.ls_id_ = slog.ls_id_;
    tablet_key.tablet_id_ = slog.tablet_id_;
    const ObReplayTabletValue value(slog.disk_addr_,
                                    slog.ls_epoch_,
                                    slog.tablet_attr_,
                                    slog.accelerate_info_);
    if (OB_FAIL(replay_tablet_disk_addr_map_.set_refactored(tablet_key, value, allow_override ? 1 : 0))) {
      LOG_WARN("fail to update tablet meta addr", K(ret), K(slog));
    } else {
      LOG_INFO("Successfully load tablet meta addr for ckpt", K(slog));
    }
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::inner_replay_put_old_tablet(const ObRedoModuleReplayParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_replay_old_deserialize(
      param.disk_addr_,
      param.buf_,
      param.disk_addr_.size(),
      true /* allow to overwrite the map's element or not */))) {
    LOG_WARN("fail to replay old tablet", K(ret), K(param));
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::inner_replay_old_deserialize(
    const ObMetaDiskAddr &addr,
    const char *buf,
    const int64_t buf_len,
    bool allow_override /* allow to overwrite the map's element or not */)
{
  int ret = OB_SUCCESS;
  ObTabletMapKey map_key;
  const ObReplayTabletValue value(addr, 0/*ls_epoch*/);
  if (OB_FAIL(ObTablet::deserialize_id(buf, buf_len, map_key.ls_id_, map_key.tablet_id_))) {
    LOG_WARN("fail to deserialize log stream id and tablet id", K(ret));
  } else if (OB_FAIL(replay_tablet_disk_addr_map_.set_refactored(map_key, value, allow_override ? 1 : 0))) {
    LOG_WARN("update tablet meta addr fail", K(ret), K(map_key), K(addr));
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::inner_replay_delete_tablet(const ObRedoModuleReplayParam &param)
{
  int ret = OB_SUCCESS;

  ObDeleteTabletLog slog_entry;
  int64_t pos = 0;

  if (OB_FAIL(slog_entry.deserialize(param.buf_, param.disk_addr_.size(), pos))) {
    LOG_WARN("fail to deserialize delete tablet slog", K(param), K(pos));
  } else {
    const ObTabletMapKey map_key(slog_entry.ls_id_, slog_entry.tablet_id_);
    if (OB_FAIL(replay_wait_gc_tablet_set_.set_refactored(slog_entry)) && OB_HASH_EXIST != ret) {
      LOG_WARN("fail to set replay wait gc tablet set", K(ret), K(slog_entry));
    } else if (OB_FAIL(replay_tablet_disk_addr_map_.erase_refactored(map_key)) && OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to erase tablet", K(ret), K(map_key), K(slog_entry));
    } else {
      ret = OB_SUCCESS;
      LOG_INFO("Successfully remove tablet for replay", K(param), K(slog_entry));
    }
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::inner_replay_gc_tablet(const ObRedoModuleReplayParam &param)
{
  int ret = OB_SUCCESS;
  ObGCTabletLog slog_entry;
  int64_t pos = 0;
  if (OB_FAIL(slog_entry.deserialize(param.buf_, param.disk_addr_.size(), pos))) {
    LOG_WARN("fail to deserialize dgc tablet slog", K(param), K(pos));
  } else if (OB_FAIL(replay_gc_tablet_set_.set_refactored(slog_entry)) && OB_HASH_EXIST != ret) {
    LOG_WARN("fail to set replay wait gc tablet set", K(ret), K(slog_entry));
  } else {
    ret = OB_SUCCESS;
    LOG_INFO("Successfully gc tablet for replay", K(param), K(slog_entry));
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::inner_replay_empty_shell_tablet(const ObRedoModuleReplayParam &param)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObEmptyShellTabletLog slog;

  if (OB_FAIL(slog.deserialize_id(param.buf_, param.disk_addr_.size(), pos))) {
    STORAGE_LOG(WARN, "failed to serialize tablet_id_", K(ret), K(param.disk_addr_.size()), K(pos));
  } else {
    const ObTabletMapKey map_key(slog.ls_id_,
                                 slog.tablet_id_);
    const ObReplayTabletValue value(param.disk_addr_, slog.ls_epoch_);
    if (OB_FAIL(replay_tablet_disk_addr_map_.set_refactored(map_key, value, 1))) {
      LOG_WARN("fail to set tablet", K(ret), K(map_key), K(value));
    }
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::parse(
  const int32_t cmd, const char *buf, const int64_t len, FILE *stream)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObRedoLogMainType main_type = ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE;
  ObRedoLogSubType sub_type = ObRedoLogSubType::OB_REDO_LOG_INVALID;
  char slog_name[ObStorageLogReplayer::MAX_SLOG_NAME_LEN];

  ObIRedoModule::parse_cmd(cmd, main_type, sub_type);
  if (OB_ISNULL(buf) || OB_ISNULL(stream) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), KP(stream), K(len));
  } else if (OB_UNLIKELY(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE != main_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("slog type does not match", K(ret), K(main_type), K(sub_type));
  } else if (OB_UNLIKELY(0 > fprintf(stream, "\ntenant slog: "))) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to write tenant slog to stream", K(ret));
  } else {
    switch (sub_type) {
      case ObRedoLogSubType::OB_REDO_LOG_CREATE_LS_COMMIT: {
        ObLSID ls_id;
        ObLSIDLog slog_entry(ls_id);
        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "create ls commit slog: ");
        if (OB_FAIL(ObStorageLogReplayer::print_slog(buf, len, slog_name, slog_entry, stream))) {
          LOG_WARN("fail to print slog", K(ret), KP(buf), K(len), K(slog_name), K(slog_entry));
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_CREATE_LS: {
        ObLSMetaLog slog_entry;
        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "create ls slog: ");
        if (OB_FAIL(ObStorageLogReplayer::print_slog(buf, len, slog_name, slog_entry, stream))) {
          LOG_WARN("fail to print slog", K(ret), KP(buf), K(len), K(slog_name), K(slog_entry));
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_UPDATE_LS: {
        ObLSMetaLog slog_entry;
        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "update ls slog: ");
        if (OB_FAIL(ObStorageLogReplayer::print_slog(buf, len, slog_name, slog_entry, stream))) {
          LOG_WARN("fail to print slog", K(ret), KP(buf), K(len), K(slog_name), K(slog_entry));
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_CREATE_LS_ABORT: {
        ObLSID ls_id;
        ObLSIDLog slog_entry(ls_id);
        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "create ls abort slog: ");
        if (OB_FAIL(ObStorageLogReplayer::print_slog(buf, len, slog_name, slog_entry, stream))) {
          LOG_WARN("fail to print slog", K(ret), KP(buf), K(len), K(slog_name), K(slog_entry));
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_DELETE_LS: {
        ObLSID ls_id;
        ObLSIDLog slog_entry(ls_id);
        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "delete ls slog: ");
        if (OB_FAIL(ObStorageLogReplayer::print_slog(buf, len, slog_name, slog_entry, stream))) {
          LOG_WARN("fail to print slog", K(ret), KP(buf), K(len), K(slog_name), K(slog_entry));
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_UPDATE_TABLET: {
        ObUpdateTabletLog slog_entry;
        int64_t pos = 0;
        int32_t length = 0;
        int32_t version = 0;

        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "update tablet slog: ");
        if (OB_FAIL(slog_entry.deserialize(buf, len, pos))) {
          LOG_WARN("fail to deserialize tablet meta", K(ret), KP(buf), K(len), K(pos));
        } else {
          ObCStringHelper helper;
          if (0 > fprintf(stream, "%s\n version:%d length:%d\n%s\n", slog_name, version, length,
              helper.convert(slog_entry))) {
            ret = OB_IO_ERROR;
            LOG_WARN("Fail to print slog to file.", K(ret));
          }
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_DELETE_TABLET: {
        ObLSID ls_id;
        ObTabletID tablet_id;
        ObDeleteTabletLog slog_entry(ls_id, tablet_id);
        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "delete tablet slog: ");
        if (OB_FAIL(ObStorageLogReplayer::print_slog(buf, len, slog_name, slog_entry, stream))) {
          LOG_WARN("fail to print slog", K(ret), KP(buf), K(len), K(slog_name), K(slog_entry));
        }
        break;
      }

      case ObRedoLogSubType::OB_REDO_LOG_EMPTY_SHELL_TABLET: {
        ObEmptyShellTabletLog slog_entry;
        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "empty shell tablet slog: ");
        if (OB_FAIL(slog_entry.deserialize_id(buf, len, pos))) {
          LOG_WARN("failed to deserialize empty shell tablet_id_", K(ret));
        } else {
          ObCStringHelper helper;
          if (0 > fprintf(stream, "%s\n%s\n", slog_name, helper.convert(slog_entry))) {
            ret = OB_IO_ERROR;
            LOG_WARN("Fail to print slog to file.", K(ret));
          }
        }
        break;
      }

      case ObRedoLogSubType::OB_REDO_LOG_UPDATE_DUP_TABLE_LS: {
        ObDupTableCkptLog slog_entry;
        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "update dup table ls meta slog: ");
        if (OB_FAIL(ObStorageLogReplayer::print_slog(buf, len, slog_name, slog_entry, stream))) {
          LOG_WARN("fail to print slog", K(ret), KP(buf), K(len), K(slog_name), K(slog_entry));
        }
        break;
      }

      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("unknown subtype", K(ret), K(sub_type));
      }
    }
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::replay_over()
{
  return OB_SUCCESS; // nothing to do.
}

int ObTenantCheckpointSlogHandler::read_from_disk_addr(const ObMetaDiskAddr &addr,
    char *buf, const int64_t buf_len, char *&r_buf, int64_t &r_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCheckpointSlogHandler not init", K(ret));
  } else if (OB_UNLIKELY(!addr.is_valid() || buf_len < addr.size()) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr), KP(buf), K(buf_len));
  } else {
    switch (addr.type()) {
      case ObMetaDiskAddr::DiskType::FILE: {
        int64_t pos = 0;
        if (OB_FAIL(read_from_slog(addr, buf, buf_len, pos))) {
          STORAGE_LOG(WARN, "fail to read from slog", K(ret), K(addr), KP(buf), K(buf_len));
        } else {
          r_buf = buf + pos;
          r_len = addr.size() - pos;
        }
        break;
      }
      case ObMetaDiskAddr::DiskType::BLOCK: {
        if (OB_FAIL(read_from_ckpt(addr, buf, buf_len, r_len))) {
          STORAGE_LOG(WARN, "fail to read from checkpoint", K(ret), K(addr), KP(buf), K(buf_len));
        } else {
          r_buf = buf;
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "unknown meta disk address type", K(ret), K(addr), KP(buf), K(buf_len));
        break;
      }
    }
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::read_from_ckpt(const ObMetaDiskAddr &phy_addr,
    char *buf, const int64_t buf_len, int64_t &r_len)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!phy_addr.is_valid()
               || !phy_addr.is_block()
               || buf_len < phy_addr.size())
               || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(phy_addr), KP(buf), K(buf_len));
  } else if (OB_FAIL(read_tablet_checkpoint_by_addr(phy_addr, buf, r_len))) {
    STORAGE_LOG(WARN, "fail to read checkpoint", K(ret), K(phy_addr), KP(buf));
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::read_from_slog(const ObMetaDiskAddr &addr,
    char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!addr.is_valid()
               || !addr.is_file()
               || buf_len < addr.size())
               || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(addr), KP(buf), K(buf_len));
  } else {
    // The reason for retrying, here, is that the current SLOG didn't handle the read and write
    // concurrency for the latest item, and an -4103 error will be returned. At present, the
    // optimized changes for SLOG are relatively large, and we will bypass it in the short term.
    int64_t retry_count = 2;
    do {
      int64_t tmp_pos = pos;
      if (OB_FAIL(ObStorageLogReader::read_log(slogger_->get_dir(), addr, buf_len, buf, tmp_pos, MTL_ID()))) {
        STORAGE_LOG(WARN, "fail to read slog", K(ret), "logger directory", slogger_->get_dir(), K(addr),
            K(buf_len), KP(buf));
        if (retry_count > 1) {
          sleep(1); // sleep 1s
        }
      } else {
        pos = tmp_pos;
      }
    } while (OB_FAIL(ret) && --retry_count > 0);
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::read_empty_shell_file(
    const ObMetaDiskAddr &addr,
    common::ObArenaAllocator &allocator,
    char *&buf,
    int64_t &buf_len)
{
  int ret = OB_SUCCESS;

  ObEmptyShellTabletLog slog;
  int64_t pos = 0;
  if (ObMetaDiskAddr::DiskType::FILE != addr.type()) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "addr type is not correct", K(ret), K(addr));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(addr.size())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (FALSE_IT(buf_len = addr.size())) {
  } else if (OB_FAIL(read_from_slog(addr, buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "fail to read from slog", K(ret), K(addr), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(slog.deserialize_id(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize id", K(ret), K(addr), KP(buf), K(buf_len), K(pos));
  } else {
    buf += pos;
    buf_len -= pos;
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::update_hidden_sys_tenant_super_block_to_real(omt::ObTenant &sys_tenant)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCheckpointSlogHandler not init", K(ret));
  } else if (OB_UNLIKELY(!common::is_sys_tenant(sys_tenant.id()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only sys tenant can call this method", K(ret), K(sys_tenant.id()));
  } else {
    lib::ObMutexGuard guard(super_block_mutex_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("failed to hold super block mutex", K(ret));
    } else {
      HEAP_VAR(ObTenantSuperBlock, new_super_block) {
        new_super_block = sys_tenant.get_super_block();
        new_super_block.is_hidden_ = false;
        if (OB_FAIL(SERVER_STORAGE_META_SERVICE.update_tenant_super_block(sys_tenant.get_epoch(), new_super_block))) {
          LOG_WARN("failed to update tenant super block", K(ret), K(new_super_block));
        } else {
          sys_tenant.set_tenant_super_block(new_super_block);
        }
      }
    }
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::update_real_sys_tenant_super_block_to_hidden(omt::ObTenant &sys_tenant)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCheckpointSlogHandler not init", K(ret));
  } else if (OB_UNLIKELY(!common::is_sys_tenant(sys_tenant.id()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only sys tenant can call this method", K(ret), K(sys_tenant.id()));
  } else {
    lib::ObMutexGuard guard(super_block_mutex_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("failed to hold super block mutex", K(ret));
    } else {
      HEAP_VARS_2((ObTenantSuperBlock, super_block, OB_SYS_TENANT_ID, /*is_hidden*/true), (ObTenantSuperBlock, old_tenant_super_block)) {
        if (OB_FAIL(TENANT_SLOGGER.get_active_cursor(super_block.replay_start_point_))) {
          LOG_WARN("get slog current cursor failed", K(ret));
        } else if (OB_UNLIKELY(!super_block.replay_start_point_.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cur_cursor is invalid", K(ret), K_(super_block.replay_start_point));
        } else {
          // acquire auto_inc_ls_epoch and preallocated_seqs from old tenant super block.
          // the tenant epoch is not changed.
          old_tenant_super_block = sys_tenant.get_super_block();
          super_block.auto_inc_ls_epoch_ = old_tenant_super_block.auto_inc_ls_epoch_;
          super_block.preallocated_seqs_ = old_tenant_super_block.preallocated_seqs_;
          if (GCTX.is_shared_storage_mode()) {
            super_block.min_file_id_ = old_tenant_super_block.min_file_id_;
            super_block.max_file_id_ = old_tenant_super_block.max_file_id_;
          }
        }

        if (FAILEDx(SERVER_STORAGE_META_SERVICE.update_tenant_super_block(
            sys_tenant.get_epoch(), super_block))) {
          LOG_WARN("fail to update tenant super block", K(ret), K(super_block));
        } else {
          sys_tenant.set_tenant_super_block(super_block);
        }
      }
    }
  }
  return ret;
}


int ObTenantCheckpointSlogHandler::create_tenant_ls_item(const ObLSID ls_id, int64_t &ls_epoch)
{
  int ret = OB_SUCCESS;
  uint64_t inf_seq = 0;
  // have to get macro_seq before get sputer_block_lock
  // update preallocate.. need the lock either
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_private_object_seq(inf_seq))) {
    LOG_WARN("fail to get tenant_object_seq", K(ret));
  } else {
    lib::ObMutexGuard guard(super_block_mutex_);
    omt::ObTenant *tenant = static_cast<omt::ObTenant*>(MTL_CTX());
    ObTenantSuperBlock tenant_super_block = tenant->get_super_block();

    int64_t i = 0;
    for (; i < tenant_super_block.ls_cnt_; i++) {
      const ObLSItem &item = tenant_super_block.ls_item_arr_[i];
      if (ls_id == item.ls_id_ && item.status_ != ObLSItemStatus::CREATE_ABORT &&
          item.status_ != ObLSItemStatus::DELETED) {
        break;
      }
    }
    if (OB_UNLIKELY(i < tenant_super_block.ls_cnt_) && ObLSItemStatus::CREATING == tenant_super_block.ls_item_arr_[i].status_) {
      ls_epoch = tenant_super_block.ls_item_arr_[i].epoch_;
      FLOG_INFO("ls item already exist", K(ret), K(i), "ls_item", tenant_super_block.ls_item_arr_[i], K(ls_epoch));
    } else if (OB_UNLIKELY(i != tenant_super_block.ls_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls item already exist", K(ret), "ls_item", tenant_super_block.ls_item_arr_[i]);
    } else if (OB_UNLIKELY(ObTenantSuperBlock::MAX_LS_COUNT == i)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("too many ls", K(ret), K(ls_id), K(tenant_super_block));
    } else {
      ObLSItem &item = tenant_super_block.ls_item_arr_[i];
      tenant_super_block.ls_cnt_ = i + 1;
      item.ls_id_ = ls_id;
      item.min_macro_seq_ = inf_seq;
      item.max_macro_seq_ = UINT64_MAX;
      item.status_ = ObLSItemStatus::CREATING;
      item.epoch_ = tenant_super_block.auto_inc_ls_epoch_++;
      ls_epoch = item.epoch_;
      if (!item.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected new ls_item", K(ret), K(item));
      } else if (OB_FAIL(SERVER_STORAGE_META_SERVICE.update_tenant_super_block(tenant->get_epoch(), tenant_super_block))) {
        LOG_WARN("fail to write tenant super block", K(ret), K(tenant->get_epoch()), K(tenant_super_block));
      } else {
        tenant->set_tenant_super_block(tenant_super_block);
        FLOG_INFO("create tenant ls item", K(ret), K(item), K(tenant_super_block), K(i));
      }
    }
  }
  if (OB_FAIL(ret)) {
    FLOG_INFO("create tenant ls item failed", K(ret), K(ls_id));
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::update_tenant_ls_item(
    const ObLSID ls_id,
    const int64_t ls_epoch,
    const ObLSItemStatus status)
{
  int ret = OB_SUCCESS;

  uint64_t sup_seq = 0;
  if (ObLSItemStatus::DELETED == status) {
    // have to get macro_seq before get sputer_block_lock
    // update preallocate.. need the lock either
    if (OB_FAIL(TENANT_SEQ_GENERATOR.get_private_object_seq(sup_seq))) {
      LOG_WARN("fail to get tenant_object_seq", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    lib::ObMutexGuard guard(super_block_mutex_);
    omt::ObTenant *tenant = static_cast<omt::ObTenant*>(MTL_CTX());
    ObTenantSuperBlock tenant_super_block = tenant->get_super_block();
    int64_t i = 0;
    for (; i < tenant_super_block.ls_cnt_; i++) {
      const ObLSItem &item = tenant_super_block.ls_item_arr_[i];
      if (ls_id == item.ls_id_ && ls_epoch == item.epoch_) {
        break;
      }
    }
    if (OB_UNLIKELY(i == tenant_super_block.ls_cnt_)) {
      if (ObLSItemStatus::DELETED != status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls not exist", K(ret), K(ls_id), K(ls_epoch), K(status));
      }
    } else {
      const ObLSItem old_item = tenant_super_block.ls_item_arr_[i];
      ObLSItem &new_item = tenant_super_block.ls_item_arr_[i];
      new_item.status_ = status;
      if (ObLSItemStatus::DELETED == status) {
        // update the supremum seq of the deleted_ls_item
        new_item.max_macro_seq_ = sup_seq;
      }
      if (!new_item.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected new ls_item", K(ret), K(new_item), K(old_item));
      } else if (OB_FAIL(SERVER_STORAGE_META_SERVICE.update_tenant_super_block(tenant->get_epoch(), tenant_super_block))) {
        LOG_WARN("fail to write tenant super block", K(ret), K(tenant->get_epoch()), K(tenant_super_block));
      } else {
        tenant->set_tenant_super_block(tenant_super_block);
        FLOG_INFO("update tenant super block ls item", K(ret), K(old_item), K(new_item), K(tenant_super_block), K(i));
      }
    }
  }
  if (OB_FAIL(ret)) {
    FLOG_INFO("update tenant ls item failed", K(ret), K(ls_id), K(ls_epoch), K(status));
  }
  return ret;
}


int ObTenantCheckpointSlogHandler::delete_tenant_ls_item(const share::ObLSID ls_id, const int64_t ls_epoch)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(super_block_mutex_);
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(MTL_CTX());
  ObTenantSuperBlock tenant_super_block = tenant->get_super_block();

  HEAP_VAR(ObTenantSuperBlock, tmp_super_block) {
    tmp_super_block = tenant_super_block;
    tmp_super_block.ls_cnt_ = 0;
    bool is_delete_hit = false;
    for (int64_t i = 0; i < tenant_super_block.ls_cnt_; i++) {
      const ObLSItem &item = tenant_super_block.ls_item_arr_[i];
      if (ls_id == item.ls_id_ && ls_epoch == item.epoch_) {
        if (ObLSItemStatus::DELETED == item.status_
            || ObLSItemStatus::CREATE_ABORT == item.status_) {
          is_delete_hit = true;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("delete ls_item whose status is not equal to deleted", K(ret), K(ls_id), K(ls_epoch), K(tenant_super_block), K(tmp_super_block));
        }
      } else {
        tmp_super_block.ls_item_arr_[tmp_super_block.ls_cnt_++] = item;
      }
    }
    if (OB_FAIL(ret)) {
      // error occurred
    } else if (OB_LIKELY(is_delete_hit)) {
      if (OB_FAIL(SERVER_STORAGE_META_SERVICE.update_tenant_super_block(tenant->get_epoch(), tmp_super_block))) {
        LOG_WARN("fail to write tenant super block", K(ret), K(tenant->get_epoch()), K(tmp_super_block), K(tenant_super_block));
      } else {
        tenant->set_tenant_super_block(tmp_super_block);
        FLOG_INFO("update tenant super block ls item (delete)", K(ret), K(ls_id), K(ls_epoch), K(tenant_super_block), K(tmp_super_block));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls not exist", K(ret), K(ls_id), K(ls_epoch), K(tenant_super_block));
    }
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::update_tenant_preallocated_seqs(
    const ObTenantMonotonicIncSeqs &preallocated_seqs)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("not support for shared-nothing", K(ret));
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    lib::ObMutexGuard guard(super_block_mutex_);
    omt::ObTenant *tenant = static_cast<omt::ObTenant*>(MTL_CTX());
    ObTenantSuperBlock tenant_super_block = tenant->get_super_block();
    tenant_super_block.preallocated_seqs_ = preallocated_seqs;

    if (OB_FAIL(SERVER_STORAGE_META_SERVICE.update_tenant_super_block(tenant->get_epoch(), tenant_super_block))) {
      LOG_WARN("fail to write tenant super block", K(ret), K(tenant->get_epoch()), K(tenant_super_block));
    } else {
      tenant->set_tenant_super_block(tenant_super_block);
      FLOG_INFO("update tenant preallocated seqs", K(ret), K(tenant_super_block));
    }
#endif
  }
  return ret;
}

}  // end namespace storage
}  // namespace oceanbase
