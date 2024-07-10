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

#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "storage/slog_ckpt/ob_tenant_storage_checkpoint_reader.h"
#include "storage/slog_ckpt/ob_tenant_storage_checkpoint_writer.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/slog_ckpt/ob_tablet_replay_create_handler.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/ob_super_block_struct.h"
#include "storage/slog/ob_storage_log_reader.h"
#include "storage/slog/ob_storage_log_replayer.h"
#include "storage/slog/ob_storage_log.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/tx/ob_timestamp_service.h"
#include "storage/tx/ob_trans_id_service.h"
#include "storage/tx/ob_dup_table_base.h"
#include "observer/omt/ob_tenant.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "sql/das/ob_das_id_service.h"
#include "storage/tablet/ob_tablet.h"
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
  if (ObServerCheckpointSlogHandler::get_instance().is_started()) {
    if (OB_FAIL(handler_->write_checkpoint(false/*is_force*/))) {
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
    last_ckpt_time_(0),
    last_frozen_version_(0),
    lock_(common::ObLatchIds::SLOG_CKPT_LOCK),
    slog_ckpt_lock_(common::ObLatchIds::SLOG_CKPT_LOCK),
    tablet_key_set_(),
    is_copying_tablets_(false),
    ckpt_cursor_(),
    ls_block_handle_(),
    tablet_block_handle_(),
    tg_id_(-1),
    write_ckpt_task_(this),
    replay_tablet_disk_addr_map_(),
    shared_block_rwriter_(),
    super_block_mutex_(),
    is_trivial_version_(false),
    shared_block_raw_rwriter_()
{
}

int ObTenantCheckpointSlogHandler::mtl_init(ObTenantCheckpointSlogHandler *&handler)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(handler->init())) {
    LOG_WARN("fail to init ObTenantCheckpointSlogHandler", K(ret));
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::init()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantCheckpointSlogHandler has inited", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::WriteCkpt, tg_id_))) {
    LOG_WARN("fail to tg create tenant", K(ret));
  } else if (OB_FAIL(shared_block_rwriter_.init())) {
    LOG_WARN("fail to init shared block reader ", K(ret));
  } else if (OB_FAIL(shared_block_raw_rwriter_.init())) {
    LOG_WARN("fail to init linked block manager", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::start()
{
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  const ObTenantSuperBlock super_block = tenant->get_super_block();
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!super_block.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant super block invalid", K(ret), K(super_block));
  } else if (FALSE_IT(ATOMIC_STORE(&is_trivial_version_, super_block.is_trivial_version()))) {
  } else if (OB_FAIL(replay_checkpoint_and_slog(super_block))) {
    LOG_WARN("fail to read_checkpoint_and_replay_slog", K(ret), K(super_block));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("ObTenantCheckpointSlogHandler TG_START failed", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, write_ckpt_task_,
               ObWriteCheckpointTask::WRITE_CHECKPOINT_INTERVAL_US, true))) {
    LOG_WARN("WriteCheckpointTask TG_SCHEDULE failed", K(ret));
  }

  LOG_INFO("finish start ObTenantCheckpointSlogHandler", K(ret), K_(tg_id), K(super_block));

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

    ls_block_handle_.reset();
    tablet_block_handle_.reset();
    tg_id_ = -1;
    replay_tablet_disk_addr_map_.destroy();
    shared_block_rwriter_.reset();
    shared_block_raw_rwriter_.reset();
    tablet_key_set_.destroy();
    ckpt_cursor_.reset();
    is_copying_tablets_ = false;
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
  } else if (OB_FAIL(replay_snapshot(super_block))) {
    LOG_WARN("fail to replay snapshot", K(ret), K(super_block));
  } else if (OB_FAIL(replay_checkpoint(super_block))) {
    LOG_WARN("fail to read_ls_checkpoint", K(ret), K(super_block));
  } else if (OB_FAIL(replay_tenant_slog(super_block.replay_start_point_))) {
    LOG_WARN("fail to replay_tenant_slog", K(ret), K(super_block));
  } else {
    replay_tablet_disk_addr_map_.destroy();
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
  } else if (OB_FAIL(MTL(ObLSService *)->replay_create_ls(ls_meta))) {
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
  ObTenantStorageCheckpointReader ls_ckpt_reader;
  ObSArray<MacroBlockId> tablet_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("ReplayCKPT", MTL_ID()));
  ObSArray<MacroBlockId> ls_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("ReplayCKPT", MTL_ID()));
  ObTenantStorageCheckpointReader::ObStorageMetaOp replay_ls_op = std::bind(
      &ObTenantCheckpointSlogHandler::replay_new_ls,
      this,
      std::placeholders::_1,
      std::placeholders::_2,
      std::placeholders::_3,
      std::ref(tablet_block_list));

  if (OB_FAIL(ls_ckpt_reader.iter_read_meta_item(
      super_block.ls_meta_entry_, replay_ls_op, ls_block_list))) {
    LOG_WARN("fail to iter replay ls", K(ret), K(super_block));
  } else if (OB_FAIL(ls_block_handle_.add_macro_blocks(ls_block_list))) {
    LOG_WARN("fail to add ls linked blocks", K(ret), K(ls_block_list));
  } else if (OB_FAIL(tablet_block_handle_.add_macro_blocks(tablet_block_list))) {
    LOG_WARN("fail to add tablet linked blocks", K(ret), K(tablet_block_list));
  }
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
  } else if (OB_FAIL(MTL(ObLSService *)->replay_create_ls(ls_ckpt_member.ls_meta_))) {
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

  if (OB_FAIL(replayer.init(MTL(ObStorageLogger *)->get_dir(), log_file_spec))) {
    LOG_WARN("fail to init slog replayer", K(ret));
  } else if (OB_FAIL(replayer.register_redo_module(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE, this))) {
    LOG_WARN("fail to register redo module", K(ret));
  } else if (OB_FAIL(replayer.replay(start_point, replay_finish_point, MTL_ID()))) {
    LOG_WARN("fail to replay tenant slog", K(ret));
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
  } else if (OB_FAIL(MTL(ObStorageLogger *)->start_log(replay_finish_point))) {
    LOG_WARN("fail to start_slog", K(ret), K(replay_finish_point));
  }

  LOG_INFO("finish replay tenant slog", K(ret), K(start_point), K(replay_finish_point));

  return ret;
}

int ObTenantCheckpointSlogHandler::read_from_share_blk(
    const ObMetaDiskAddr &addr,
    common::ObArenaAllocator &allocator,
    char *&buf,
    int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  ObSharedBlockReadHandle read_handle(allocator);
  ObSharedBlockReadInfo read_info;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000;
  read_info.addr_ = addr;
  if (OB_FAIL(ObSharedBlockReaderWriter::async_read(read_info, read_handle))) {
    LOG_WARN("fail to read tablet from macro block", K(ret), K(read_info));
  } else if (OB_FAIL(read_handle.wait())) {
    LOG_WARN("fail to wait for read handle", K(ret));
  } else if (OB_FAIL(read_handle.get_data(allocator, buf, buf_len))) {
    LOG_WARN("fail to get data from read handle", K(ret), KP(buf), K(buf_len));
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::read_from_disk(
    const ObMetaDiskAddr &addr,
    common::ObArenaAllocator &allocator,
    char *&buf,
    int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  char *read_buf = nullptr;
  const int64_t read_buf_len = addr.size();
  if (!ATOMIC_LOAD(&is_trivial_version_)) {
    if (ObMetaDiskAddr::DiskType::FILE == addr.type()) {
      if (OB_FAIL(read_empty_shell_file(addr, allocator, buf, buf_len))) {
        LOG_WARN("fail to read empty shell", K(ret), K(addr), K(buf), K(buf_len));
      }
    } else {
      if (OB_FAIL(read_from_share_blk(addr, allocator, buf, buf_len))) {
        LOG_WARN("fail to read from share block", K(ret), K(addr), K(buf), K(buf_len));
      }
    }
  } else if (OB_ISNULL(read_buf = static_cast<char*>(allocator.alloc(read_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate buffer", K(ret), K(read_buf_len), KP(read_buf));
  } else if (OB_FAIL(read_from_disk_addr(addr, read_buf, read_buf_len, buf, buf_len))) {
    LOG_WARN("fail to read tablet from addr", K(ret), K(addr), KP(read_buf), K(read_buf_len));
  }
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
    if (OB_FAIL(replay_tablet_disk_addr_map_.set_refactored(tablet_key, slog.disk_addr_, /*allow_override*/ 0))) {
      LOG_WARN("fail to update tablet meta addr", K(ret), K(slog));
    }
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::report_slog(
    const ObTabletMapKey &tablet_key,
    const ObMetaDiskAddr &slog_addr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCheckpointSlogHandler hasn't been inited", K(ret));
  } else if (OB_UNLIKELY(!slog_addr.is_valid() || !slog_addr.is_file())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("slog_addr is invalid", K(ret), K(slog_addr));
  } else {
    int64_t file_id;
    int64_t offset;
    int64_t size;
    if (is_copying_tablets_) {
      if (OB_UNLIKELY(!ckpt_cursor_.is_valid())) {
        LOG_WARN("checkpoint cursor is invalid", K(ret), K(ckpt_cursor_));
      } else if (OB_FAIL(slog_addr.get_file_addr(file_id, offset, size))) {
        LOG_WARN("fail to get slog file addr", K(ret), K(slog_addr));
      } else if (file_id < ckpt_cursor_.file_id_
          || (file_id == ckpt_cursor_.file_id_ && offset < ckpt_cursor_.offset_)) {
        LOG_INFO("tablet slog cursor is smaller, no need to add it to the set",
          K(ret), K(ckpt_cursor_), K(file_id), K(offset));
      } else if (OB_FAIL(tablet_key_set_.set_refactored(tablet_key, 1 /* whether allow override */))) {
        LOG_WARN("fail to insert element into tablet key set", K(ret), K(tablet_key));
      }
    }
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::check_slog(const ObTabletMapKey &tablet_key, bool &has_slog)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCheckpointSlogHandler hasn't been inited", K(ret));
  } else if (OB_UNLIKELY(!tablet_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet key is invalid", K(ret), K(tablet_key));
  } else {
    int tmp_ret = tablet_key_set_.exist_refactored(tablet_key);
    if (OB_HASH_EXIST == tmp_ret) {
      has_slog = true;
      LOG_INFO("tablet slog has been written, no need to write checkpoint", K(tmp_ret), K(tablet_key));
    } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST != tmp_ret)) {
      ret = tmp_ret;
      LOG_WARN("fail to check whether tablet slog has been written", K(ret), K(tablet_key));
    } else {
      has_slog = false;
    }
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::add_macro_blocks(
    const common::ObIArray<blocksstable::MacroBlockId> &ls_block_list,
    const common::ObIArray<blocksstable::MacroBlockId> &tablet_block_list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_block_handle_.add_macro_blocks(ls_block_list))) {
    LOG_WARN("fail to add ls macro blocks", K(ret));
  } else if (OB_FAIL(tablet_block_handle_.add_macro_blocks(tablet_block_list))) {
    LOG_WARN("fail to add tablet blocks", K(ret));
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::write_checkpoint(bool is_force)
{
  int ret = OB_SUCCESS;
  int64_t alert_interval = ObWriteCheckpointTask::FAIL_WRITE_CHECKPOINT_ALERT_INTERVAL;
  int64_t min_interval = ObWriteCheckpointTask::RETRY_WRITE_CHECKPOINT_MIN_INTERVAL;

  uint64_t tenant_id = MTL_ID();
  ObTenantSuperBlock super_block(tenant_id);
  ObTenantStorageCheckpointWriter tenant_storage_ckpt_writer;
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  const ObTenantSuperBlock last_super_block = tenant->get_super_block();

  //Don't compare to MTL(ObTenantTabletScheduler*)->get_frozen_version(), because we expect to do
  //checkpoint after merge finish.
  // 1) avoid IO traffic between ckpt and major
  // 2) truncate slog totally generated by major tablet
  const int64_t broadcast_version = MTL(compaction::ObTenantTabletScheduler*)->get_frozen_version();
  const int64_t frozen_version = MTL(compaction::ObTenantTabletScheduler*)->get_inner_table_merged_scn();
  const bool is_major_doing = (frozen_version != broadcast_version);
  bool is_writing_checkpoint_set = false;
  const int64_t start_time = ObTimeUtility::current_time();
  int64_t cost_time = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCheckpointSlogHandler not init", K(ret));
  } else {
    // we can't just return warn, since it will clean copy status before exiting
    // the only way is to wait without timeout
    while (!ATOMIC_BCAS(&is_writing_checkpoint_, false, true)) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) { // 10s
        LOG_INFO("wait until last checkpoint finished");
      }
      ob_usleep(100 * 1000); // 100ms
    }
    is_writing_checkpoint_set = true;
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_UNLIKELY(tenant->is_hidden())) {
    LOG_INFO("maybe hidden sys, skip checkpoint");
  } else if (OB_UNLIKELY(!last_super_block.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tenant super block", K(ret), K(last_super_block));
  } else if (OB_FAIL(get_cur_cursor())) {
    LOG_WARN("get slog current cursor fail", K(ret));
  } else if (OB_UNLIKELY(!ckpt_cursor_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ckpt_cursor_ is invalid", K(ret));
  } else if (is_force // alter system command triggered
             || last_super_block.is_old_version()  // compat upgrade
             || ((start_time > last_ckpt_time_ + min_interval) // slog is long
                 && !is_major_doing
                 && ckpt_cursor_.newer_than(last_super_block.replay_start_point_)
                 &&(ckpt_cursor_.log_id_ - last_super_block.replay_start_point_.log_id_ >= ObWriteCheckpointTask::MIN_WRITE_CHECKPOINT_LOG_CNT))) {
    DEBUG_SYNC(AFTER_CHECKPOINT_GET_CURSOR);
    super_block.replay_start_point_ = ckpt_cursor_;
    if (OB_FAIL(tenant_storage_ckpt_writer.init(ObTenantStorageMetaType::CKPT))) {
      LOG_WARN("fail to init tenant_storage_meta_writer", K(ret));
    } else if (OB_FAIL(tenant_storage_ckpt_writer.record_meta(super_block.ls_meta_entry_))) {
      LOG_WARN("fail to write_checkpoint", K(ret));
    }
    clean_copy_status();

    if (OB_SUCC(ret)) {
      lib::ObMutexGuard guard(super_block_mutex_);
      super_block.copy_snapshots_from(tenant->get_super_block());
      if (OB_FAIL(ObServerCheckpointSlogHandler::get_instance().write_tenant_super_block_slog(super_block))) {
        LOG_WARN("fail to write_tenant_super_block_slog", K(ret), K(super_block));
      } else {
        tenant->set_tenant_super_block(super_block);
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(update_tablet_meta_addr_and_block_list(
        last_super_block.is_trivial_version(), tenant_storage_ckpt_writer))) {
      LOG_ERROR("fail to update_tablet_meta_addr_and_block_list", K(ret), K(last_super_block));
      // abort if failed, because it cannot be rolled back if partially success.
      // otherwise, updates need to be transactional.
      ob_usleep(1000 * 1000);
      ob_abort();
    } else if (OB_FAIL(MTL(ObStorageLogger *)->remove_useless_log_file(ckpt_cursor_.file_id_, MTL_ID()))) {
      LOG_WARN("fail to remove_useless_log_file", K(ret), K(super_block));
    } else {
      ATOMIC_STORE(&is_trivial_version_, super_block.is_trivial_version());
      last_ckpt_time_ = start_time;
      last_frozen_version_ = frozen_version;
      cost_time = ObTimeUtility::current_time() - start_time;
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(tenant_storage_ckpt_writer.rollback())) {
      LOG_ERROR("fail to rollback checkpoint, macro blocks leak", K(tmp_ret));
    }

    FLOG_INFO("finish write tenant checkpoint", K(ret), K(last_super_block), K(super_block),
        K_(last_ckpt_time), K(start_time), K(broadcast_version),K(frozen_version), K_(last_frozen_version),
        K(is_force), K(cost_time));
    SERVER_EVENT_ADD("storage", "write slog checkpoint", "tenant_id", tenant_id,
        "ret", ret, "cursor", ckpt_cursor_, "frozen_version", frozen_version, "cost_time(us)", cost_time);
  }
  clean_copy_status(); // in case fail after get_cur_cursor
  if (is_writing_checkpoint_set) {
    ATOMIC_STORE(&is_writing_checkpoint_, false);
  }
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
  } else if (OB_FAIL(ObServerCheckpointSlogHandler::get_instance().write_tenant_super_block_slog(super_block))) {
    LOG_WARN("fail to write_tenant_super_block_slog", K(ret), K(super_block));
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
  } else if (OB_FAIL(ObServerCheckpointSlogHandler::get_instance().write_tenant_super_block_slog(super_block))) {
    LOG_WARN("fail to write_tenant_super_block_slog", K(ret), K(super_block));
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
  } else if (OB_FAIL(ObServerCheckpointSlogHandler::get_instance().write_tenant_super_block_slog(super_block))) {
    LOG_WARN("fail to write_tenant_super_block_slog", K(ret), K(super_block));
  } else {
    tenant->set_tenant_super_block(super_block);
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::get_cur_cursor()
{
  int ret = OB_SUCCESS;
  TCWLockGuard guard(slog_ckpt_lock_);
  tablet_key_set_.destroy();
  if (OB_FAIL(MTL(ObStorageLogger *)->get_active_cursor(ckpt_cursor_))) {
    LOG_WARN("fail to get current cursor", K(ret));
  } else if (OB_FAIL(tablet_key_set_.create(BUCKET_NUM, ObModIds::OB_HASH_BUCKET, ObModIds::OB_HASH_BUCKET, MTL_ID()))) {
    LOG_WARN("fail to create tablet key set", K(ret));
  } else {
    is_copying_tablets_ = true;
  }
  return ret;
}

void ObTenantCheckpointSlogHandler::clean_copy_status()
{
  TCWLockGuard guard(slog_ckpt_lock_);
  is_copying_tablets_ = false;
  tablet_key_set_.destroy();
}

int ObTenantCheckpointSlogHandler::update_tablet_meta_addr_and_block_list(
    const bool is_replay_old,
    ObTenantStorageCheckpointWriter &meta_writer)
{
  int ret = OB_SUCCESS;
  ObIArray<MacroBlockId> *meta_block_list = nullptr;
  // the two operations(update tablet_meta_addr and update tablet_block_handle) should be a
  // transaction, so update_tablet_meta_addr is supposed to be placed within the lock, but this can
  // lead to dead lock with read_tablet_checkpoint_by_addr which is called by t3m. the calling of
  // read_tablet_checkpoint_by_addr involves two locks, one in t3m(as lock_A) and one in the current
  // class (as lock_B). when t3m loads the tablet by address, lock A first and then lock B .
  // howerver, update_tablet_meta_addr_and_block_list locks B first and then locks A in t3m to
  // update the tablet addr. to resolve the dead lock, update_tablet_meta_addr is moved out of lock,
  // but this may cause t3m read a new addr which is not in the tablet_block_handle_. when this
  // happens, t3m needs to retry.
  if (OB_FAIL(meta_writer.batch_compare_and_swap_tablet(is_replay_old))) {
    LOG_WARN("fail to update_tablet_meta_addr", K(ret), K(is_replay_old));
  }

  TCWLockGuard guard(lock_);
  if (OB_FAIL(ret)) {
  } else {
    do {
      if (OB_FAIL(meta_writer.get_ls_block_list(meta_block_list))) {
        LOG_WARN("fail to get_ls_block_list", K(ret));
      } else if (OB_FAIL(ls_block_handle_.add_macro_blocks(*meta_block_list))) {
        LOG_WARN("fail to add_macro_blocks", K(ret));
      } else if (OB_FAIL(meta_writer.get_tablet_block_list(meta_block_list))) {
        LOG_WARN("fail to get_tablet_block_list", K(ret));
      } else if (OB_FAIL(tablet_block_handle_.add_macro_blocks(*meta_block_list))) {
        LOG_WARN("fail to set_tablet_block_list", K(ret));
      }
    } while (OB_ALLOCATE_MEMORY_FAILED == ret);
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
  } else if (OB_FAIL(MTL(ObLSService *)->replay_create_ls(slog_entry.get_ls_meta()))) {
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
  } else if (OB_FAIL(ls_ptr->set_dup_table_ls_meta(
                 slog_entry.get_dup_ls_meta()))) {
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
    if (OB_FAIL(replay_tablet_disk_addr_map_.set_refactored(tablet_key, slog.disk_addr_, allow_override ? 1 : 0))) {
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

  if (OB_FAIL(ObTablet::deserialize_id(buf, buf_len, map_key.ls_id_, map_key.tablet_id_))) {
    LOG_WARN("fail to deserialize log stream id and tablet id", K(ret));
  } else if (OB_FAIL(replay_tablet_disk_addr_map_.set_refactored(map_key, addr, allow_override ? 1 : 0))) {
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
    if (OB_FAIL(replay_tablet_disk_addr_map_.erase_refactored(map_key)) && OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to erase tablet", K(ret), K(map_key), K(slog_entry));
    } else {
      ret = OB_SUCCESS;
      LOG_INFO("Successfully remove tablet for replay", K(param), K(slog_entry));
    }
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
    if (OB_FAIL(replay_tablet_disk_addr_map_.set_refactored(map_key, param.disk_addr_, 1))) {
      LOG_WARN("fail to set tablet", K(ret), K(map_key), K(param.disk_addr_));
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
        } else if (0 > fprintf(stream, "%s\n version:%d length:%d\n%s\n", slog_name, version, length, to_cstring(slog_entry))) {
          ret = OB_IO_ERROR;
          LOG_WARN("Fail to print slog to file.", K(ret));
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
        } else if (0 > fprintf(stream, "%s\n%s\n", slog_name, to_cstring(slog_entry))) {
          ret = OB_IO_ERROR;
          LOG_WARN("Fail to print slog to file.", K(ret));
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
  ObStorageLogger *logger = MTL(ObStorageLogger*);

  if (OB_UNLIKELY(!addr.is_valid()
               || !addr.is_file()
               || buf_len < addr.size())
               || OB_ISNULL(buf)
               || OB_ISNULL(logger)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(addr), KP(buf), K(buf_len), KP(logger));
  } else {
    // The reason for retrying, here, is that the current SLOG didn't handle the read and write
    // concurrency for the latest item, and an -4103 error will be returned. At present, the
    // optimized changes for SLOG are relatively large, and we will bypass it in the short term.
    int64_t retry_count = 2;
    do {
      int64_t tmp_pos = pos;
      if (OB_FAIL(ObStorageLogReader::read_log(logger->get_dir(), addr, buf_len, buf, tmp_pos, MTL_ID()))) {
        STORAGE_LOG(WARN, "fail to read slog", K(ret), "logger directory", logger->get_dir(), K(addr),
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


}  // end namespace storage
}  // namespace oceanbase
