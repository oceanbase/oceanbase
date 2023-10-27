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

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace blocksstable;
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

int ObTenantCheckpointSlogHandler::ObReplayCreateTabletTask::init(
    const int64_t task_idx, ObTenantBase *tenant_base, ObTenantCheckpointSlogHandler *handler)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task has been inited", K(ret), KPC(this));
  } else {
    idx_ = task_idx;
    tenant_base_ = tenant_base;
    tablet_addr_arr_.reset();
    tnt_ckpt_slog_handler_ = handler;
    handler->inc_inflight_replay_tablet_task_cnt();
    is_inited_ = true;
  }
  return ret;
}

void ObTenantCheckpointSlogHandler::ObReplayCreateTabletTask::destroy()
{
  if (IS_INIT) {
    tnt_ckpt_slog_handler_->dec_inflight_replay_tablet_task_cnt();
    idx_ = -1;
    tenant_base_ = nullptr;
    tnt_ckpt_slog_handler_ = nullptr;
    tablet_addr_arr_.reset();
    is_inited_ = false;
  }
}
int ObTenantCheckpointSlogHandler::ObReplayCreateTabletTask::execute()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not init", K(ret), KPC(this));
  } else {
    ObTenantSwitchGuard guard(tenant_base_);
    if (OB_UNLIKELY(MTL(ObTenantCheckpointSlogHandler*) != tnt_ckpt_slog_handler_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ObTenantCheckpointSlogHandler", K(ret), KPC(this));
    } else if (OB_FAIL(tnt_ckpt_slog_handler_->replay_create_tablets_per_task(tablet_addr_arr_))) {
      LOG_WARN("fail to execute replay_create_tablets_per_task", K(ret), KPC(this));
    } else {
      FLOG_INFO("successfully execute replay create tablet task", KPC(this));
    }
  }
  if (OB_FAIL(ret)) {
    tnt_ckpt_slog_handler_->set_replay_create_tablet_errcode(ret);
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::ObReplayCreateTabletTask::add_tablet_addr(
    const ObTabletMapKey &tablet_key, const ObMetaDiskAddr &tablet_addr, bool &is_enough)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not init", K(ret));
  } else if (OB_FAIL(tablet_addr_arr_.push_back(std::make_pair(tablet_key, tablet_addr)))) {
    LOG_WARN("fail to push_back", K(ret), K(*this));
  } else if (tablet_addr_arr_.count() >= TABLET_NUM_PER_TASK) {
    is_enough = true;
  } else {
    is_enough = false;
  }
  return ret;
}

ObTenantCheckpointSlogHandler::ObTenantCheckpointSlogHandler()
  : is_inited_(false),
    is_writing_checkpoint_(false),
    last_ckpt_time_(0),
    last_frozen_version_(0),
    inflight_replay_tablet_task_cnt_(0),
    finished_replay_tablet_cnt_(0),
    replay_create_tablet_errcode_(OB_SUCCESS),
    lock_(common::ObLatchIds::SLOG_CKPT_LOCK),
    slog_check_lock_(common::ObLatchIds::SLOG_CKPT_LOCK),
    tablet_key_set_(),
    is_copying_tablets_(false),
    ckpt_cursor_(),
    ls_block_handle_(),
    tablet_block_handle_(),
    tg_id_(-1),
    write_ckpt_task_(this),
    replay_tablet_disk_addr_map_(),
    shared_block_rwriter_()
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
  const bool is_replay_old = super_block.is_old_version();

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

  ObTenantStorageCheckpointReader::ObCheckpointMetaOp replay_ls_op =
      std::bind(&ObTenantCheckpointSlogHandler::replay_ls_meta,
      this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

  ObTenantStorageCheckpointReader::ObCheckpointMetaOp replay_tablet_op =
      std::bind(&ObTenantCheckpointSlogHandler::replay_tablet,
      this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

  if (!replay_ls_op.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replay_ls_op invalid", K(ret));
  } else if (!replay_tablet_op.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replay_tablet_op invalid", K(ret));
  } else if (OB_FAIL(tenant_storage_ckpt_reader.iter_read_checkpoint_item(
      super_block.ls_meta_entry_, replay_ls_op, meta_block_list))) {
    LOG_WARN("fail to replay ls meta checkpoint", K(ret));
  } else if (OB_FAIL(ls_block_handle_.add_macro_blocks(meta_block_list))) {
    LOG_WARN("fail to add_macro_blocks", K(ret));
  } else if (OB_FAIL(tenant_storage_ckpt_reader.iter_read_checkpoint_item(
      super_block.tablet_meta_entry_, replay_tablet_op, meta_block_list))) {
    LOG_WARN("fail to replay tablet checkpoint", K(ret));
  } else if (OB_FAIL(tablet_block_handle_.add_macro_blocks(meta_block_list))) {
    LOG_WARN("fail to add_macro_blocks", K(ret));
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::replay_new_checkpoint(const ObTenantSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
  const MacroBlockId &ls_meta_entry = super_block.ls_meta_entry_;
  ObLinkedMacroBlockItemReader ls_ckpt_reader;
  ObSArray<MacroBlockId> tablet_block_list;

  if (OB_UNLIKELY(IS_EMPTY_BLOCK_LIST(ls_meta_entry))) {
    LOG_INFO("no ls checkpoint", K(ret));
  } else if (OB_FAIL(ls_ckpt_reader.init(ls_meta_entry))) {
    LOG_WARN("fail to init log stream item reader", K(ret), K(ls_meta_entry));
  } else {
    char *item_buf = nullptr;
    int64_t item_buf_len = 0;
    ObLSCkptMember ls_ckpt_member;
    int64_t pos = 0;
    ObMetaDiskAddr addr;
    while (OB_SUCC(ret)) {
      item_buf = nullptr;
      item_buf_len = 0;
      pos = 0;
      ls_ckpt_member.reset();
      if (OB_FAIL(ls_ckpt_reader.get_next_item(item_buf, item_buf_len, addr))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next log stream item", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(ls_ckpt_member.deserialize(item_buf, item_buf_len, pos))) {
        LOG_WARN("fail to deserialize ls ckpt member", K(ret), KP(item_buf), K(item_buf_len), K(pos));
      } else if (OB_FAIL(MTL(ObLSService *)->replay_create_ls(ls_ckpt_member.ls_meta_))) {
        LOG_WARN("fail to replay put ls", K(ret), K(ls_ckpt_member));
      } else if (OB_FAIL(replay_dup_table_ls_meta(ls_ckpt_member.dup_ls_meta_))) {
        LOG_WARN("fail to replay set dup table ls meta", K(ret), K(ls_ckpt_member));
      } else if (OB_FAIL(replay_new_tablet_checkpoint(ls_ckpt_member.tablet_meta_entry_, tablet_block_list))) {
        LOG_WARN("fail to replay new tablet ckpt", K(ret), K(ls_ckpt_member));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ls_block_handle_.add_macro_blocks(ls_ckpt_reader.get_meta_block_list()))) {
      LOG_WARN("fail to add ls macro blocks", K(ret));
    } else if (OB_FAIL(tablet_block_handle_.add_macro_blocks(tablet_block_list))) {
      LOG_WARN("fail to add tablet macro blocks", K(ret));
    }
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::replay_new_tablet_checkpoint(
    const blocksstable::MacroBlockId &tablet_entry_block,
    ObIArray<MacroBlockId> &tablet_block_list)
{
  int ret = OB_SUCCESS;
  ObLinkedMacroBlockItemReader tablet_ckpt_reader;

  if (OB_UNLIKELY(IS_EMPTY_BLOCK_LIST(tablet_entry_block))) {
    LOG_INFO("no tablet checkpoint", K(tablet_entry_block));
  } else if (OB_FAIL(tablet_ckpt_reader.init(tablet_entry_block))) {
    LOG_WARN("fail to init tablet ckpt reader", K(ret), K(tablet_entry_block));
  } else {
    char *item_buf = nullptr;
    int64_t item_buf_len = 0;
    int64_t pos = 0;
    ObMetaDiskAddr addr;
    while (OB_SUCC(ret)) {
      item_buf = nullptr;
      item_buf_len = 0;
      pos = 0;
      if (OB_FAIL(tablet_ckpt_reader.get_next_item(item_buf, item_buf_len, addr))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next log stream item", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(inner_replay_deserialize(
          item_buf, item_buf_len, false /* allow to overwrite the map's element or not */))) {
        LOG_WARN("fail to replay tablet", K(ret), KP(item_buf), K(item_buf_len));
      }
    }

    if (OB_SUCC(ret)) {
      const ObIArray<MacroBlockId> &macro_block_list = tablet_ckpt_reader.get_meta_block_list();
      for (int64_t i = 0; i < macro_block_list.count() && OB_SUCC(ret); i++) {
        if (OB_FAIL(tablet_block_list.push_back(macro_block_list.at(i)))) {
          LOG_WARN("fail to push back macro block id", K(ret));
        }
      }
    }
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

int ObTenantCheckpointSlogHandler::replay_dup_table_ls_meta(
    const transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta &dup_ls_meta)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(dup_ls_meta.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", K(ret), K(dup_ls_meta));
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
  } else if (OB_FAIL(replayer.register_redo_module(
               ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE, this))) {
    LOG_WARN("fail to register redo module", K(ret));
  } else if (OB_FAIL(replayer.replay(start_point, replay_finish_point, MTL_ID()))) {
    LOG_WARN("fail to replay tenant slog", K(ret));
  } else if (OB_FAIL(concurrent_replay_load_tablets())) {
    LOG_WARN("fail to concurrent replay load tablets", K(ret));
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
  ObSharedBlockReadHandle read_handle;
  ObSharedBlockReadInfo read_info;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  read_info.addr_ = addr;
  if (OB_FAIL(shared_block_rwriter_.async_read(read_info, read_handle))) {
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
  const ObTenantSuperBlock super_block = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant())->get_super_block();
  if (OB_UNLIKELY(!super_block.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("super block is invalid", K(ret), K(super_block));
  } else if (!super_block.is_old_version()) {
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

int ObTenantCheckpointSlogHandler::check_is_need_record_transfer_info(
    const share::ObLSID &src_ls_id,
    const share::SCN &transfer_start_scn,
    bool &is_need)
{
  int ret = OB_SUCCESS;
  ObLSService* ls_srv = nullptr;
  ObLSHandle src_ls_handle;
  ObLS *src_ls = NULL;
  is_need = false;
  if (!src_ls_id.is_valid() || !transfer_start_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src_ls_id or transfer_start_scn is invalid", K(ret), K(src_ls_id), K(transfer_start_scn));
  } else if (OB_ISNULL(ls_srv = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls srv should not be NULL", K(ret), KP(ls_srv));
  } else if (OB_FAIL(ls_srv->get_ls(src_ls_id, src_ls_handle, ObLSGetMod::STORAGE_MOD))) {
    if (OB_LS_NOT_EXIST == ret) {
      is_need = false;
      LOG_WARN("source ls is not exist", KR(ret), K(src_ls_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ls", KR(ret), K(src_ls_id));
    }
  } else if (OB_ISNULL(src_ls = src_ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", KR(ret), K(src_ls_id));
  } else if (src_ls->get_ls_meta().get_clog_checkpoint_scn() < transfer_start_scn) {
    is_need = true;
    LOG_INFO("src ls max decided scn is smaller than transfer start scn, need wait clog replay", K(ret),
        K(src_ls_id), K(transfer_start_scn), "ls_meta", src_ls->get_ls_meta());
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::record_ls_transfer_info(
    const ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    const ObTabletTransferInfo &tablet_transfer_info)
{
  int ret = OB_SUCCESS;
  storage::ObLS *ls = NULL;
  bool is_need = false;
  ObMigrationStatus current_migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  ObMigrationStatus new_migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  ObLSRestoreStatus ls_restore_status(ObLSRestoreStatus::LS_RESTORE_STATUS_MAX);
  if (!ls_handle.is_valid() || !tablet_transfer_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_handle), K(tablet_transfer_info));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream not exist", K(ret));
  } else if (OB_FAIL(ls->get_migration_status(current_migration_status))) {
    LOG_WARN("failed to get ls migration status", K(ret));
  } else if (OB_FAIL(ObMigrationStatusHelper::trans_reboot_status(current_migration_status, new_migration_status))) {
    LOG_WARN("failed to trans fail status", K(ret), "ls_id", ls->get_ls_id(),
        K(current_migration_status), K(new_migration_status));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE != new_migration_status) {
    LOG_INFO("The log stream does not need to record transfer_info", "ls_id", ls->get_ls_id(), K(current_migration_status), K(new_migration_status));
  } else if (OB_FAIL(ls->get_restore_status(ls_restore_status))) {
    LOG_WARN("failed to get ls restore status", K(ret), KPC(ls));
  } else if (ls_restore_status.is_in_restore_and_before_quick_restore()) {
    LOG_INFO("the log stream in restore and before quick restore, no need to record transfer info", "ls_id", ls->get_ls_id(), K(ls_restore_status));
  } else if (!tablet_transfer_info.has_transfer_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should have transfer table", K(ret), "ls_id", ls->get_ls_id(), K(tablet_id), K(tablet_transfer_info));
  } else if (ls->get_ls_startup_transfer_info().is_valid()) {
    if (ls->get_ls_startup_transfer_info().ls_id_ != tablet_transfer_info.ls_id_
        || ls->get_ls_startup_transfer_info().transfer_start_scn_ != tablet_transfer_info.transfer_start_scn_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The transfer_info of different tablet records on the same ls is different", K(ret), "ls_id", ls->get_ls_id(),
          K(tablet_id), K(tablet_transfer_info), "ls_startup_transfer_info", ls->get_ls_startup_transfer_info());
    }
  } else if (OB_FAIL(check_is_need_record_transfer_info(tablet_transfer_info.ls_id_,
      tablet_transfer_info.transfer_start_scn_, is_need))) {
    LOG_WARN("failed to check is need record ls", K(ret), "ls_id", ls->get_ls_id(), K(tablet_id), K(tablet_transfer_info));
  } else if (!is_need) {
    // do nothing
  } else if (OB_FAIL(ls->get_ls_startup_transfer_info().init(tablet_transfer_info.ls_id_,
      tablet_transfer_info.transfer_start_scn_))) {
    LOG_WARN("failed to init ls transfer info", K(ret), "ls_id", ls->get_ls_id(), K(tablet_id), K(tablet_transfer_info));
  }
  return ret;
}
int ObTenantCheckpointSlogHandler::concurrent_replay_load_tablets()
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  const int64_t total_tablet_cnt = replay_tablet_disk_addr_map_.size();
  int64_t cost_time_us = 0;
  ReplayTabletDiskAddrMap::iterator iter = replay_tablet_disk_addr_map_.begin();
  ObReplayCreateTabletTask *task = nullptr;
  int64_t task_idx = 0;
  while (OB_SUCC(ret) && iter != replay_tablet_disk_addr_map_.end()) {
    if (nullptr == task) {
      if (OB_ISNULL(task = reinterpret_cast<ObReplayCreateTabletTask*>(
          SERVER_STARTUP_TASK_HANDLER.get_task_allocator().alloc(sizeof(ObReplayCreateTabletTask))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc task buf", K(ret));
      } else if (FALSE_IT(task = new(task) ObReplayCreateTabletTask())) {
      } else if (OB_FAIL(task->init(task_idx++, share::ObTenantEnv::get_tenant(), this))) {
        LOG_WARN("fail to init ObReplayCreateTabletTask", K(ret), KPC(task));
      }
    }
    if (OB_SUCC(ret)) {
      bool is_enough = false;
      if (OB_FAIL(task->add_tablet_addr(iter->first, iter->second, is_enough))) {
        LOG_WARN("fail to add tablet", K(ret), K(iter->first), K(iter->second), KPC(task));
      } else if (is_enough) { // tablet count of this task is enough and will create a new task at next round
        if (OB_FAIL(add_replay_create_tablet_task(task))) {
          LOG_WARN("fail to add replay tablet task", K(ret), KPC(task), K(inflight_replay_tablet_task_cnt_));
        } else {
          task = nullptr;
          ++iter;
        }
      } else {
        ++iter;
      }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(task)) {
      task->~ObReplayCreateTabletTask();
      SERVER_STARTUP_TASK_HANDLER.get_task_allocator().free(task);
      task = nullptr;
    }
  }

  if (OB_SUCC(ret)) { // handle the last task
    if (OB_NOT_NULL(task) && OB_FAIL(add_replay_create_tablet_task(task))) {
      LOG_WARN("fail to add last replay tablet task", K(ret), KPC(task), K(inflight_replay_tablet_task_cnt_));
      task->~ObReplayCreateTabletTask();
      SERVER_STARTUP_TASK_HANDLER.get_task_allocator().free(task);
      task = nullptr;
    }
  }
  // waiting all task finish even if failure has occurred
  while (ATOMIC_LOAD(&inflight_replay_tablet_task_cnt_) != 0) {
    LOG_INFO("waiting replay create tablet task finish", K(inflight_replay_tablet_task_cnt_));
    ob_usleep(10 * 1000); // 10ms
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ATOMIC_LOAD(&replay_create_tablet_errcode_))) {
      LOG_WARN("ObReplayCreateTabletTask has failed", K(ret));
    } else if (ATOMIC_LOAD(&finished_replay_tablet_cnt_) != total_tablet_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("finished replay tablet cnt mismatch", K(ret), K_(finished_replay_tablet_cnt), K(total_tablet_cnt));
    }
  }

  cost_time_us = ObTimeUtility::current_time() - start_time;
  FLOG_INFO("finish concurrently repaly load tablets", K(ret), K(total_tablet_cnt), K(cost_time_us));

  return ret;
}

int ObTenantCheckpointSlogHandler::add_replay_create_tablet_task(ObReplayCreateTabletTask *task)
{
  int ret = OB_SUCCESS;
  bool need_retry = false;
  FLOG_INFO("add replay tablet task", KPC(task), K(inflight_replay_tablet_task_cnt_));
  do {
    need_retry = false;
    if (OB_FAIL(ATOMIC_LOAD(&replay_create_tablet_errcode_))) {
      LOG_WARN("ObReplayCreateTabletTask has failed", K(ret), K(inflight_replay_tablet_task_cnt_));
    } else if (OB_FAIL(SERVER_STARTUP_TASK_HANDLER.push_task(task))) {
      if (OB_EAGAIN == ret) {
        LOG_INFO("task queue is full, wait and retry", KPC(task), K(inflight_replay_tablet_task_cnt_));
        need_retry = true;
        ob_usleep(10 * 1000); // 10ms
      } else {
        LOG_WARN("fail to push task", K(ret), KPC(task), K(inflight_replay_tablet_task_cnt_));
      }
    }
  } while(OB_FAIL(ret) && need_retry);

  return ret;
}

int ObTenantCheckpointSlogHandler::replay_create_tablets_per_task(
    const ObIArray<std::pair<ObTabletMapKey, ObMetaDiskAddr>> &tablet_addr_arr)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = 0;
  ObTabletTransferInfo tablet_transfer_info;

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_addr_arr.count(); i++) {
    ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "ReplayTablet"));
    const ObTabletMapKey &key = tablet_addr_arr.at(i).first;
    const ObMetaDiskAddr &addr = tablet_addr_arr.at(i).second;
    ObLSTabletService *ls_tablet_svr = nullptr;
    ObLSHandle ls_handle;
    tablet_transfer_info.reset();
    if (OB_FAIL(ATOMIC_LOAD(&replay_create_tablet_errcode_))) {
      LOG_WARN("replay create has already failed", K(ret));
    } else {
      // io maybe timeout, so need retry
      int64_t max_retry_time = 5;
      do {
        if (OB_FAIL(read_from_disk(addr, allocator, buf, buf_len))) {
          LOG_WARN("fail to read from disk", K(ret), K(addr), KP(buf), K(buf_len));
        } else if (OB_FAIL(get_tablet_svr(key.ls_id_, ls_tablet_svr, ls_handle))) {
          LOG_WARN("fail to get ls tablet service", K(ret));
        } else if (OB_FAIL(ls_tablet_svr->replay_create_tablet(addr, buf, buf_len, key.tablet_id_, tablet_transfer_info))) {
          LOG_WARN("fail to create tablet for replay", K(ret), K(key), K(addr));
        }
      } while (OB_FAIL(ret) && OB_TIMEOUT == ret && max_retry_time-- > 0);

      if (OB_SUCC(ret)) {
        if (tablet_transfer_info.has_transfer_table() &&
            OB_FAIL(record_ls_transfer_info(ls_handle, key.tablet_id_, tablet_transfer_info))) {
          LOG_WARN("fail to record_ls_transfer_info", K(ret), K(key), K(tablet_transfer_info));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    inc_finished_replay_tablet_cnt(tablet_addr_arr.count());
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
    TCRLockGuard guard(slog_check_lock_);
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

int ObTenantCheckpointSlogHandler::write_checkpoint(bool is_force)
{
  int ret = OB_SUCCESS;
  int64_t alert_interval = ObWriteCheckpointTask::FAIL_WRITE_CHECKPOINT_ALERT_INTERVAL;
  int64_t min_interval = ObWriteCheckpointTask::RETRY_WRITE_CHECKPOINT_MIN_INTERVAL;

  uint64_t tenant_id = MTL_ID();
  ObTenantSuperBlock tmp_super_block(tenant_id);
  ObTenantStorageCheckpointWriter tenant_storage_ckpt_writer;
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  ObTenantSuperBlock last_super_block = tenant->get_super_block();

  //Don't compare to MTL(ObTenantTabletScheduler*)->get_frozen_version(), because we expect to do
  //checkpoint after merge finish.
  // 1) avoid IO traffic between ckpt and major
  // 2) truncate slog totally generated by major tablet
  const int64_t broadcast_version = MTL(ObTenantTabletScheduler*)->get_frozen_version();
  const int64_t frozen_version = MTL(ObTenantTabletScheduler*)->get_inner_table_merged_scn();
  const bool is_major_doing = (frozen_version != broadcast_version);
  bool is_writing_checkpoint_set = false;
  const int64_t start_time = ObTimeUtility::current_time();
  int64_t cost_time = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCheckpointSlogHandler not init", K(ret));
  } else {
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
             || (last_frozen_version_ < frozen_version && !is_major_doing) // major complete
             || ((start_time > last_ckpt_time_ + min_interval) // slog is long
                 && !is_major_doing
                 && ckpt_cursor_.newer_than(last_super_block.replay_start_point_)
                 &&(ckpt_cursor_.log_id_ - last_super_block.replay_start_point_.log_id_ >= ObWriteCheckpointTask::MIN_WRITE_CHECKPOINT_LOG_CNT))) {
    DEBUG_SYNC(AFTER_CHECKPOINT_GET_CURSOR);

    tmp_super_block.replay_start_point_ = ckpt_cursor_;
    if (OB_FAIL(tenant_storage_ckpt_writer.init())) {
      LOG_WARN("fail to init tenant_storage_ckpt_writer_", K(ret));
    } else if (OB_FAIL(tenant_storage_ckpt_writer.write_checkpoint(tmp_super_block))) {
      LOG_WARN("fail to write_checkpoint", K(ret));
    }
    clean_copy_status();
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ObServerCheckpointSlogHandler::get_instance().write_tenant_super_block_slog(tmp_super_block))) {
      LOG_WARN("fail to write_tenant_super_block_slog", K(ret), K(tmp_super_block));
    } else if (OB_FAIL(update_tablet_meta_addr_and_block_list(
        last_super_block.is_old_version(), tenant_storage_ckpt_writer))) {
      LOG_ERROR("fail to update_tablet_meta_addr_and_block_list", K(ret), K(last_super_block));
      // abort if failed, because it cannot be rolled back if partially success.
      // otherwise, updates need to be transactional.
      ob_usleep(1000 * 1000);
      ob_abort();
    } else {
      tenant->set_tenant_super_block(tmp_super_block);
      if (OB_FAIL(MTL(ObStorageLogger *)->remove_useless_log_file(ckpt_cursor_.file_id_, MTL_ID()))) {
        LOG_WARN("fail to remove_useless_log_file", K(ret), K(tmp_super_block));
      } else {
        last_ckpt_time_ = start_time;
        last_frozen_version_ = frozen_version;
        cost_time = ObTimeUtility::current_time() - start_time;
      }
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(tenant_storage_ckpt_writer.rollback())) {
      LOG_ERROR("fail to rollback checkpoint, macro blocks leak", K(tmp_ret));
    }

    FLOG_INFO("finish write tenant checkpoint", K(ret), K(last_super_block), K(tmp_super_block),
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

int ObTenantCheckpointSlogHandler::get_cur_cursor()
{
  int ret = OB_SUCCESS;
  TCWLockGuard guard(slog_check_lock_);
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
  TCWLockGuard guard(slog_check_lock_);
  is_copying_tablets_ = false;
  tablet_key_set_.destroy();
}

int ObTenantCheckpointSlogHandler::update_tablet_meta_addr_and_block_list(
    const bool is_replay_old,
    ObTenantStorageCheckpointWriter &ckpt_writer)
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
  if (OB_FAIL(ckpt_writer.batch_compare_and_swap_tablet(is_replay_old))) {
    LOG_WARN("fail to update_tablet_meta_addr", K(ret), K(is_replay_old));
  }

  TCWLockGuard guard(lock_);
  if (OB_FAIL(ret)) {
  } else {
    do {
      if (OB_FAIL(ckpt_writer.get_ls_block_list(meta_block_list))) {
        LOG_WARN("fail to get_ls_block_list", K(ret));
      } else if (OB_FAIL(ls_block_handle_.add_macro_blocks(*meta_block_list))) {
        LOG_WARN("fail to add_macro_blocks", K(ret));
      } else if (OB_FAIL(ckpt_writer.get_tablet_block_list(meta_block_list))) {
        LOG_WARN("fail to get_tablet_block_list", K(ret));
      } else if (OB_FAIL(tablet_block_handle_.add_macro_blocks(*meta_block_list))) {
        LOG_WARN("fail to set_tablet_block_list", K(ret));
      }
      if (OB_UNLIKELY(OB_ALLOCATE_MEMORY_FAILED == ret)) {
        LOG_WARN("memory is insufficient, retry", K(ret));
      }
    } while (OB_UNLIKELY(OB_ALLOCATE_MEMORY_FAILED == ret));
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
  ObLSIDLog slog_entry(ls_id);
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

int ObTenantCheckpointSlogHandler::get_tablet_svr(
    const ObLSID &ls_id,
    ObLSTabletService *&ls_tablet_svr,
    ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls handle", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_tablet_svr = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet service is null", K(ret), K(ls_id));
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

        if (OB_FAIL(slog_entry.deserialize(buf, len, pos))) {
          LOG_WARN("fail to deserialize tablet meta", K(ret), KP(buf), K(len), K(pos));
        }
        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "update tablet slog: ");
        if (0 > fprintf(stream, "%s\n version:%d length:%d\n%s\n", slog_name, version, length, to_cstring(slog_entry))) {
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
