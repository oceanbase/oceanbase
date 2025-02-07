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

#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/slog_ckpt/ob_server_checkpoint_reader.h"
#include "storage/slog_ckpt/ob_server_checkpoint_writer.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "storage/ob_super_block_struct.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_tenant.h"
#include "storage/slog/ob_storage_log_replayer.h"
#include "storage/slog/ob_storage_log.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "storage/slog/ob_storage_logger.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

void ObServerCheckpointSlogHandler::ObWriteCheckpointTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  if (SERVER_STORAGE_META_SERVICE.is_started()) {
    if (OB_FAIL(handler_->write_checkpoint(false/*is_force*/))) {
      LOG_WARN("fail to write checkpoint", K(ret));
    }
  } else {
    // 必须等待所有的slog回放完成才能做ckpt,否则有些macro block可能没有被mark
    LOG_INFO("slog replay not finish, do not write checkpoint");
  }
}

ObServerCheckpointSlogHandler::ObServerCheckpointSlogHandler()
  : is_inited_(false),
    is_writing_checkpoint_(false),
    server_slogger_(nullptr),
    lock_(common::ObLatchIds::SLOG_CKPT_LOCK),
    server_meta_block_handle_(),
    write_ckpt_task_(this),
    task_timer_()
{
}

int ObServerCheckpointSlogHandler::init(ObStorageLogger *server_slogger)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObServerCheckpointSlogHandler has inited", K(ret));
  } else if (OB_FAIL(task_timer_.set_run_wrapper(MTL_CTX()))) {
    LOG_WARN("fail to set timer's run wrapper", K(ret));
  } else if (OB_FAIL(task_timer_.init("ServerCkptSlogHandler"))) {
    LOG_WARN("fail to init task timer", K(ret));
  } else if (OB_FAIL(task_timer_.schedule(write_ckpt_task_,
      ObWriteCheckpointTask::WRITE_CHECKPOINT_INTERVAL_US, true /*repeate*/))) {
    LOG_WARN("fail to schedule write checkpoint task", K(ret));
  } else {
    server_slogger_ = server_slogger;
    is_inited_ = true;
  }
  return ret;
}

int ObServerCheckpointSlogHandler::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(task_timer_.start())) { // start checkpoint task after finsh replay slog
    LOG_WARN("fail to start task timer", K(ret));
  }
  return ret;
}

int ObServerCheckpointSlogHandler::start_replay(TENANT_META_MAP &tenant_meta_map)
{
  int ret = OB_SUCCESS;

  const ObServerSuperBlock &super_block = OB_STORAGE_OBJECT_MGR.get_server_super_block();
  ObLogCursor replay_finish_point;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!super_block.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("super block is invalid", K(ret), K(super_block));
  } else {
    tenant_meta_map_for_replay_ = &tenant_meta_map;

    if (OB_FAIL(read_checkpoint(super_block))) {
      LOG_WARN("fail to read_checkpoint", K(ret));
    } else if (OB_FAIL(replay_server_slog(super_block.body_.replay_start_point_, replay_finish_point))) {
      LOG_WARN("fail to replay_sever_slog", K(ret), K(super_block));
    } else if (OB_FAIL(server_slogger_->start_log(replay_finish_point))) {
      LOG_WARN("fail to start slog", K(ret));
    }
  }
  return ret;
}

int ObServerCheckpointSlogHandler::do_post_replay_work()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.first_mark_device())) { // mark must after finish replay slog
    LOG_WARN("fail to first mark device", K(ret));
  } else if (OB_FAIL(try_write_checkpoint_for_compat())) {
    LOG_WARN("fail to try write checkpoint for compat", K(ret));
  } else {
    tenant_meta_map_for_replay_ = nullptr;
  }
  return ret;
}

void ObServerCheckpointSlogHandler::stop()
{
  task_timer_.stop();
}

void ObServerCheckpointSlogHandler::wait()
{
  task_timer_.wait();
}

void ObServerCheckpointSlogHandler::destroy()
{
  is_inited_ = false;
  task_timer_.destroy();
}

int ObServerCheckpointSlogHandler::try_write_checkpoint_for_compat()
{
  int ret = OB_SUCCESS;
  common::ObArray<omt::ObTenantMeta> tenant_metas;
  omt::ObMultiTenant *omt = GCTX.omt_;
  if (OB_ISNULL(omt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, omt is nullptr", K(ret));
  } else if (OB_FAIL(omt->get_tenant_metas_for_ckpt(tenant_metas))) {
    LOG_WARN("fail to get tenant metas", K(ret), KP(omt));
  } else {
    bool need_svr_ckpt = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_metas.size(); ++i) {
      ObTenantSuperBlock &super_block = tenant_metas.at(i).super_block_;
      if (!super_block.is_old_version()) {
        // nothing to do.
      } else {
        MTL_SWITCH(super_block.tenant_id_) {
          if (OB_FAIL(MTL(ObTenantStorageMetaService*)->write_checkpoint(true/*is_force*/))) {
            LOG_WARN("fail to write tenant slog checkpoint", K(ret));
          } else {
            // we don't write checkpoint or update super_block for hidden tenant
            // so it is necessary to update version here
            if (super_block.is_hidden_) {
              super_block.version_ = ObTenantSuperBlock::TENANT_SUPER_BLOCK_VERSION;
              omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
              tenant->set_tenant_super_block(super_block);
            }
            need_svr_ckpt = true;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (need_svr_ckpt && OB_FAIL(write_checkpoint(true/*is_force*/))) {
        LOG_WARN("fail to write server checkpoint", K(ret));
      }
    }
  }
  return ret;
}

int ObServerCheckpointSlogHandler::read_checkpoint(const ObServerSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
  ObServerCheckpointReader server_ckpt_reader;

  if (OB_FAIL(server_ckpt_reader.read_checkpoint(super_block))) {
    LOG_WARN("fail to read checkpoint", K(ret), K(super_block));
  } else if (OB_FAIL(set_meta_block_list(server_ckpt_reader.get_meta_block_list()))) {
    LOG_WARN("fail to set meta block list", K(ret));
  } else if (OB_FAIL(server_ckpt_reader.get_tenant_metas(*tenant_meta_map_for_replay_))) {
    LOG_WARN("fail to get tenant metas", K(ret));
  }
  return ret;
}

int ObServerCheckpointSlogHandler::set_meta_block_list(ObIArray<MacroBlockId> &meta_block_list)
{
  int ret = OB_SUCCESS;
  TCWLockGuard guard(lock_);
  if (OB_FAIL(server_meta_block_handle_.add_macro_blocks(meta_block_list))) {
    LOG_WARN("fail to add_macro_blocks", K(ret));
  }
  return ret;
}

int ObServerCheckpointSlogHandler::get_meta_block_list(ObIArray<MacroBlockId> &meta_block_list)
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);
  meta_block_list.reset();
  const ObIArray<blocksstable::MacroBlockId> &block_list = server_meta_block_handle_.get_meta_block_list();

  for (int64_t i = 0; OB_SUCC(ret) && i < block_list.count(); ++i) {
    if (OB_FAIL(meta_block_list.push_back(block_list.at(i)))) {
      LOG_WARN("fail to push back meta block", K(ret));
    }
  }
  return ret;
}

int ObServerCheckpointSlogHandler::replay_server_slog(const ObLogCursor &replay_start_point,
                                                      ObLogCursor &replay_finish_point)
{
  int ret = OB_SUCCESS;
  ObStorageLogReplayer replayer;
  blocksstable::ObLogFileSpec log_file_spec;
  log_file_spec.retry_write_policy_ = "normal";
  log_file_spec.log_create_policy_ = "normal";
  log_file_spec.log_write_policy_ = "truncate";

  if (OB_FAIL(replayer.init(server_slogger_->get_dir(), log_file_spec))) {
    LOG_WARN("fail to init slog replayer", K(ret));
  } else if (OB_FAIL(replayer.register_redo_module(
    ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT, this))) {
    LOG_WARN("fail to register redo module", K(ret));
  } else if (OB_FAIL(replayer.replay(replay_start_point, replay_finish_point, OB_SERVER_TENANT_ID))) {
    LOG_WARN("fail to replay server slog", K(ret));
  } else if (OB_FAIL(replayer.replay_over())) {
    LOG_WARN("fail to replay over server slog", K(ret));
  }
  return ret;
}

int ObServerCheckpointSlogHandler::replay(const ObRedoModuleReplayParam &param)
{
  int ret = OB_SUCCESS;
  const char *buf = param.buf_;
  const int64_t len = param.disk_addr_.size();
  ObRedoLogMainType main_type = ObRedoLogMainType::OB_REDO_LOG_MAX;
  enum ObRedoLogSubType sub_type;
  ObIRedoModule::parse_cmd(param.cmd_, main_type, sub_type);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT != main_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong redo log type.", K(ret), K(main_type), K(sub_type));
  } else {
    switch (sub_type) {
      case ObRedoLogSubType::OB_REDO_LOG_CREATE_TENANT_PREPARE: {
        if (OB_FAIL(replay_create_tenant_prepare(buf, len))) {
          LOG_WARN("failed to replay put tenant", K(ret), K(param));
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_CREATE_TENANT_COMMIT: {
        if (OB_FAIL(replay_create_tenant_commit(buf, len))) {
          LOG_WARN("failed to replay create tenant commit", K(ret), K(param));
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_CREATE_TENANT_ABORT: {
        if (OB_FAIL(replay_create_tenant_abort(buf, len))) {
          LOG_WARN("failed to replay create tenant abort", K(ret), K(param));
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_DELETE_TENANT_PREPARE: {
        if (OB_FAIL(replay_delete_tenant_prepare(buf, len))) {
          LOG_WARN("failed to replay delete tenant prepare", K(ret), K(param));
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_DELETE_TENANT_COMMIT: {
        if (OB_FAIL(replay_delete_tenant_commit(buf, len))) {
          LOG_WARN("failed to replay delete tenant commit", K(ret), K(param));
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_UPDATE_TENANT_UNIT: {
        if (OB_FAIL(replay_update_tenant_unit(buf, len))) {
          LOG_WARN("failed to replay update tenant unit", K(ret), K(param));
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_UPDATE_TENANT_SUPER_BLOCK: {
        if (OB_FAIL(replay_update_tenant_super_block(buf, len))) {
          LOG_WARN("failed to replay update tenant super block", K(ret), K(param));
        }
        break;
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("unknown subtype", K(ret), K(sub_type), K(param));
      }
    }
  }

  return ret;
}

int ObServerCheckpointSlogHandler::parse(
  const int32_t cmd, const char *buf, const int64_t len, FILE *stream)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObRedoLogMainType main_type = ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT;
  ObRedoLogSubType sub_type = ObRedoLogSubType::OB_REDO_LOG_INVALID;
  char slog_name[ObStorageLogReplayer::MAX_SLOG_NAME_LEN];

  ObIRedoModule::parse_cmd(cmd, main_type, sub_type);
  if (OB_ISNULL(buf) || OB_ISNULL(stream) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), KP(stream), K(len));
  } else if (OB_UNLIKELY(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT != main_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("slog type does not match", K(ret), K(main_type), K(sub_type));
  } else if (OB_UNLIKELY(0 > fprintf(stream, "\nserver slog: "))) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to write server slog to stream", K(ret));
  } else {
    switch (sub_type) {
      case ObRedoLogSubType::OB_REDO_LOG_CREATE_TENANT_PREPARE: {
        omt::ObTenantMeta tenant_meta;
        ObCreateTenantPrepareLog slog_entry(tenant_meta);
        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "create tenant prepare slog: ");
        if (OB_FAIL(ObStorageLogReplayer::print_slog(buf, len, slog_name, slog_entry, stream))) {
          LOG_WARN("fail to print slog", K(ret), KP(buf), K(len), K(slog_name), K(slog_entry));
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_CREATE_TENANT_COMMIT: {
        uint64_t tenant_id;
        ObCreateTenantCommitLog slog_entry(tenant_id);
        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "create tenant commit slog: ");
        if (OB_FAIL(ObStorageLogReplayer::print_slog(buf, len, slog_name, slog_entry, stream))) {
          LOG_WARN("fail to print slog", K(ret), KP(buf), K(len), K(slog_name), K(slog_entry));
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_CREATE_TENANT_ABORT: {
        uint64_t tenant_id;
        ObCreateTenantAbortLog slog_entry(tenant_id);
        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "create tenant abort slog: ");
        if (OB_FAIL(ObStorageLogReplayer::print_slog(buf, len, slog_name, slog_entry, stream))) {
          LOG_WARN("fail to print slog", K(ret), KP(buf), K(len), K(slog_name), K(slog_entry));
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_DELETE_TENANT_PREPARE: {
        uint64_t tenant_id;
        ObDeleteTenantPrepareLog slog_entry(tenant_id);
        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "delete tenant prepare slog: ");
        if (OB_FAIL(ObStorageLogReplayer::print_slog(buf, len, slog_name, slog_entry, stream))) {
          LOG_WARN("fail to print slog", K(ret), KP(buf), K(len), K(slog_name), K(slog_entry));
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_DELETE_TENANT_COMMIT: {
        uint64_t tenant_id;
        ObDeleteTenantPrepareLog slog_entry(tenant_id);
        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "delete tenant commit slog: ");
        if (OB_FAIL(ObStorageLogReplayer::print_slog(buf, len, slog_name, slog_entry, stream))) {
          LOG_WARN("fail to print slog", K(ret), KP(buf), K(len), K(slog_name), K(slog_entry));
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_UPDATE_TENANT_UNIT: {
        share::ObUnitInfoGetter::ObTenantConfig tenant_unit;
        ObUpdateTenantUnitLog slog_entry(tenant_unit);
        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "update tenant unit slog: ");
        if (OB_FAIL(ObStorageLogReplayer::print_slog(buf, len, slog_name, slog_entry, stream))) {
          LOG_WARN("fail to print slog", K(ret), KP(buf), K(len), K(slog_name), K(slog_entry));
        }
        break;
      }
      case ObRedoLogSubType::OB_REDO_LOG_UPDATE_TENANT_SUPER_BLOCK: {
        ObTenantSuperBlock super_block;
        ObUpdateTenantSuperBlockLog slog_entry(super_block);
        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "update tenant super block slog: ");
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

int ObServerCheckpointSlogHandler::replay_create_tenant_prepare(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  omt::ObTenantMeta meta;
  int64_t pos = 0;
  ObCreateTenantPrepareLog log_entry(meta);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerCheckpointSlogHandler is not initialized", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to decode log entry", K(ret));
  } else if (ObTenantCreateStatus::CREATING != meta.create_status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant create_status should be creating in prepare log", K(ret), K(meta));
  } else {
    // 可能已经在快照中, 如果之后任然发现prepare日志, 以之后的为准，即使快照已经表明create commit
    if (OB_FAIL(tenant_meta_map_for_replay_->set_refactored(meta.super_block_.tenant_id_, meta, 1))) {
      LOG_WARN("failed to set tenant meta map", K(ret), K(meta));
    }
  }

  return ret;
}

int ObServerCheckpointSlogHandler::replay_create_tenant_commit(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  int64_t pos = 0;
  ObCreateTenantCommitLog log_entry(tenant_id);
  omt::ObTenantMeta meta;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerCheckpointSlogHandler is not initialized", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to decode log entry", K(ret));
  } else if (OB_FAIL(tenant_meta_map_for_replay_->get_refactored(tenant_id, meta))) {
    LOG_WARN("failed to get tenant meta", K(ret), K(meta));
  } else if (ObTenantCreateStatus::CREATING != meta.create_status_ &&
      ObTenantCreateStatus::CREATED != meta.create_status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant create_status mismatch", K(ret), K(meta));
  } else if (FALSE_IT(meta.create_status_ = ObTenantCreateStatus::CREATED)) {
  } else if (OB_FAIL(tenant_meta_map_for_replay_->set_refactored(tenant_id, meta, 1))) {
    LOG_ERROR("failed to set tenant meta map", K(ret), K(meta));
  }

  return ret;
}

int ObServerCheckpointSlogHandler::replay_create_tenant_abort(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  int64_t pos = 0;
  ObCreateTenantAbortLog log_entry(tenant_id);
  omt::ObTenantMeta meta;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerCheckpointSlogHandler is not initialized", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to decode log entry", K(ret));
  } else if (OB_FAIL(tenant_meta_map_for_replay_->get_refactored(tenant_id, meta))) {
    if (OB_HASH_NOT_EXIST == ret) {
      LOG_INFO("tenant not exist when replay create abort slog", K(ret), K(tenant_id));
      ret = OB_SUCCESS;
      // no nothing
    } else {
      LOG_WARN("failed to get tenant meta", K(ret), K(meta));
    }
  // meta.create_status_== CREATE_COMMIT may because the status in memory is set to commit
  // and a checkpoint is created at this time,  but then the commit log fails to be written,
  // so an abort log is written.
  } else if (ObTenantCreateStatus::CREATING != meta.create_status_ &&
      ObTenantCreateStatus::CREATED != meta.create_status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant create_status mismatch", K(ret), K(meta));
  } else if (FALSE_IT(meta.create_status_ = ObTenantCreateStatus::CREATE_ABORT)) {
  } else if (OB_FAIL(tenant_meta_map_for_replay_->set_refactored(tenant_id, meta, 1))) {
    LOG_ERROR("failed to set tenant meta map", K(ret), K(meta));
  }

  return ret;
}

int ObServerCheckpointSlogHandler::replay_delete_tenant_prepare(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  int64_t pos = 0;
  ObDeleteTenantPrepareLog log_entry(tenant_id);
  omt::ObTenantMeta meta;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerCheckpointSlogHandler is not initialized", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to decode log entry", K(ret));
  } else if (OB_FAIL(tenant_meta_map_for_replay_->get_refactored(tenant_id, meta))) {
    if (OB_HASH_NOT_EXIST == ret) {
      LOG_INFO("tenant not exist when replay delete prepare slog", K(ret), K(tenant_id));
      ret = OB_SUCCESS;
      // no nothing
    } else {
      LOG_WARN("failed to get tenant meta", K(ret), K(tenant_id));
    }
  // meta.create_status_== DELETING may because the status in memory is set to DELETING
  // and a checkpoint is created when exit the lock for preventing to do ckpt.
  } else if (ObTenantCreateStatus::CREATED != meta.create_status_ &&
      ObTenantCreateStatus::DELETING != meta.create_status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant create_status mismatch", K(ret), K(meta));
  } else if (FALSE_IT(meta.create_status_ = ObTenantCreateStatus::DELETING)) {
  } else if (OB_FAIL(tenant_meta_map_for_replay_->set_refactored(tenant_id, meta, 1))) {
    LOG_ERROR("failed to set tenant meta map", K(ret), K(meta));
  }
  return ret;
}

int ObServerCheckpointSlogHandler::replay_delete_tenant_commit(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  int64_t pos = 0;
  ObDeleteTenantCommitLog log_entry(tenant_id);
  omt::ObTenantMeta meta;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerCheckpointSlogHandler is not initialized", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to decode log entry", K(ret));
  } else if (OB_FAIL(tenant_meta_map_for_replay_->get_refactored(tenant_id, meta))) {
    if (OB_HASH_NOT_EXIST == ret) {
      LOG_INFO("tenant not exist when replay delete commit slog", K(ret), K(tenant_id));
      ret = OB_SUCCESS;
      // no nothing
    } else {
      LOG_WARN("failed to get tenant meta", K(ret), K(tenant_id));
    }
  } else if (ObTenantCreateStatus::DELETING != meta.create_status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant create_status mismatch", K(ret), K(meta));
  } else if (FALSE_IT(meta.create_status_ = ObTenantCreateStatus::DELETED)) {
  } else if (OB_FAIL(tenant_meta_map_for_replay_->set_refactored(tenant_id, meta, 1))) {
    LOG_ERROR("failed to set tenant meta map", K(ret), K(meta));
  }
  return ret;
}

int ObServerCheckpointSlogHandler::replay_update_tenant_unit(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  share::ObUnitInfoGetter::ObTenantConfig unit;
  ObUpdateTenantUnitLog log_entry(unit);
  SMART_VAR(omt::ObTenantMeta, tenant_meta) {
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObServerCheckpointSlogHandler is not initialized", K(ret));
    } else if (OB_ISNULL(buf) || buf_len <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
    } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
      LOG_WARN("failed to decode log entry", K(ret));
    } else if (OB_FAIL(tenant_meta_map_for_replay_->get_refactored(unit.tenant_id_, tenant_meta))) {
      LOG_WARN("failed to get tenant meta", K(ret), K(unit));
    } else if (FALSE_IT(tenant_meta.unit_ = unit)) {
    } else if (OB_FAIL(tenant_meta_map_for_replay_->set_refactored(unit.tenant_id_, tenant_meta, 1))) {
      LOG_WARN("failed to set tenant meta map", K(ret), K(unit));
    }
  }

  return ret;
}

int ObServerCheckpointSlogHandler::replay_update_tenant_super_block(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTenantSuperBlock super_block;
  ObUpdateTenantSuperBlockLog log_entry(super_block);

  HEAP_VAR(omt::ObTenantMeta, tenant_meta) {
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObServerCheckpointSlogHandler is not initialized", K(ret));
    } else if (OB_ISNULL(buf) || buf_len <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
    } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
      LOG_WARN("failed to decode log entry", K(ret));
    } else if (OB_FAIL(tenant_meta_map_for_replay_->get_refactored(super_block.tenant_id_, tenant_meta))) {
      LOG_WARN("failed to get tenant meta", K(ret), K(super_block));
    } else if (FALSE_IT(tenant_meta.super_block_ = super_block)) {
    } else if (OB_FAIL(tenant_meta_map_for_replay_->set_refactored(super_block.tenant_id_, tenant_meta, 1))) {
      LOG_WARN("failed to set tenant meta map", K(ret), K(super_block));
    }
  }
  return ret;
}

int ObServerCheckpointSlogHandler::replay_over()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObServerCheckpointSlogHandler::write_checkpoint(bool is_force)
{
  int ret = OB_SUCCESS;

  static int64_t last_write_time_ = 0;
  static ObLogCursor last_slog_cursor_;

  ObLogCursor cur_cursor;
  int64_t alert_interval = ObWriteCheckpointTask::FAIL_WRITE_CHECKPOINT_ALERT_INTERVAL;
  int64_t min_interval = ObWriteCheckpointTask::RETRY_WRITE_CHECKPOINT_MIN_INTERVAL;
  bool is_writing_checkpoint_set = false;
  const int64_t start_time = ObTimeUtility::current_time();
  int64_t cost_time = 0;


  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if(!ATOMIC_BCAS(&is_writing_checkpoint_, false, true)) {
    ret = OB_NEED_WAIT;
    LOG_WARN("is writing checkpoint, need wait", K(ret));
  } else {
    is_writing_checkpoint_set = true;
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(server_slogger_->get_active_cursor(cur_cursor))) {
    LOG_WARN("get server slog current cursor fail", K(ret));
  } else if (OB_UNLIKELY(!cur_cursor.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur_cursor is invalid", K(ret));
  } else if (((start_time > last_write_time_ + min_interval) && cur_cursor.newer_than(last_slog_cursor_)
      && (cur_cursor.log_id_ - last_slog_cursor_.log_id_ >= ObWriteCheckpointTask::MIN_WRITE_CHECKPOINT_LOG_CNT))
      || is_force) {
    ObServerCheckpointWriter server_ckpt_writer;
    if (OB_FAIL(server_ckpt_writer.init(server_slogger_))) {
      LOG_WARN("fail to init ObServerCheckpointWriter", K(ret));
    } else if (OB_FAIL(server_ckpt_writer.write_checkpoint(cur_cursor))) {
      LOG_WARN("failt to write server checkpoint", K(ret));
    } else if (OB_FAIL(set_meta_block_list(server_ckpt_writer.get_meta_block_list()))) {
      LOG_WARN("fail to set meta block list", K(ret));
    } else {
      last_write_time_ = start_time;
      last_slog_cursor_ = cur_cursor;
      cost_time = ObTimeUtility::current_time() - start_time;
    }
    SERVER_EVENT_ADD("storage", "write slog checkpoint", "tenant_id", OB_SERVER_TENANT_ID,
        "ret", ret, "cursor", cur_cursor, "cost_time(us)", cost_time);

    LOG_INFO("finish write server checkpoint", K(ret), K(last_slog_cursor_), K(cur_cursor),
        K_(last_write_time), K(start_time), K(is_force), K(cost_time));
  }

  if (is_writing_checkpoint_set) {
    ATOMIC_STORE(&is_writing_checkpoint_, false);
  }

  return ret;
}

}  // end namespace storage
}  // namespace oceanbase
