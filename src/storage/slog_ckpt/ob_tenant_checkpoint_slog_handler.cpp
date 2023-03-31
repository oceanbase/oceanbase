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
#include "storage/slog/ob_storage_log_replayer.h"
#include "storage/slog/ob_storage_log.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/tx/ob_timestamp_service.h"
#include "storage/tx/ob_trans_id_service.h"
#include "observer/omt/ob_tenant.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "sql/das/ob_das_id_service.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace blocksstable;
namespace storage
{
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
    lock_(),
    ls_block_handle_(),
    tablet_block_handle_(),
    tg_id_(-1),
    write_ckpt_task_(this),
    replay_tablet_disk_addr_map_()
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
    LOG_WARN("fail to replay_tenant_slog", K(ret));
  } else if (OB_FAIL(MTL(ObLSService*)->gc_ls_after_replay_slog())) {
    LOG_WARN("fail to gc ls after replay slog", K(ret));
  } else {
    replay_tablet_disk_addr_map_.destroy();
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::replay_checkpoint(const ObTenantSuperBlock &super_block)
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
  } else if (OB_FAIL(ls_block_handle_.add_macro_blocks(meta_block_list, true /*switch handle*/))) {
    LOG_WARN("fail to add_macro_blocks", K(ret));
  } else if (OB_FAIL(tenant_storage_ckpt_reader.iter_read_checkpoint_item(
      super_block.tablet_meta_entry_, replay_tablet_op, meta_block_list))) {
    LOG_WARN("fail to replay tablet checkpoint", K(ret));
  } else if (OB_FAIL(tablet_block_handle_.add_macro_blocks(meta_block_list, true /*switch handle*/))) {
    LOG_WARN("fail to add_macro_blocks", K(ret));
  }

  LOG_INFO("finish replay tenant checkpoint", K(ret), K(super_block));

  return ret;
}

int ObTenantCheckpointSlogHandler::replay_ls_meta(
  const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  UNUSED(addr);
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
  ObTabletMapKey map_key;

  if (OB_UNLIKELY(!addr.is_valid() || nullptr == buf || buf_len <= 0 || !addr.is_block())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(addr));
  } else if (OB_FAIL(ObTablet::deserialize_id(buf, buf_len, map_key.ls_id_, map_key.tablet_id_))) {
    LOG_WARN("fail to deserialize log stream id and tablet id", K(ret));
  } else if (OB_FAIL(replay_tablet_disk_addr_map_.set_refactored(map_key, addr, 0/*should not exist*/))) {
    LOG_WARN("update tablet meta addr fail", K(ret), K(map_key), K(addr));
  } else {
    LOG_INFO("Successfully load tablet ckpt", K(map_key), K(addr));
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
  } else if (OB_FAIL(replay_load_tablets())) {
    LOG_WARN("fail to replay load tablets", K(ret));
  } else if (OB_FAIL(replayer.replay_over())) {
    LOG_WARN("fail to replay over", K(ret));
  } else if (OB_FAIL(MTL(ObStorageLogger *)->start_log(replay_finish_point))) {
    LOG_WARN("fail to start_slog", K(ret), K(replay_finish_point));
  }

  LOG_INFO("finish replay tenant slog", K(ret), K(start_point), K(replay_finish_point));

  return ret;
}

int ObTenantCheckpointSlogHandler::replay_load_tablets()
{
  int ret = OB_SUCCESS;
  const ObMemAttr mem_attr(MTL_ID(), "TenantReplay");
  char *buf = nullptr;
  int64_t buf_len = 0;
  char *r_buf = nullptr;
  int64_t r_len = 0;
  ObArray<ObTabletMapKey> tablets;
  ReplayTabletDiskAddrMap::iterator iter = replay_tablet_disk_addr_map_.begin();
  while (OB_SUCC(ret) && iter != replay_tablet_disk_addr_map_.end()) {
    const ObTabletMapKey &key = iter->first;
    if (OB_FAIL(tablets.push_back(key))) {
      LOG_WARN("fail to push table key into array", K(ret), K(key));
    } else {
      ++iter;
    }
  }
  if (OB_SUCC(ret)) {
    std::sort(tablets.begin(), tablets.end(), [](ObTabletMapKey &l, ObTabletMapKey &r) {
      bool ret = true;
      if (l.tablet_id_.is_inner_tablet() && !r.tablet_id_.is_inner_tablet()) {
        ret = true;
      } else if (!l.tablet_id_.is_inner_tablet() && r.tablet_id_.is_inner_tablet()) {
        ret = false;
      } else { // Both of tablets is inner or non-inner.
        if (l.ls_id_ < r.ls_id_) {
          ret = true;
        } else if (l.ls_id_ > r.ls_id_) {
          ret = false;
        } else if (l.tablet_id_ < r.tablet_id_) {
          ret = true;
        } else {
          ret = false;
        }
      }
      return ret;
    });
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tablets.count(); ++i) {
    const ObTabletMapKey &map_key = tablets.at(i);
    ObMetaDiskAddr tablet_addr;
    ObLSTabletService *ls_tablet_svr = nullptr;
    ObLSHandle ls_handle;
    if (OB_FAIL(replay_tablet_disk_addr_map_.get_refactored(map_key, tablet_addr))) {
      LOG_WARN("fail to get tablet address", K(ret), K(map_key));
    } else {
      if (OB_NOT_NULL(buf)) {
        if (buf_len >= tablet_addr.size()) {
          // reuse last buf to reduce malloc
        } else {
          ob_free(buf);
          buf = nullptr;
          buf_len = 0;
        }
      }
      if (OB_ISNULL(buf)) {
        if (OB_ISNULL(buf = (char*)ob_malloc(tablet_addr.size(), mem_attr))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate tablet buffer", K(ret), K(tablet_addr));
        } else {
          buf_len = tablet_addr.size();
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(read_from_disk_addr(tablet_addr, buf, buf_len, r_buf, r_len))) {
      LOG_WARN("fail to read tablet from addr", K(ret), K(tablet_addr));
    } else if (OB_FAIL(get_tablet_svr(map_key.ls_id_, ls_tablet_svr, ls_handle))) {
     LOG_WARN("fail to get ls tablet service", K(ret));
    } else if (OB_FAIL(ls_tablet_svr->replay_create_tablet(
        tablet_addr, r_buf, r_len, map_key.tablet_id_))) {
     LOG_WARN("fail to create tablet for replay", K(ret), K(map_key), K(tablet_addr));
    }
    LOG_INFO("Successfully load tablet", K(map_key), K(tablet_addr));
  }
  if (OB_NOT_NULL(buf)) {
    ob_free(buf);
    buf = nullptr;
  }
  return ret;
}

int ObTenantCheckpointSlogHandler::write_checkpoint(bool is_force)
{
  int ret = OB_SUCCESS;
  ObLogCursor cur_cursor;
  int64_t alert_interval = ObWriteCheckpointTask::FAIL_WRITE_CHECKPOINT_ALERT_INTERVAL;
  int64_t min_interval = ObWriteCheckpointTask::RETRY_WRITE_CHECKPOINT_MIN_INTERVAL;

  uint64_t tenant_id = MTL_ID();
  ObTenantSuperBlock tmp_super_block(tenant_id);
  ObTenantStorageCheckpointWriter tenant_storage_ckpt_writer;
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  ObTenantSuperBlock last_super_block = tenant->get_super_block();


  int64_t frozen_version = MTL(ObTenantTabletScheduler*)->get_frozen_version();
  bool is_writing_checkpoint_set = false;
  const int64_t start_time = ObTimeUtility::current_time();
  int64_t cost_time = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCheckpointSlogHandler not init", K(ret));
  } else if (!ATOMIC_BCAS(&is_writing_checkpoint_, false, true)) {
    ret = OB_NEED_WAIT;
    LOG_WARN("is writing checkpoint, need wait", K(ret));
  } else {
    is_writing_checkpoint_set = true;
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_UNLIKELY(tenant->is_hidden())) {
    LOG_INFO("maybe hidden sys, skip checkpoint");
  } else if (OB_UNLIKELY(!last_super_block.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tenant super block", K(ret), K(last_super_block));
  } else if (OB_FAIL(MTL(ObStorageLogger *)->get_active_cursor(cur_cursor))) {
    LOG_WARN("get slog current cursor fail", K(ret));
  } else if (OB_UNLIKELY(!cur_cursor.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur_cursor is invalid", K(ret));
  } else if (is_force || (last_frozen_version_ < frozen_version) ||
    ((start_time > last_ckpt_time_ + min_interval) && cur_cursor.newer_than(last_super_block.replay_start_point_) &&
      (cur_cursor.log_id_ - last_super_block.replay_start_point_.log_id_ >=
        ObWriteCheckpointTask::MIN_WRITE_CHECKPOINT_LOG_CNT))) {
    tmp_super_block.replay_start_point_ = cur_cursor;
    if (OB_FAIL(tenant_storage_ckpt_writer.init())) {
      LOG_WARN("fail to init tenant_storage_ckpt_writer_", K(ret));
    } else if (OB_FAIL(tenant_storage_ckpt_writer.write_checkpoint(tmp_super_block))) {
      LOG_WARN("fail to write_checkpoint", K(ret));
    } else if (OB_FAIL(ObServerCheckpointSlogHandler::get_instance().write_tenant_super_block_slog(tmp_super_block))) {
      LOG_WARN("fail to write_tenant_super_block_slog", K(ret), K(tmp_super_block));
    } else if (OB_FAIL(update_tablet_meta_addr_and_block_list(tenant_storage_ckpt_writer))) {
      LOG_WARN("fail to update_tablet_meta_addr_and_block_list", K(ret));
      // abort if failed, because it cannot be rolled back if partially success.
      // otherwise, updates need to be transactional.
      ob_abort();
    } else if (FALSE_IT(tenant->set_tenant_super_block(tmp_super_block))) {
    } else if (OB_FAIL(MTL(ObStorageLogger *)->remove_useless_log_file(cur_cursor.file_id_, MTL_ID()))) {
      LOG_WARN("fail to remove_useless_log_file", K(ret), K(tmp_super_block));
    } else {
      last_ckpt_time_ = start_time;
      last_frozen_version_ = frozen_version;
      cost_time = ObTimeUtility::current_time() - start_time;
    }
    FLOG_INFO("finish write tenant checkpoint", K(ret), K(last_super_block), K(tmp_super_block),
        K_(last_ckpt_time), K(start_time), K(frozen_version), K_(last_frozen_version), K(is_force), K(cost_time));
    SERVER_EVENT_ADD("storage", "write slog checkpoint", "tenant_id", tenant_id,
        "ret", ret, "cursor", cur_cursor, "frozen_version", frozen_version, "cost_time(us)", cost_time);
  }

  if (is_writing_checkpoint_set) {
    ATOMIC_STORE(&is_writing_checkpoint_, false);
  }

  return ret;
}

int ObTenantCheckpointSlogHandler::update_tablet_meta_addr_and_block_list(
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
  if (OB_FAIL(ckpt_writer.update_tablet_meta_addr())) {
    LOG_WARN("fail to update_tablet_meta_addr", K(ret));
  }

  TCWLockGuard guard(lock_);
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to update_tablet_meta_addr", K(ret));
  } else if (OB_FAIL(ckpt_writer.get_ls_block_list(meta_block_list))) {
    LOG_WARN("fail to get_ls_block_list", K(ret));
  } else if (OB_FAIL(ls_block_handle_.add_macro_blocks(*meta_block_list, true /*switch handle*/))) {
    LOG_WARN("fail to add_macro_blocks", K(ret));
  } else if (OB_FAIL(ckpt_writer.get_tablet_block_list(meta_block_list))) {
    LOG_WARN("fail to get_tablet_block_list", K(ret));
  } else if (OB_FAIL(
               tablet_block_handle_.add_macro_blocks(*meta_block_list, true /*switch handle*/))) {
    LOG_WARN("fail to set_tablet_block_list", K(ret));
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
    case ObRedoLogSubType::OB_REDO_LOG_PUT_TABLET: {
      if (OB_FAIL(inner_replay_put_tablet(param))) {
        LOG_WARN("fail to replay create tablet slog", K(ret), K(param));
      }
      break;
    }
    case ObRedoLogSubType::OB_REDO_LOG_DELETE_TABLET: {
      if (OB_FAIL(inner_replay_delete_tablet(param))) {
        LOG_WARN("fail to replay delete tablet slog", K(param));
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
    LOG_INFO("successfully replay ls meta slog", K(param), K(pos));
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
    LOG_INFO("successfully replay ls meta slog", K(param), K(pos));
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

int ObTenantCheckpointSlogHandler::inner_replay_put_tablet(const ObRedoModuleReplayParam &param)
{
  int ret = OB_SUCCESS;
  ObTabletMapKey map_key;

  if (OB_FAIL(ObTablet::deserialize_id(param.buf_, param.disk_addr_.size(), map_key.ls_id_, map_key.tablet_id_))) {
    LOG_WARN("fail to deserialize ls id and tablet id", K(param));
  } else if (OB_FAIL(replay_tablet_disk_addr_map_.set_refactored(map_key, param.disk_addr_, 1/*overwrite if exist*/))) {
    LOG_WARN("update tablet meta addr fail", K(ret), K(map_key), K(param.disk_addr_));
  } else {
    LOG_INFO("Successfully load tablet from slog", K(map_key), K(param.disk_addr_));
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

int ObTenantCheckpointSlogHandler::get_tablet_svr(
    const ObLSID &ls_id,
    ObLSTabletService *&ls_tablet_svr,
    ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls handle", K(ls_id));
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
      case ObRedoLogSubType::OB_REDO_LOG_PUT_TABLET: {
        ObTabletMeta tablet_meta;
        ObArenaAllocator allocator;
        int64_t pos = 0;
        int32_t length = 0;
        int32_t version = 0;

        if (OB_FAIL(serialization::decode_i32(buf, len, pos, &version))) {
          LOG_WARN("failed to deserialize version");
        } else if (OB_FAIL(serialization::decode_i32(buf, len, pos, &length))) {
          LOG_WARN("failed to deserialize length");
        } else if (OB_FAIL(tablet_meta.deserialize(allocator, buf, len, pos))) {
          LOG_WARN("fail to deserialize tablet meta", K(ret), KP(buf), K(len), K(pos));
        }
        snprintf(slog_name, ObStorageLogReplayer::MAX_SLOG_NAME_LEN, "put tablet slog: ");
        if (0 > fprintf(stream, "%s\n version:%d length:%d\n%s\n", slog_name, version, length, to_cstring(tablet_meta))) {
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
  int ret = OB_SUCCESS;

  common::ObSharedGuard<ObLSIterator> ls_iter;
  ObLS *ls = nullptr;
  ObLSTabletService *ls_tablet_svr = nullptr;
  if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls iter", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next ls", K(ret));
        }
      } else if (nullptr == ls) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is null", K(ret));
      } else if (OB_FAIL(ls->finish_slog_replay())) {
        LOG_WARN("finish replay failed", K(ret), KPC(ls));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
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


}  // end namespace storage
}  // namespace oceanbase
