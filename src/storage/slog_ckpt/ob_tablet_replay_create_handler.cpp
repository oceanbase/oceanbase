
/**
 * Copyright (c) 2023 OceanBase
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

#include "ob_tablet_replay_create_handler.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_tenant.h"
#include "share/scn.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/high_availability/ob_tablet_transfer_info.h"
#include "storage/high_availability/ob_storage_ha_struct.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "storage/blockstore/ob_shared_block_reader_writer.h"

namespace oceanbase
{
using namespace observer;
using namespace share;
using namespace blocksstable;

namespace storage
{

int ObTabletReplayCreateTask::init(
    const int64_t task_idx,
    const ObTabletReplayCreateTask::Type type,
    share::ObTenantBase *tenant_base,
    ObTabletReplayCreateHandler *handler)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task has been inited", K(ret), KPC(this));
  } else {
    idx_ = task_idx;
    type_ = type;
    tenant_base_ = tenant_base;
    handler_ = handler;
    handler->inc_inflight_task_cnt();
    is_inited_ = true;
  }
  return ret;
}

void ObTabletReplayCreateTask::destroy()
{
  if (IS_INIT) {
    handler_->dec_inflight_task_cnt();
    idx_ = -1;
    type_ = Type::MAX;
    tenant_base_ = nullptr;
    handler_ = nullptr;
    replay_item_range_arr_.reset();
    tablet_cnt_ = 0;
    is_inited_ = false;
  }
}
int ObTabletReplayCreateTask::execute()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not init", K(ret), KPC(this));
  } else {
    ObTenantSwitchGuard guard(tenant_base_);
    if (Type::DISCRETE == type_ &&
        OB_FAIL(handler_->replay_discrete_tablets(replay_item_range_arr_))) {
      LOG_WARN("fail to execute replay_discrete_tablets", K(ret), KPC(this));
    } else if (Type::AGGREGATE == type_ &&
        OB_FAIL(handler_->replay_aggregate_tablets(replay_item_range_arr_))) {
    } else {
      FLOG_INFO("successfully execute replay create tablet task", KPC(this));
      handler_->inc_finished_tablet_cnt(tablet_cnt_);
    }
  }
  if (OB_FAIL(ret)) {
    handler_->set_errcode(ret);
  }
  return ret;
}

int ObTabletReplayCreateTask::add_item_range(const ObTabletReplayItemRange &range, bool &is_enough)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not init", K(ret));
  } else if (OB_FAIL(replay_item_range_arr_.push_back(range))) {
    LOG_WARN("fail to push_back", K(ret), K(*this));
  } else {
    is_enough = false;
    tablet_cnt_ += range.second - range.first;
    if (AGGREGATE == type_) {
      if (replay_item_range_arr_.count() >= MAX_AGGREGATE_BLOCK_CNT_PER_TASK) {
        is_enough = true;
      }
    } else {
      if (tablet_cnt_ >= MAX_DISCRETE_TABLET_CNT_PER_TASK) {
        is_enough = true;
      }
    }
  }
  return ret;
}

// ObMetaDiskAddr order: FILE < BLOCK < RAW_BLOCK
// the FILE disk type is only for empty shell tablet
bool ObTabletReplayItem::operator<(const ObTabletReplayItem &r) const
{
  bool ret = false;
  if (addr_.type() < r.addr_.type()) {
    ret = true;
  } else if (addr_.type() == r.addr_.type()) {
    if (addr_.is_block()) {
      if (addr_.block_id() < r.addr_.block_id()) {
        ret = true;
      } else {
        // addrs in same block are no need to sort between themself by offset
      }
    } else {
      // FILE addrs are no need to sort between themself
    }
  }
  return ret;
}

//============================= ObTabletReplayCreateHandler ==============================//
ObTabletReplayCreateHandler::ObTabletReplayCreateHandler()
  : is_inited_(false),
    allocator_("TabletReplay", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    task_idx_(0),
    inflight_task_cnt_(0),
    finished_tablet_cnt_(0),
    errcode_(OB_SUCCESS),
    total_tablet_item_arr_(nullptr),
    total_tablet_cnt_(0),
    aggrgate_task_(nullptr),
    discrete_task_(nullptr)
{
}

int ObTabletReplayCreateHandler::init(
    const common::hash::ObHashMap<ObTabletMapKey, ObMetaDiskAddr> &tablet_item_map,
    const ObTabletRepalyOperationType replay_type)
{
  int ret = OB_SUCCESS;
  int64_t cost_time_us = 0;
  const int64_t start_time = ObTimeUtility::current_time();
  total_tablet_cnt_ = tablet_item_map.size();
  common::hash::ObHashMap<ObTabletMapKey, ObMetaDiskAddr>::const_iterator iter = tablet_item_map.begin();

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletReplayCreateHandler has inited", K(ret));
  } else if (0 == total_tablet_cnt_) {
    // do nothing
  } else if (OB_ISNULL(total_tablet_item_arr_ =
      static_cast<ObTabletReplayItem*>(allocator_.alloc(total_tablet_cnt_ * sizeof(ObTabletReplayItem))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc tablet_addr_arr", K(ret), K(total_tablet_cnt_));
  } else {
    int64_t i = 0;
    for ( ; iter != tablet_item_map.end(); iter++, i++) {
      total_tablet_item_arr_[i] = ObTabletReplayItem(iter->first, iter->second);
    }
    if (i != total_tablet_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet count mismatch", K(ret), K(i), K(total_tablet_cnt_));
    } else {
      lib::ob_sort(total_tablet_item_arr_, total_tablet_item_arr_ + total_tablet_cnt_);
    }
  }

  if (OB_SUCC(ret)) {
    cost_time_us = ObTimeUtility::current_time() - start_time;
    FLOG_INFO("finish init ObTabletReplayCreateHandler", K(ret), K(total_tablet_cnt_), K(cost_time_us));
    replay_type_ = replay_type;
    is_inited_ = true;
  }

  return ret;
}

int ObTabletReplayCreateHandler::concurrent_replay(ObStartupAccelTaskHandler* startup_accel_handler)
{
// for version <= 4.1 or FILE type addr, only support discrete replay
#define ADD_ITEM_RANGE_TO_TASK(                                                        \
    startup_accel_handler, start_item_idx, end_item_idx, only_support_discrete)        \
  ObTabletReplayItemRange range(start_item_idx, end_item_idx);                         \
  if (only_support_discrete) {                                                         \
    if (OB_FAIL(add_item_range_to_task_(startup_accel_handler,                         \
        ObTabletReplayCreateTask::DISCRETE, range, discrete_task_))) {                 \
      LOG_WARN("fail to add_item_range_to_task_", K(ret), KPC(discrete_task_));        \
    }                                                                                  \
  } else if (is_suitable_to_aggregate_(tablet_cnt_in_block, valid_size_in_block)) {    \
    if (OB_FAIL(add_item_range_to_task_(startup_accel_handler,                         \
        ObTabletReplayCreateTask::AGGREGATE, range, aggrgate_task_))) {                \
      LOG_WARN("fail to add_item_range_to_task_", K(ret), KPC(aggrgate_task_));        \
    }                                                                                  \
  } else {                                                                             \
    if (OB_FAIL(add_item_range_to_task_(startup_accel_handler,                         \
        ObTabletReplayCreateTask::DISCRETE, range, discrete_task_))) {                 \
      LOG_WARN("fail to add_item_range_to_task_", K(ret), KPC(discrete_task_));        \
    }                                                                                  \
  }

#define ADD_LAST_TASK(startup_accel_handler, task)                                     \
  if (OB_SUCC(ret)) {                                                                  \
    if (OB_NOT_NULL(task) && OB_FAIL(add_task_(startup_accel_handler, task))) {        \
      LOG_WARN("fail to add last task", K(ret), KPC(task), K(inflight_task_cnt_));     \
      task->~ObTabletReplayCreateTask();                                               \
      startup_accel_handler->get_task_allocator().free(task);                          \
      task = nullptr;                                                                  \
    }                                                                                  \
  }

  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletReplayCreateHandler not inited", K(ret));
  } else if (OB_ISNULL(startup_accel_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("startup_accel_handler is unexpected nullptr", K(ret));
  } else if (0 == total_tablet_cnt_) {
    // do nothing
  } else {
    int64_t tablet_cnt_in_block = 0;
    int64_t valid_size_in_block = 0;
    const bool is_old_version = static_cast<omt::ObTenant*>(MTL_CTX())->get_super_block().is_old_version();

    // <1> ObMetaDiskAddr order is FILE < BLOCK < RAW_BLOCK, so handle FILE type addrs firstly
    int64_t i = 0;
    while (i < total_tablet_cnt_ && total_tablet_item_arr_[i].addr_.is_file()) {
      i++;
    }
    if (i > 0) { // addrs of the file type is expected not much, so only use one task here
      ADD_ITEM_RANGE_TO_TASK(startup_accel_handler, 0, i, true);
    }

    // <2> handle block addr
    MacroBlockId pre_block_id = total_tablet_item_arr_[i].addr_.block_id();
    MacroBlockId curr_block_id;
    for ( ; OB_SUCC(ret) && i < total_tablet_cnt_; i++ ) {
      curr_block_id = total_tablet_item_arr_[i].addr_.block_id();
      if (pre_block_id == curr_block_id) {
        tablet_cnt_in_block ++;
        valid_size_in_block += upper_align(total_tablet_item_arr_[i].addr_.size(), 4096);
      } else {
        ADD_ITEM_RANGE_TO_TASK(
            startup_accel_handler, i - tablet_cnt_in_block, i, is_old_version); // [start_item_idx, end_item_idx)
        pre_block_id = curr_block_id;
        tablet_cnt_in_block = 1;
        valid_size_in_block = upper_align(total_tablet_item_arr_[i].addr_.size(), 4096);
      }
    }
    if (OB_SUCC(ret)) {  // handle last range
      ADD_ITEM_RANGE_TO_TASK(
          startup_accel_handler, total_tablet_cnt_ - tablet_cnt_in_block, total_tablet_cnt_, is_old_version);
    }
    // handle last task
    ADD_LAST_TASK(startup_accel_handler, aggrgate_task_);
    ADD_LAST_TASK(startup_accel_handler, discrete_task_);
#undef ADD_ITEM_RANGE_TO_TASK
#undef ADD_LAST_TASK

    // <3> waiting all task finish even if failure has occurred
    while (ATOMIC_LOAD(&inflight_task_cnt_) != 0) {
      LOG_INFO("waiting all inflight replay create tablet task finish", K(inflight_task_cnt_));
      ob_usleep(20 * 1000); // 20ms
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ATOMIC_LOAD(&errcode_))) {
        LOG_WARN("ObReplayCreateTabletTask has failed", K(ret));
      } else if (ATOMIC_LOAD(&finished_tablet_cnt_) != total_tablet_cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("finished replay tablet cnt mismatch", K(ret), K_(finished_tablet_cnt), K(total_tablet_cnt_));
      }
    }
  }

  int64_t cost_time_us = ObTimeUtility::current_time() - start_time;
  FLOG_INFO("finish concurrently repaly tablets", K(ret), K(total_tablet_cnt_), K(cost_time_us));
  return ret;
}

int ObTabletReplayCreateHandler::add_item_range_to_task_(ObStartupAccelTaskHandler* startup_accel_handler,
    const ObTabletReplayCreateTask::Type type, const ObTabletReplayItemRange &range, ObTabletReplayCreateTask *&task)
{
  int ret = OB_SUCCESS;

  if (nullptr == task) {
    if (OB_ISNULL(task = reinterpret_cast<ObTabletReplayCreateTask*>(
          startup_accel_handler->get_task_allocator().alloc(sizeof(ObTabletReplayCreateTask))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc task buf", K(ret));
    } else if (FALSE_IT(task = new(task) ObTabletReplayCreateTask())) {
    } else if (OB_FAIL(task->init(task_idx_++, type, MTL_CTX(), this))) {
      LOG_WARN("fail to init ObTabletReplayCreateTask", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    bool is_enough = false;
    if (OB_FAIL(task->add_item_range(range, is_enough))) {
      LOG_WARN("fail to add tablet item range", K(ret), K(range.first), K(range.second), KPC(task));
    } else if (is_enough) { // tablet count of this task is enough and will create a new task at next round
      if (OB_FAIL(add_task_(startup_accel_handler, task))) {
        LOG_WARN("fail to add replay tablet task", K(ret), KPC(task), K(inflight_task_cnt_));
      } else {
        task = nullptr;
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(task)) {
    task->~ObTabletReplayCreateTask();
    startup_accel_handler->get_task_allocator().free(task);
    task = nullptr;
  }
  return ret;
}

int ObTabletReplayCreateHandler::add_task_(ObStartupAccelTaskHandler* startup_accel_handler,
                                           ObTabletReplayCreateTask *task)
{
  int ret = OB_SUCCESS;
  bool need_retry = false;
  FLOG_INFO("add replay tablet task", KPC(task), K(inflight_task_cnt_));
  do {
    need_retry = false;
    if (OB_FAIL(ATOMIC_LOAD(&errcode_))) {
      LOG_WARN("someone ObTabletReplayCreateTask has failed", K(ret), K(inflight_task_cnt_));
    } else if (OB_FAIL(startup_accel_handler->push_task(task))) {
      if (OB_EAGAIN == ret) {
        LOG_INFO("task queue is full, wait and retry", KPC(task), K(inflight_task_cnt_));
        need_retry = true;
        ob_usleep(20 * 1000); // 20ms
      } else {
        LOG_WARN("fail to push task", K(ret), KPC(task), K(inflight_task_cnt_));
      }
    }
  } while(OB_FAIL(ret) && need_retry);

  return ret;
}

int ObTabletReplayCreateHandler::get_tablet_svr_(
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

int ObTabletReplayCreateHandler::replay_discrete_tablets(const ObIArray<ObTabletReplayItemRange> &range_arr)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = 0;
  ObArenaAllocator io_allocator("DiscreteRep", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletReplayCreateHandler not inited", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < range_arr.count(); i++) {
    for (int64_t idx = range_arr.at(i).first; OB_SUCC(ret) && idx < range_arr.at(i).second; idx++) {
      io_allocator.reuse();
      const ObTabletReplayItem &replay_item = total_tablet_item_arr_[idx];
      if (OB_FAIL(ATOMIC_LOAD(&errcode_))) {
        LOG_WARN("replay create has already failed", K(ret));
      } else {
        // io maybe timeout, so need retry
        int64_t max_retry_time = 5;
        do {
          if (OB_FAIL(MTL(ObTenantCheckpointSlogHandler*)->read_from_disk(replay_item.addr_, io_allocator, buf, buf_len))) {
            LOG_WARN("fail to read from disk", K(ret), K(replay_item), KP(buf), K(buf_len));
          }
        } while (OB_FAIL(ret) && OB_TIMEOUT == ret && max_retry_time-- > 0);
        if (OB_SUCC(ret) && OB_FAIL(do_replay(replay_item, buf, buf_len, io_allocator))) {
          LOG_WARN("fail to do replay", K(ret), K(replay_item));
        }
      }
    }
  }
  return ret;
}

int ObTabletReplayCreateHandler::replay_aggregate_tablets(const ObIArray<ObTabletReplayItemRange> &range_arr)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = 0;
  ObArenaAllocator io_allocator("AggregateRep", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  char *io_buf = nullptr;
  const int64_t io_buf_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletReplayCreateHandler not inited", K(ret));
  } else if (OB_ISNULL(io_buf =
      reinterpret_cast<char*>(io_allocator.alloc(io_buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc macro read info buffer", K(ret), K(io_buf_size));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < range_arr.count(); i++) {
    ObMacroBlockHandle macro_handle;
    ObMacroBlockReadInfo read_info;
    read_info.offset_ = 0;
    read_info.buf_ = io_buf;
    read_info.size_ = io_buf_size;
    read_info.io_timeout_ms_ = 20000; // 20s
    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
    read_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
    read_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    read_info.macro_block_id_ = total_tablet_item_arr_[range_arr.at(i).first].addr_.block_id();
    if (OB_FAIL(ObBlockManager::read_block(read_info, macro_handle))) {
      LOG_WARN("fail to read block", K(ret), K(read_info));
    }
    for (int64_t idx = range_arr.at(i).first; OB_SUCC(ret) && idx < range_arr.at(i).second; idx++) {
      const ObTabletReplayItem &replay_item = total_tablet_item_arr_[idx];
      if (OB_FAIL(ATOMIC_LOAD(&errcode_))) {
        LOG_WARN("replay create has already failed", K(ret));
      } else if (OB_FAIL(ObSharedBlockReaderWriter::parse_data_from_macro_block(macro_handle, replay_item.addr_, buf, buf_len))) {
        LOG_WARN("fail to parse_data_from_macro_block", K(ret), K(macro_handle), K(replay_item), K(i), K(idx));
      } else if (OB_FAIL(do_replay(replay_item, buf, buf_len, io_allocator))) {
        LOG_WARN("fail to do replay", K(ret), K(replay_item));
      }
    }
  }

  return ret;
}

int ObTabletReplayCreateHandler::do_replay(
    const ObTabletReplayItem &replay_item,
    const char *buf,
    const int64_t buf_len,
    ObArenaAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  switch (replay_type_) {
    case ObTabletRepalyOperationType::REPLAY_CREATE_TABLET:
      if (OB_FAIL(replay_create_tablet(replay_item, buf, buf_len))) {
        LOG_WARN("fail to replay create tablet", K(ret), K(replay_item));
      }
      break;
    case ObTabletRepalyOperationType::REPLAY_INC_MACRO_REF:
      if (OB_FAIL(replay_inc_macro_ref(replay_item, buf, buf_len, allocator))) {
        LOG_WARN("fail to replay inc macro ref", K(ret), K(replay_item));
      }
      break;
    case ObTabletRepalyOperationType::REPLAY_CLONE_TABLET:
      if (OB_FAIL(replay_clone_tablet(replay_item, buf, buf_len))) {
        LOG_WARN("fail to replay clone tablet", K(ret), K(replay_item));
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type", K(ret), K(replay_item), K(replay_type_));
      break;
  }
  return ret;
}

int ObTabletReplayCreateHandler::replay_create_tablet(const ObTabletReplayItem &replay_item, const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObLSTabletService *ls_tablet_svr = nullptr;
  ObLSHandle ls_handle;
  ObTabletTransferInfo tablet_transfer_info;
  const ObTabletMapKey &key = replay_item.key_;
  const ObMetaDiskAddr &addr = replay_item.addr_;
  ObLSRestoreStatus ls_restore_status;

  if (OB_FAIL(get_tablet_svr_(key.ls_id_, ls_tablet_svr, ls_handle))) {
    LOG_WARN("fail to get ls tablet service", K(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_restore_status(ls_restore_status))) {
    LOG_WARN("fail to get ls handle", K(ret), K(key));
  } else if (ls_restore_status.is_in_clone_and_tablet_meta_incomplete()) {
    LOG_INFO("the ls is_in_clone_and_tablet_meta_incomplete", K(key), K(ls_restore_status));
  } else if (OB_FAIL(ls_tablet_svr->replay_create_tablet(addr, buf, buf_len, key.tablet_id_, tablet_transfer_info))) {
    LOG_WARN("fail to create tablet for replay", K(ret), K(key), K(addr));
  } else if (tablet_transfer_info.has_transfer_table() &&
      OB_FAIL(record_ls_transfer_info_(ls_handle, key.tablet_id_, tablet_transfer_info))) {
    LOG_WARN("fail to record_ls_transfer_info", K(ret), K(key), K(tablet_transfer_info));
  }
  return ret;
}

int ObTabletReplayCreateHandler::replay_inc_macro_ref(
    const ObTabletReplayItem &replay_item,
    const char *buf,
    const int64_t buf_len,
    ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const ObTabletMapKey &key = replay_item.key_;
  const ObMetaDiskAddr &addr = replay_item.addr_;
  ObTablet tablet;
  int64_t pos = 0;
  tablet.set_tablet_addr(addr);
  if (OB_FAIL(tablet.inc_snapshot_ref_cnt(allocator, buf, buf_len, pos))) {
    LOG_WARN("fail to increase macro ref cnt", K(ret), K(tablet));
  }
  return ret;
}

int ObTabletReplayCreateHandler::replay_clone_tablet(const ObTabletReplayItem &replay_item, const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObLSTabletService *ls_tablet_svr = nullptr;
  ObLSHandle ls_handle;
  const ObTabletMapKey &key = replay_item.key_;
  const ObMetaDiskAddr &addr = replay_item.addr_;
  ObTabletTransferInfo tablet_transfer_info;
  ObTabletHandle tablet_handle;
  if (OB_FAIL(get_tablet_svr_(key.ls_id_, ls_tablet_svr, ls_handle))) {
    LOG_WARN("fail to get ls tablet service", K(ret));
  } else if (OB_FAIL(ls_tablet_svr->replay_create_tablet(addr, buf, buf_len, key.tablet_id_, tablet_transfer_info))) {
    LOG_WARN("fail to create tablet for replay", K(ret), K(key), K(addr));
  } else if (tablet_transfer_info.has_transfer_table() &&
      OB_FAIL(record_ls_transfer_info_(ls_handle, key.tablet_id_, tablet_transfer_info))) {
    LOG_WARN("fail to record_ls_transfer_info", K(ret), K(key), K(tablet_transfer_info));
  } else if (OB_FAIL(ls_tablet_svr->get_tablet(key.tablet_id_,
                                               tablet_handle,
                                               ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US * 10,
                                               ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("fail to get tablet", K(ret), K(key), K(addr));
  }
  return ret;
}

int ObTabletReplayCreateHandler::check_is_need_record_transfer_info_(
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

int ObTabletReplayCreateHandler::record_ls_transfer_info_(
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
  } else if (ls_restore_status.is_before_restore_to_consistent_scn()) {
    LOG_INFO("the log stream in restore is before restore to consistent scn, no need to record transfer info", "ls_id", ls->get_ls_id(), K(ls_restore_status));
  }else if (!tablet_transfer_info.has_transfer_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should have transfer table", K(ret), "ls_id", ls->get_ls_id(), K(tablet_id), K(tablet_transfer_info));
  } else if (ls->get_ls_startup_transfer_info().is_valid()) {
    if (ls->get_ls_startup_transfer_info().ls_id_ != tablet_transfer_info.ls_id_
        || ls->get_ls_startup_transfer_info().transfer_start_scn_ != tablet_transfer_info.transfer_start_scn_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The transfer_info of different tablet records on the same ls is different", K(ret), "ls_id", ls->get_ls_id(),
          K(tablet_id), K(tablet_transfer_info), "ls_startup_transfer_info", ls->get_ls_startup_transfer_info());
    }
  } else if (OB_FAIL(check_is_need_record_transfer_info_(tablet_transfer_info.ls_id_,
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

} // namespace storage
} // namespace oceanbase
