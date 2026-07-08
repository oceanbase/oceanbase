/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/slog_ckpt/ob_tenant_slog_checkpoint_util.h"

#include "observer/omt/ob_tenant.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/slog_ckpt/ob_linked_macro_block_writer.h"
#include "storage/tablet/ob_tablet_mds_table_mini_merger.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/ob_super_block_struct.h"

namespace oceanbase
{
namespace storage
{
using DiskedTabletFilterOp = ObTenantSlogCkptUtil::DiskedTabletFilterOp;
using TabletDfgtPicker = ObTenantSlogCkptUtil::TabletDefragmentPicker;
using MetaBlockListApplier = ObTenantSlogCkptUtil::MetaBlockListApplier;
using ParallelStartupTaskHdl = ObTenantSlogCkptUtil::ParallelStartupTaskHandler;

// ========================================
//    ObTenantSlogCkptUtil common methods
// =========================================
/**
 * @brief Persist the tablet and apply the tablet to t3m.
 * If OB_SERVER_OUTOF_DISK_SPACE error occurs during tablet persistence,
 * it will RETRY up to 10 times, and will sleep for 1000us to yield CPU,
 * waiting for the bg macro block manager to reclaim idle macro blocks.
 */
int ObTenantSlogCkptUtil::write_and_apply_tablet(
    const ObTabletStorageParam &storage_param,
    ObTenantMetaMemMgr &t3m,
    ObLSService &ls_service,
    ObTenantStorageMetaService &tsms,
    ObArenaAllocator &allocator,
    bool &skipped)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  enum:uint8_t {
    NEED_RETRY,
    SKIPPED,
    DONE
  } status = NEED_RETRY;
  const int max_retry = 10;
  int retry = 0;

  ObTabletHandle new_tablet_handle;
  ObTablet *new_tablet = nullptr;

  const ObTabletMapKey &tablet_key = storage_param.tablet_key_;
  const ObMetaDiskAddr &original_addr = storage_param.original_addr_;

  if (OB_UNLIKELY(!storage_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid tablet storage param", K(ret), K(storage_param));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    STORAGE_LOG(WARN, "fail to get min data version", K(ret));
  } else if (OB_FAIL(ls_service.get_ls(tablet_key.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "failed to get ls", K(ret), K(tablet_key));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null ls", K(ret), K(ls), K(ls_handle));
  }


  while (NEED_RETRY == status && OB_SUCC(ret)) {
    new_tablet = nullptr;
    new_tablet_handle.reset();
    allocator.reset();
    ObTabletHandle old_tablet_handle;
    ObTablet *old_tablet = nullptr;
    ObTabletHandle tmp_tablet_handle;
    bool force_retry = false;

    if (OB_FAIL(t3m.get_tablet_with_allocator(WashTabletPriority::WTP_LOW, tablet_key, allocator, old_tablet_handle))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // tablet may be deleted, skip this tablet's defragment
        STORAGE_LOG(INFO, "tablet may be deleted, just skip", K(ret), K(tablet_key));
        status = SKIPPED;
        ret = OB_SUCCESS;
      } else if (OB_ALLOCATE_MEMORY_FAILED == ret) {
        STORAGE_LOG(WARN, "failed to get tablet with allocator, try to retry", K(ret), K(tablet_key));
        force_retry = true;
      } else {
        STORAGE_LOG(WARN, "failed to get tablet with allocator", K(ret), K(storage_param));
      }
    } else if (OB_ISNULL(old_tablet = old_tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "old tablet is null", K(ret), K(tablet_key));
    } else if (!original_addr.is_equal_for_persistence(old_tablet->get_tablet_addr())) {
      // tablet addr mismatch, skip this tablet's defragment
      status = SKIPPED;
      STORAGE_LOG(INFO, "this tablet has been updated, just skip", K(ret), K(storage_param), K(old_tablet->get_tablet_addr()));
    } else {
      ObTablet *src_tablet = nullptr;
      const bool need_compat = old_tablet->get_version() < ObTablet::VERSION_V4;
      force_retry = need_compat;
      int64_t ls_epoch = 0;
      int64_t tablet_meta_version = 0;
      if (!need_compat) {
        src_tablet = old_tablet;
      } else if (OB_FAIL(ObTenantSlogCkptUtil::handle_old_version_tablet_for_compat(
                          t3m,
                          allocator,
                          tablet_key,
                          *old_tablet,
                          tmp_tablet_handle))) {
        STORAGE_LOG(WARN, "failed to handle old version tablet for compat", K(ret), K(tablet_key), KPC(old_tablet));
      } else {
        src_tablet = tmp_tablet_handle.get_obj();
      }

      int32_t private_transfer_epoch = 0;
      if (OB_FAIL(ret)) {
      } else if (SKIPPED == status) {
      } else if (GCTX.is_shared_storage_mode() &&
                 OB_FAIL(ls->get_tablet_svr()->alloc_private_tablet_meta_version_with_lock(tablet_key, tablet_meta_version))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          STORAGE_LOG(INFO, "skip writing snapshot for this tablet", K(tablet_key));
          status = SKIPPED;
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "failed to alloc tablet meta version", K(ret), K(tablet_key));
        }
      } else if (OB_NOT_NULL(src_tablet) && OB_FAIL(src_tablet->get_ls_epoch(ls_epoch))) {
        STORAGE_LOG(WARN, "failed to get ls epoch", K(ret), K(tablet_key));
      } else if (OB_NOT_NULL(src_tablet) && OB_FAIL(src_tablet->get_private_transfer_epoch(private_transfer_epoch))) {
        STORAGE_LOG(WARN, "failed to get private transfer epoch", K(ret), "tablet_meta", src_tablet->get_tablet_meta());
      }

      const ObTabletPersisterParam param(data_version, tablet_key.ls_id_, ls_epoch, tablet_key.tablet_id_, private_transfer_epoch, tablet_meta_version);
      if (OB_FAIL(ret)) {
      } else if (SKIPPED == status) {
      } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(param, *src_tablet, new_tablet_handle))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          // tablet may be deleted, skip this tablet's defragment
          STORAGE_LOG(INFO, "tablet may be deleted, just skip", K(ret), K(tablet_key), K(data_version));
          status = SKIPPED;
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "failed to persist and transform tablet", K(ret), K(tablet_key), K(need_compat), KPC(src_tablet), K(data_version));
        }
      } else {
        // everything is ok.
        status = DONE;
      }
    }

    if (NEED_RETRY == status) {
      OB_ASSERT(OB_SUCCESS != ret);
      if (OB_SERVER_OUTOF_DISK_SPACE != ret && (!force_retry || OB_ALLOCATE_MEMORY_FAILED != ret)) {
        // only retry when server is out of disk space
        STORAGE_LOG(WARN, "some other errors occurred, abandoning the retry for processing the tablet",
          K(ret), K(tablet_key));
        break;
      }
      if (retry < max_retry || force_retry) {
        STORAGE_LOG(WARN, "failed to process tablet, will retry after 50ms", K(ret), K(tablet_key), K(retry), K(max_retry), K(force_retry));
        ret = OB_SUCCESS;
        ++retry;
        // sleep 1000us before retry
        ob_usleep(1000);
      } else {
        STORAGE_LOG(WARN, "reached maximum retry attempts, but still failed to process the tablet",
          K(ret), K(tablet_key), K(max_retry));
        break;
      }
    }
  }

  if (OB_SUCC(ret) && status == DONE) {
    // succeed to move tablet to the new place, now trying to apply defragment tablet to t3m.
    if (OB_FAIL(ls->apply_defragment_tablet(t3m, tablet_key, original_addr, new_tablet_handle, tsms))) {
      STORAGE_LOG(WARN, "failed to apply defragment tablet", K(ret), K(tablet_key), K(original_addr));
      if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  skipped = (SKIPPED == status);
  return ret;
}

int ObTenantSlogCkptUtil::handle_old_version_tablet_for_compat(
    ObTenantMetaMemMgr &t3m,
    ObArenaAllocator &allocator,
    const ObTabletMapKey &tablet_key,
    const ObTablet &old_tablet,
    ObTabletHandle &new_tablet_handle)
{
  int ret = OB_SUCCESS;
  bool has_tablet_status = false;
  ObTablet *new_tablet = nullptr;
  ObTableHandleV2 mds_mini_sstable;

  if (OB_FAIL(ObMdsDataCompatHelper::generate_mds_mini_sstable(old_tablet, allocator,
    mds_mini_sstable, has_tablet_status))) {
      if (OB_NO_NEED_UPDATE == ret) {
        ret = OB_SUCCESS;
      } else if (OB_EMPTY_RESULT == ret) {
        ret = OB_SUCCESS;
        STORAGE_LOG(INFO, "empty mds data in old tablet, no need to generate mds mini sstable");
      } else {
        STORAGE_LOG(WARN, "failed to generate mds mini sstable", K(ret), K(tablet_key));
      }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(acquire_tmp_tablet_for_compat(t3m, tablet_key, allocator, new_tablet_handle))) {
    STORAGE_LOG(WARN, "failed to create tmp tablet", K(ret), K(tablet_key));
  } else if (FALSE_IT(new_tablet = new_tablet_handle.get_obj())) {
  } else if (OB_FAIL(new_tablet->init_for_compat(allocator, has_tablet_status, old_tablet, mds_mini_sstable))) {
    STORAGE_LOG(WARN, "failed to init tablet for compat", K(ret), K(tablet_key), K(has_tablet_status));
  } else {
    STORAGE_LOG(INFO, "succeed to handle mds data for tablet", K(ret), K(tablet_key), K(has_tablet_status), K(mds_mini_sstable));
  }
  return ret;
}

int ObTenantSlogCkptUtil::acquire_tmp_tablet_for_compat(
    ObTenantMetaMemMgr &t3m,
    const ObTabletMapKey &tablet_key,
    ObArenaAllocator &allocator,
    ObTabletHandle &handle)
{

  TIMEGUARD_INIT(STORAGE, 10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(tablet_key));
  } else if (CLICK_FAIL(t3m.acquire_tmp_tablet(WashTabletPriority::WTP_HIGH, tablet_key, allocator, handle))) {
    STORAGE_LOG(WARN, "failed to acquire temporary tablet", K(ret), K(tablet_key));
  } else if (OB_ISNULL(handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "new tablet is null", K(ret), K(handle));
  }
  return ret;
}

// ==========================
//      LSCkptInheritOp
// ==========================
int ObTenantSlogCkptUtil::LSCkptInheritOp::operator()(
    const ObMetaDiskAddr &addr,
    const char *buf,
    const int64_t buf_len)
{
  OB_ASSERT(ls_id_.is_valid());

  struct DummyOp final
  {
  public:
    int operator()(
      const ObMetaDiskAddr &addr,
      const char *buf,
      const int64_t buf_len)
    {
      return OB_SUCCESS;
    }
  };

  int ret = OB_SUCCESS;
  ObLSCkptMember ls_ckpt_member;
  int64_t pos = 0;

  if (OB_FAIL(ls_ckpt_member.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to deserialize ls_ckpt_member", K(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(!ls_ckpt_member.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid ls_ckpt_memeber", K(ret), K(ls_ckpt_member));
  } else {
    LOG_INFO("LSCkptInheritOp: processing ls_ckpt_member from old checkpoint",
             "target_ls_id", ls_id_, K(ls_ckpt_member));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (ls_ckpt_member.ls_meta_.ls_id_ == ls_id_) {
    ObSEArray<MacroBlockId, 2> tmp_block_list;
    if (OB_UNLIKELY(!tablet_block_list_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected duplicate ls ckpt members", K(ret));
    } else if (OB_FAIL(ObTenantStorageCheckpointReader::iter_read_meta_item(ls_ckpt_member.tablet_meta_entry_,
                                                                            DummyOp(),
                                                                            tmp_block_list))) {
      LOG_WARN("failed to iterator tablet block list", K(ret), K(ls_id_), K(ls_ckpt_member.tablet_meta_entry_));
    } else if (OB_UNLIKELY(tmp_block_list.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty tablet block list", K(ret), K(ls_id_), K(ls_ckpt_member.tablet_meta_entry_),
        K(tmp_block_list));
    } else if (OB_FAIL(tablet_block_list_.reserve(tmp_block_list.count()))) {
      LOG_WARN("failed to reserve tablet_block_list", K(ret));
    } else if (OB_FAIL(tablet_block_list_.assign(tmp_block_list))) {
      LOG_WARN("failed to assign tablet_block_list", K(ret), K(tmp_block_list));
    }
  } else if (OB_FAIL(ls_item_writer_.write_item(buf, buf_len, /*item idx*/nullptr))) {
    LOG_WARN("failed to write ls_ckpt_member", K(ret), KP(buf), K(buf_len));
  }
  return ret;
}

// ================================
//    TabletDefragmentPicker
// ================================
TabletDfgtPicker::TabletDefragmentPicker(const ObMemAttr &mem_attr)
  : allocator_(mem_attr),
    mem_attr_(mem_attr),
    total_tablet_size_(0)
{
}

TabletDfgtPicker::~TabletDefragmentPicker() { reset_(); }

int TabletDfgtPicker::add_tablet(const ObTabletStorageParam &param)
{
  STORAGE_LOG(DEBUG, "add tablet", K(param));
  OB_ASSERT(!param.original_addr_.is_memory() && !param.original_addr_.is_none());

  int ret = OB_SUCCESS;
  MacroBlockId block_id;
  int64_t tablet_offset = 0, tablet_size = 0;
  if (OB_FAIL(param.original_addr_.get_block_addr(block_id, tablet_offset, tablet_size))) {
    STORAGE_LOG(WARN, "failed to get block address from param", K(ret), K(param));
  } else {
    int64_t final_tablet_size = 0;
    if (ObMetaDiskAddr::DiskType::RAW_BLOCK == param.original_addr_.type()) {
      final_tablet_size = common::ob_aligned_to2(tablet_size, DIO_READ_ALIGN_SIZE);
    } else {
      // DON'T upper_align if tablet is persisted by batch
      final_tablet_size = tablet_size;
    }
    OB_ASSERT(final_tablet_size < OB_DEFAULT_MACRO_BLOCK_SIZE);
    MapValue *value = nullptr;
    void *alloc = nullptr;
    if (OB_SUCC(map_.get_refactored(block_id, value))) {
      if (OB_ISNULL(value)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "value of shared macro block map is nullptr, which is not allowed", K(ret), K(block_id));
      } else if (OB_FAIL(value->tablet_storage_params_.push_back(param))) {
        STORAGE_LOG(WARN, "failed to update shared macro block map", K(ret), K(param));
      } else {
        value->total_occupied_ += final_tablet_size;
        OB_ASSERT(value->total_occupied_ <= OB_DEFAULT_MACRO_BLOCK_SIZE);
      }
    } else if (OB_HASH_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "failed to get value from shared macro block map", K(ret), K(block_id));
    } else if (FALSE_IT(ret = OB_SUCCESS)) { // insert if block not exists.
    } else if (OB_ISNULL(alloc = reinterpret_cast<MapValue*>(allocator_.alloc(sizeof(MapValue))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to allocate memory", K(ret), K(sizeof(MapValue)));
    } else if (FALSE_IT(value = new(alloc)MapValue())) { // call constructor of MapValue
    } else if (FALSE_IT(value->total_occupied_ = final_tablet_size)) { // set occupied
    } else if (FALSE_IT(value->tablet_storage_params_.set_attr(mem_attr_))) { // set mem attr
    } else if (OB_FAIL(value->tablet_storage_params_.push_back(param))) { // add param
      value->~MapValue();
      allocator_.free(value);
      STORAGE_LOG(WARN, "failed to insert tablet shared macro block map", K(ret), K(param), K(block_id));
    } else if (OB_FAIL(map_.set_refactored(block_id, value))) {
      value->~MapValue();
      allocator_.free(value);
      STORAGE_LOG(WARN, "failed to insert tablet shared macro block map", K(ret), K(param), K(block_id));
    }

    if (OB_SUCC(ret)) {
      total_tablet_size_ += final_tablet_size;
    }
  }
  return ret;
}

/// @brief: pick tablets in descending order of space amplification ratio.
struct TabletDfgtPicker::InnerPicker
{
public:
  InnerPicker(const double &size_amp_threshold)
    : picked_(NULL),
      min_occupied_(INT_MAX64),
      size_amp_threshold_(size_amp_threshold)
  {
  }

  void reset()
  {
    min_occupied_ = INT_MAX64;
    picked_ = NULL;
  }

  int operator()(const EntryType &entry)
  {
    int ret = OB_SUCCESS;
    const MapValue &val = *entry.second;
    OB_ASSERT(entry.first.is_valid());
    const int64_t total_occupied_aligned = upper_align(val.total_occupied_, DIO_READ_ALIGN_SIZE);
    const double size_amp = ObTenantSlogCkptUtil::cal_size_amplification(1, total_occupied_aligned);
    if (size_amp < size_amp_threshold_) {
      // nothing to do if size amp unreached the threshold.
    } else if (min_occupied_ > val.total_occupied_) {
      min_occupied_ = val.total_occupied_;
      picked_ = &entry;
    }
    return ret;
  }

public:
  const ObTenantSlogCkptUtil::TabletDefragmentPicker::EntryType *picked_;
  int64_t min_occupied_;
  const double size_amp_threshold_;
};

/// COMMENT: check performance
/// TODO(yeqiyi.yqy): trim @c map_ by @c size_amp_threshold before picking tablets
int TabletDfgtPicker::pick_tablets_for_defragment(
    ObIArray<ObTabletStorageParam> &result,
    const double &size_amp_threshold,
    const int64_t &tablet_cnt_threshold,
    const int64_t tablet_size_threshold,
    int64_t &picked_tablet_size)
{
  int ret = OB_SUCCESS;

  picked_tablet_size = 0;
  InnerPicker picker(size_amp_threshold);
  /// adjust @c tablet_size_threshold
  int aligned_tablet_size_threshold = common::ob_aligned_to2(tablet_size_threshold, DIO_READ_ALIGN_SIZE);
  STORAGE_LOG(DEBUG, "start picking tablets", K(size_amp_threshold), K(aligned_tablet_size_threshold));


  while (OB_SUCC(ret) && !map_.empty()) {
    if (OB_FAIL(map_.foreach_refactored(picker))) {
      STORAGE_LOG(WARN, "failed to pick tablet", K(ret));
    } else if (picker.picked_ != NULL) {
      // one tablet has been picked.
      const MacroBlockId picked_block = picker.picked_->first;
      OB_ASSERT(picked_block.is_valid());

      STORAGE_LOG(DEBUG, "pick shared macro block", K(picked_block)); // for debug

      if (OB_ISNULL(picker.picked_->second)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "value of shared macro block map is nullptr, which is not allowed", K(ret), K(picked_block));
      } else {
        const MapValue &picked_value = *picker.picked_->second;

        if (aligned_tablet_size_threshold > 0 && picked_tablet_size + picked_value.total_occupied_ >= aligned_tablet_size_threshold) {
          // reach the maximum size of picked tablet, finish picking.
          STORAGE_LOG(INFO, "the total size of picked tablets reach the threshold", K(picked_tablet_size), K(aligned_tablet_size_threshold));
          break;
        }

        if (result.count() >= tablet_cnt_threshold) {
          // reach the maximum count of picked tablet, finish picking.
          STORAGE_LOG(INFO, "the count of picked tablets reach the threshold", K(result.count()), K(tablet_cnt_threshold));
          break;
        }

        if (OB_FAIL(::append(result, picked_value.tablet_storage_params_))) {
          STORAGE_LOG(WARN, "failed to pick tablet from shared macro block", K(ret), K(picked_block));
        } else {
          picked_tablet_size += picked_value.total_occupied_;
        }
      }

      // remove block from map if everything is ok.
      if (OB_SUCC(ret) &&
          OB_FAIL(remove_(picked_block))) {
        STORAGE_LOG(WARN, "failed to remove shared block from map", K(ret), K(picked_block));
      }
    } else {
      // none block was picked, just break
      break;
    }
    picker.reset();
  }
  return ret;
}

void TabletDfgtPicker::reset_()
{
  total_tablet_size_ = 0;
  for (SharedMacroBlockMap::iterator it = map_.begin(); it != map_.end(); ++it) {
    MapValue *val = it->second;
    if (OB_NOT_NULL(val)) {
      val->~MapValue();
      allocator_.free(val);
    }
  }
  map_.destroy();
  allocator_.reset();
}

/// NOTE: block must exists at @c map_
int TabletDfgtPicker::remove_(const MacroBlockId &block_id)
{
  int ret = OB_SUCCESS;
  MapValue *val = nullptr;
  if (OB_FAIL(map_.erase_refactored(block_id, &val))) {
    STORAGE_LOG(WARN, "failed to remove block from map", K(ret), K(block_id));
  } else if (OB_ISNULL(val)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null val founded", K(ret), K(block_id), K(val));
  } else {
    val->~MapValue();
    allocator_.free(val);
  }
  return ret;
}

// ==========================
//    MetaBlockListApplier
// ==========================
MetaBlockListApplier::MetaBlockListApplier(
  common::TCRWLock *lock,
  ObMetaBlockListHandle *ls_block_handle,
  ObMetaBlockListHandle *tablet_block_handle,
  ObMetaBlockListHandle *wait_gc_tablet_block_handle)
  : lock_(lock),
    ls_block_handle_(ls_block_handle),
    tablet_block_handle_(tablet_block_handle),
    wait_gc_tablet_block_handle_(wait_gc_tablet_block_handle)
{
}

int MetaBlockListApplier::is_valid() const
{
  return nullptr != lock_ &&
         nullptr != ls_block_handle_ &&
         nullptr != tablet_block_handle_ &&
         nullptr != wait_gc_tablet_block_handle_;
}

int MetaBlockListApplier::apply_from(
    const ObIArray<MacroBlockId> &ls_block_list,
    const ObIArray<MacroBlockId> &tablet_block_list,
    const ObIArray<MacroBlockId> *wait_gc_tablet_block_list)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "meta block list applier is invalid", K(ret));
  } else {
    do {
      {
        TCWLockGuard guard(*lock_);
        if (OB_FAIL(ls_block_handle_->add_macro_blocks(ls_block_list))) {
          STORAGE_LOG(WARN, "failed to add ls block list", K(ret));
        } else if (OB_FAIL(tablet_block_handle_->add_macro_blocks(tablet_block_list))) {
          STORAGE_LOG(WARN, "failed to add tablet block list", K(ret));
        } else if (nullptr != wait_gc_tablet_block_list
                  && OB_FAIL(wait_gc_tablet_block_handle_->add_macro_blocks(*wait_gc_tablet_block_list))) {
          STORAGE_LOG(WARN, "failed to add wait gc tablet block list", K(ret));
        }
      }
      if (OB_ALLOCATE_MEMORY_FAILED == ret) {
        ob_usleep(1000);
      }
    } while (OB_ALLOCATE_MEMORY_FAILED == ret);
  }
  return ret;
}

// ================================
//    ParallelStartupTaskHandler
// ================================
ParallelStartupTaskHdl::ParallelStartupTaskHandler()
  : startup_accel_task_hdl_(GCTX.startup_accel_handler_),
    errcode_(OB_SUCCESS),
    inflight_task_cnt_(0),
    finished_task_cnt_(0),
    all_task_cnt_(0)
{
}


int ParallelStartupTaskHdl::wait()
{
  /// @c startup_accel_task_hdl_ must be inited
  OB_ASSERT(nullptr != startup_accel_task_hdl_);
  OB_ASSERT(OB_INIT_TWICE == startup_accel_task_hdl_->init(observer::SERVER_ACCEL));
  int ret = OB_SUCCESS;

  const int64_t max_log_interval = 5_s;
  const int64_t start_time = ObTimeUtility::current_time();
  int64_t log_interval = 100_ms;
  int64_t last_print_time = -1;
  int64_t print_cnt = 0;
  while (get_inflight_task_cnt_() > 0) {
    int64_t cur_time = ObTimeUtility::current_time();
    if (last_print_time == -1 ||
      cur_time - last_print_time > log_interval) {
      int64_t waiting_cost_us = cur_time - start_time;
      STORAGE_LOG(INFO, "waiting all inflight tasks finish",
        K(get_inflight_task_cnt_()), K(waiting_cost_us));
      last_print_time = cur_time;
      ++print_cnt;
      // grow log interval to prevent printing too much log...
      if (print_cnt % 10 == 0) {
        log_interval = std::min(max_log_interval,
          static_cast<int64_t>(log_interval + /*100ms*/1e5));
      }

    }
    ob_usleep(20_ms); // yield
  }

  if (OB_FAIL(get_errcode_())) {
    STORAGE_LOG(WARN, "some task has failed", K(ret));
  } else if (OB_UNLIKELY(get_finished_task_cnt_() != all_task_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "finished tasks count mismatch", K(ret), K(get_finished_task_cnt_()), K(all_task_cnt_));
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase