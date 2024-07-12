/**
 * Copyright (c) 2022 OceanBase
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

#include "storage/tablet/ob_tablet_persister.h"
#include "storage/ob_storage_schema.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/tablet/ob_tablet_block_aggregated_info.h"
#include "storage/slog_ckpt/ob_linked_macro_block_writer.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tablet/ob_tablet_block_header.h"
#include "storage/tablet/ob_tablet_slog_helper.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"

using namespace std::placeholders;
using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

int ObSharedBlockIndex::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(shared_macro_id_.hash(hash_val))) {
    LOG_WARN("fail to calculate macro id's hash value", K(ret), K(shared_macro_id_));
  } else {
    hash_val *= nested_offset_;
  }
  return ret;
}

OB_INLINE bool ObSharedBlockIndex::operator ==(const ObSharedBlockIndex &other) const
{
  return other.shared_macro_id_ == shared_macro_id_ && other.nested_offset_ == nested_offset_;
}

ObTabletTransformArg::ObTabletTransformArg()
  : rowkey_read_info_ptr_(nullptr),
    tablet_macro_info_ptr_(nullptr),
    tablet_meta_(),
    table_store_addr_(),
    storage_schema_addr_(),
    tablet_macro_info_addr_(),
    is_row_store_(true),
    ddl_kvs_(nullptr),
    ddl_kv_count_(0),
    memtable_count_(0)
{
  MEMSET(memtables_, 0x0, sizeof(memtables_));
}

ObTabletTransformArg::~ObTabletTransformArg()
{
  reset();
}

void ObTabletTransformArg::reset()
{
  rowkey_read_info_ptr_ = nullptr;
  tablet_macro_info_ptr_ = nullptr;
  tablet_meta_.reset();
  table_store_addr_.reset();
  storage_schema_addr_.reset();
  tablet_macro_info_addr_.reset();
  is_row_store_ = true;
  ddl_kvs_ = nullptr;
  ddl_kv_count_ = 0;
  for (int64_t i = 0; i < MAX_MEMSTORE_CNT; ++i) {
    memtables_[i] = nullptr;
  }
  memtable_count_ = 0;
}

bool ObTabletTransformArg::is_valid() const
{
  return table_store_addr_.is_none() ^ (nullptr != rowkey_read_info_ptr_)
      && tablet_meta_.is_valid()
      && table_store_addr_.is_valid()
      && storage_schema_addr_.is_valid()
      && tablet_macro_info_addr_.is_valid();
}


bool ObSSTablePersistWrapper::is_valid() const
{
  return nullptr != sstable_
      && sstable_->is_sstable()
      && sstable_->is_valid();
}

int ObSSTablePersistWrapper::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("wrapper is unexpected not valid", K(ret));
  } else if (OB_FAIL(sstable_->serialize_full_table(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize full sstable", K(ret), KPC(sstable_));
  }
  return ret;
}

int64_t ObSSTablePersistWrapper::get_serialize_size() const
{
  int64_t len = 0;
  if (OB_UNLIKELY(!is_valid())) {
    // do nothing
  } else {
    len = sstable_->get_full_serialize_size();
  }
  return len;
}

//==================================== ObMultiTimeStats====================================//

ObMultiTimeStats::TimeStats::TimeStats(const char *owner)
   : owner_(owner),
     start_ts_(ObTimeUtility::current_time()),
     last_ts_(start_ts_),
     click_count_(0),
     has_extra_info_(false)
{
  memset(click_, 0, sizeof(click_));
  memset(click_str_, 0, sizeof(click_str_));
}

void ObMultiTimeStats::TimeStats::click(const char *step_name)
{
  const int64_t cur_ts = ObTimeUtility::current_time();
  if (OB_LIKELY(click_count_ < MAX_CLICK_COUNT)) {
    click_str_[click_count_] = step_name;
    click_[click_count_++] = (int32_t)(cur_ts - last_ts_);
    last_ts_ = cur_ts;
  }
}

int64_t ObMultiTimeStats::TimeStats::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t i = 0;
  ret = databuff_printf(buf, buf_len, pos, "owner:'%s' total=%ld%s",
      owner_, last_ts_ - start_ts_, click_count_ > 0 ? ", time_dist: " : "");

  if (OB_SUCC(ret) && click_count_ > 0) {
    ret = databuff_printf(buf, buf_len, pos, "%s=%d", click_str_[0], click_[0]);
  }
  for (int i = 1; OB_SUCC(ret) && i < click_count_; i++) {
    ret = databuff_printf(buf, buf_len, pos, ", %s=%d", click_str_[i], click_[i]);
  }
  if (OB_SUCC(ret)) {
    if (has_extra_info_) {
      ret = databuff_printf(buf, buf_len, pos, " %s:%s", "extra_info", extra_info_);
    }
  }
  if (OB_FAIL(ret)) {
    pos = 0;
  }
  return pos;
}

int ObMultiTimeStats::TimeStats::set_extra_info(const char *fmt, ...)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  va_list args;
  va_start(args, fmt);

  if (OB_FAIL(databuff_vprintf(extra_info_, MAX_EXTRA_INFO_LENGTH, pos, fmt, args))) {
    LOG_WARN("fail to databuff_vprintf", K(ret));
  } else {
    has_extra_info_ = true;
  }
  va_end(args);

  return ret;
}

ObMultiTimeStats::ObMultiTimeStats(ObArenaAllocator *allocator)
  : allocator_(allocator), stats_(nullptr), stats_count_(0)
{
}

ObMultiTimeStats::~ObMultiTimeStats()
{
  for (int64_t i = 0; i < stats_count_; i++) {
    stats_[i].~TimeStats();
  }
  stats_count_ = 0;
  stats_ = nullptr;
}

int ObMultiTimeStats::acquire_stats(const char *owner, ObMultiTimeStats::TimeStats *&stats)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(stats_count_ > MAX_STATS_CNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("too many time stats", K(ret), K(stats_count_));
  } else if (OB_ISNULL(stats_) &&
      OB_ISNULL(stats_ = reinterpret_cast<TimeStats*>(allocator_->alloc(sizeof(TimeStats) *  MAX_STATS_CNT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    new (&stats_[stats_count_]) TimeStats(owner);
    stats = &stats_[stats_count_];
    stats_count_++;
  }
  return ret;
}

int64_t ObMultiTimeStats::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  for (int64_t i = 0; i < stats_count_; i++) {
    databuff_printf(buf, buf_len, pos, "stats[%ld]: ", i);
    BUF_PRINTO(stats_[i]);
    if (i != stats_count_-1) {
      J_NEWLINE();
    }
  }
  J_OBJ_END();
  return pos;
}

ObTabletPersister::ObTabletPersister(const int64_t ctx_id)
  : allocator_("TblPersist", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ctx_id),
    multi_stats_(&allocator_)
{
}
ObTabletPersister::~ObTabletPersister()
{
}
void ObTabletPersister::print_time_stats(
    const ObMultiTimeStats::TimeStats &time_stats,
    const int64_t stats_warn_threshold,
    const int64_t print_interval)
{
  int ret = OB_SUCCESS;
  if (time_stats.get_total_time() > stats_warn_threshold) {
    if (REACH_TIME_INTERVAL(100_ms)) {
      LOG_WARN("[TABLET PERSISTER TIME STATS] cost too much time\n", K_(multi_stats));
    }
  } else if (REACH_TIME_INTERVAL(print_interval)) {
    FLOG_INFO("[TABLET PERSISTER TIME STATS]\n", K_(multi_stats));
  }
}

int ObTabletPersister::persist_and_transform_tablet(
    const ObTablet &old_tablet,
    ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;
  const int64_t ctx_id = share::is_reserve_mode()
                       ? ObCtxIds::MERGE_RESERVE_CTX_ID
                       : ObCtxIds::DEFAULT_CTX_ID;
  ObTabletPersister persister(ctx_id);
  ObMultiTimeStats::TimeStats *time_stats = nullptr;
  common::ObSEArray<ObSharedBlocksWriteCtx, 16> total_write_ctxs;
  ObLinkedMacroBlockItemWriter linked_writer;
  ObTabletSpaceUsage space_usage;
  int64_t total_tablet_meta_size = 0;
  ObTabletMacroInfo tablet_macro_info;
  total_write_ctxs.set_attr(lib::ObMemAttr(MTL_ID(), "TblMetaWriCtx", ctx_id));
  ObSArray<MacroBlockId> shared_meta_id_arr;

  if (OB_UNLIKELY(!old_tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid old tablet to persist", K(ret), K(old_tablet));
  } else if (OB_FAIL(persister.multi_stats_.acquire_stats("persist_and_transform_tablet", time_stats))) {
    LOG_WARN("fail to acquire time stats", K(ret));
  } else if (OB_FAIL(persister.persist_and_fill_tablet(
      old_tablet, linked_writer, total_write_ctxs, new_handle, space_usage, tablet_macro_info, shared_meta_id_arr))) {
    LOG_WARN("fail to persist and fill tablet", K(ret), K(old_tablet));
  } else if (FALSE_IT(time_stats->click("persist_and_fill_tablet"))) {
  } else if (OB_FAIL(check_tablet_meta_ids(shared_meta_id_arr, *(new_handle.get_obj())))) {
    LOG_WARN("fail to check whether tablet meta's macro ids match", K(ret), K(shared_meta_id_arr), KPC(new_handle.get_obj()));
  } else if (FALSE_IT(time_stats->click("check_tablet_meta_ids"))) {
  } else if (OB_FAIL(persister.persist_aggregated_meta(tablet_macro_info, new_handle, space_usage))) {
    LOG_WARN("fail to persist aggregated tablet", K(ret), K(new_handle), KPC(new_handle.get_obj()));
  } else {
    time_stats->click("persist_aggregated_meta");
    persister.print_time_stats(*time_stats, 20_ms, 1_s);
  }
  return ret;
}

// !!!attention shouldn't be called by empty shell
/*static*/ int ObTabletPersister::persist_and_transform_only_tablet_meta(
    const ObTablet &old_tablet,
    ObITabletMetaModifier &modifier,
    ObTabletHandle &new_tablet)
{
  int ret = OB_SUCCESS;
  ObTabletPersister persister;
  ObMultiTimeStats::TimeStats *time_stats = nullptr;
  ObTabletMacroInfo *macro_info = nullptr;
  bool in_memory = false;

  if (OB_UNLIKELY(!old_tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid old tablet", K(ret), K(old_tablet));
  } else if (OB_UNLIKELY(old_tablet.allocator_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this isn't supported for the tablet from allocator", K(ret), K(old_tablet));
  } else if (OB_UNLIKELY(!old_tablet.hold_ref_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old tablet doesn't hold ref cnt", K(ret), K(old_tablet));
  } else if (OB_FAIL(persister.multi_stats_.acquire_stats("persist_and_transform_only_tablet_meta", time_stats))) {
    LOG_WARN("fail to acquire time stats", K(ret));
  } else if (OB_FAIL(old_tablet.load_macro_info(persister.allocator_, macro_info, in_memory))) {
    LOG_WARN("fail to fetch macro info", K(ret));
  } else if (FALSE_IT(time_stats->click("load_macro_info"))) {
  } else if (OB_FAIL(persister.modify_and_fill_tablet(old_tablet, modifier, new_tablet))) {
    LOG_WARN("fail to modify and fill tablet", K(ret), K(old_tablet));
  } else {
    time_stats->click("modify_and_fill_tablet");
    ObTabletSpaceUsage space_usage = old_tablet.get_tablet_meta().space_usage_;
    space_usage.shared_meta_size_ -= upper_align(old_tablet.get_tablet_addr().size(), DIO_READ_ALIGN_SIZE);
    if (OB_FAIL(persister.persist_aggregated_meta(*macro_info, new_tablet, space_usage))) {
      LOG_WARN("fail to persist aggregated meta", K(ret), KPC(macro_info), K(new_tablet), K(space_usage));
    } else {
      time_stats->click("persist_aggregated_meta");
      persister.print_time_stats(*time_stats, 20_ms, 1_s);
    }
  }
  if (OB_NOT_NULL(macro_info) && !in_memory) {
    macro_info->~ObTabletMacroInfo();
    macro_info = nullptr;
  }
  return ret;
}

int ObTabletPersister::modify_and_fill_tablet(
    const ObTablet &old_tablet,
    ObITabletMetaModifier &modifier,
    ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;
  const ObTabletMeta &tablet_meta = old_tablet.get_tablet_meta();
  const ObTabletMapKey key(tablet_meta.ls_id_, tablet_meta.tablet_id_);
  const char* buf = reinterpret_cast<const char *>(&old_tablet);
  const bool try_smaller_pool = old_tablet.get_try_cache_size() > ObTenantMetaMemMgr::NORMAL_TABLET_POOL_SIZE
                                ? false : true;
  ObMetaObjBufferHeader &buf_header = ObMetaObjBufferHelper::get_buffer_header(const_cast<char *>(buf));
  ObTabletTransformArg arg;
  ObTabletPoolType type;
  ObMultiTimeStats::TimeStats *time_stats = nullptr;
  if (OB_FAIL(multi_stats_.acquire_stats("persist_and_transform_only_tablet_meta", time_stats))) {
    LOG_WARN("fail to acquire time stats", K(ret));
  } else if (OB_FAIL(ObTenantMetaMemMgr::get_tablet_pool_type(buf_header.buf_len_, type))) {
    LOG_WARN("fail to get tablet pool type", K(ret), K(buf_header));
  } else if (OB_FAIL(acquire_tablet(type, key, try_smaller_pool, new_handle))) {
    LOG_WARN("fail to acqurie tablet", K(ret), K(type), K(new_handle));
  } else if (OB_FAIL(convert_tablet_to_mem_arg(old_tablet, arg))) {
    LOG_WARN("fail to convert tablet to mem arg", K(ret), K(arg), K(old_tablet));
  } else if (FALSE_IT(time_stats->click("convert_tablet_to_mem_arg"))) {
  } else if (OB_FAIL(transform(arg, new_handle.get_buf(), new_handle.get_buf_len()))) {
    LOG_WARN("fail to transform tablet", K(ret), K(arg),
        KP(new_handle.get_buf()), K(new_handle.get_buf_len()), K(old_tablet));
  } else if (FALSE_IT(new_handle.get_obj()->set_next_tablet_guard(old_tablet.next_tablet_guard_))) {
  } else if (OB_FAIL(modifier.modify_tablet_meta(new_handle.get_obj()->tablet_meta_))) {
    LOG_WARN("fail to modify tablet meta", K(ret), KPC(new_handle.get_obj()));
  } else if (OB_FAIL(new_handle.get_obj()->check_ready_for_read_if_need(old_tablet))) {
    LOG_WARN("fail to check ready for read if need", K(ret), K(old_tablet), K(new_handle));
  } else {
    time_stats->click("transform_and_modify");
  }
  return ret;
}

/*static*/ int ObTabletPersister::copy_from_old_tablet(
    const ObTablet &old_tablet,
    ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(old_tablet.allocator_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this isn't supported for the tablet from allocator", K(ret), K(old_tablet));
  } else if (OB_UNLIKELY(!old_tablet.hold_ref_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old tablet doesn't hold ref cnt", K(ret), K(old_tablet));
  } else {
    const ObTabletMeta &tablet_meta = old_tablet.get_tablet_meta();
    const ObTabletMapKey key(tablet_meta.ls_id_, tablet_meta.tablet_id_);
    const char* buf = reinterpret_cast<const char *>(&old_tablet);
    const bool try_smaller_pool = old_tablet.get_try_cache_size() > ObTenantMetaMemMgr::NORMAL_TABLET_POOL_SIZE
                                  ? false : true;
    ObMetaObjBufferHeader &buf_header = ObMetaObjBufferHelper::get_buffer_header(const_cast<char *>(buf));
    ObTabletTransformArg arg;
    ObTabletPoolType type;
    ObTabletPersister persister;
    ObMultiTimeStats::TimeStats *time_stats = nullptr;

    if (OB_FAIL(persister.multi_stats_.acquire_stats("copy_from_old_tablet", time_stats))) {
      LOG_WARN("fail to acquire time stats", K(ret));
    } else if (OB_FAIL(ObTenantMetaMemMgr::get_tablet_pool_type(buf_header.buf_len_, type))) {
      LOG_WARN("fail to get tablet pool type", K(ret), K(buf_header));
    } else if (OB_FAIL(acquire_tablet(type, key, try_smaller_pool, new_handle))) {
      LOG_WARN("fail to acqurie tablet", K(ret), K(type), K(new_handle));
    } else if (OB_FAIL(convert_tablet_to_mem_arg(old_tablet, arg))) {
      LOG_WARN("fail to convert tablet to mem arg", K(ret), K(arg), K(old_tablet));
    } else if (FALSE_IT(time_stats->click("convert_tablet_to_mem_arg"))) {
    } else if (OB_FAIL(persister.transform(arg, new_handle.get_buf(), new_handle.get_buf_len()))) {
      LOG_WARN("fail to transform tablet", K(ret), K(arg), KP(new_handle.get_buf()),
          K(new_handle.get_buf_len()), K(old_tablet));
    } else {
      time_stats->click("transform");
      persister.print_time_stats(*time_stats, 20_ms, 1_s);
      new_handle.get_obj()->set_next_tablet_guard(old_tablet.next_tablet_guard_);
      new_handle.get_obj()->set_tablet_addr(old_tablet.get_tablet_addr());
      if (OB_FAIL(new_handle.get_obj()->inc_macro_ref_cnt())) {
        LOG_WARN("fail to increase macro ref cnt for new tablet", K(ret), K(new_handle));
      }
    }
  }
  return ret;
}

int ObTabletPersister::convert_tablet_to_mem_arg(
    const ObTablet &tablet,
    ObTabletTransformArg &arg)
{
  int ret = OB_SUCCESS;
  arg.reset();
  if (OB_UNLIKELY(!tablet.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("old tablet isn't valid, don't allow to degrade tablet memory", K(ret), K(tablet));
  } else if (OB_FAIL(arg.tablet_meta_.assign(tablet.tablet_meta_))) {
    LOG_WARN("fail to copy tablet meta", K(ret), K(tablet));
  } else {
    arg.tablet_macro_info_addr_ = tablet.macro_info_addr_.addr_;
    arg.tablet_macro_info_ptr_ = tablet.macro_info_addr_.ptr_;
    arg.rowkey_read_info_ptr_ = tablet.rowkey_read_info_;
    arg.table_store_addr_ = tablet.table_store_addr_.addr_;
    arg.storage_schema_addr_ = tablet.storage_schema_addr_.addr_;
    arg.is_row_store_ = tablet.is_row_store();
    arg.ddl_kvs_ = tablet.ddl_kvs_;
    arg.ddl_kv_count_ = tablet.ddl_kv_count_;
    MEMCPY(arg.memtables_, tablet.memtables_, sizeof(ObIMemtable*) * MAX_MEMSTORE_CNT);
    arg.memtable_count_ = tablet.memtable_count_;
  }
  return ret;
}

int ObTabletPersister::convert_tablet_to_disk_arg(
      const ObTablet &tablet,
      common::ObIArray<ObSharedBlocksWriteCtx> &total_write_ctxs,
      ObTabletPoolType &type,
      ObTabletTransformArg &arg,
      int64_t &total_tablet_meta_size,
      ObBlockInfoSet &block_info_set)
{
  int ret = OB_SUCCESS;
  ObMultiTimeStats::TimeStats *time_stats = nullptr;
  arg.reset();

  common::ObSEArray<ObSharedBlockWriteInfo, 2> write_infos;
  const int64_t ctx_id = share::is_reserve_mode()
                       ? ObCtxIds::MERGE_RESERVE_CTX_ID
                       : ObCtxIds::DEFAULT_CTX_ID;
  write_infos.set_attr(lib::ObMemAttr(MTL_ID(), "WriteInfos", ctx_id));
  // fetch member wrapper
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(multi_stats_.acquire_stats("convert_tablet_to_disk_arg", time_stats))) {
    LOG_WARN("fail to acquire time stats", K(ret));
  } else if (OB_FAIL(arg.tablet_meta_.assign(tablet.tablet_meta_))) {
    LOG_WARN("fail to copy tablet meta", K(ret), K(tablet));
  } else if (FALSE_IT(arg.rowkey_read_info_ptr_ = tablet.rowkey_read_info_)) {
  // } else if (FALSE_IT(arg.extra_medium_info_ = tablet.mds_data_.extra_medium_info_)) {
  // TODO: @luhaopeng.lhp after mds_mvs joint debugging completed
  } else if (OB_FAIL(fetch_table_store_and_write_info(tablet, table_store_wrapper,
      write_infos, total_write_ctxs, total_tablet_meta_size, block_info_set))) {
    LOG_WARN("fail to fetch table store and write info", K(ret));
  } else {
    time_stats->click("fetch_table_store_and_write_info");
    arg.ddl_kvs_ = tablet.ddl_kvs_;
    arg.ddl_kv_count_ = tablet.ddl_kv_count_;
    arg.memtable_count_ = tablet.memtable_count_;
    MEMCPY(arg.memtables_, tablet.memtables_, sizeof(arg.memtables_));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(load_storage_schema_and_fill_write_info(tablet, allocator_, write_infos))) {
    LOG_WARN("fail to load storage schema and fill write info", K(ret));
  } else if (FALSE_IT(time_stats->click("load_storage_schema"))) {
  } else if (OB_FAIL(write_and_fill_args(write_infos, arg, total_write_ctxs, total_tablet_meta_size, block_info_set.shared_meta_block_info_set_))) {
    LOG_WARN("fail to write and fill address", K(ret), K(write_infos));
  } else if (FALSE_IT(time_stats->click("write_and_fill_args"))) {
  } else {
    const int64_t try_cache_size = tablet.get_try_cache_size() + table_store_wrapper.get_member()->get_try_cache_size();
    if (try_cache_size > ObTenantMetaMemMgr::NORMAL_TABLET_POOL_SIZE) {
      type = ObTabletPoolType::TP_LARGE;
    }
    arg.is_row_store_ = tablet.is_row_store();
  }

  return ret;
}

int ObTabletPersister::persist_and_fill_tablet(
    const ObTablet &old_tablet,
    ObLinkedMacroBlockItemWriter &linked_writer,
    common::ObIArray<ObSharedBlocksWriteCtx> &total_write_ctxs,
    ObTabletHandle &new_handle,
    ObTabletSpaceUsage &space_usage,
    ObTabletMacroInfo &tablet_macro_info,
    ObIArray<MacroBlockId> &shared_meta_id_arr)
{
  int ret = OB_SUCCESS;
  ObTabletTransformArg arg;
  ObBlockInfoSet block_info_set;
  ObMultiTimeStats::TimeStats *time_stats = nullptr;

  const ObTabletMeta &tablet_meta = old_tablet.get_tablet_meta();
  const ObTabletMapKey key(tablet_meta.ls_id_, tablet_meta.tablet_id_);
  ObTabletPoolType type = ObTabletPoolType::TP_NORMAL;
  bool try_smaller_pool = true;

  if (OB_FAIL(multi_stats_.acquire_stats("persist_and_fill_tablet", time_stats))) {
    LOG_WARN("fail to acquire time stats", K(ret));
  } else if (OB_FAIL(block_info_set.init())) {
    LOG_WARN("fail to init macro id set", K(ret));
  } else if (old_tablet.is_empty_shell()) {
    if (OB_FAIL(convert_tablet_to_mem_arg(old_tablet, arg))) {
      LOG_WARN("fail to conver tablet to mem arg", K(ret), K(old_tablet));
    } else {
      time_stats->click("convert_tablet_to_mem_arg");
    }
  } else if (OB_FAIL(convert_tablet_to_disk_arg(
      old_tablet, total_write_ctxs, type, arg, space_usage.shared_meta_size_, block_info_set))) {
    LOG_WARN("fail to conver tablet to disk arg", K(ret), K(old_tablet));
  } else {
    time_stats->click("convert_tablet_to_disk_arg");
    if (old_tablet.get_try_cache_size() > ObTenantMetaMemMgr::NORMAL_TABLET_POOL_SIZE) {
      try_smaller_pool = false;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tablet_macro_info.init(allocator_, block_info_set, linked_writer))) {
    LOG_WARN("fail to init tablet block id arrary", K(ret));
  } else {
    arg.tablet_macro_info_addr_.set_none_addr();
    arg.tablet_macro_info_ptr_ = &tablet_macro_info;
    time_stats->click("init_tabelt_macro_info");
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(acquire_tablet(type, key, try_smaller_pool, new_handle))) {
    LOG_WARN("fail to acquire tablet", K(ret), K(key), K(type));
  } else if (OB_FAIL(transform(arg, new_handle.get_buf(), new_handle.get_buf_len()))) {
    LOG_WARN("fail to transform old tablet", K(ret), K(arg), K(new_handle), K(type));
  } else {
    time_stats->click("transform");
    space_usage.data_size_ = block_info_set.data_block_info_set_.size() * DEFAULT_MACRO_BLOCK_SIZE;
    space_usage.meta_size_ = block_info_set.meta_block_info_set_.size() * DEFAULT_MACRO_BLOCK_SIZE;
    int64_t shared_data_size = 0;
    for (ObBlockInfoSet::MapIterator iter = block_info_set.shared_data_block_info_map_.begin();
        iter != block_info_set.shared_data_block_info_map_.end();
        ++iter) {
      shared_data_size += iter->second;
    }
    for (ObBlockInfoSet::SetIterator iter = block_info_set.shared_meta_block_info_set_.begin();
        OB_SUCC(ret) && iter != block_info_set.shared_meta_block_info_set_.end();
        ++iter) {
      if (OB_FAIL(shared_meta_id_arr.push_back(iter->first))) {
        LOG_WARN("fail to push back macro id", K(ret), K(iter->first));
      }
    }
    space_usage.shared_data_size_ = shared_data_size;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(new_handle.get_obj()->calc_sstable_occupy_size(space_usage.occupy_bytes_))) {
      LOG_WARN("failed to calc tablet occupy_size", K(ret), KPC(new_handle.get_obj()));
    } else {
      new_handle.get_obj()->set_space_usage_(space_usage);
    }
  }

  return ret;
}

int ObTabletPersister::transform_empty_shell(const ObTablet &old_tablet, ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;

  ObLinkedMacroBlockItemWriter linked_writer;
  common::ObArray<ObSharedBlocksWriteCtx> total_write_ctxs;
  ObTabletSpaceUsage space_usage;
  ObTabletMacroInfo tablet_macro_info;
  ObTabletPersister persister;
  ObSArray<MacroBlockId> shared_meta_id_arr;

  if (OB_UNLIKELY(!old_tablet.is_empty_shell())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only support transform empty shell", K(ret), K(old_tablet));
  } else if (OB_FAIL(persister.persist_and_fill_tablet(old_tablet, linked_writer,
      total_write_ctxs, new_handle, space_usage, tablet_macro_info, shared_meta_id_arr))) {
    LOG_WARN("fail to persist old empty shell", K(ret), K(old_tablet));
  } else {
    new_handle.get_obj()->tablet_meta_.space_usage_ = space_usage;
  }

  return ret;
}

int ObTabletPersister::check_tablet_meta_ids(
    const ObIArray<blocksstable::MacroBlockId> &shared_meta_id_arr,
    const ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  ObSArray<MacroBlockId> meta_ids;
  if (OB_FAIL(tablet.get_tablet_first_second_level_meta_ids(meta_ids))) {
    LOG_WARN("fail to get tablet meta ids", K(ret), K(tablet));
  } else if (OB_UNLIKELY(meta_ids.count() > shared_meta_id_arr.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num of macro blocks doesn't match", K(ret), K(meta_ids.count()), K(shared_meta_id_arr));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_ids.count(); i++) {
      for (int64_t j = 0; !found && j < shared_meta_id_arr.count(); j++) {
        if (meta_ids.at(i) == shared_meta_id_arr.at(j)) {
          found = true;
        }
      }
      if (OB_UNLIKELY(!found)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet meta macro block doesn't match", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletPersister::acquire_tablet(
    const ObTabletPoolType &type,
    const ObTabletMapKey &key,
    const bool try_smaller_pool,
    ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_FAIL(t3m->acquire_tablet_from_pool(type, WashTabletPriority::WTP_HIGH, key, new_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
    } else if (ObTabletPoolType::TP_LARGE == type
        && try_smaller_pool
        && OB_SUCC(t3m->acquire_tablet_from_pool(ObTabletPoolType::TP_NORMAL, WashTabletPriority::WTP_HIGH, key, new_handle))) {
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to acquire tablet from pool", K(ret), K(key), K(type));
    }
  }

  if (OB_SUCC(ret) && OB_ISNULL(new_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new tablet is null", K(ret), K(new_handle));
  }
  return ret;
}

int ObTabletPersister::persist_aggregated_meta(
    const ObTabletMacroInfo &tablet_macro_info,
    ObTabletHandle &new_handle,
    ObTabletSpaceUsage &space_usage)
{
  int ret = OB_SUCCESS;
  ObMacroInfoIterator macro_iter;
  bool inc_success = false;
  ObTablet *new_tablet = new_handle.get_obj();
  ObTenantCheckpointSlogHandler *ckpt_slog_handler = MTL(ObTenantCheckpointSlogHandler*);
  ObSharedBlockWriteInfo write_info;
  ObSharedBlockWriteHandle handle;
  ObSharedBlocksWriteCtx write_ctx;
  const int64_t secondary_meta_size = tablet_macro_info.get_serialize_size();
  MacroBlockId macro_id;
  int64_t offset = 0;
  int64_t size = 0;

  if (OB_FAIL(fill_tablet_write_info(allocator_, new_tablet, tablet_macro_info, write_info))) {
    LOG_WARN("fail to fill write info", K(ret), KPC(new_tablet));
  } else if (OB_FAIL(ckpt_slog_handler->get_shared_block_raw_reader_writer().async_write(write_info, handle))) {
    LOG_WARN("fail to async write", K(ret), "write_info", write_info);
  } else if (OB_FAIL(handle.get_write_ctx(write_ctx))) {
    LOG_WARN("fail to batch get address", K(ret), K(handle));
  } else if (FALSE_IT(new_tablet->set_tablet_addr(write_ctx.addr_))) {
  } else if (OB_FAIL(write_ctx.addr_.get_block_addr(macro_id, offset, size))) {
    LOG_WARN("fail to get block addr", K(ret), K(write_ctx));
  } else if (OB_FAIL(new_tablet->set_macro_info_addr(macro_id, offset + (size - secondary_meta_size), secondary_meta_size, ObMetaDiskAddr::DiskType::RAW_BLOCK))) {
    LOG_WARN("fail to set macro info addr", K(ret), K(macro_id), K(offset), K(size), K(secondary_meta_size));
  } else if (OB_FAIL(macro_iter.init(ObTabletMacroType::MAX, tablet_macro_info))) {
    LOG_WARN("fail to init macro info iter", K(ret), K(tablet_macro_info));
  } else if (OB_FAIL(inc_ref_with_macro_iter(*new_tablet, macro_iter))) {
    LOG_WARN("fail to increase macro ref cnt", K(ret));
  } else {
    space_usage.shared_meta_size_ += upper_align(write_ctx.addr_.size(), DIO_READ_ALIGN_SIZE);
    new_tablet->tablet_meta_.space_usage_ = space_usage;
  }
  return ret;
}

int ObTabletPersister::inc_ref_with_macro_iter(ObTablet &tablet, ObMacroInfoIterator &macro_iter)
{
  int ret = OB_SUCCESS;
  bool inc_tablet_macro_ref_success = false;
  bool inc_other_macro_ref_success = false;
  const ObMetaDiskAddr &tablet_addr = tablet.tablet_addr_;

  if (OB_FAIL(ObTablet::inc_addr_ref_cnt(tablet_addr, inc_tablet_macro_ref_success))) {
    LOG_WARN("fail to increase tablet macro ref", K(ret), K(tablet_addr));
  } else if (OB_FAIL(tablet.inc_ref_with_macro_iter(macro_iter, inc_other_macro_ref_success))) {
    LOG_WARN("fail to increase ref cnt from macro iter", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (inc_tablet_macro_ref_success) {
      ObTablet::dec_addr_ref_cnt(tablet_addr);
    }
    if (inc_other_macro_ref_success) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(macro_iter.reuse())) {
        LOG_WARN("fail to reuse macro info iterator", K(tmp_ret));
      } else {
        tablet.dec_ref_with_macro_iter(macro_iter);
      }
    }
  } else {
    tablet.hold_ref_cnt_ = true;
  }
  return ret;
}

int ObTabletPersister::fill_tablet_write_info(
    common::ObArenaAllocator &allocator,
    const ObTablet *tablet,
    const ObTabletMacroInfo &tablet_macro_info,
    ObSharedBlockWriteInfo &write_info)
{
  int ret = OB_SUCCESS;
  ObInlineSecondaryMeta inline_meta(&tablet_macro_info, ObSecondaryMetaType::TABLET_MACRO_INFO);
  ObSArray<ObInlineSecondaryMeta> meta_arr;

  if (OB_ISNULL(tablet) || OB_UNLIKELY(!tablet_macro_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KPC(tablet), K(tablet_macro_info));
  } else if (OB_FAIL(meta_arr.push_back(inline_meta))) {
    LOG_WARN("fail to push back inline meta", K(ret), K(inline_meta));
  } else {
    const int64_t size = tablet->get_serialize_size(meta_arr);
    char *buf = static_cast<char *>(allocator.alloc(size));
    int64_t pos = 0;
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for tablet serialize", K(ret), K(size));
    } else if (OB_FAIL(tablet->serialize(buf, size, pos, meta_arr))) {
      LOG_WARN("fail to serialize tablet", K(ret), KPC(tablet), K(inline_meta), K(size), K(pos));
    } else {
      write_info.buffer_ = buf;
      write_info.offset_ = 0;
      write_info.size_ = size;
      write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    }
  }
  return ret;
}

int ObTabletPersister::convert_arg_to_tablet(const ObTabletTransformArg &arg, ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_FAIL(tablet.tablet_meta_.assign(arg.tablet_meta_))) {
    LOG_WARN("fail to copy tablet meta", K(ret), K(arg.tablet_meta_));
  // TODO: @luhaopeng.lhp after mds_mvs joint debugging completed, delete mds_data_.tablet_status_cache_
  } else if (OB_FAIL(tablet.assign_memtables(arg.memtables_, arg.memtable_count_))) {
    LOG_WARN("fail to assign memtables", K(ret), KP(arg.memtables_), K(arg.memtable_count_));
  } else {
    tablet.table_store_addr_.addr_ = arg.table_store_addr_;
    tablet.storage_schema_addr_.addr_ = arg.storage_schema_addr_;
    tablet.macro_info_addr_.addr_ = arg.tablet_macro_info_addr_;
  }
  return ret;
}

int ObTabletPersister::transform(const ObTabletTransformArg &arg, char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  ObTablet *tiny_tablet = reinterpret_cast<ObTablet *>(buf);
  ObMultiTimeStats::TimeStats *time_stats = nullptr;

  if (len <= sizeof(ObTablet) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(len));
  } else if (OB_FAIL(multi_stats_.acquire_stats("transform", time_stats))) {
    LOG_WARN("fail to acquire time stats", K(ret));
  } else if (OB_FAIL(convert_arg_to_tablet(arg, *tiny_tablet))) {
    LOG_WARN("fail to convert arg to tablet", K(ret), K(arg.tablet_meta_));
  } else {
    // buf related
    int64_t start_pos = sizeof(ObTablet);
    int64_t remain = len - start_pos;
    common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "Transform"));

    LOG_DEBUG("TINY TABLET: tablet", KP(buf), K(start_pos), K(remain));
    // rowkey read info related
    int64_t rowkey_read_info_size = 0;
    if (OB_SUCC(ret) && OB_NOT_NULL(arg.rowkey_read_info_ptr_)) {
      rowkey_read_info_size = arg.rowkey_read_info_ptr_->get_deep_copy_size();
      if (OB_UNLIKELY(remain < rowkey_read_info_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet memory buffer not enough for rowkey read info", K(ret), K(remain), K(rowkey_read_info_size));
      } else if (OB_FAIL(arg.rowkey_read_info_ptr_->deep_copy(
          buf + start_pos, remain, tiny_tablet->rowkey_read_info_))) {
        LOG_WARN("fail to deep copy rowkey read info to tablet", K(ret), KPC(arg.rowkey_read_info_ptr_));
      } else if (OB_ISNULL(tiny_tablet->rowkey_read_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr for rowkey read info deep copy", K(ret));
      } else {
        remain -= rowkey_read_info_size;
        start_pos += rowkey_read_info_size;
      }
      LOG_DEBUG("TINY TABLET: tablet + rowkey_read_info", KP(buf), K(start_pos), K(remain));
    }

    // ddl_kvs_ related
    if (OB_SUCC(ret) && (arg.ddl_kv_count_ > 0)) {
      const int ddl_kvs_size = sizeof(ObITable*) * ObTablet::DDL_KV_ARRAY_SIZE;
      if (OB_UNLIKELY(remain < ddl_kvs_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet memory buffer not enough for ddl kvs", K(ret), K(remain), K(ddl_kvs_size));
      } else {
        tiny_tablet->ddl_kvs_ = reinterpret_cast<ObDDLKV**>(buf + start_pos);
        if (OB_FAIL(tiny_tablet->assign_ddl_kvs(arg.ddl_kvs_, arg.ddl_kv_count_))) {
          LOG_WARN("fail to assign ddl_kvs_", K(ret), KP(arg.ddl_kvs_), K(arg.ddl_kv_count_), KP(buf), K(start_pos));
        } else {
          remain -= ddl_kvs_size;
          start_pos += ddl_kvs_size;
        }
      }
      LOG_DEBUG("TINY TABLET: tablet + ddl_kvs", KP(buf), K(start_pos), K(remain), K(tiny_tablet->ddl_kv_count_));
    }

    // table store related
    ObTabletTableStore *table_store = nullptr;
    if (OB_SUCC(ret)) {
      time_stats->click("before_load_table_store");
      if (arg.table_store_addr_.is_none()) {
        void *ptr = nullptr;
        if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObTabletTableStore)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate a buffer", K(ret), "sizeof", sizeof(ObTabletTableStore));
        } else {
          table_store = new (ptr) ObTabletTableStore();
          if (OB_FAIL(table_store->init(allocator, *tiny_tablet))) {
            LOG_WARN("fail to init table store", K(ret), K(*tiny_tablet));
          } else {
            time_stats->click("init_table_store");
          }
        }
      } else if (OB_FAIL(load_table_store(allocator, *tiny_tablet, arg.table_store_addr_, table_store))) {
        LOG_WARN("fail to load table store", K(ret), KPC(tiny_tablet), K(arg.table_store_addr_));
      } else {
        time_stats->click("load_table_store");
      }
    }

    int64_t remain_size_before_cache_table_store = 0;
    int64_t table_store_size = 0;
    if (OB_SUCC(ret)) {
      remain_size_before_cache_table_store = remain;
      table_store_size = table_store->get_deep_copy_size();
      if (OB_LIKELY((remain - table_store_size) >= 0)) {
        if (OB_FAIL(table_store->batch_cache_sstable_meta(allocator, remain - table_store_size))) {
          LOG_WARN("fail to batch cache sstable meta", K(ret), K(remain), K(table_store_size));
        } else {
          ObIStorageMetaObj *table_store_obj = nullptr;
          table_store_size = table_store->get_deep_copy_size();
          if (OB_FAIL(table_store->deep_copy(buf + start_pos, remain, table_store_obj))) {
            LOG_WARN("fail to deep copy table store v2", K(ret), K(table_store));
          } else if (OB_ISNULL(table_store_obj)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr for rowkey table store deep copy", K(ret), K(table_store_obj));
          } else {
            time_stats->click("cache_table_store");
            tiny_tablet->table_store_addr_.ptr_ = static_cast<ObTabletTableStore *>(table_store_obj);
            remain -= table_store_size;
            start_pos += table_store_size;
          }
        }
      } else {
        LOG_DEBUG("TINY TABLET: no enough memory for tablet store", K(rowkey_read_info_size), K(remain),
            K(table_store_size));
      }
    }

    // id_array related
    if (OB_SUCC(ret)) {
      LOG_INFO("TINY TABLET: tablet + rowkey_read_info + tablet store + auto_inc_seq", KP(buf), K(start_pos), K(remain));
      ObTabletMacroInfo *tablet_macro_info_obj = nullptr;
      if (OB_ISNULL(arg.tablet_macro_info_ptr_)) {
        // no need to prefetch id_array, since we only need it when recycling tablet
      } else {
        int64_t tablet_macro_info_size = arg.tablet_macro_info_ptr_->get_deep_copy_size();
        if (remain >= tablet_macro_info_size) {
          if (OB_FAIL(arg.tablet_macro_info_ptr_->deep_copy(buf + start_pos, remain, tablet_macro_info_obj))) {
            LOG_WARN("fail to deep copy block id array", K(ret));
          } else {
            time_stats->click("cache_macro_info");
            tiny_tablet->macro_info_addr_.ptr_ = tablet_macro_info_obj;
            remain -= tablet_macro_info_size;
            start_pos += tablet_macro_info_size;
          }
        } else {
          LOG_DEBUG("TINY TABLET: no enough memory for tablet macro info", K(rowkey_read_info_size), K(remain), K(tablet_macro_info_size));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(tiny_tablet->table_store_cache_.init(table_store->get_major_sstables(),
                                                       table_store->get_minor_sstables(),
                                                       arg.is_row_store_))) {
        LOG_WARN("failed to init table store cache", K(ret), KPC(table_store), K(arg));
      } else {
        time_stats->click("init_table_store_cache");
        tiny_tablet->is_inited_ = true;
      }
      LOG_DEBUG("succeed to transform", "tablet_id", tiny_tablet->tablet_meta_.tablet_id_,
        KPC(tiny_tablet->table_store_addr_.ptr_), K(tiny_tablet->macro_info_addr_),
        "tablet_buf_len", len, K(remain_size_before_cache_table_store), K(table_store_size), KPC(arg.tablet_macro_info_ptr_));
    }
  }
  return ret;
}

int ObTabletPersister::batch_write_sstable_info(
    common::ObIArray<ObSharedBlockWriteInfo> &write_infos,
    common::ObIArray<ObSharedBlocksWriteCtx> &write_ctxs,
    common::ObIArray<ObMetaDiskAddr> &addrs,
    common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs,
    ObBlockInfoSet &block_info_set)
{
  int ret = OB_SUCCESS;
  ObSharedBlockBatchHandle handle;
  ObTenantCheckpointSlogHandler *ckpt_slog_hanlder = MTL(ObTenantCheckpointSlogHandler*);
  if (OB_ISNULL(ckpt_slog_hanlder)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, ckpt slog handler is nullptr", K(ret), KP(ckpt_slog_hanlder));
  } else if (OB_FAIL(ckpt_slog_hanlder->get_shared_block_reader_writer().async_batch_write(write_infos, handle))) {
    LOG_WARN("fail to batch async write", K(ret), K(write_infos));
  } else if (OB_FAIL(handle.batch_get_write_ctx(write_ctxs))) {
    LOG_WARN("fail to batch get addr", K(ret), K(handle));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < write_ctxs.count(); ++i) {
      ObSharedBlocksWriteCtx &write_ctx = write_ctxs.at(i);
      if (OB_UNLIKELY(!write_ctx.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid addr", K(ret), K(i), K(write_ctx));
      } else if (OB_FAIL(addrs.push_back(write_ctx.addr_))) {
        LOG_WARN("fail to push sstable addr to array", K(ret), K(i), K(write_ctx));
      } else if (OB_FAIL(meta_write_ctxs.push_back(write_ctx))) {
        LOG_WARN("fail to push write ctxs to array", K(ret), K(i), K(write_ctx));
      } else if (OB_FAIL(block_info_set.shared_meta_block_info_set_.set_refactored(write_ctx.addr_.block_id(), 0 /*whether to overwrite*/))) {
        if (OB_HASH_EXIST != ret) {
          LOG_WARN("fail to push macro id into set", K(ret), K(i), K(write_ctx));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObTabletPersister::convert_macro_info_map(SharedMacroMap &shared_macro_map, ObBlockInfoSet::TabletMacroMap &aggregated_info_map)
{
  int ret = OB_SUCCESS;
  ObSharedBlockIndex shared_blk_index;
  int64_t occupy_size = 0;
  int64_t accumulated_size = 0;
  for (SharedMacroIterator iter = shared_macro_map.begin(); OB_SUCC(ret) && iter != shared_macro_map.end(); ++iter) {
    shared_blk_index = iter->first;
    occupy_size = iter->second;
    accumulated_size = 0;
    if (OB_FAIL(aggregated_info_map.get_refactored(shared_blk_index.shared_macro_id_, accumulated_size))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get accumulated size", K(ret), K(shared_blk_index));
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(aggregated_info_map.set_refactored(
        shared_blk_index.shared_macro_id_,
        accumulated_size + occupy_size,
        1/*whether to overwrite*/))) {
      LOG_WARN("fail to update aggregated info map", K(ret), K(shared_blk_index), K(accumulated_size), K(occupy_size));
    }
  }
  return ret;
}

int ObTabletPersister::fetch_and_persist_co_sstable(
    common::ObArenaAllocator &allocator,
    ObCOSSTableV2 *co_sstable,
    common::ObIArray<ObSharedBlocksWriteCtx> &sstable_meta_write_ctxs,
    common::ObIArray<ObMetaDiskAddr> &cg_addrs,
    int64_t &total_tablet_meta_size,
    ObBlockInfoSet &block_info_set,
    SharedMacroMap &shared_macro_map)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObSharedBlocksWriteCtx, 16> cg_write_ctxs;
  common::ObSEArray<ObSharedBlockWriteInfo, 16> cg_write_infos;
  const int64_t ctx_id = share::is_reserve_mode()
                       ? ObCtxIds::MERGE_RESERVE_CTX_ID
                       : ObCtxIds::DEFAULT_CTX_ID;
  cg_write_ctxs.set_attr(lib::ObMemAttr(MTL_ID(), "CGWriteCtxs", ctx_id));
  cg_write_infos.set_attr(lib::ObMemAttr(MTL_ID(), "CGWriteInfos", ctx_id));
  int64_t total_size = 0;
  cg_addrs.reset();
  ObSSTableMetaHandle co_meta_handle;

  if (OB_ISNULL(co_sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), KPC(co_sstable));
  } else if (FALSE_IT(total_size = co_sstable->get_serialize_size())) {
  } else if (total_size < SSTABLE_MAX_SERIALIZE_SIZE) {
    // do nothing
  } else if (OB_FAIL(co_sstable->get_meta(co_meta_handle))) {
    LOG_WARN("failed to get co meta handle", K(ret), KPC(co_sstable));
  } else {
    const ObSSTableArray &cg_sstables = co_meta_handle.get_sstable_meta().get_cg_sstables();
    ObSSTable *cg_sstable = nullptr;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < cg_sstables.count(); ++idx) {
      cg_sstable = cg_sstables[idx];
      ObSSTablePersistWrapper wrapper(cg_sstable);
      if (OB_FAIL(fill_write_info(allocator, &wrapper, cg_write_infos))) {
        LOG_WARN("failed to fill sstable write info", K(ret));
      } else if (OB_FAIL(copy_sstable_macro_info(*cg_sstable, shared_macro_map, block_info_set))) {
        LOG_WARN("fail to call sstable macro info", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (0 < cg_write_infos.count()
        && OB_FAIL(batch_write_sstable_info(cg_write_infos, cg_write_ctxs, cg_addrs, sstable_meta_write_ctxs, block_info_set))) {
      LOG_WARN("failed to batch write sstable", K(ret));
    } else if (OB_UNLIKELY(cg_addrs.count() != cg_sstables.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected cg addrs count", K(ret), K(cg_addrs.count()), K(cg_sstables.count()));
    } else {
      int64_t sstable_meta_size = 0;
      for (int64_t i = 0; i < cg_addrs.count(); i++) {
        sstable_meta_size += cg_addrs.at(i).size();
      }
      total_tablet_meta_size += upper_align(sstable_meta_size, DIO_READ_ALIGN_SIZE);
    }
  }

  return ret;
}

int ObTabletPersister::fetch_and_persist_sstable(
    ObTableStoreIterator &table_iter,
    ObTabletTableStore &new_table_store,
    common::ObIArray<ObSharedBlocksWriteCtx> &sstable_meta_write_ctxs,
    int64_t &total_tablet_meta_size,
    ObBlockInfoSet &block_info_set)
{
  int ret = OB_SUCCESS;
  SharedMacroMap shared_macro_map;
  common::ObSEArray<ObITable *, 8> tables;
  common::ObSEArray<ObCOSSTableV2 *, 1> big_co_tables; // used to recycle mem
  common::ObSEArray<ObMetaDiskAddr, 8> addrs;
  common::ObSEArray<ObMetaDiskAddr, 8> cg_addrs;
  common::ObSEArray<ObSharedBlocksWriteCtx, 8> write_ctxs;
  common::ObSEArray<ObSharedBlockWriteInfo, 8> write_infos;
  const int64_t ctx_id = share::is_reserve_mode()
                       ? ObCtxIds::MERGE_RESERVE_CTX_ID
                       : ObCtxIds::DEFAULT_CTX_ID;
  tables.set_attr(lib::ObMemAttr(MTL_ID(), "PerstTables", ctx_id));
  big_co_tables.set_attr(lib::ObMemAttr(MTL_ID(), "PerstBigTbls", ctx_id));
  addrs.set_attr(lib::ObMemAttr(MTL_ID(), "PerstAddrs", ctx_id));
  cg_addrs.set_attr(lib::ObMemAttr(MTL_ID(), "PerstCGAddrs", ctx_id));
  write_ctxs.set_attr(lib::ObMemAttr(MTL_ID(), "PerstWriteCtxs", ctx_id));
  write_infos.set_attr(lib::ObMemAttr(MTL_ID(), "PerstWriteInfos", ctx_id));

  ObArenaAllocator tmp_allocator("PersistSSTable", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ctx_id);
  ObITable *table = nullptr;
  ObMultiTimeStats::TimeStats *time_stats = nullptr;
  int32_t large_co_sstable_cnt = 0;
  int32_t small_co_sstable_cnt = 0;
  int32_t normal_sstable_cnt = 0;
  int32_t cg_sstable_cnt = 0;

  if (OB_FAIL(multi_stats_.acquire_stats("fetch_and_persist_sstable", time_stats))) {
    LOG_WARN("fail to acquire stats", K(ret));
  } else if (OB_FAIL(shared_macro_map.create(ObTablet::SHARED_MACRO_BUCKET_CNT, "ObBlockInfoMap", "SharedBlkNode", MTL_ID()))) {
    LOG_WARN("fail to create shared macro map", K(ret));
  }
  while (OB_SUCC(ret) && OB_SUCC(table_iter.get_next(table))) {
    if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, table is nullptr", K(ret), KPC(table));
    } else if (table->is_co_sstable() && table->get_serialize_size() > SSTABLE_MAX_SERIALIZE_SIZE) {
      large_co_sstable_cnt++;
      ObCOSSTableV2 *co_sstable = static_cast<ObCOSSTableV2 *>(table);
      // serialize full co sstable and shell cg sstables when the serialize size of CO reached the limit.
      FLOG_INFO("cannot full serialize CO within 2MB buffer, should serialize CO with Shell CG", K(ret), KPC(table));
      cg_addrs.reset();
      ObCOSSTableV2 *tmp_co_sstable = nullptr;
      ObSSTableMetaHandle co_meta_handle;

      if (OB_FAIL(co_sstable->get_meta(co_meta_handle))) {
        LOG_WARN("failed to get co meta handle", K(ret), KPC(co_sstable));
      } else if (OB_FAIL(fetch_and_persist_co_sstable(
          allocator_, co_sstable, sstable_meta_write_ctxs, cg_addrs, total_tablet_meta_size, block_info_set, shared_macro_map))) {
        LOG_WARN("fail to persist co sstable", K(ret));
      } else if (OB_FAIL(co_sstable->deep_copy(tmp_allocator, cg_addrs, tmp_co_sstable))) {
        LOG_WARN("failed to deep copy co sstable", K(ret), KPC(co_sstable));
      } else {
        cg_sstable_cnt += co_meta_handle.get_sstable_meta().get_cg_sstables().count();
        ObSSTablePersistWrapper wrapper(tmp_co_sstable);
        if (OB_FAIL(fill_write_info(allocator_, &wrapper, write_infos))) {
          LOG_WARN("failed to fill sstable write info", K(ret));
        } else if (OB_FAIL(tables.push_back(tmp_co_sstable))) {
          LOG_WARN("failed to add table", K(ret));
        } else if (OB_FAIL(big_co_tables.push_back(tmp_co_sstable))) {
          LOG_WARN("failed to add co to big tables", K(ret));
        } else if (OB_FAIL(copy_sstable_macro_info(*tmp_co_sstable, shared_macro_map, block_info_set))) {
          LOG_WARN("fail to call sstable macro info", K(ret));
        }
      }
    } else {
      // Statistics the number of macroblocks of cg sstables
      int64_t cg_sstable_meta_size = 0;
      if (table->is_co_sstable()) {
        small_co_sstable_cnt++;
        ObCOSSTableV2 *co_sstable = static_cast<ObCOSSTableV2 *>(table);
        ObSSTableMetaHandle co_meta_handle;

        if (OB_FAIL(co_sstable->get_meta(co_meta_handle))) {
          LOG_WARN("failed to get co meta handle", K(ret), KPC(co_sstable));
        } else {
          const ObSSTableArray &cg_sstables = co_meta_handle.get_sstable_meta().get_cg_sstables();
          cg_sstable_cnt += cg_sstables.count();

          ObSSTable *cg_sstable = nullptr;
          for (int64_t idx = 0; OB_SUCC(ret) && idx < cg_sstables.count(); ++idx) {
            cg_sstable = cg_sstables[idx];
            const ObMetaDiskAddr &sstable_addr = cg_sstable->get_addr();
            if (OB_FAIL(copy_sstable_macro_info(*cg_sstable, shared_macro_map, block_info_set))) {
              LOG_WARN("fail to call sstable macro info", K(ret));
            } else if (sstable_addr.is_block()) {
              // this cg sstable has been persisted before
              cg_sstable_meta_size += sstable_addr.size();
              if (OB_FAIL(block_info_set.shared_meta_block_info_set_.set_refactored(sstable_addr.block_id(), 0 /*whether to overwrite*/))) {
                if (OB_HASH_EXIST != ret) {
                  LOG_WARN("fail to push macro id into set", K(ret), K(sstable_addr));
                } else {
                  ret = OB_SUCCESS;
                }
              }
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        total_tablet_meta_size += upper_align(cg_sstable_meta_size, DIO_READ_ALIGN_SIZE);
        normal_sstable_cnt++;
        ObSSTable *sstable = static_cast<ObSSTable *>(table);
        ObSSTablePersistWrapper wrapper(static_cast<ObSSTable *>(table));
        if (OB_FAIL(fill_write_info(allocator_, &wrapper, write_infos))) {
          LOG_WARN("failed to fill sstable write info", K(ret));
        } else if (OB_UNLIKELY(sstable->has_padding_meta_cache())) {
          /*
           * The following defend log used ONLY in 4_2_x upgrade 4_3_x scenario !!!
           * We should fill invalid fields of SSTable's meta cache when deserialize tablet.
           */
          LOG_ERROR_RET(OB_ERR_UNEXPECTED, "meet unexpected padding meta cache", KPC(sstable));
        }
        if (FAILEDx(tables.push_back(table))) {
          LOG_WARN("fail to push back sstable address", K(ret), K(tables));
        } else if (OB_FAIL(copy_sstable_macro_info(*sstable, shared_macro_map, block_info_set))) {
          LOG_WARN("fail to call sstable macro info", K(ret));
        }
      }
    }
  } // end while
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (FAILEDx(time_stats->set_extra_info("%s:%ld,%s:%ld,%s:%ld,%s:%ld",
      "large_co_sst_cnt", large_co_sstable_cnt, "small_co_sst_cnt", small_co_sstable_cnt,
      "normal_sst_cnt", normal_sstable_cnt, "cg_sst_cnt", cg_sstable_cnt))) {
    LOG_WARN("fail to set time stats extra info", K(ret));
  } else if (FALSE_IT(time_stats->click("fill_all_sstable_write_info"))) {
  } else if (OB_FAIL(convert_macro_info_map(shared_macro_map, block_info_set.shared_data_block_info_map_))) {
    LOG_WARN("fail to convert shared data block info map", K(ret));
  } else if (write_infos.count() > 0
      && OB_FAIL(batch_write_sstable_info(write_infos, write_ctxs, addrs, sstable_meta_write_ctxs, block_info_set))) {
    LOG_WARN("failed to batch write sstable", K(ret));
  } else if (FALSE_IT(time_stats->click("batch_write_sstable_info"))) {
  } else if (OB_FAIL(new_table_store.init(allocator_, tables, addrs))) {
    LOG_WARN("fail to init new table store", K(ret), K(tables), K(addrs));
  } else {
    time_stats->click("init_new_table_store");
    int64_t sstable_meta_size = 0;
    for (int64_t i = 0; i < addrs.count(); i++) {
      sstable_meta_size += addrs.at(i).size();
    }
    total_tablet_meta_size += upper_align(sstable_meta_size, DIO_READ_ALIGN_SIZE);
  }

  // recycle big co table mem
  for (int64_t idx = 0; idx < big_co_tables.count(); ++idx) {
    ObCOSSTableV2 *co_table = big_co_tables.at(idx);
    if (OB_NOT_NULL(co_table)) {
      co_table->~ObCOSSTableV2();
      tmp_allocator.free(co_table);
    }
  }
  return ret;
}

int ObTabletPersister::copy_sstable_macro_info(const ObSSTable &sstable,
                                               SharedMacroMap &shared_macro_map,
                                               ObBlockInfoSet &block_info_set)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  if (OB_FAIL(sstable.get_meta(meta_handle))) {
    LOG_WARN("fail to get sstable meta handle", K(ret), K(sstable));
  } else if (sstable.is_small_sstable() && OB_FAIL(copy_shared_macro_info(
      meta_handle.get_sstable_meta().get_macro_info(),
      shared_macro_map,
      block_info_set.meta_block_info_set_))) {
    LOG_WARN("fail to copy shared macro's info", K(ret), K(meta_handle.get_sstable_meta().get_macro_info()));
  } else if (!sstable.is_small_sstable()
      && OB_FAIL(copy_data_macro_ids(meta_handle.get_sstable_meta().get_macro_info(), block_info_set))) {
    LOG_WARN("fail to copy tablet's data macro ids", K(ret), K(meta_handle.get_sstable_meta().get_macro_info()));
  }
  return ret;
}

int ObTabletPersister::copy_shared_macro_info(
    const blocksstable::ObSSTableMacroInfo &macro_info,
    SharedMacroMap &shared_macro_map,
    ObBlockInfoSet::TabletMacroSet &meta_id_set)
{
  int ret = OB_SUCCESS;
  ObMacroIdIterator iter;
  MacroBlockId macro_id;
  if (OB_FAIL(macro_info.get_data_block_iter(iter))) {
    LOG_WARN("fail to get data block iterator", K(ret));
  } else if (OB_FAIL(iter.get_next_macro_id(macro_id))) {
    LOG_WARN("fail to get shared macro id", K(ret), K(iter));
  } else {
    ObSharedBlockIndex block_idx(macro_id, macro_info.get_nested_offset());
    if (OB_FAIL(shared_macro_map.set_refactored(block_idx, macro_info.get_nested_size(), 0/*whether to overwrite*/))) {
      if (OB_HASH_EXIST != ret) {
        LOG_WARN("fail to push shared macro info into map", K(ret), K(macro_id), K(macro_info));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  iter.reset();
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(macro_info.get_other_block_iter(iter))) {
    LOG_WARN("fail to get other block iterator", K(ret));
  } else if (OB_FAIL(do_copy_ids(iter, meta_id_set))) {
    LOG_WARN("fail to copy other block ids", K(ret));
  }
  return ret;
}

int ObTabletPersister::copy_data_macro_ids(
    const blocksstable::ObSSTableMacroInfo &macro_info,
    ObBlockInfoSet &block_info_set)
{
  int ret = OB_SUCCESS;
  ObMacroIdIterator iter;
  MacroBlockId macro_id;

  if (OB_FAIL(macro_info.get_data_block_iter(iter))) {
    LOG_WARN("fail to get data block iterator", K(ret));
  } else if (OB_FAIL(do_copy_ids(iter, block_info_set.data_block_info_set_))) {
    LOG_WARN("fail to copy data block ids", K(ret), K(iter));
  } else if (FALSE_IT(iter.reset())) {
  } else if (OB_FAIL(macro_info.get_other_block_iter(iter))) {
    LOG_WARN("fail to get other block iterator", K(ret));
  } else if (OB_FAIL(do_copy_ids(iter, block_info_set.meta_block_info_set_))) {
    LOG_WARN("fail to copy other block ids", K(ret), K(iter));
  } else if (FALSE_IT(iter.reset())) {
  } else if (OB_FAIL(macro_info.get_linked_block_iter(iter))) {
    LOG_WARN("fail to get linked block iterator", K(ret));
  } else if (OB_FAIL(do_copy_ids(iter, block_info_set.meta_block_info_set_))) {
    LOG_WARN("fail to copy linked block ids", K(ret), K(iter));
  }
  return ret;
}

int ObTabletPersister::do_copy_ids(
    blocksstable::ObMacroIdIterator &iter,
    ObBlockInfoSet::TabletMacroSet &id_set)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.get_next_macro_id(macro_id))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next macro id", K(ret), K(macro_id));
      }
    } else if (OB_FAIL(id_set.set_refactored(macro_id, 0 /*whether to overwrite*/))) {
      if (OB_HASH_EXIST != ret) {
        LOG_WARN("fail to push macro id into set", K(ret), K(macro_id));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return OB_ITER_END == ret ? OB_SUCCESS : ret;
}

int ObTabletPersister::write_and_fill_args(
    const common::ObIArray<ObSharedBlockWriteInfo> &write_infos,
    ObTabletTransformArg &arg,
    common::ObIArray<ObSharedBlocksWriteCtx> &total_write_ctxs,
    int64_t &total_tablet_meta_size,
    ObBlockInfoSet::TabletMacroSet &meta_block_id_set)
{
  int ret = OB_SUCCESS;
  ObTenantCheckpointSlogHandler *ckpt_slog_handler = MTL(ObTenantCheckpointSlogHandler*);
  ObSharedBlockReaderWriter &reader_writer = ckpt_slog_handler->get_shared_block_reader_writer();
  ObSharedBlockBatchHandle handle;
  ObMetaDiskAddr* addr[] = { // NOTE: The order must be the same as the batch async write.
    &arg.table_store_addr_,
    &arg.storage_schema_addr_,
  };
  constexpr int64_t total_addr_cnt = sizeof(addr) / sizeof(addr[0]);
  int64_t none_addr_cnt = 0;
  for (int64_t i = 0; i < total_addr_cnt; ++i) {
    if (addr[i]->is_none()) {
      ++none_addr_cnt;
    }
  }

  common::ObSEArray<ObSharedBlocksWriteCtx, sizeof(addr)/sizeof(addr[0])> write_ctxs;
  const int64_t ctx_id = share::is_reserve_mode()
                       ? ObCtxIds::MERGE_RESERVE_CTX_ID
                       : ObCtxIds::DEFAULT_CTX_ID;
  write_ctxs.set_attr(lib::ObMemAttr(MTL_ID(), "WriteCtxs", ctx_id));

  if (OB_UNLIKELY(total_addr_cnt != write_infos.count() + none_addr_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(total_addr_cnt), "write_info_count", write_infos.count(), K(none_addr_cnt));
  } else if (OB_FAIL(reader_writer.async_batch_write(write_infos, handle))) {
    LOG_WARN("fail to batch async write", K(ret));
  } else if (OB_FAIL(handle.batch_get_write_ctx(write_ctxs))) {
    LOG_WARN("fail to batch get addr", K(ret), K(handle));
  } else if (OB_UNLIKELY(write_infos.count() != write_ctxs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write ctx count does not equal to write info count", K(ret),
        "write_info_count", write_infos.count(),
        "write_ctx_count", write_ctxs.count(),
        K(write_ctxs), K(handle));
  } else {
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < total_addr_cnt; ++i) {
      if (addr[i]->is_none()) {
        // skip none addr
      } else {
        const ObSharedBlocksWriteCtx &write_ctx = write_ctxs.at(pos++);
        if (OB_UNLIKELY(!write_ctx.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected write ctx", K(ret), K(i), K(write_ctx), K(handle));
        } else if (OB_FAIL(total_write_ctxs.push_back(write_ctx))) {
          LOG_WARN("fail to push write ctx to array", K(ret), K(i), K(write_ctx));
        } else if (OB_FAIL(meta_block_id_set.set_refactored(write_ctx.addr_.block_id(), 0 /*whether to overwrite*/))) {
          if (OB_HASH_EXIST != ret) {
            LOG_WARN("fail to push macro id into set", K(ret), K(write_ctx.addr_));
          } else {
            ret = OB_SUCCESS;
          }
        }
        if (OB_SUCC(ret)) {
          *addr[i] = write_ctx.addr_;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t tmp_meta_size = 0;
    for (int64_t i = 0; i < total_addr_cnt; i++) {
      if (!addr[i]->is_none()) {
        tmp_meta_size += addr[i]->size();
      }
    }
    total_tablet_meta_size += upper_align(tmp_meta_size, DIO_READ_ALIGN_SIZE);
  }

  return ret;
}

int ObTabletPersister::load_table_store(
    common::ObArenaAllocator &allocator,
    const ObTablet &tablet,
    const ObMetaDiskAddr &addr,
    ObTabletTableStore *&table_store)
{
  int ret = OB_SUCCESS;
  void *ptr = nullptr;
  ObTabletTableStore *tmp_store = nullptr;
  ObArenaAllocator io_allocator(common::ObMemAttr(MTL_ID(), "PersisterTmpIO"));
  if (OB_UNLIKELY(!addr.is_block())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("address type isn't disk", K(ret), K(addr));
  } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObTabletTableStore)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate a buffer", K(ret), "sizeof", sizeof(ObTabletTableStore));
  } else {
    tmp_store = new (ptr) ObTabletTableStore();
    char *io_buf = nullptr;
    int64_t buf_len = -1;
    int64_t io_pos = 0;
    ObSharedBlockReadInfo read_info;
    ObSharedBlockReadHandle io_handle(io_allocator);
    ObMultiTimeStats::TimeStats *time_stats = nullptr;

    read_info.addr_ = addr;
    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000;
    if (OB_FAIL(multi_stats_.acquire_stats("load_table_store", time_stats))) {
      LOG_WARN("fail to acquire stats", K(ret));
    } else if (OB_FAIL(ObSharedBlockReaderWriter::async_read(read_info, io_handle))) {
      LOG_WARN("fail to async read", K(ret), K(read_info));
    } else if (OB_FAIL(io_handle.wait())) {
      LOG_WARN("fail to wait io_hanlde", K(ret), K(read_info));
    } else if (FALSE_IT(time_stats->click("read_io"))) {
    } else if (OB_FAIL(io_handle.get_data(io_allocator, io_buf, buf_len))) {
      LOG_WARN("fail to get data", K(ret), K(read_info));
    } else if (OB_FAIL(tmp_store->deserialize(allocator, tablet, io_buf, buf_len, io_pos))) {
      LOG_WARN("fail to deserialize table store", K(ret), K(tablet), KP(io_buf), K(buf_len));
    } else {
      time_stats->click("deserialize_table_store");
      table_store = tmp_store;
      LOG_DEBUG("succeed to load table store", K(ret), K(addr), KPC(table_store), K(tablet));
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(tmp_store)) {
      // avoid memory leak, like: ObMajorChecksumInfo::column_checksums_
      tmp_store->~ObTabletTableStore();
    }
    if (OB_NOT_NULL(ptr)) {
      // ObArenaAllocator has no effect, but is a safety measure
      allocator.free(ptr);
    }
  }
  return ret;
}

int ObTabletPersister::transform_tablet_memory_footprint(
    const ObTablet &old_tablet,
    char *buf,
    const int64_t len)
{
  int ret = OB_SUCCESS;
  ObTabletTransformArg arg;
  ObTabletPersister persister;
  ObMultiTimeStats::TimeStats *time_stats = nullptr;
  if (OB_FAIL(persister.multi_stats_.acquire_stats("transform_tablet_memory_footprint", time_stats))) {
    LOG_WARN("fail to acquire stats", K(ret));
  } else if (OB_UNLIKELY(!old_tablet.hold_ref_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old tablet doesn't hold ref cnt", K(ret), K(old_tablet));
  } else if (OB_FAIL(convert_tablet_to_mem_arg(old_tablet, arg))) {
    LOG_WARN("fail to convert tablet to mem arg", K(ret), K(arg), KP(buf), K(len), K(old_tablet));
  } else if (FALSE_IT(time_stats->click("convert_tablet_to_mem_arg"))) {
  } else if (OB_FAIL(persister.transform(arg, buf, len))) {
    LOG_WARN("fail to transform tablet", K(ret), K(arg), KP(buf), K(len), K(old_tablet));
  } else {
    time_stats->click("transform");
    ObTablet *tablet = reinterpret_cast<ObTablet *>(buf);
    tablet->set_next_tablet_guard(old_tablet.next_tablet_guard_);
    tablet->set_tablet_addr(old_tablet.get_tablet_addr());
    tablet->hold_ref_cnt_ = old_tablet.hold_ref_cnt_;
    persister.print_time_stats(*time_stats, 20_ms, 1_s);
  }
  return ret;
}

int ObTabletPersister::fetch_table_store_and_write_info(
    const ObTablet &tablet,
    ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
    common::ObIArray<ObSharedBlockWriteInfo> &write_infos,
    common::ObIArray<ObSharedBlocksWriteCtx> &sstable_meta_write_ctxs,
    int64_t &total_tablet_meta_size,
    ObBlockInfoSet &block_info_set)
{
  int ret = OB_SUCCESS;
  ObTabletTableStore new_table_store;
  ObMultiTimeStats::TimeStats *time_stats = nullptr;
  const ObTabletTableStore *table_store = nullptr;
  ObTableStoreIterator table_iter;
  if (OB_FAIL(multi_stats_.acquire_stats("persist_and_fill_tablet", time_stats))) {
    LOG_WARN("fail to acquire time stats", K(ret));
  } else if (OB_FAIL(tablet.fetch_table_store(wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(wrapper.get_member(table_store))) {
    LOG_WARN("fail to get table store from wrapper", K(ret), K(wrapper));
  } else if (FALSE_IT(time_stats->click("fetch_table_store"))) {
  } else if (OB_FAIL(table_store->get_all_sstable(table_iter))) {
    LOG_WARN("fail to get all sstable iterator", K(ret), KPC(table_store));
  } else if (FALSE_IT(time_stats->click("get_all_sstable"))) {
  } else if (OB_FAIL(fetch_and_persist_sstable(
      table_iter, new_table_store, sstable_meta_write_ctxs, total_tablet_meta_size, block_info_set))) {
    LOG_WARN("fail to fetch and persist sstable", K(ret), K(table_iter));
  } else if (FALSE_IT(time_stats->click("fetch_and_persist_sstable"))) {
  } else if (OB_FAIL(fill_write_info(allocator_, &new_table_store, write_infos))) {
    LOG_WARN("fail to fill table store write info", K(ret), K(new_table_store));
  } else {
    time_stats->click("fill_write_info");
  }
  return ret;
}

int ObTabletPersister::load_storage_schema_and_fill_write_info(
    const ObTablet &tablet,
    common::ObArenaAllocator &allocator,
    common::ObIArray<ObSharedBlockWriteInfo> &write_infos)
{
  int ret = OB_SUCCESS;
  ObStorageSchema *storage_schema = nullptr;
  if (OB_FAIL(tablet.load_storage_schema(allocator, storage_schema))) {
    LOG_WARN("fail to load storage schema", K(ret));
  } else if (OB_ISNULL(storage_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage schema is null", K(ret), KP(storage_schema));
  } else if (OB_FAIL(fill_write_info(allocator, storage_schema, write_infos))) {
    LOG_WARN("fail to fill write info", K(ret), KP(storage_schema));
  }
  ObTabletObjLoadHelper::free(allocator, storage_schema);
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
