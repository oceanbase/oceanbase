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

#include "lib/ob_errno.h"
#include <cstdint>
#define USING_LOG_PREFIX STORAGE

#include "storage/tablet/ob_tablet.h"

#include "common/ob_clock_generator.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/objectpool/ob_server_object_pool.h"
#include "lib/utility/ob_macro_utils.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/palf/palf_options.h"
#include "share/ob_ls_id.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "storage/ob_dml_running_ctx.h"
#include "storage/ob_i_store.h"
#include "storage/ob_i_table.h"
#include "storage/ob_row_reshape.h"
#include "storage/ob_sync_tablet_seq_clog.h"
#include "storage/ob_storage_schema.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"
#include "storage/access/ob_dml_param.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/ob_sstable_sec_meta_iterator.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/compaction/ob_extra_medium_info.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/meta_mem/ob_storage_meta_cache.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/access/ob_rows_info.h"
#include "storage/ddl/ob_ddl_clog.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_memtable_mgr.h"
#include "storage/tablet/ob_tablet_ddl_info.h"
#include "storage/tablet/ob_tablet_medium_info_reader.h"
#include "storage/tablet/ob_tablet_mds_node_dump_operator.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tablet/ob_tablet_binding_mds_user_data.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_medium_list_checker.h"
#include "storage/memtable/ob_row_conflict_handler.h"
#include "storage/tablet/ob_tablet_binding_info.h"

namespace oceanbase
{
using namespace memtable;
using namespace share;
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace logservice;
using namespace compaction;
using namespace palf;

namespace storage
{
#define ALLOC_AND_INIT(allocator, addr, args...)                                  \
  do {                                                                            \
    if (OB_SUCC(ret)) {                                                           \
      if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, addr.ptr_))) {  \
        LOG_WARN("fail to allocate and new object", K(ret));                      \
      } else if (OB_FAIL(addr.get_ptr()->init(allocator, args))) {                \
        LOG_WARN("fail to initialize tablet member", K(ret), K(addr));            \
      }                                                                           \
    }                                                                             \
  } while (false)                                                                 \

#define IO_AND_DESERIALIZE(allocator, meta_addr, meta_ptr, args...)                                         \
  do {                                                                                                      \
    if (OB_SUCC(ret)) {                                                                                     \
      ObArenaAllocator io_allocator(common::ObMemAttr(MTL_ID(), "TmpIO"));                                  \
      char *io_buf = nullptr;                                                                               \
      int64_t buf_len = -1;                                                                                 \
      int64_t io_pos = 0;                                                                                   \
      if (OB_FAIL(ObTabletObjLoadHelper::read_from_addr(io_allocator, meta_addr, io_buf, buf_len))) {       \
        LOG_WARN("read table store failed", K(ret));                                                        \
      } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, meta_ptr))) {                      \
        LOG_WARN("alloc and new table store ptr failed", K(ret), K(buf_len), K(io_pos));                    \
      } else if (OB_FAIL(meta_ptr->deserialize(allocator, ##args, io_buf, buf_len, io_pos))) {              \
        LOG_WARN("deserialize failed", K(ret), K(buf_len), K(io_pos));                                      \
      }                                                                                                     \
    }                                                                                                       \
  } while (false)                                                                                           \

ObTablet::ObTablet()
  : version_(TABLET_VERSION_V2),
    length_(0),
    wash_score_(INT64_MIN),
    mds_data_(),
    ref_cnt_(0),
    next_tablet_guard_(),
    tablet_meta_(),
    rowkey_read_info_(nullptr),
    table_store_addr_(),
    storage_schema_addr_(),
    memtable_count_(0),
    ddl_kvs_(nullptr),
    ddl_kv_count_(0),
    pointer_hdl_(),
    tablet_addr_(),
    allocator_(nullptr),
    memtables_lock_(),
    memtable_mgr_(nullptr),
    log_handler_(nullptr),
    next_tablet_(nullptr),
    hold_ref_cnt_(false),
    is_inited_(false),
    mds_cache_lock_(),
    tablet_status_cache_(),
    ddl_data_cache_()
{
#if defined(__x86_64__) && !defined(ENABLE_OBJ_LEAK_CHECK)
  static_assert(sizeof(ObTablet) + sizeof(ObRowkeyReadInfo) == 1576, "The size of ObTablet will affect the meta memory manager, and the necessity of adding new fields needs to be considered.");
#endif
  MEMSET(memtables_, 0x0, sizeof(memtables_));
}

ObTablet::~ObTablet()
{
  reset();
}

void ObTablet::reset()
{
  FLOG_INFO("reset tablet", KP(this), "ls_id", tablet_meta_.ls_id_, "tablet_id", tablet_meta_.tablet_id_, K(lbt()));

  reset_memtable();
  reset_ddl_memtables();
  storage_schema_addr_.reset();
  table_store_addr_.reset();
  wash_score_ = INT64_MIN;
  tablet_meta_.reset();
  mds_data_.reset();
  tablet_addr_.reset();
  memtable_mgr_ = nullptr;
  log_handler_ = nullptr;
  pointer_hdl_.reset();
  if (nullptr != rowkey_read_info_) {
    rowkey_read_info_->reset();
    rowkey_read_info_->~ObRowkeyReadInfo();
    rowkey_read_info_ = nullptr;
  }
  tablet_status_cache_.reset();
  ddl_data_cache_.reset();
  next_tablet_guard_.reset();
  // allocator_ = nullptr;  can't reset allocator_ which would be used when gc tablet
  version_ = TABLET_VERSION_V2;
  length_ = 0;
  next_tablet_ = nullptr;
  hold_ref_cnt_ = false;
  is_inited_ = false;
}

int ObTablet::init_for_first_time_creation(
    common::ObArenaAllocator &allocator,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const common::ObTabletID &data_tablet_id,
    const share::SCN &create_scn,
    const int64_t snapshot_version,
    const share::schema::ObTableSchema &table_schema,
    const lib::Worker::CompatMode compat_mode,
    const ObTabletTableStoreFlag &store_flag,
    blocksstable::ObSSTable *sstable,
    ObFreezer *freezer)
{
  int ret = OB_SUCCESS;
  const int64_t default_max_sync_medium_scn = 0;
  ObITable **ddl_kvs_addr = nullptr;
  int64_t ddl_kv_count = 0;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(!data_tablet_id.is_valid())
      //|| OB_UNLIKELY(create_scn <= OB_INVALID_TIMESTAMP)
      || OB_UNLIKELY(OB_INVALID_VERSION == snapshot_version)
      || OB_UNLIKELY(!table_schema.is_valid())
      || OB_UNLIKELY(lib::Worker::CompatMode::INVALID == compat_mode)
      || OB_ISNULL(freezer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(tablet_id), K(data_tablet_id),
        K(create_scn), K(snapshot_version), K(table_schema), K(compat_mode), KP(freezer));
  } else if (OB_UNLIKELY(!pointer_hdl_.is_valid())
      || OB_ISNULL(memtable_mgr_)
      || OB_ISNULL(log_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet pointer handle is invalid", K(ret), K_(pointer_hdl), K_(memtable_mgr), K_(log_handler));
  } else if (OB_FAIL(init_shared_params(ls_id, tablet_id, table_schema.get_schema_version(),
      default_max_sync_medium_scn, compat_mode, freezer))) {
    LOG_WARN("failed to init shared params", K(ret), K(ls_id), K(tablet_id), K(compat_mode), KP(freezer));
  } else if (OB_FAIL(tablet_meta_.init(ls_id, tablet_id, data_tablet_id,
      create_scn, snapshot_version, compat_mode, store_flag, table_schema.get_schema_version()/*create_schema_version*/))) {
    LOG_WARN("failed to init tablet meta", K(ret), K(ls_id), K(tablet_id), K(data_tablet_id),
        K(create_scn), K(snapshot_version), K(compat_mode), K(store_flag));
  } else if (is_ls_inner_tablet() && OB_FAIL(inner_create_memtable())) {
    LOG_WARN("failed to create first memtable", K(ret), K(tablet_id));
  } else if (OB_FAIL(pull_memtables(allocator, ddl_kvs_addr, ddl_kv_count))) {
    LOG_WARN("fail to pull memtable", K(ret));
  } else {
    ddl_kvs_ = ddl_kvs_addr;
    ddl_kv_count_ = ddl_kv_count;
    ALLOC_AND_INIT(allocator, table_store_addr_, (*this), sstable);
    ALLOC_AND_INIT(allocator, storage_schema_addr_, table_schema, compat_mode,
      true/*skip_column_info*/, ObStorageSchema::STORAGE_SCHEMA_VERSION_V2);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_sstable_column_checksum())) {
    LOG_WARN("failed to check sstable column checksum", K(ret), KPC(this));
  } else if (OB_FAIL(build_read_info(allocator))) {
    LOG_WARN("failed to build read info", K(ret));
  } else if (!is_ls_inner_tablet() && OB_FAIL(mds_data_.init_for_first_creation(allocator))) {
    LOG_WARN("failed to init mds data", K(ret));
  } else if (is_ls_inner_tablet() && OB_FAIL(mds_data_.init_with_tablet_status(allocator, ObTabletStatus::NORMAL, ObTabletMdsUserDataType::CREATE_TABLET))) {
    LOG_WARN("failed to init mds data for ls inner tablet", K(ret));
  } else if (FALSE_IT(set_mem_addr())) {
  } else if (OB_FAIL(inner_inc_macro_ref_cnt())) {
    LOG_WARN("failed to increase macro ref cnt", K(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("succeeded to init tablet for first time creation", K(ret), KPC(sstable), K(*this));
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }

  return ret;
}

int ObTablet::init_for_merge(
    common::ObArenaAllocator &allocator,
    const ObUpdateTableStoreParam &param,
    const ObTablet &old_tablet)
{
  int ret = OB_SUCCESS;
  int64_t max_sync_schema_version = 0;
  int64_t input_max_sync_schema_version = 0;
  common::ObArenaAllocator tmp_arena_allocator(common::ObMemAttr(MTL_ID(), "InitTablet"));
  ObTabletMemberWrapper<ObTabletTableStore> old_table_store_wrapper;
  const ObTabletTableStore *old_table_store = nullptr;
  const ObStorageSchema *old_storage_schema = nullptr;
  const ObTabletMdsData &old_mds_data = old_tablet.mds_data_;
  const bool update_in_major_type_merge = param.need_report_ && param.sstable_->is_major_sstable();
  int64_t finish_medium_scn = 0;
  ObITable **ddl_kvs_addr = nullptr;
  int64_t ddl_kv_count = 0;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!param.is_valid())
      || OB_UNLIKELY(!old_tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param), K(old_tablet));
  } else if (OB_UNLIKELY(!pointer_hdl_.is_valid())
      || OB_ISNULL(memtable_mgr_)
      || OB_ISNULL(log_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet pointer handle is invalid", K(ret), K_(pointer_hdl), K_(memtable_mgr), K_(log_handler));
  } else if (param.need_check_transfer_seq_ && OB_FAIL(check_transfer_seq_equal(old_tablet, param.transfer_seq_))) {
    LOG_WARN("failed to check transfer seq eq", K(ret), K(old_tablet), K(param));
  } else if (OB_FAIL(old_tablet.get_max_sync_storage_schema_version(max_sync_schema_version))) {
    LOG_WARN("failed to get max sync storage schema version", K(ret));
  } else if (OB_FAIL(old_tablet.load_storage_schema(tmp_arena_allocator, old_storage_schema))) {
    LOG_WARN("failed to load storage schema", K(ret), K(old_tablet));
  } else if (FALSE_IT(input_max_sync_schema_version = MIN(MAX(param.storage_schema_->schema_version_,
      old_storage_schema->schema_version_), max_sync_schema_version))) {
    // use min schema version to avoid lose storage_schema in replay/reboot
  } else if (OB_FAIL(tablet_meta_.init(old_tablet.tablet_meta_,
      param.snapshot_version_, param.multi_version_start_,
      input_max_sync_schema_version,
      param.clog_checkpoint_scn_, param.ddl_info_))) {
    LOG_WARN("failed to init tablet meta", K(ret), K(old_tablet), K(param),
        K(input_max_sync_schema_version));
  } else if (OB_FAIL(choose_and_save_storage_schema(allocator, *old_storage_schema, *param.storage_schema_))) {
    LOG_WARN("failed to choose and save storage schema", K(ret), K(old_tablet), K(param));
  } else if (OB_FAIL(old_tablet.fetch_table_store(old_table_store_wrapper))) {
    LOG_WARN("failed to fetch old table store", K(ret), K(old_tablet));
  } else if (OB_FAIL(old_table_store_wrapper.get_member(old_table_store))) {
    LOG_WARN("failed to get old table store", K(ret));
  } else if (OB_FAIL(pull_memtables(allocator, ddl_kvs_addr, ddl_kv_count))) {
    LOG_WARN("failed to pull memtable", K(ret));
  } else {
    ddl_kvs_ = ddl_kvs_addr;
    ddl_kv_count_ = ddl_kv_count;
    ALLOC_AND_INIT(allocator, table_store_addr_, (*this), param, (*old_table_store));
  }

  if (FAILEDx(try_update_start_scn())) {
    LOG_WARN("failed to update start scn", K(ret), K(param), K(table_store_addr_));
  } else if (OB_FAIL(try_update_ddl_checkpoint_scn())) {
    LOG_WARN("failed to update clog checkpoint ts", K(ret), K(param), K(table_store_addr_));
  } else if (OB_FAIL(try_update_table_store_flag(param))) {
    LOG_WARN("failed to update table store flag", K(ret), K(param), K(table_store_addr_));
  } else if (OB_FAIL(get_finish_medium_scn(finish_medium_scn))) {
    LOG_WARN("failed to get finish medium scn", K(ret));
  } else if (OB_FAIL(mds_data_.init_for_evict_medium_info(allocator, old_mds_data, finish_medium_scn, param.merge_type_))) {
    LOG_WARN("failed to init mds data", K(ret), K(old_mds_data), K(finish_medium_scn), "merge_type", param.merge_type_);
  } else if (OB_FAIL(build_read_info(allocator))) {
    LOG_WARN("failed to build read info", K(ret));
  } else if (OB_FAIL(check_medium_list())) {
    LOG_WARN("failed to check medium list", K(ret), K(param), K(old_tablet));
  } else if (OB_FAIL(check_sstable_column_checksum())) {
    LOG_WARN("failed to check sstable column checksum", K(ret), KPC(this));
  } else if (FALSE_IT(set_mem_addr())) {
  } else if (OB_FAIL(inner_inc_macro_ref_cnt())) {
    LOG_WARN("failed to increase macro ref cnt", K(ret));
  } else {
    if (old_tablet.get_tablet_meta().has_next_tablet_) {
      set_next_tablet_guard(old_tablet.next_tablet_guard_);
    }
    is_inited_ = true;
    LOG_INFO("succeeded to init tablet for mini/minor/major merge", K(ret), K(param), K(old_tablet), KPC(this));
  }

  if (OB_SUCC(ret) && update_in_major_type_merge) {
    const ObSSTable *major_table = param.sstable_;
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(major_table)) { // init tablet with no major table, skip to init report info
    } else if (OB_TMP_FAIL(ObTabletMeta::init_report_info(major_table,
        old_tablet.tablet_meta_.report_status_.cur_report_version_, tablet_meta_.report_status_))) {
      LOG_WARN("failed to init report info", K(tmp_ret));
    }
  }


  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }

  ObTablet::free_storage_schema(tmp_arena_allocator, old_storage_schema);

  return ret;
}

int ObTablet::init_for_mds_table_dump(
    common::ObArenaAllocator &allocator,
    const ObTablet &old_tablet,
    const share::SCN &flush_scn,
    const ObTabletMdsData &mds_table_data,
    const ObTabletMdsData &base_data)
{
  TIMEGUARD_INIT(STORAGE, 10_ms);
  int ret = OB_SUCCESS;
  allocator_ = &allocator;
  common::ObArenaAllocator tmp_arena_allocator(common::ObMemAttr(MTL_ID(), "InitTabletMDS"));
  ObTabletMemberWrapper<ObTabletTableStore> old_table_store_wrapper;
  const ObTabletTableStore *old_table_store = nullptr;
  const ObStorageSchema *old_storage_schema = nullptr;
  int64_t finish_medium_scn = 0;
  ObITable **ddl_kvs_addr = nullptr;
  int64_t ddl_kv_count = 0;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!pointer_hdl_.is_valid())
      || OB_ISNULL(memtable_mgr_)
      || OB_ISNULL(log_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet pointer handle is invalid", K(ret), K_(pointer_hdl), K_(memtable_mgr), K_(log_handler));
  } else if (CLICK_FAIL(old_tablet.fetch_table_store(old_table_store_wrapper))) {
    LOG_WARN("failed to fetch old table store", K(ret), K(old_tablet));
  } else if (CLICK_FAIL(old_table_store_wrapper.get_member(old_table_store))) {
    LOG_WARN("failed to get old table store", K(ret));
  } else if (CLICK_FAIL(old_tablet.load_storage_schema(tmp_arena_allocator, old_storage_schema))) {
    LOG_WARN("failed to load storage schema", K(ret), K(old_tablet));
  } else if (CLICK_FAIL(tablet_meta_.init(old_tablet.tablet_meta_, flush_scn))) {
    LOG_WARN("failed to init tablet meta", K(ret), K(old_tablet), K(flush_scn));
  } else if (CLICK_FAIL(pull_memtables(allocator, ddl_kvs_addr, ddl_kv_count))) {
    LOG_WARN("fail to pull memtable", K(ret));
  } else if (CLICK_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, table_store_addr_.ptr_))) {
    LOG_WARN("fail to alloc and new table store object", K(ret), K_(table_store_addr));
  } else if (CLICK_FAIL(table_store_addr_.get_ptr()->init(*allocator_, *this, *old_table_store))) {
    LOG_WARN("fail to init table store", K(ret), KPC(old_table_store));
  } else if (CLICK_FAIL(get_finish_medium_scn(finish_medium_scn))) {
    LOG_WARN("failed to get finish medium scn", K(ret));
  } else if (CLICK_FAIL(mds_data_.init_for_mds_table_dump(allocator, mds_table_data, base_data, finish_medium_scn))) {
    LOG_WARN("failed to init mds data", K(ret), K(finish_medium_scn));
  } else {
    ddl_kvs_ = ddl_kvs_addr;
    ddl_kv_count_ = ddl_kv_count;
    ALLOC_AND_INIT(allocator, storage_schema_addr_, *old_storage_schema);
  }

  if (CLICK() && FAILEDx(build_read_info(*allocator_))) {
    LOG_WARN("failed to build read info", K(ret));
  } else if (CLICK_FAIL(check_medium_list())) {
    LOG_WARN("failed to check medium list", K(ret), KPC(this));
  } else if (CLICK_FAIL(check_sstable_column_checksum())) {
    LOG_WARN("failed to check sstable column checksum", K(ret), KPC(this));
  } else if (FALSE_IT(set_mem_addr())) {
  } else if (CLICK_FAIL(inner_inc_macro_ref_cnt())) {
    LOG_WARN("failed to increase macro ref cnt", K(ret));
  } else {
    if (old_tablet.get_tablet_meta().has_next_tablet_) {
      set_next_tablet_guard(old_tablet.next_tablet_guard_);
    }
    is_inited_ = true;
    LOG_INFO("succeeded to init tablet for mds table dump", K(ret), K(old_tablet), K(flush_scn), KPC(this), K(mds_table_data), K(base_data));
  }
  ObTablet::free_storage_schema(tmp_arena_allocator, old_storage_schema);

  return ret;
}

int ObTablet::init_with_migrate_param(
    common::ObArenaAllocator &allocator,
    const ObMigrationTabletParam &param,
    const bool is_update,
    ObFreezer *freezer)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = param.ls_id_;
  const common::ObTabletID &tablet_id = param.tablet_id_;
  allocator_ = &allocator;
  ObITable **ddl_kvs_addr = nullptr;
  int64_t ddl_kv_count = 0;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!param.is_valid())
      || OB_ISNULL(freezer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param), KP(freezer));
  } else if (OB_UNLIKELY(!pointer_hdl_.is_valid())
      || OB_ISNULL(memtable_mgr_)
      || OB_ISNULL(log_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet pointer handle is invalid", K(ret), K_(pointer_hdl), K_(memtable_mgr), K_(log_handler));
  } else if (!is_update && OB_FAIL(init_shared_params(ls_id, tablet_id,
      param.max_sync_storage_schema_version_,
      param.max_serialized_medium_scn_,
      param.compat_mode_,
      freezer))) {
    LOG_WARN("failed to init shared params", K(ret), K(ls_id), K(tablet_id), KP(freezer));
  } else if (OB_FAIL(tablet_meta_.init(param))) {
    LOG_WARN("failed to init tablet meta", K(ret), K(param));
  }

  if (OB_SUCC(ret)) {
    if (param.is_empty_shell()) {
      int64_t pos = 0;
      ObString data = param.mds_data_.tablet_status_committed_kv_.v_.user_data_;
      ObTabletCreateDeleteMdsUserData user_data;
      if (OB_FAIL(user_data.deserialize(data.ptr(), data.length(), pos))) {
        LOG_WARN("fail to deserialize tablet status cache", K(ret));
      } else if (OB_FAIL(mds_data_.init_empty_shell(user_data))) {
        LOG_WARN("failed to init mds data", K(ret), K(user_data));
      } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, table_store_addr_.ptr_))) {
        LOG_WARN("fail to allocate and new rowkey read info", K(ret));
      } else if (OB_FAIL(table_store_addr_.ptr_->init(allocator, *this))) {
        LOG_WARN("fail to init table store", K(ret));
      } else {
        table_store_addr_.addr_.set_none_addr();
        storage_schema_addr_.addr_.set_none_addr();
        is_inited_ = true;
        LOG_INFO("succeeded to init empty shell tablet", K(ret), K(param), KPC(this));
      }
    } else {
      if (OB_FAIL(mds_data_.init_by_full_memory_mds_data(*allocator_, param.mds_data_))) {
        LOG_WARN("failed to assign mds data", K(ret), K(param));
      } else if (OB_FAIL(pull_memtables(allocator, ddl_kvs_addr, ddl_kv_count))) {
        LOG_WARN("fail to pull memtable", K(ret));
      } else {
        ddl_kvs_ = ddl_kvs_addr;
        ddl_kv_count_ = ddl_kv_count;
        ALLOC_AND_INIT(allocator, table_store_addr_, (*this), nullptr/*ObTableHandleV2*/);
        ALLOC_AND_INIT(allocator, storage_schema_addr_, param.storage_schema_);
      }

      if (FAILEDx(build_read_info(allocator))) {
        LOG_WARN("fail to build read info", K(ret));
      } else if (OB_FAIL(check_medium_list())) {
        LOG_WARN("failed to check medium list", K(ret), K(param));
      } else if (OB_FAIL(check_sstable_column_checksum())) {
        LOG_WARN("failed to check sstable column checksum", K(ret), KPC(this));
      } else if (FALSE_IT(set_mem_addr())) {
      } else if (OB_FAIL(inner_inc_macro_ref_cnt())) {
        LOG_WARN("failed to increase macro ref cnt", K(ret));
      } else {
        is_inited_ = true;
        LOG_INFO("succeeded to init tablet with migration tablet param", K(ret), K(param), KPC(this));
      }
    }
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }

  return ret;
}

int ObTablet::init_for_defragment(
    common::ObArenaAllocator &allocator,
    const ObIArray<ObITable *> &tables,
    const ObTablet &old_tablet)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_arena_allocator(common::ObMemAttr(MTL_ID(), "InitTablet"));
  ObTabletMemberWrapper<ObTabletTableStore> old_table_store_wrapper;
  const ObTabletTableStore *old_table_store = nullptr;
  const ObStorageSchema *old_storage_schema = nullptr;
  const ObTabletMdsData &old_mds_data = old_tablet.mds_data_;
  allocator_ = &allocator;
  ObITable **ddl_kvs_addr = nullptr;
  int64_t ddl_kv_count = 0;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet has been inited", K(ret));
  } else if (OB_UNLIKELY(!old_tablet.is_valid() || 0 == tables.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("old tablet is invalid", K(ret), K(old_tablet));
  } else if (OB_UNLIKELY(!pointer_hdl_.is_valid())
      || OB_ISNULL(memtable_mgr_)
      || OB_ISNULL(log_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet pointer handle is invalid", K(ret), K_(pointer_hdl), K_(memtable_mgr), K_(log_handler));
  } else if (OB_FAIL(old_tablet.load_storage_schema(tmp_arena_allocator, old_storage_schema))) {
    LOG_WARN("failed to load storage schema", K(ret), K(old_tablet));
  } else if (OB_FAIL(old_tablet.fetch_table_store(old_table_store_wrapper))) {
    LOG_WARN("failed to fetch old table store", K(ret), K(old_tablet));
  } else if (OB_FAIL(old_table_store_wrapper.get_member(old_table_store))) {
    LOG_WARN("failed to get old table store", K(ret));
  } else if (OB_FAIL(tablet_meta_.init(old_tablet.tablet_meta_,
      old_tablet.get_snapshot_version(),
      old_tablet.get_multi_version_start(),
      old_tablet.tablet_meta_.max_sync_storage_schema_version_))) {
    LOG_WARN("fail to init tablet_meta", K(ret), K(old_tablet.tablet_meta_));
  } else if (OB_FAIL(pull_memtables(allocator, ddl_kvs_addr, ddl_kv_count))) {
    LOG_WARN("fail to pull memtable", K(ret));
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, table_store_addr_.ptr_))) {
    LOG_WARN("fail to alloc and new table store object", K(ret), K_(table_store_addr));
  } else if (OB_FAIL(table_store_addr_.get_ptr()->init(*allocator_, *this, tables, *old_table_store))) {
    LOG_WARN("fail to init table store", K(ret), K(old_tablet), K(tables));
  } else {
    ddl_kvs_ = ddl_kvs_addr;
    ddl_kv_count_ = ddl_kv_count;
    ALLOC_AND_INIT(allocator, storage_schema_addr_, *old_storage_schema);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(mds_data_.init_for_evict_medium_info(allocator, old_mds_data, 0/*finish_medium_scn*/))) {
    LOG_WARN("failed to init mds data", K(ret), K(old_mds_data));
  } else if (OB_FAIL(build_read_info(*allocator_))) {
    LOG_WARN("fail to build read info", K(ret));
  } else if (OB_FAIL(check_medium_list())) {
    LOG_WARN("failed to check medium list", K(ret), KPC(this));
  } else if (OB_FAIL(check_sstable_column_checksum())) {
    LOG_WARN("failed to check sstable column checksum", K(ret), KPC(this));
  } else if (FALSE_IT(set_mem_addr())) {
  } else if (OB_FAIL(inner_inc_macro_ref_cnt())) {
    LOG_WARN("failed to increase macro ref cnt", K(ret));
  } else {
    if (old_tablet.get_tablet_meta().has_next_tablet_) {
      set_next_tablet_guard(old_tablet.next_tablet_guard_);
    }
    is_inited_ = true;
    LOG_INFO("succeeded to init tablet for sstable defragmentation", K(ret), K(old_tablet), KPC(this));
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }

  ObTablet::free_storage_schema(tmp_arena_allocator, old_storage_schema);

  return ret;
}

int ObTablet::handle_transfer_replace_(const ObBatchUpdateTableStoreParam &param)
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;
  if (!param.is_valid() || !param.is_transfer_replace_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else if (OB_FAIL(tablet_meta_.reset_transfer_table())) {
    LOG_WARN("failed to set finish tansfer replace", K(ret), K(tablet_meta_), K(param));
  } else if (OB_FAIL(tablet_meta_.ha_status_.set_restore_status(param.restore_status_))) {
    LOG_WARN("failed to set tablet restore status", K(ret), "restore_status", param.restore_status_);
  } else if (OB_FAIL(fetch_table_store(wrapper))) {
    LOG_WARN("failed to fetch table store", K(ret), "tablet_id", tablet_meta_.tablet_id_);
  } else if (tablet_meta_.ha_status_.is_restore_status_full()
      && wrapper.get_member()->get_major_sstables().empty()) {
    // In case of restore, if restore status is FULL, major sstable must be exist after replace.
    ret = OB_INVALID_TABLE_STORE;
    LOG_WARN("tablet should be exist major sstable", K(ret), "tablet_id", tablet_meta_.tablet_id_);
  } else if (tablet_meta_.has_transfer_table()) {
    ret = OB_TRANSFER_SYS_ERROR;
    LOG_WARN("transfer table should not exist", K(ret), K_(tablet_meta));
  }
  return ret;
}

int ObTablet::init_for_sstable_replace(
    common::ObArenaAllocator &allocator,
    const ObBatchUpdateTableStoreParam &param,
    const ObTablet &old_tablet)
{
  int ret = OB_SUCCESS;
  allocator_ = &allocator;
  common::ObArenaAllocator tmp_arena_allocator(common::ObMemAttr(MTL_ID(), "InitTablet"));
  ObTabletMemberWrapper<ObTabletTableStore> old_table_store_wrapper;
  const ObTabletTableStore *old_table_store = nullptr;
  const ObStorageSchema *old_storage_schema = nullptr;
  const ObStorageSchema *storage_schema = nullptr;
  int64_t finish_medium_scn = 0;
  ObITable **ddl_kvs_addr = nullptr;
  int64_t ddl_kv_count = 0;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!param.is_valid()) || OB_UNLIKELY(!old_tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param), K(old_tablet));
  } else if (OB_UNLIKELY(!pointer_hdl_.is_valid())
      || OB_ISNULL(memtable_mgr_)
      || OB_ISNULL(log_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet pointer handle is invalid", K(ret), K_(pointer_hdl), K_(memtable_mgr), K_(log_handler));
  } else if (OB_FAIL(old_tablet.load_storage_schema(tmp_arena_allocator, old_storage_schema))) {
    LOG_WARN("failed to load storage schema", K(ret), K(old_tablet));
  } else if (OB_FAIL(old_tablet.fetch_table_store(old_table_store_wrapper))) {
    LOG_WARN("failed to fetch old table store", K(ret), K(old_tablet));
  } else if (OB_FAIL(old_table_store_wrapper.get_member(old_table_store))) {
    LOG_WARN("failed to get old table store", K(ret));
  } else if (FALSE_IT(storage_schema = OB_ISNULL(param.tablet_meta_) ? old_storage_schema : &param.tablet_meta_->storage_schema_)) {
  } else if (OB_FAIL(tablet_meta_.init(old_tablet.tablet_meta_, param.tablet_meta_
      // this interface for migration to batch update table store
      // use max schema to make sure sstable and schema match
      ))) {
    LOG_WARN("failed to init tablet meta", K(ret), K(old_tablet), K(param));
  } else if (OB_FAIL(pull_memtables(allocator, ddl_kvs_addr, ddl_kv_count))){
    LOG_WARN("fail to pull memtable", K(ret));
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, table_store_addr_.ptr_))) {
    LOG_WARN("fail to alloc and new table store object", K(ret), K_(table_store_addr));
  } else if (OB_FAIL(table_store_addr_.ptr_->build_ha_new_table_store(allocator, *this, param, *old_table_store))) {
    LOG_WARN("failed to init table store", K(ret), K(old_tablet));
  } else if (OB_FAIL(choose_and_save_storage_schema(*allocator_, *old_storage_schema, *storage_schema))) {
    LOG_WARN("failed to choose and save storage schema", K(ret), K(old_tablet), K(param));
  } else if (OB_FAIL(try_update_start_scn())) {
    LOG_WARN("failed to update start scn", K(ret), K(param), K(table_store_addr_));
  } else if (OB_FAIL(get_finish_medium_scn(finish_medium_scn))) {
    LOG_WARN("failed to get finish medium scn", K(ret));
  } else if (nullptr != param.tablet_meta_
      && OB_FAIL(mds_data_.init_for_merge_with_full_mds_data(allocator, old_tablet.mds_data_, param.tablet_meta_->mds_data_.medium_info_list_, finish_medium_scn))) {
    LOG_WARN("failed to init mds data", K(ret), "mds_data", param.tablet_meta_->mds_data_, K(finish_medium_scn));
  } else if (nullptr == param.tablet_meta_
      && OB_FAIL(mds_data_.init_for_evict_medium_info(allocator, old_tablet.mds_data_, finish_medium_scn))) {
    LOG_WARN("failed to init mds data", K(ret), "mds_data", old_tablet.mds_data_, K(finish_medium_scn));
  } else if (OB_FAIL(build_read_info(*allocator_))) {
    LOG_WARN("failed to build read info", K(ret));
  } else if (OB_FAIL(check_medium_list())) {
    LOG_WARN("failed to check medium list", K(ret), K(param), K(old_tablet));
  } else if (OB_FAIL(check_sstable_column_checksum())) {
    LOG_WARN("failed to check sstable column checksum", K(ret), KPC(this));
  } else if (param.is_transfer_replace_ && OB_FAIL(handle_transfer_replace_(param))) {
    LOG_WARN("failed to handle transfer replace", K(ret), K(param));
  } else if (FALSE_IT(set_mem_addr())) {
  } else if (OB_FAIL(inner_inc_macro_ref_cnt())) {
    LOG_WARN("failed to increase macro ref cnt", K(ret));
  } else {
    ddl_kvs_ = ddl_kvs_addr;
    ddl_kv_count_ = ddl_kv_count;
    if (old_tablet.get_tablet_meta().has_next_tablet_) {
      set_next_tablet_guard(old_tablet.next_tablet_guard_);
    }
    is_inited_ = true;
    LOG_INFO("succeeded to init tablet for ha build new table store", K(ret), K(param), K(old_tablet), KPC(this));
  }

  if (OB_SUCC(ret)) {
    DEBUG_SYNC(HA_REPORT_META_TABLE);
    const ObSSTable *last_major = static_cast<const ObSSTable *>(table_store_addr_.get_ptr()->get_major_sstables().get_boundary_table(true/*last*/));
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(last_major)) { // init tablet with no major table, skip to init report info
    } else if (OB_TMP_FAIL(ObTabletMeta::init_report_info(last_major,
      old_tablet.tablet_meta_.report_status_.cur_report_version_, tablet_meta_.report_status_))) {
      LOG_WARN("failed to init report info", K(tmp_ret));
    }
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }

  ObTablet::free_storage_schema(tmp_arena_allocator, old_storage_schema);

#ifdef ERRSIM
  ObErrsimBackfillPointType point_type(ObErrsimBackfillPointType::TYPE::ERRSIM_REPLACE_SWAP_BEFORE);
  if (param.errsim_point_info_.is_errsim_point(point_type)) {
    ret = OB_EAGAIN;
    LOG_WARN("[ERRSIM TRANSFER] errsim transfer swap tablet before", K(ret), K(param));
  }
#endif
  return ret;
}

int ObTablet::fetch_table_store(ObTabletMemberWrapper<ObTabletTableStore> &wrapper) const
{
  TIMEGUARD_INIT(STORAGE, 10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_store_addr_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table store addr", K(ret), K_(table_store_addr));
  } else if (table_store_addr_.is_memory_object()
             || table_store_addr_.is_none_object()) {
    if (OB_ISNULL(table_store_addr_.get_ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table store addr ptr is null", K(ret), K_(table_store_addr));
    } else {
      wrapper.set_member(table_store_addr_.get_ptr());
    }
  } else {
    ObStorageMetaHandle handle;
    ObStorageMetaKey meta_key(MTL_ID(), table_store_addr_.addr_);
    if (CLICK_FAIL(OB_STORE_CACHE.get_storage_meta_cache().get_meta(
                   ObStorageMetaValue::MetaType::TABLE_STORE, meta_key, handle, this))) {
      LOG_WARN("get meta failed", K(ret), K(meta_key));
    } else if (CLICK_FAIL(wrapper.set_cache_handle(handle))) {
      LOG_WARN("wrapper set cache handle failed", K(ret), K(meta_key), K_(table_store_addr));
    }
  }
  return ret;
}

int ObTablet::fetch_autoinc_seq(ObTabletMemberWrapper<ObTabletAutoincSeq> &wrapper) const
{
  int ret = OB_SUCCESS;
  const ObTabletComplexAddr<ObTabletAutoincSeq> &auto_inc_seq_addr = mds_data_.auto_inc_seq_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!auto_inc_seq_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid auto inc seq addr", K(ret));
  } else if (auto_inc_seq_addr.is_none_object()) {
    wrapper.set_member(nullptr);  // nullptr for none object
  } else if (auto_inc_seq_addr.is_memory_object()) {
    if (OB_ISNULL(auto_inc_seq_addr.get_ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("auto inc seq addr ptr is null", K(ret), K_(mds_data_.auto_inc_seq));
    } else {
      wrapper.set_member(auto_inc_seq_addr.get_ptr());
    }
  } else {
    ObStorageMetaHandle handle;
    ObStorageMetaKey meta_key(MTL_ID(), auto_inc_seq_addr.addr_);
    if (OB_FAIL(OB_STORE_CACHE.get_storage_meta_cache().get_meta(
                ObStorageMetaValue::MetaType::AUTO_INC_SEQ, meta_key, handle, this))) {
      LOG_WARN("get meta failed", K(ret), K(meta_key));
    } else if (OB_FAIL(wrapper.set_cache_handle(handle))) {
      LOG_WARN("wrapper set cache handle failed", K(ret), K(meta_key), K(auto_inc_seq_addr));
    }
  }
  return ret;
}

int ObTablet::load_storage_schema(
    common::ObArenaAllocator &allocator,
    const ObStorageSchema *&storage_schema) const
{
  int ret = OB_SUCCESS;
  ObStorageSchema *schema = nullptr;
  if (OB_UNLIKELY(!storage_schema_addr_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid storage schema address", K(ret), K(storage_schema_addr_));
  } else if (storage_schema_addr_.is_memory_object()) {
    if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, schema))) {
      LOG_WARN("alloc and new failed", K(ret));
    } else if (OB_FAIL(schema->init(allocator, *storage_schema_addr_.ptr_))) {
      LOG_WARN("failed to copy storage schema", K(ret));
    }
  } else {
    IO_AND_DESERIALIZE(allocator, storage_schema_addr_.addr_, schema);
  }

  if (OB_FAIL(ret)) {
    ObTablet::free_storage_schema(allocator, schema);
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to load storage schema", K(ret), K_(storage_schema_addr));
  } else if (OB_UNLIKELY(!schema->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid schema", K(ret), K_(storage_schema_addr), KPC(schema));

    ObTablet::free_storage_schema(allocator, schema);
  } else {
    storage_schema = schema;
  }
  return ret;
}

void ObTablet::free_storage_schema(common::ObIAllocator &allocator, const ObStorageSchema *storage_schema)
{
  if (OB_NOT_NULL(storage_schema)) {
    storage_schema->~ObStorageSchema();
    allocator.free(const_cast<ObStorageSchema*>(storage_schema));
  }
}

int ObTablet::read_medium_info_list(
    common::ObArenaAllocator &allocator,
    const compaction::ObMediumCompactionInfoList *&medium_info_list) const
{
  int ret = OB_SUCCESS;
  const ObTabletComplexAddr<ObTabletDumpedMediumInfo> &complex_addr = mds_data_.medium_info_list_;
  int64_t finish_medium_scn = 0;
  ObTabletDumpedMediumInfo mds_table_medium_info_list;
  const ObTabletDumpedMediumInfo *base_medium_info_list = nullptr;
  ObTabletDumpedMediumInfo fused_medium_info_list;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(fused_medium_info_list.init_for_first_creation(allocator))) {
    LOG_WARN("failed to init medium info list", K(ret));
  } else if (OB_FAIL(get_finish_medium_scn(finish_medium_scn))) {
    LOG_WARN("failed to get finish medium scn", K(ret));
  } else if (OB_FAIL(read_mds_table_medium_info_list(allocator, mds_table_medium_info_list))) {
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to read mds table medium info list", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTabletMdsData::load_medium_info_list(allocator, complex_addr, base_medium_info_list))) {
    LOG_WARN("failed to load medium info list", K(ret));
  } else if (nullptr != base_medium_info_list
      && OB_FAIL(ObTabletMdsData::copy_medium_info_list(finish_medium_scn, *base_medium_info_list, fused_medium_info_list))) {
    LOG_WARN("failed to copy base medium info list", K(ret));
  } else if (OB_FAIL(ObTabletMdsData::copy_medium_info_list(finish_medium_scn, mds_table_medium_info_list, fused_medium_info_list))) {
    LOG_WARN("failed to copy mds table medium info list", K(ret));
  } else {
    compaction::ObMediumCompactionInfoList *tmp_list = nullptr;
    if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, tmp_list))) {
      LOG_WARN("failed to alloc and new", K(ret));
    } else if (OB_FAIL(tmp_list->init(allocator, mds_data_.extra_medium_info_, &fused_medium_info_list))) {
      LOG_WARN("failed to init", K(ret));
    } else {
      medium_info_list = tmp_list;
    }

    if (OB_FAIL(ret)) {
      if (nullptr != tmp_list) {
        tmp_list->compaction::ObMediumCompactionInfoList::~ObMediumCompactionInfoList();
        allocator.free(tmp_list);
      }
    }
  }

  ObTabletMdsData::free_medium_info_list(allocator, base_medium_info_list);

  return ret;
}

int ObTablet::init_with_update_medium_info(
    common::ObArenaAllocator &allocator,
    const ObTablet &old_tablet)
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObTabletMemberWrapper<ObTabletAutoincSeq> auto_inc_seqwrapper;
  const ObTabletMeta &old_tablet_meta = old_tablet.tablet_meta_;
  const ObTabletTableStore *old_table_store = nullptr;
  const ObStorageSchema *old_storage_schema = nullptr;
  const ObTabletMdsData &old_mds_data = old_tablet.mds_data_;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!old_tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(old_tablet));
  } else if (OB_UNLIKELY(!pointer_hdl_.is_valid())
      || OB_ISNULL(memtable_mgr_)
      || OB_ISNULL(log_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet pointer handle is invalid", K(ret), K_(pointer_hdl), K_(pointer_hdl), K_(memtable_mgr), K_(log_handler));
  } else if (OB_FAIL(assign_memtables(old_tablet.memtables_, old_tablet.memtable_count_))) {
    LOG_WARN("fail to assign memtables", K(ret));
  } else if (OB_ISNULL(ddl_kvs_ = static_cast<ObITable**>(allocator.alloc(sizeof(ObITable*) * DDL_KV_ARRAY_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ddl_kvs_", K(ret), KP(ddl_kvs_));
  } else if (OB_FAIL(assign_ddl_kvs(old_tablet.ddl_kvs_, old_tablet.ddl_kv_count_))) {
    LOG_WARN("fail to assign ddl kvs", K(ret), KP(old_tablet.ddl_kvs_), K(old_tablet.ddl_kv_count_));
  } else if (OB_FAIL(old_tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret), K(old_tablet));
  } else if (OB_FAIL(table_store_wrapper.get_member(old_table_store))) {
    LOG_WARN("fail to get table store", K(ret), K(table_store_wrapper));
  } else if (OB_FAIL(old_tablet.load_storage_schema(allocator, old_storage_schema))) {
    LOG_WARN("fail to load storage schema", K(ret));
  } else if (OB_FAIL(tablet_meta_.init(allocator, old_tablet_meta))) {
    LOG_WARN("failed to init tablet meta", K(ret), K(old_tablet_meta));
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, table_store_addr_.ptr_))) {
    LOG_WARN("fail to allocate and new table store", K(ret));
  } else if (OB_FAIL(table_store_addr_.ptr_->init(allocator, *this, *old_table_store))) {
    LOG_WARN("fail to copy table store", K(ret), KPC(old_table_store));
  } else if (OB_FAIL(try_update_start_scn())) {
    LOG_WARN("failed to update start scn", K(ret), KPC(old_table_store));
  } else if (OB_FAIL(mds_data_.init_with_update_medium_info(allocator, old_mds_data))) {
    LOG_WARN("failed to init mds data", K(ret), K(old_mds_data));
  } else if (FALSE_IT(set_mem_addr())) {
  } else if (OB_FAIL(inner_inc_macro_ref_cnt())) {
    LOG_WARN("failed to increase macro ref cnt", K(ret));
  } else {
    ALLOC_AND_INIT(allocator, storage_schema_addr_, *old_storage_schema);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(build_read_info(allocator))) {
      LOG_WARN("failed to build read info", K(ret));
    } else {
      if (old_tablet.get_tablet_meta().has_next_tablet_) {
        set_next_tablet_guard(old_tablet.next_tablet_guard_);
      }
      LOG_INFO("succeeded to init tablet with update medium info", K(ret), K(this), K(old_tablet));
      is_inited_ = true;
    }
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }

  ObTablet::free_storage_schema(allocator, old_storage_schema);

  return ret;
}

int ObTablet::init_empty_shell(
  ObArenaAllocator &allocator,
  const ObTablet &old_tablet)
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData user_data;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(old_tablet.get_tablet_meta().has_next_tablet_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old tablet should not have next tablet", K(ret), K(old_tablet.get_tablet_meta()));
  } else if (OB_FAIL(old_tablet.ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data))) {
    LOG_WARN("old tablet get ObTabletCreateDeleteMdsUserData failed", K(ret), K(old_tablet));
  } else if (user_data.tablet_status_ != ObTabletStatus::DELETED &&
             user_data.tablet_status_ != ObTabletStatus::TRANSFER_OUT_DELETED) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("old tablet status is not deleted", K(ret), K(user_data.tablet_status_));
  } else if (OB_FAIL(tablet_meta_.assign(old_tablet.tablet_meta_))) {
    LOG_WARN("assign old tablet meta to empty shell failed", K(ret), K(old_tablet.tablet_meta_));
  } else if (OB_FAIL(mds_data_.init_empty_shell(user_data))) {
    LOG_WARN("failed to init mds data", K(ret), K(user_data));
  } else if (OB_FAIL(wait_release_memtables_())) {
    LOG_ERROR("fail to release memtables", K(ret), K(old_tablet));
  } else if (OB_FAIL(mark_mds_table_switched_to_empty_shell_())) {// to avoid calculate it's rec_scn
    LOG_WARN("fail to mark mds table switched to empty shell", K(ret), K(old_tablet));
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, table_store_addr_.ptr_))) {
    LOG_WARN("fail to allocate and new rowkey read info", K(ret));
  } else if (OB_FAIL(table_store_addr_.ptr_->init(allocator, *this))) {
    LOG_WARN("fail to init table store", K(ret));
  } else {
    table_store_addr_.addr_.set_none_addr();
    storage_schema_addr_.addr_.set_none_addr();
    tablet_meta_.clog_checkpoint_scn_ = user_data.delete_commit_scn_ > tablet_meta_.clog_checkpoint_scn_ ?
                                        user_data.delete_commit_scn_ : tablet_meta_.clog_checkpoint_scn_;
    tablet_meta_.mds_checkpoint_scn_ = user_data.delete_commit_scn_;
    is_inited_ = true;
    LOG_INFO("init empty shell", K(ret), K(old_tablet), KPC(this));
  }


  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }

  return ret;
}

int ObTablet::check_sstable_column_checksum() const
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "CKColCKS"));
  const ObStorageSchema *storage_schema = nullptr;
  ObTableStoreIterator iter;
  int64_t schema_col_cnt = 0;
  int64_t sstable_col_cnt = 0;
  if (OB_UNLIKELY(!table_store_addr_.is_valid() || !storage_schema_addr_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to check tablet ", K(ret), K(table_store_addr_), K(storage_schema_addr_));
  } else if (OB_FAIL(load_storage_schema(allocator, storage_schema))) {
    LOG_WARN("fail to load storage schema", K(ret));
  } else if (OB_FAIL(storage_schema->get_stored_column_count_in_sstable(schema_col_cnt))) {
    LOG_WARN("failed to get stored column count of storage schema", K(ret), KPC(this));
  } else if (OB_FAIL(inner_get_all_sstables(iter))) {
    LOG_WARN("fail to get all sstables", K(ret));
  } else {
    ObITable *table = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next(table))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next table", K(ret), KPC(this));
        }
      } else if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, table is nullptr", K(ret), KPC(table));
      } else {
        ObSSTable *cur = reinterpret_cast<ObSSTable *>(table);
        ObSSTableMetaHandle meta_handle;
        if (OB_FAIL(cur->get_meta(meta_handle))) {
          LOG_WARN("fail to get sstable meta", K(ret), KPC(cur), KPC(this));
        } else if (cur->is_major_sstable() && meta_handle.get_sstable_meta().is_empty()) {
          // since empty major sstable may have wrong column count, skip for compatibility from 4.0 to 4.1
        } else if ((sstable_col_cnt = meta_handle.get_sstable_meta().get_col_checksum_cnt()) > schema_col_cnt) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("The storage schema is older than the sstable, and cann’t explain the data.",
              K(ret), K(sstable_col_cnt), K(schema_col_cnt), KPC(cur), KPC(storage_schema));
        }
      }
    }
  }
  ObTablet::free_storage_schema(allocator, storage_schema);
  return ret;
}

int ObTablet::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  const int64_t length = get_self_size();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_UNLIKELY(!is_valid() && !is_empty_shell())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet is invalid", K(ret), K(*this));
  } else if (TABLET_VERSION_V2 != version_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid version", K(ret), K_(version));
  } else if (OB_UNLIKELY(length > len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer's length is not enough", K(ret), K(length), K(len - new_pos));
  } else if (OB_FAIL(serialization::encode_i32(buf, len, new_pos, version_))) {
    LOG_WARN("failed to serialize tablet meta's version", K(ret), K(len), K(new_pos), K_(version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i32(buf, len, new_pos, length))) {
    LOG_WARN("failed to serialize tablet meta's length", K(ret), K(len), K(new_pos), K(length));
  } else if (new_pos - pos < length && OB_FAIL(tablet_meta_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize tablet meta", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(table_store_addr_.addr_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize table store addr", K(ret), K(len), K(new_pos), K(table_store_addr_));
  } else if (new_pos - pos < length && OB_FAIL(storage_schema_addr_.addr_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize storage schema addr", K(ret), K(len), K(new_pos), K(table_store_addr_));
  } else if (!is_empty_shell() && new_pos - pos < length && OB_FAIL(rowkey_read_info_->serialize(buf, len, new_pos))) {
    LOG_WARN("fail to serialize rowkey read info", K(ret), KPC(rowkey_read_info_));
  } else if (new_pos - pos < length && OB_FAIL(mds_data_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize mds data", K(ret), K(len), K(new_pos));
  } else if (OB_UNLIKELY(length != new_pos - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet's length doesn't match standard length", K(ret), K(new_pos), K(pos), K(length), KPC(this));
  } else if (tablet_meta_.has_next_tablet_ && OB_FAIL(next_tablet_guard_.get_obj()->serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize next tablet", K(ret), K(len), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

int ObTablet::rollback_ref_cnt(
    common::ObArenaAllocator &allocator,
    const char *buf,
    const int64_t len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot deserialize inited tablet meta", K(ret), K_(is_inited));
  } else if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)
      || OB_UNLIKELY(len <= pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, len, new_pos, (int32_t *)&version_))) {
    LOG_WARN("failed to deserialize tablet meta's version", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, len, new_pos, (int32_t *)&length_))) {
    LOG_WARN("failed to deserialize tablet meta's length", K(ret), K(len), K(new_pos));
  } else if (OB_UNLIKELY(length_ > len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer's length is not enough", K(ret), K(length_), K(len - new_pos));
  } else {
    do {
      if (OB_FAIL(load_deserialize_v2(allocator, buf, len, pos, new_pos, false))) {
        LOG_WARN("fail to load deserialize tablet v2", K(ret), K(length_), K(len - new_pos), KPC(this));
      }
    } while (ignore_ret(ret));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_UNLIKELY(length_ != new_pos - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet's length doesn't match standard length", K(ret), K(new_pos), K(pos), K_(length));
  } else if (tablet_meta_.has_next_tablet_) {
    ObTablet next_tablet;
    next_tablet.set_tablet_addr(tablet_addr_);
    if (OB_FAIL(next_tablet.rollback_ref_cnt(allocator, buf, len, new_pos))) {
      LOG_WARN("fail to deserialize next tablet for rollback", K(ret), KP(buf), K(len), K(new_pos));
    }
  }

  if (OB_SUCC(ret)) {
    hold_ref_cnt_ = true;
    dec_macro_ref_cnt();
    pos = new_pos;
  }
  return ret;
}

int ObTablet::deserialize(
    common::ObArenaAllocator &allocator,
    const char *buf,
    const int64_t len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(load_deserialize(allocator, buf, len, pos))) {
    LOG_WARN("fail to load deserialize tablet", K(ret), KP(buf), K(len), K(pos));
  } else if (OB_FAIL(deserialize_post_work(allocator))) {
    LOG_WARN("fail to deserialzie post work", K(ret));
  }
  return ret;
}

int ObTablet::load_deserialize(
    common::ObArenaAllocator &allocator,
    const char *buf,
    const int64_t len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot deserialize inited tablet meta", K(ret), K_(is_inited));
  } else if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)
      || OB_UNLIKELY(len <= pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, len, new_pos, (int32_t *)&version_))) {
    LOG_WARN("failed to deserialize tablet meta's version", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, len, new_pos, (int32_t *)&length_))) {
    LOG_WARN("failed to deserialize tablet meta's length", K(ret), K(len), K(new_pos));
  } else if (OB_UNLIKELY(length_ > len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer's length is not enough", K(ret), K(length_), K(len - new_pos));
  } else if (TABLET_VERSION_V2 == version_ && OB_FAIL(load_deserialize_v2(allocator, buf, len, pos, new_pos))) {
    LOG_WARN("failed to load deserialize v2", K(ret), K(length_), K(len - new_pos), KPC(this));
  } else if (TABLET_VERSION == version_ && OB_FAIL(load_deserialize_v1(allocator, buf, len, pos, new_pos))) {
    LOG_WARN("failed to load deserialize v1", K(ret), K(length_), K(len - new_pos), KPC(this));
  } else if (OB_UNLIKELY(length_ != new_pos - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet's length doesn't match standard length", K(ret), K(new_pos), K(pos), K_(length));
  } else if (tablet_meta_.has_next_tablet_) {
    const ObTabletMapKey key(tablet_meta_.ls_id_, tablet_meta_.tablet_id_);
    if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, next_tablet_guard_))) {
      LOG_WARN("failed to acquire tablet", K(ret), K(key));
    } else if (OB_ISNULL(next_tablet_guard_.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("next tablet is null", K(ret));
    } else if (FALSE_IT(next_tablet_guard_.get_obj()->tablet_addr_ = tablet_addr_)) {
    } else if (OB_FAIL(next_tablet_guard_.get_obj()->load_deserialize(allocator, buf, len, new_pos))) {
      LOG_WARN("failed to deserialize next tablet", K(ret), K(len), K(new_pos));
    }
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  } else if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObTablet::deserialize_post_work(common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot deserialize inited tablet meta", K(ret), K_(is_inited));
  } else if (TABLET_VERSION_V2 == version_) {
    if (!table_store_addr_.addr_.is_none()) {
      IO_AND_DESERIALIZE(allocator, table_store_addr_.addr_, table_store_addr_.ptr_, *this);
      if (FAILEDx(table_store_addr_.ptr_->batch_cache_sstable_meta(allocator, INT64_MAX))) {// cache all
        LOG_WARN("fail to cache all of sstable", K(ret), KPC(table_store_addr_.ptr_));
      }
    } else {
      ALLOC_AND_INIT(allocator, table_store_addr_, (*this));
    }
  }
  if (FAILEDx(inner_inc_macro_ref_cnt())) {
    LOG_WARN("failed to increase macro ref cnt", K(ret));
  } else {
    ObArenaAllocator arena_allocator(common::ObMemAttr(MTL_ID(), "TmpSchema"));
    const ObStorageSchema *schema = nullptr;
    if (!is_empty_shell()) {
      if (OB_FAIL(load_storage_schema(arena_allocator, schema))) {
        LOG_WARN("load storage schema failed", K(ret));
      } else if (tablet_meta_.max_sync_storage_schema_version_ > schema->schema_version_) {
        LOG_INFO("tablet meta status is not right, upgrade may happened. fix max_sync_schema_version on purpose",
            K(tablet_meta_.max_sync_storage_schema_version_),
            K(schema->schema_version_));
        tablet_meta_.max_sync_storage_schema_version_ = schema->schema_version_;
      }
      ObTablet::free_storage_schema(arena_allocator, schema);
    }
    if (OB_SUCC(ret) && tablet_meta_.has_next_tablet_) {
      if (next_tablet_guard_.get_obj()->deserialize_post_work(allocator)) {
        LOG_WARN("fail to deserialize post work for next tablet", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      version_ = TABLET_VERSION_V2;
      is_inited_ = true;
      LOG_INFO("succeed to load deserialize tablet", K(ret), KPC(this));
    }
  }
  if (OB_FAIL(ret) && OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObTablet::load_deserialize_v1(
    common::ObArenaAllocator &allocator,
    const char *buf,
    const int64_t len,
    const int64_t pos,
    int64_t &new_pos)
{
  int ret = OB_SUCCESS;
  ObTabletAutoincSeq auto_inc_seq;
  ObTabletTxMultiSourceDataUnit tx_data;
  ObTabletBindingInfo ddl_data;
  ObMediumCompactionInfoList info_list;
  ObITable **ddl_kvs_addr = nullptr;
  int64_t ddl_kv_count = 0;

  if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, table_store_addr_.ptr_))) {
    LOG_WARN("fail to allocate and new table store", K(ret));
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, storage_schema_addr_.ptr_))) {
    LOG_WARN("fail to allocate and new storage schema", K(ret));
  } else if (new_pos - pos < length_ && OB_FAIL(deserialize_meta_v1(allocator, buf, len, new_pos,
      auto_inc_seq, tx_data, ddl_data))) {
    LOG_WARN("failed to deserialize tablet meta", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(table_store_addr_.ptr_->deserialize(allocator, *this, buf, len, new_pos))) {
    LOG_WARN("failed to deserialize table store", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(storage_schema_addr_.ptr_->deserialize(allocator, buf, len, new_pos))) {
    LOG_WARN("failed to deserialize storage schema", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(info_list.deserialize(allocator, buf, len, new_pos))) {
    LOG_WARN("failed to deserialize medium compaction info list", K(ret), K(len), K(new_pos));
  } else if (OB_UNLIKELY(length_ != new_pos - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet's length doesn't match standard length", K(ret), K(new_pos), K(pos), K_(length));
  }

  // old mds data convert to new mds data
  if (FAILEDx(ObTabletMdsData::build_mds_data(allocator, auto_inc_seq, tx_data, tablet_meta_.create_scn_,
                                              ddl_data, tablet_meta_.clog_checkpoint_scn_, info_list, mds_data_))) {
    LOG_WARN("failed to build mds data", K(ret));
  } else if (OB_FAIL(tablet_meta_.transfer_info_.init())) {
    LOG_WARN("failed to init transfer info", K(ret), K(tablet_meta_));
  } else {
    tablet_meta_.mds_checkpoint_scn_ = tablet_meta_.clog_checkpoint_scn_;
    LOG_INFO("succeeded to build mds data", K(ret), "ls_id", tablet_meta_.ls_id_, "tablet_id", tablet_meta_.tablet_id_, K(mds_data_));
  }

  if (FAILEDx(build_read_info(allocator))) {
    LOG_WARN("failed to build read info", K(ret));
  } else if (OB_FAIL(pull_memtables(allocator, ddl_kvs_addr, ddl_kv_count))) {
    LOG_WARN("fail to pull memtable", K(ret), K(len), K(new_pos));
  } else {
    ddl_kvs_ = ddl_kvs_addr;
    ddl_kv_count_ = ddl_kv_count;
    set_mem_addr();
    mds_data_.set_mem_addr();
  }
  return ret;
}

int ObTablet::deserialize_meta_v1(
    common::ObArenaAllocator &allocator,
    const char *buf,
    const int64_t len,
    int64_t &pos,
    ObTabletAutoincSeq &auto_inc_seq,
    ObTabletTxMultiSourceDataUnit &tx_data,
    ObTabletBindingInfo &ddl_data)
{
  int ret = OB_SUCCESS;

  int64_t new_pos = pos;
  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)
      || OB_UNLIKELY(len <= pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, len, new_pos, &tablet_meta_.version_))) {
    LOG_WARN("failed to deserialize tablet meta's version", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, len, new_pos, &tablet_meta_.length_))) {
    LOG_WARN("failed to deserialize tablet meta's length", K(ret), K(len), K(new_pos));
  } else if (tablet_meta_.TABLET_META_VERSION == tablet_meta_.version_) {
    int8_t compat_mode = -1;
    tablet_meta_.ddl_execution_id_ = 0;
    if (OB_UNLIKELY(tablet_meta_.length_ > len - pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buffer's length is not enough", K(ret), K(tablet_meta_.length_), K(len - new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(tablet_meta_.ls_id_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize ls id", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(tablet_meta_.tablet_id_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize tablet id", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(tablet_meta_.data_tablet_id_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize data tablet id", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(tablet_meta_.ref_tablet_id_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize ref tablet id", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(serialization::decode_bool(buf, len, new_pos, &tablet_meta_.has_next_tablet_))) {
      LOG_WARN("failed to deserialize has_next_tablet_", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(tablet_meta_.create_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize create scn", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(tablet_meta_.start_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize start scn", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(tablet_meta_.clog_checkpoint_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize clog checkpoint ts", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(tablet_meta_.ddl_checkpoint_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize ddl checkpoint ts", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &tablet_meta_.snapshot_version_))) {
      LOG_WARN("failed to deserialize snapshot version", K(ret), K(len));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &tablet_meta_.multi_version_start_))) {
      LOG_WARN("failed to deserialize multi version start", K(ret), K(len));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(serialization::decode_i8(buf, len, new_pos, &compat_mode))) {
      LOG_WARN("failed to deserialize compat mode", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(auto_inc_seq.deserialize(allocator, buf, len, new_pos))) { // get autoinc seq
      LOG_WARN("failed to deserialize auto inc seq", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(tablet_meta_.ha_status_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize restore status", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(tablet_meta_.report_status_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize report status", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(tx_data.deserialize(buf, len, new_pos))) { // get tx data
      LOG_WARN("failed to deserialize multi source data", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(ddl_data.deserialize(buf, len, new_pos))) { // get ddl data
      LOG_WARN("failed to deserialize ddl data", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(tablet_meta_.table_store_flag_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize table store flag", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(tablet_meta_.ddl_start_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize ddl start log ts", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &tablet_meta_.ddl_snapshot_version_))) {
      LOG_WARN("failed to deserialize ddl snapshot version", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &tablet_meta_.max_sync_storage_schema_version_))) {
      LOG_WARN("failed to deserialize max_sync_storage_schema_version", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &tablet_meta_.ddl_execution_id_))) {
      LOG_WARN("failed to deserialize ddl execution id", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &tablet_meta_.ddl_data_format_version_))) {
      LOG_WARN("failed to deserialize ddl cluster version", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &tablet_meta_.max_serialized_medium_scn_))) {
      LOG_WARN("failed to deserialize max serialized medium scn", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(tablet_meta_.ddl_commit_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize ddl commit scn", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < tablet_meta_.length_ && OB_FAIL(tablet_meta_.mds_checkpoint_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize mds checkpoint scn", K(ret), K(len), K(new_pos));
    } else if (OB_UNLIKELY(tablet_meta_.length_ != new_pos - pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet's length doesn't match standard length", K(ret), K(new_pos), K(pos), K_(tablet_meta_.length));
    } else {
      pos = new_pos;
      tablet_meta_.compat_mode_ = static_cast<lib::Worker::CompatMode>(compat_mode);
      tablet_meta_.is_inited_ = true;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid version", K(ret), K_(tablet_meta_.version));
  }

  return ret;

}

int ObTablet::load_deserialize_v2(
    common::ObArenaAllocator &allocator,
    const char *buf,
    const int64_t len,
    const int64_t pos,
    int64_t &new_pos,
    const bool prepare_memtable)
{
  int ret = OB_SUCCESS;
  ObITable **ddl_kvs_addr = nullptr;
  int64_t ddl_kv_count = 0;
  if (new_pos - pos < length_ && OB_FAIL(tablet_meta_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize tablet meta", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(table_store_addr_.addr_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize table store addr", K(ret), K(len), K(new_pos));
  } else if (FALSE_IT(table_store_addr_.addr_.set_seq(tablet_addr_.seq()))) {
  } else if (new_pos - pos < length_ && OB_FAIL(storage_schema_addr_.addr_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize storage schema addr", K(ret), K(len), K(new_pos));
  } else if (!is_empty_shell() && OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, rowkey_read_info_))) {
    LOG_WARN("fail to allocate and new rowkey read info", K(ret));
  } else if (!is_empty_shell() && new_pos - pos < length_ && OB_FAIL(rowkey_read_info_->deserialize(allocator, buf, len, new_pos))) {
    LOG_WARN("fail to deserialize rowkey read info", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(mds_data_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize mds data", K(ret), K(len), K(new_pos));
  } else if (prepare_memtable && OB_FAIL(pull_memtables(allocator, ddl_kvs_addr, ddl_kv_count))) {
    LOG_WARN("fail to pull memtable", K(ret), K(len), K(new_pos));
  } else {
    ddl_kvs_ = ddl_kvs_addr;
    ddl_kv_count_ = ddl_kv_count;
  }
  return ret;
}

int ObTablet::deserialize(
    const char *buf,
    const int64_t len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  char* tablet_buf = reinterpret_cast<char *>(this);
  ObMetaObjBufferHeader &buf_header = ObMetaObjBufferHelper::get_buffer_header(tablet_buf);
  int64_t remain = buf_header.buf_len_ - sizeof(ObTablet);
  int64_t start_pos = sizeof(ObTablet);
  ObArenaAllocator allocator;
  ObITable **ddl_kvs_addr = nullptr;
  int64_t ddl_kv_count = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot deserialize inited tablet meta", K(ret), K_(is_inited));
  } else if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)
      || OB_UNLIKELY(len <= pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, len, new_pos, (int32_t *)&version_))) {
    LOG_WARN("failed to deserialize tablet meta's version", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, len, new_pos, (int32_t *)&length_))) {
    LOG_WARN("failed to deserialize tablet meta's length", K(ret), K(len), K(new_pos));
  } else if (TABLET_VERSION_V2 != version_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid version", K(ret), K_(version));
  } else {
    if (OB_UNLIKELY(length_ > len - pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buffer's length is not enough", K(ret), K(length_), K(len - new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(tablet_meta_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize tablet meta", K(ret), K(len), K(new_pos));
    } else if (OB_FAIL(pull_memtables(allocator, ddl_kvs_addr, ddl_kv_count))) {
      LOG_WARN("fail to pull memtable", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(table_store_addr_.addr_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize table read info addr", K(ret), K(len), K(new_pos));
    } else if (FALSE_IT(table_store_addr_.addr_.set_seq(tablet_addr_.seq()))) {
    } else if (new_pos - pos < length_ && OB_FAIL(storage_schema_addr_.addr_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize table store addr", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && !table_store_addr_.addr_.is_none()) {
      ObRowkeyReadInfo *rowkey_read_info = nullptr;
      if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, rowkey_read_info))) {
        LOG_WARN("fail to deserialize rowkey read info", K(ret), K(len), K(new_pos));
      } else if (OB_FAIL(rowkey_read_info->deserialize(allocator, buf, len, new_pos))) {
        LOG_WARN("fail to deserialize rowkey read info", K(ret), K(len), K(new_pos));
      } else if (remain < rowkey_read_info->get_deep_copy_size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet memory buffer not enough for rowkey read info", K(ret), K(remain), K(rowkey_read_info->get_deep_copy_size()));
      } else if (OB_FAIL(rowkey_read_info->deep_copy(
          tablet_buf + start_pos, remain, rowkey_read_info_))) {
        LOG_WARN("fail to deep copy rowkey read info to tablet", K(ret), KPC(rowkey_read_info), K(remain));
      } else if (OB_ISNULL(rowkey_read_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr for rowkey read info deep copy", K(ret));
      } else {
        remain -= rowkey_read_info_->get_deep_copy_size();
        start_pos += rowkey_read_info_->get_deep_copy_size();
      }
    }

    if (OB_FAIL(ret)) { // need to dec ddl kv ref cnt.
      ddl_kvs_ = ddl_kvs_addr;
      ddl_kv_count_ = ddl_kv_count;
      reset_ddl_memtables();
    } else {
      // `pull_memtables` pulls ddl kvs into `ddl_kvs_addr` array which allocated by `allocator`.
      // tiny tablet needs to deep copy `ddl_kvs_addr` array to `tablet_buf + start_pos`, and CANNOT additionally
      // inc ref count. Cause `pull_memtables` already done this.
      if (OB_NOT_NULL(ddl_kvs_addr)) {
        const int64_t ddl_kv_size = sizeof(ObITable*) * DDL_KV_ARRAY_SIZE;
        if (remain < ddl_kv_size) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to deep copy ddl kv to tablet", K(ret), K(remain), K(ddl_kv_size), K(ddl_kv_count));
        } else {
          ddl_kv_count_ = ddl_kv_count;
          ddl_kvs_ = reinterpret_cast<ObITable**>(tablet_buf + start_pos);
          if (OB_ISNULL(MEMCPY(ddl_kvs_, ddl_kvs_addr, ddl_kv_size))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to memcpy ddl_kvs", K(ret), KP(ddl_kvs_), KP(ddl_kvs_addr), K(ddl_kv_count_));
          } else {
            start_pos += ddl_kv_size;
            remain -= ddl_kv_size;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (new_pos - pos < length_ && OB_FAIL(mds_data_.deserialize(buf, len, new_pos))) {
        LOG_WARN("failed to deserialize mds data", K(ret), K(len), K(new_pos));
      }
    }
    if (OB_SUCC(ret)) {
      ObTabletTableStore *table_store = nullptr;
      if (table_store_addr_.addr_.is_none()) {
        if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, table_store))) {
          LOG_WARN("failed to alloc and new table store");
        } else if (OB_FAIL(table_store->init(allocator, *this))) {
          LOG_WARN("failed to init table store");
        }
      } else {
          IO_AND_DESERIALIZE(allocator, table_store_addr_.addr_, table_store, *this);
      }
      if (OB_SUCC(ret)) {
        int64_t table_store_size = table_store->get_deep_copy_size();
        ObIStorageMetaObj *table_store_obj = nullptr;
        if (remain < table_store_size) {
          LOG_INFO("tablet memory buffer not enough for table store", K(ret), K(remain), K(table_store_size));
        } else if (OB_FAIL(table_store->batch_cache_sstable_meta(allocator, remain - table_store_size))) {
          LOG_WARN("fail to batch cache sstable meta", K(ret), K(remain), K(table_store_size));
        } else if (FALSE_IT(table_store_size = table_store->get_deep_copy_size())) { // re-get deep size
        } else if (OB_FAIL(table_store->deep_copy(tablet_buf + start_pos, remain, table_store_obj))) {
          LOG_WARN("fail to deep copy table store to tablet", K(ret), KPC(table_store));
        } else if (OB_ISNULL(table_store_obj)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr for table store deep copy", K(ret));
        } else {
          table_store_addr_.ptr_ = static_cast<ObTabletTableStore *>(table_store_obj);
          remain -= table_store_size;
          start_pos += table_store_size;
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObTabletAutoincSeq *auto_inc_seq = nullptr;
      if (mds_data_.auto_inc_seq_.addr_.is_none()) {
        mds_data_.auto_inc_seq_.ptr_ = nullptr;
      } else {
        IO_AND_DESERIALIZE(allocator, mds_data_.auto_inc_seq_.addr_, auto_inc_seq);
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(auto_inc_seq)) {
        const int auto_inc_seq_size = auto_inc_seq->get_deep_copy_size();
        ObIStorageMetaObj *auto_inc_seq_obj = nullptr;
        if (OB_UNLIKELY(remain < auto_inc_seq_size)) {
          LOG_INFO("tablet memory buffer not enough for auto inc seq", K(ret), K(remain), K(auto_inc_seq_size));
        } else if (OB_FAIL(auto_inc_seq->deep_copy(
            tablet_buf + start_pos, remain, auto_inc_seq_obj))) {
          LOG_WARN("fail to deep copy auto inc seq to tablet", K(ret), KPC(auto_inc_seq));
        } else if (OB_ISNULL(auto_inc_seq_obj)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr for auto inc seq deep copy", K(ret));
        } else {
          mds_data_.auto_inc_seq_.ptr_ = static_cast<ObTabletAutoincSeq *>(auto_inc_seq_obj);
          remain -= auto_inc_seq_size;
          start_pos += auto_inc_seq_size;
        }
      }
    }

    if (OB_SUCC(ret) && tablet_meta_.has_next_tablet_) {
      ObTabletHandle next_tablet_handle;
      const ObTabletMapKey key(tablet_meta_.ls_id_, tablet_meta_.tablet_id_);
      if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tablet_from_pool(ObTabletPoolType::TP_NORMAL, key, next_tablet_handle))) {
        LOG_WARN("failed to acquire tablet", K(ret), K(key));
      } else if (OB_ISNULL(next_tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("next tablet is null", K(ret));
      } else if (FALSE_IT(next_tablet_handle.get_obj()->set_tablet_addr(tablet_addr_))) {
      } else if (OB_FAIL(next_tablet_handle.get_obj()->deserialize(buf, len, new_pos))) {
        LOG_WARN("failed to deserialize next tablet", K(ret), K(len), K(new_pos));
      } else {
        set_next_tablet_guard(next_tablet_handle);
      }
    }

    if (OB_SUCC(ret)) {
      pos = new_pos;
      is_inited_ = true;
      // must succeed if hold_ref_cnt_ has been set to true
      hold_ref_cnt_ = true;
      LOG_INFO("succeed to load deserialize tablet", K(ret), KPC(this));
    }
  }
  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObTablet::get_tablet_meta_ids(ObIArray<MacroBlockId> &meta_ids) const
{
  int ret = OB_SUCCESS;
  const ObMetaDiskAddr &tablet_status_uncommitted_kv_addr = mds_data_.tablet_status_.uncommitted_kv_.addr_;
  const ObMetaDiskAddr &tablet_status_committed_kv_addr = mds_data_.tablet_status_.committed_kv_.addr_;
  const ObMetaDiskAddr &aux_tablet_info_uncommitted_kv_addr = mds_data_.aux_tablet_info_.uncommitted_kv_.addr_;
  const ObMetaDiskAddr &aux_tablet_info_committed_kv_addr = mds_data_.aux_tablet_info_.committed_kv_.addr_;
  const ObMetaDiskAddr &medium_info_list_addr = mds_data_.medium_info_list_.addr_;
  const ObMetaDiskAddr &auto_inc_seq_addr = mds_data_.auto_inc_seq_.addr_;

  if (OB_UNLIKELY(tablet_meta_.has_next_tablet_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("shouldn't have next tablet", K(ret), K(tablet_meta_), K(next_tablet_guard_));
  } else if (OB_FAIL(parse_meta_addr(tablet_addr_, meta_ids))) {
    LOG_WARN("fail to parse tablet addr", K(ret), K(tablet_addr_));
  } else if (OB_FAIL(parse_meta_addr(table_store_addr_.addr_, meta_ids))) {
    LOG_WARN("fail to parse table store addr", K(ret), K(table_store_addr_.addr_));
  } else if (OB_FAIL(parse_meta_addr(storage_schema_addr_.addr_, meta_ids))) {
    LOG_WARN("fail to parse storage schema addr", K(ret), K(storage_schema_addr_.addr_));
  } else if (OB_FAIL(parse_meta_addr(tablet_status_uncommitted_kv_addr, meta_ids))) {
    LOG_WARN("fail to parse tablet_status_uncommitted_kv_addr", K(ret), K(tablet_status_uncommitted_kv_addr));
  } else if (OB_FAIL(parse_meta_addr(tablet_status_committed_kv_addr, meta_ids))) {
    LOG_WARN("fail to parse tablet_status_committed_kv_addr", K(ret), K(tablet_status_committed_kv_addr));
  } else if (OB_FAIL(parse_meta_addr(aux_tablet_info_uncommitted_kv_addr, meta_ids))) {
    LOG_WARN("fail to parse aux_tablet_info_uncommitted_kv_addr", K(ret), K(aux_tablet_info_uncommitted_kv_addr));
  } else if (OB_FAIL(parse_meta_addr(aux_tablet_info_committed_kv_addr, meta_ids))) {
    LOG_WARN("fail to parse aux_tablet_info_committed_kv_addr", K(ret), K(aux_tablet_info_committed_kv_addr));
  } else if (OB_FAIL(parse_meta_addr(medium_info_list_addr, meta_ids))) {
    LOG_WARN("fail to parse medium_info_list_addr", K(ret), K(medium_info_list_addr));
  } else if (OB_FAIL(parse_meta_addr(auto_inc_seq_addr, meta_ids))) {
    LOG_WARN("fail to parse auto_inc_seq_addr", K(ret), K(auto_inc_seq_addr));
  }
  return ret;
}

int ObTablet::parse_meta_addr(const ObMetaDiskAddr &addr, ObIArray<MacroBlockId> &meta_ids)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  if (addr.is_block()) {
    if (OB_UNLIKELY(!addr.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet_status_uncommitted_kv_addr is invalid", K(ret), K(addr));
    } else if (FALSE_IT(macro_id = addr.block_id())) {
    } else if (OB_FAIL(meta_ids.push_back(macro_id))) {
      LOG_WARN("fail to push back macro id", K(ret), K(macro_id));
    }
  }
  return ret;
}

int ObTablet::inc_macro_ref_cnt()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet hasn't been inited", K(ret), K(is_inited_));
  } else if (OB_FAIL(inner_inc_macro_ref_cnt())) {
    LOG_WARN("fail to increase macro ref cnt", K(ret));
  } else if (tablet_meta_.has_next_tablet_
      && OB_FAIL(next_tablet_guard_.get_obj()->inc_macro_ref_cnt())) {
    LOG_WARN("fail to increase macro ref cnt for next tablet",
        K(ret), KPC(next_tablet_guard_.get_obj()));
  }
  return ret;
}

int ObTablet::inner_inc_macro_ref_cnt()
{
  int ret = OB_SUCCESS;
  bool inc_table_store_ref = false;
  bool inc_storage_schema_ref = false;
  bool inc_medium_info_list_ref = false;
  bool inc_tablet_ref = false;
  bool inc_table_store_member_ref = false;
  bool inc_sstable_meta_ref = false;
  bool inc_tablet_status_uncommitted_kv_ref = false;
  bool inc_tablet_status_committed_kv_ref = false;
  bool inc_aux_tablet_info_uncommitted_kv_ref = false;
  bool inc_aux_tablet_info_committed_kv_ref = false;
  bool inc_auto_inc_ref = false;

  const ObTabletComplexAddr<mds::MdsDumpKV> &tablet_status_uncommitted_kv_addr = mds_data_.tablet_status_.uncommitted_kv_;
  const ObTabletComplexAddr<mds::MdsDumpKV> &tablet_status_committed_kv_addr = mds_data_.tablet_status_.committed_kv_;
  const ObTabletComplexAddr<mds::MdsDumpKV> &aux_tablet_info_uncommitted_kv_addr = mds_data_.aux_tablet_info_.uncommitted_kv_;
  const ObTabletComplexAddr<mds::MdsDumpKV> &aux_tablet_info_committed_kv_addr = mds_data_.aux_tablet_info_.committed_kv_;
  const ObTabletComplexAddr<ObTabletDumpedMediumInfo> &medium_info_list_addr = mds_data_.medium_info_list_;
  const ObTabletComplexAddr<share::ObTabletAutoincSeq> &auto_inc_seq_addr = mds_data_.auto_inc_seq_;

  if (OB_FAIL(check_meta_addr())) {
    LOG_WARN("fail to check meta addrs", K(ret));
  } else if (OB_FAIL(inc_linked_block_ref_cnt(medium_info_list_addr.addr_, inc_medium_info_list_ref))) {
    LOG_WARN("fail to increase macro blocks' ref cnt for medium info list", K(ret), K(medium_info_list_addr));
  } else if (OB_FAIL(inc_addr_ref_cnt(table_store_addr_.addr_, inc_table_store_ref))) {
    LOG_WARN("fail to increase macro blocks' ref cnt for table store", K(ret), K(table_store_addr_.addr_));
  } else if (OB_FAIL(inc_addr_ref_cnt(storage_schema_addr_.addr_, inc_storage_schema_ref))) {
    LOG_WARN("fail to increase macro blocks' ref cnt for storage schema", K(ret), K(storage_schema_addr_.addr_));
  } else if (OB_FAIL(inc_addr_ref_cnt(tablet_status_uncommitted_kv_addr.addr_, inc_tablet_status_uncommitted_kv_ref))) {
    LOG_WARN("fail to increase macro blocks' ref cnt for tablet status uncommitted kv", K(ret), K(tablet_status_uncommitted_kv_addr.addr_));
  } else if (OB_FAIL(inc_addr_ref_cnt(tablet_status_committed_kv_addr.addr_, inc_tablet_status_committed_kv_ref))) {
    LOG_WARN("fail to increase macro blocks' ref cnt for tablet status committed kv", K(ret), K(tablet_status_committed_kv_addr.addr_));
  } else if (OB_FAIL(inc_addr_ref_cnt(aux_tablet_info_uncommitted_kv_addr.addr_, inc_aux_tablet_info_uncommitted_kv_ref))) {
    LOG_WARN("fail to increase macro blocks' ref cnt for aux tablet info uncommitted kv", K(ret), K(aux_tablet_info_uncommitted_kv_addr.addr_));
  } else if (OB_FAIL(inc_addr_ref_cnt(aux_tablet_info_committed_kv_addr.addr_, inc_aux_tablet_info_committed_kv_ref))) {
    LOG_WARN("fail to increase macro blocks' ref cnt for aux tablet info committed kv", K(ret), K(aux_tablet_info_committed_kv_addr.addr_));
  } else if (OB_FAIL(inc_addr_ref_cnt(auto_inc_seq_addr.addr_, inc_auto_inc_ref))) {
    LOG_WARN("fail to increase macro blocks' ref cnt for auto inc seq", K(ret), K(auto_inc_seq_addr.addr_));
  } else if (OB_FAIL(inc_addr_ref_cnt(tablet_addr_, inc_tablet_ref))) {
    LOG_WARN("fail to increase macro blocks' ref cnt for 4k tablet", K(ret), K(tablet_addr_));
  } else if (OB_FAIL(inc_table_store_ref_cnt(inc_table_store_member_ref))) {
    LOG_WARN("fail to increase macro blocks' ref cnt for sstable meta", K(ret));
  } else {
    hold_ref_cnt_ = true;
  }
  FLOG_INFO("the tablet that inner increases ref cnt is", K(ret),
      K(is_inited_), K(tablet_meta_.ls_id_), K(tablet_meta_.tablet_id_), K(table_store_addr_.addr_),
      K(auto_inc_seq_addr.addr_), K(storage_schema_addr_.addr_), K(medium_info_list_addr.addr_),
      K(tablet_status_uncommitted_kv_addr.addr_), K(tablet_status_committed_kv_addr.addr_),
      K(aux_tablet_info_uncommitted_kv_addr.addr_), K(aux_tablet_info_committed_kv_addr.addr_),
      K(tablet_addr_), KP(this), K(lbt()));

  if (OB_FAIL(ret)) {
    if (inc_medium_info_list_ref) {
      dec_linked_block_ref_cnt(medium_info_list_addr.addr_);
    }
    if (inc_table_store_ref) {
      dec_addr_ref_cnt(table_store_addr_.addr_);
    }
    if (inc_storage_schema_ref) {
      dec_addr_ref_cnt(storage_schema_addr_.addr_);
    }
    if (inc_tablet_status_uncommitted_kv_ref) {
      dec_addr_ref_cnt(tablet_status_uncommitted_kv_addr.addr_);
    }
    if (inc_tablet_status_committed_kv_ref) {
      dec_addr_ref_cnt(tablet_status_committed_kv_addr.addr_);
    }
    if (inc_aux_tablet_info_uncommitted_kv_ref) {
      dec_addr_ref_cnt(aux_tablet_info_uncommitted_kv_addr.addr_);
    }
    if (inc_aux_tablet_info_committed_kv_ref) {
      dec_addr_ref_cnt(aux_tablet_info_committed_kv_addr.addr_);
    }
    if (inc_auto_inc_ref) {
      dec_addr_ref_cnt(auto_inc_seq_addr.addr_);
    }
    if (inc_tablet_ref) {
      dec_addr_ref_cnt(tablet_addr_);
    }
    if (inc_table_store_member_ref) {
      dec_table_store_ref_cnt();
    }
  }
  return ret;
}

void ObTablet::dec_macro_ref_cnt()
{
  int ret = OB_SUCCESS;
  const ObTabletComplexAddr<mds::MdsDumpKV> &tablet_status_uncommitted_kv_addr = mds_data_.tablet_status_.uncommitted_kv_;
  const ObTabletComplexAddr<mds::MdsDumpKV> &tablet_status_committed_kv_addr = mds_data_.tablet_status_.committed_kv_;
  const ObTabletComplexAddr<mds::MdsDumpKV> &aux_tablet_info_uncommitted_kv_addr = mds_data_.aux_tablet_info_.uncommitted_kv_;
  const ObTabletComplexAddr<mds::MdsDumpKV> &aux_tablet_info_committed_kv_addr = mds_data_.aux_tablet_info_.committed_kv_;
  const ObTabletComplexAddr<ObTabletDumpedMediumInfo> &medium_info_list_addr = mds_data_.medium_info_list_;
  const ObTabletComplexAddr<share::ObTabletAutoincSeq> &auto_inc_seq_addr = mds_data_.auto_inc_seq_;
  // We don't need to recursively decrease macro ref cnt, since we will push both them to gc queue
  if (OB_UNLIKELY(!hold_ref_cnt_)) {
    FLOG_INFO("tablet doesn't hold ref cnt, no need to dec ref cnt",
      K(is_inited_), K(tablet_meta_.ls_id_), K(tablet_meta_.tablet_id_), K(table_store_addr_.addr_.is_valid()),
      K(auto_inc_seq_addr.addr_), K(storage_schema_addr_.addr_), K(medium_info_list_addr.addr_),
      K(tablet_status_uncommitted_kv_addr.addr_), K(tablet_status_committed_kv_addr.addr_),
      K(aux_tablet_info_uncommitted_kv_addr.addr_), K(aux_tablet_info_committed_kv_addr.addr_),
      K(tablet_addr_), KP(this), K(lbt()));
  } else if (OB_FAIL(check_meta_addr())) {
    LOG_WARN("fail to check meta addrs", K(ret));
  } else {
    FLOG_INFO("the tablet that decreases ref cnt is",
        K(is_inited_), K(tablet_meta_.ls_id_), K(tablet_meta_.tablet_id_), K(table_store_addr_.addr_),
        K(auto_inc_seq_addr.addr_), K(storage_schema_addr_.addr_), K(medium_info_list_addr.addr_),
        K(tablet_status_uncommitted_kv_addr.addr_), K(tablet_status_committed_kv_addr.addr_),
        K(aux_tablet_info_uncommitted_kv_addr.addr_), K(aux_tablet_info_committed_kv_addr.addr_),
        K(tablet_addr_), KP(this), K(lbt()));
    // the order can't be changed, must be sstable blocks' ref cnt -> tablet meta blocks' ref cnt
    dec_linked_block_ref_cnt(medium_info_list_addr.addr_);
    dec_table_store_ref_cnt();
    dec_addr_ref_cnt(table_store_addr_.addr_);
    dec_addr_ref_cnt(storage_schema_addr_.addr_);
    dec_addr_ref_cnt(tablet_status_uncommitted_kv_addr.addr_);
    dec_addr_ref_cnt(tablet_status_committed_kv_addr.addr_);
    dec_addr_ref_cnt(aux_tablet_info_uncommitted_kv_addr.addr_);
    dec_addr_ref_cnt(aux_tablet_info_committed_kv_addr.addr_);
    dec_addr_ref_cnt(auto_inc_seq_addr.addr_);
    dec_addr_ref_cnt(tablet_addr_);
  }
}

int ObTablet::inc_table_store_ref_cnt(bool &inc_success)
{
  int ret = OB_SUCCESS;
  inc_success = false;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (table_store_addr_.addr_.is_none()) {
    // skip empty shell
  } else if (OB_FAIL(fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member()->inc_macro_ref())) {
    LOG_WARN("fail to increase sstables' ref cnt",
      K(ret), K(table_store_wrapper), K(table_store_addr_.addr_));
  } else {
    inc_success = true;
  }
  return ret;
}

void ObTablet::dec_table_store_ref_cnt()
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (table_store_addr_.addr_.is_none()) {
    // skip empty shell
  } else {
    do {
      ret = fetch_table_store(table_store_wrapper);
    } while (ignore_ret(ret));
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to fetch table store", K(ret));
    } else {
      table_store_wrapper.get_member()->dec_macro_ref();
    }
  }
}

int ObTablet::inc_addr_ref_cnt(const ObMetaDiskAddr &addr, bool &inc_success)
{
  int ret = OB_SUCCESS;
  inc_success = false;
  MacroBlockId macro_id;
  int64_t offset;
  int64_t size;
  if (addr.is_block()) { // skip full/old/empty_shell tablet
    if (OB_FAIL(addr.get_block_addr(macro_id, offset, size))) {
      LOG_WARN("fail to get macro id from addr", K(ret), K(addr));
    } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_id))) {
      LOG_ERROR("fail to increase macro block's ref cnt", K(ret), K(macro_id));
    } else {
      inc_success = true;
    }
  }
  return ret;
}

void ObTablet::dec_addr_ref_cnt(const ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  int64_t offset;
  int64_t size;
  if (addr.is_block()) { // skip full/old/empty_shell tablet
    if (OB_FAIL(addr.get_block_addr(macro_id, offset, size))) {
      LOG_ERROR("fail to get macro id from addr, macro block leaks", K(ret), K(addr));
    } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
      LOG_ERROR("fail to decrease macro block's ref cnt, macro block leaks", K(ret), K(macro_id));
    }
  }
}

int ObTablet::inc_linked_block_ref_cnt(const ObMetaDiskAddr &head_addr, bool &inc_success)
{
  int ret = OB_SUCCESS;
  inc_success = false;
  ObSharedBlockLinkIter iter;
  MacroBlockId macro_id;
  int64_t block_cnt = 0;

  if (head_addr.is_block()) {
    if (OB_FAIL(iter.init(head_addr))) {
      LOG_WARN("fail to init link iter", K(ret), K(head_addr));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter.get_next_macro_id(macro_id))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next macro id", K(ret));
          }
        } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_id))) {
          LOG_ERROR("fail to increase linked blocks' ref cnt", K(ret), K(macro_id));
        } else {
          block_cnt++;
        }
      }
    }
  }
  if (OB_FAIL(ret) && 0 != block_cnt) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(iter.reuse())) {
      LOG_WARN("fail to reuse link iter", K(tmp_ret), K(iter));
    } else {
      for (int64_t i = 0; i < block_cnt; i++) {
        if (OB_TMP_FAIL(iter.get_next_macro_id(macro_id))) {
          LOG_ERROR("fail to get next macro id for rollback, macro block leaks", K(tmp_ret), K(macro_id));
        } else if (OB_TMP_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
          LOG_ERROR("fail to decrease ref cnt, macro block leaks", K(tmp_ret), K(macro_id));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    inc_success = true;
  }
  return ret;
}

void ObTablet::dec_linked_block_ref_cnt(const ObMetaDiskAddr &head_addr)
{
  int ret = OB_SUCCESS;
  ObSharedBlockLinkIter iter;
  MacroBlockId macro_id;
  if (head_addr.is_block()) {
    if (OB_FAIL(iter.init(head_addr))) {
      LOG_ERROR("fail to init link iter, macro block leaks", K(ret), K(head_addr));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter.get_next_macro_id(macro_id))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else if (ignore_ret(ret)) {
            ret = OB_SUCCESS;
            // retry
          } else {
            LOG_ERROR("fail to get next macro id, macro block leaks", K(ret));
          }
        } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
          LOG_ERROR("fail to decrease linked blocks' ref cnt, macro block leaks", K(ret), K(macro_id));
        }
      }
    }
  }
}

bool ObTablet::ignore_ret(const int ret)
{
  return OB_ALLOCATE_MEMORY_FAILED == ret || OB_TIMEOUT == ret || OB_DISK_HUNG == ret;
}

void ObTablet::set_mem_addr()
{
  if (!table_store_addr_.addr_.is_none() && !storage_schema_addr_.addr_.is_none()) {
    table_store_addr_.addr_.set_mem_addr(0, sizeof(ObTabletTableStore));
    storage_schema_addr_.addr_.set_mem_addr(0, sizeof(ObStorageSchema));
  }
  tablet_addr_.set_mem_addr(0, sizeof(ObTablet));
}

int ObTablet::check_meta_addr() const
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;

  const ObTabletComplexAddr<mds::MdsDumpKV> &tablet_status_uncommitted_kv_addr = mds_data_.tablet_status_.uncommitted_kv_;
  const ObTabletComplexAddr<mds::MdsDumpKV> &tablet_status_committed_kv_addr = mds_data_.tablet_status_.committed_kv_;
  const ObTabletComplexAddr<mds::MdsDumpKV> &aux_tablet_info_uncommitted_kv_addr = mds_data_.aux_tablet_info_.uncommitted_kv_;
  const ObTabletComplexAddr<mds::MdsDumpKV> &aux_tablet_info_committed_kv_addr = mds_data_.aux_tablet_info_.committed_kv_;
  const ObTabletComplexAddr<ObTabletDumpedMediumInfo> &medium_info_list_addr = mds_data_.medium_info_list_;
  const ObTabletComplexAddr<share::ObTabletAutoincSeq> &auto_inc_seq_addr = mds_data_.auto_inc_seq_;

  if (OB_UNLIKELY(!table_store_addr_.addr_.is_valid() || !auto_inc_seq_addr.addr_.is_valid()
      || !storage_schema_addr_.addr_.is_valid() || !medium_info_list_addr.addr_.is_valid()
      || !tablet_addr_.is_valid() || !tablet_status_uncommitted_kv_addr.is_valid()
      || !tablet_status_committed_kv_addr.is_valid() || !aux_tablet_info_uncommitted_kv_addr.is_valid()
      || !aux_tablet_info_committed_kv_addr.is_valid()))
  {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta addrs are invalid", K(ret), K(ls_id), K(tablet_id), K(tablet_addr_), K(table_store_addr_.addr_),
        K(auto_inc_seq_addr.addr_), K(storage_schema_addr_.addr_), K(medium_info_list_addr.addr_),
        K(tablet_status_uncommitted_kv_addr.addr_), K(tablet_status_committed_kv_addr.addr_),
        K(aux_tablet_info_uncommitted_kv_addr.addr_), K(aux_tablet_info_committed_kv_addr.addr_));
  } else if (((tablet_addr_.is_block() ^ table_store_addr_.addr_.is_block())
      || (tablet_addr_.is_block() ^ storage_schema_addr_.addr_.is_block()))
      && ((tablet_addr_.is_block() ^ table_store_addr_.addr_.is_none())
      || (tablet_addr_.is_block() ^ storage_schema_addr_.addr_.is_none()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta addrs are inconsistent", K(ret), K(ls_id), K(tablet_id),
        K(tablet_addr_), K(table_store_addr_.addr_), K(storage_schema_addr_.addr_));
  }

  return ret;
}

int ObTablet::get_multi_version_start(SCN &scn) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(scn.convert_for_tx(tablet_meta_.multi_version_start_))) {
    LOG_WARN("fail to convert scn", K(ret), K(tablet_meta_.multi_version_start_));
  }
  return ret;
}

int ObTablet::get_snapshot_version(SCN &scn) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(scn.convert_for_tx(tablet_meta_.snapshot_version_))) {
    LOG_WARN("fail to convert scn", K(ret), K(tablet_meta_.snapshot_version_));
  }
  return ret;
}

int64_t ObTablet::get_serialize_size() const
{
  int64_t size = get_self_size();
  if (tablet_meta_.has_next_tablet_) {
    size += next_tablet_guard_.get_obj()->get_serialize_size();
  }
  return size;
}

int64_t ObTablet::get_self_size() const
{
  int64_t size =0;
  size += serialization::encoded_length_i32(version_);
  size += serialization::encoded_length_i32(length_);
  size += tablet_meta_.get_serialize_size();
  size += storage_schema_addr_.addr_.get_serialize_size();
  size += table_store_addr_.addr_.get_serialize_size();
  size += is_empty_shell() ? 0 : rowkey_read_info_->get_serialize_size();
  size += mds_data_.get_serialize_size();
  return size;
}

void ObTablet::set_next_tablet_guard(const ObTabletHandle &next_tablet_guard)
{
  if (OB_UNLIKELY(next_tablet_guard.is_valid())) {
    int ret = OB_NOT_SUPPORTED;
    LOG_ERROR("shouldn't have next tablet", K(ret), KPC(this));
  }
}

void ObTablet::set_tablet_addr(const ObMetaDiskAddr &tablet_addr)
{
  tablet_addr_ = tablet_addr;
  if (tablet_meta_.has_next_tablet_ && next_tablet_guard_.is_valid()) {
    next_tablet_guard_.get_obj()->set_tablet_addr(tablet_addr);
  }
}

void ObTablet::trim_tablet_list()
{
  tablet_meta_.has_next_tablet_ = false;
  next_tablet_guard_.reset();
}

int ObTablet::deserialize_id(
    const char *buf,
    const int64_t len,
    share::ObLSID &ls_id,
    common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  int32_t version = 0;
  int32_t length = 0;
  int64_t pos = 0;
  if (OB_FAIL(serialization::decode_i32(buf, len, pos, (int32_t *)&version))) {
    LOG_WARN("fail to deserialize tablet meta's version", K(ret), K(len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, len, pos, (int32_t *)&length))) {
    LOG_WARN("fail to deserialize tablet meta's length", K(ret), K(len), K(pos));
  } else if (TABLET_VERSION == version || TABLET_VERSION_V2 == version) {
    if (OB_FAIL(ObTabletMeta::deserialize_id(buf, len, pos, ls_id, tablet_id))) {
      LOG_WARN("fail to deserialize ls_id and tablet_id from tablet meta", K(ret), K(len));
    }
  }

  return ret;
}

int ObTablet::get_max_sync_medium_scn(int64_t &max_medium_snapshot) const
{
  int ret = OB_SUCCESS;
  max_medium_snapshot = 0;
  ObTabletMemtableMgr *data_memtable_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (tablet_meta_.tablet_id_.is_special_merge_tablet()) {
    // do nothing
  } else if (OB_FAIL(get_tablet_memtable_mgr(data_memtable_mgr))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else {
    max_medium_snapshot = data_memtable_mgr->get_medium_info_recorder().get_max_saved_version();
  }
  return ret;
}

int ObTablet::get_max_sync_storage_schema_version(int64_t &max_schema_version) const
{
  int ret = OB_SUCCESS;
  max_schema_version = 0;
  ObTabletMemtableMgr *data_memtable_mgr = nullptr;
  if (is_ls_inner_tablet()) {
    // do nothing
  } else if (OB_FAIL(get_tablet_memtable_mgr(data_memtable_mgr))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else {
    max_schema_version = data_memtable_mgr->get_storage_schema_recorder().get_max_saved_version();
  }
  return ret;
}

int ObTablet::try_update_storage_schema(
    const int64_t table_id,
    const int64_t schema_version,
    ObIAllocator &allocator,
    const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  ObTabletMemtableMgr *data_memtable_mgr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (tablet_meta_.tablet_id_.is_special_merge_tablet()) {
    // do nothing
  } else if (OB_FAIL(get_tablet_memtable_mgr(data_memtable_mgr))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else if (OB_FAIL(data_memtable_mgr->get_storage_schema_recorder().try_update_storage_schema(
      table_id, schema_version, allocator, timeout_ts))) {
    LOG_WARN("fail to record storage schema", K(ret), K(table_id), K(schema_version), K(timeout_ts));
  }
  return ret;
}

int ObTablet::get_max_column_cnt_on_schema_recorder(int64_t &max_column_cnt)
{
  int ret = OB_SUCCESS;
  ObTabletMemtableMgr *data_memtable_mgr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (tablet_meta_.tablet_id_.is_special_merge_tablet()) {
    // do nothing
  } else if (OB_FAIL(get_tablet_memtable_mgr(data_memtable_mgr))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else {
    max_column_cnt = data_memtable_mgr->get_storage_schema_recorder().get_max_column_cnt();
  }
  return ret;
}

// be careful to use this max_schem_version on storage_schema
int ObTablet::get_max_schema_version(int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  schema_version = -1;
  common::ObSEArray<ObTableHandleV2, 8> table_handle_array;
  ObIMemtableMgr *memtable_mgr = nullptr;

  if (OB_FAIL(get_memtable_mgr(memtable_mgr))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else if (OB_FAIL(memtable_mgr->get_all_memtables(table_handle_array))) {
    LOG_WARN("failed to get memtables", K(ret));
  } else {
    const memtable::ObMemtable *memtable = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_handle_array.count(); ++i) {
      const ObTableHandleV2 &handle = table_handle_array[i];
      if (OB_UNLIKELY(!handle.is_valid())) {
        ret = OB_ERR_SYS;
        LOG_WARN("invalid memtable", K(ret), K(handle));
      } else if (OB_FAIL(handle.get_data_memtable(memtable))) {
        LOG_WARN("fail to get memtable", K(ret), K(handle));
      } else if (OB_ISNULL(memtable)) {
        ret = OB_ERR_SYS;
        LOG_WARN("memtable is null", K(ret), KP(memtable));
      } else {
        schema_version = common::max(schema_version, memtable->get_max_schema_version());
      }
    }
  }
  return ret;
}

int ObTablet::check_schema_version_for_bounded_staleness_read(
    const int64_t table_version_for_read,
    const int64_t data_max_schema_version,
    const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  int64_t cur_table_version = OB_INVALID_VERSION;
  int64_t tenant_schema_version = OB_INVALID_VERSION;

  if (table_version_for_read >= data_max_schema_version) {
    // read schema version is biger than max schema version of data, pass
  } else {
    // read schema version is smaller than max schema version of data, two possible cases:
    // 1. max schema version of data is max schema version of table, return schema error, asking for schema refresh
    //
    //    standalone pg is in this case
    //
    // 2. max schema version of data is max schema version of multiple table partitions
    //
    //    It is the case when pg contains multiple partitions, it can only return max schema version of all partitions
    //
    // To differentiate the above two cases, check with the help of local schema version

    const uint64_t tenant_id = MTL_ID();
    ObMultiVersionSchemaService *schema_service = MTL(ObTenantSchemaService*)->get_schema_service();
    ObSchemaGetterGuard schema_guard;
    // get schema version of this table in schema service
    if (OB_ISNULL(schema_service)) {
      ret = OB_NOT_INIT;
      LOG_WARN("invalid schema service", K(ret), K(schema_service));
    } else if (OB_FAIL(schema_service->get_tenant_full_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("schema service get tenant schema guard fail", K(ret), K(tenant_id),
          K(table_id));
    } else if (OB_FAIL(schema_guard.get_schema_version(TABLE_SCHEMA, tenant_id, table_id, cur_table_version))) {
      LOG_WARN("get table schema version fail", K(ret), K(tenant_id), K(table_id));
    }

    // check whether input table version and schema version of this table in schema service same
    // if not same, refresh schema
    else if (OB_UNLIKELY(table_version_for_read != cur_table_version)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("schema version for read mismatch", K(ret), K(table_id),
          K(table_version_for_read), K(cur_table_version), K(data_max_schema_version));
    }
    // get max schema version of the tenant
    else if (OB_FAIL(schema_service->get_tenant_refreshed_schema_version(
        tenant_id, tenant_schema_version))) {
      LOG_WARN("get tenant refreshed schema version fail", K(ret), K(tenant_id));
    } else if (tenant_schema_version >= data_max_schema_version) {
      // if max schema version of the tenant is bigger than data's schema version,
      // then schema of read operation is newer than data's
    } else {
      ret = OB_SCHEMA_NOT_UPTODATE;
      LOG_WARN("schema is not up to date for read, need refresh", K(ret),
          K(table_version_for_read), K(cur_table_version), K(tenant_schema_version),
          K(data_max_schema_version), K(table_id), K(tenant_id));
    }
  }

  LOG_DEBUG("check schema version for bounded staleness read", K(ret),
      K(data_max_schema_version), K(table_version_for_read), K(cur_table_version),
      K(tenant_schema_version), K(table_id));
  return ret;
}

int ObTablet::lock_row(
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    const common::ObNewRow &row)
{
  int ret = OB_SUCCESS;
  ObStorageTableGuard guard(this, store_ctx, true);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!relative_table.is_valid()
             || !store_ctx.is_valid()
             || !row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(ret), K(relative_table), K(store_ctx), K(row));
  } else if (OB_UNLIKELY(relative_table.get_tablet_id() != tablet_meta_.tablet_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet id doesn't match", K(ret), K(relative_table.get_tablet_id()), K(tablet_meta_.tablet_id_));
  } else if (OB_FAIL(try_update_storage_schema(relative_table.get_table_id(),
      relative_table.get_schema_version(),
      store_ctx.mvcc_acc_ctx_.get_mem_ctx()->get_query_allocator(),
      store_ctx.timeout_))) {
    LOG_WARN("fail to record table schema", K(ret));
  } else if (OB_FAIL(guard.refresh_and_protect_table(relative_table))) {
    LOG_WARN("fail to protect table", K(ret), "tablet_id", tablet_meta_.tablet_id_);
  }
  if (OB_SUCC(ret)) {
    ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), ObModIds::OB_STORE_ROW_LOCK_CHECKER));
    ObMemtable *write_memtable = nullptr;
    ObTableIterParam param;
    ObTableAccessContext context;

    if (OB_FAIL(prepare_memtable(relative_table, store_ctx, write_memtable))) {
      LOG_WARN("prepare write memtable fail", K(ret), K(relative_table));
    } else if (OB_FAIL(prepare_param_ctx(allocator, relative_table, store_ctx, param, context))) {
      LOG_WARN("prepare param ctx fail, ", K(ret));
    } else if (OB_FAIL(write_memtable->lock(param, context, row))) {
      LOG_WARN("failed to lock write_memtable", K(ret), K(row));
    }
  }
  return ret;
}

int ObTablet::lock_row(
    ObRelativeTable &relative_table,
    storage::ObStoreCtx &store_ctx,
    const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObStorageTableGuard guard(this, store_ctx, true);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!relative_table.is_valid()
             || !store_ctx.is_valid()
             || !rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
                K(ret), K(relative_table), K(store_ctx), K(rowkey));
  } else if (OB_UNLIKELY(relative_table.get_tablet_id() != tablet_meta_.tablet_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet id doesn't match", K(ret), K(relative_table.get_tablet_id()), K(tablet_meta_.tablet_id_));
  } else if (OB_FAIL(try_update_storage_schema(relative_table.get_table_id(),
      relative_table.get_schema_version(),
      store_ctx.mvcc_acc_ctx_.get_mem_ctx()->get_query_allocator(),
      store_ctx.timeout_))) {
    LOG_WARN("fail to record table schema", K(ret));
  } else if (OB_FAIL(guard.refresh_and_protect_table(relative_table))) {
    LOG_WARN("fail to protect table", K(ret));
  } else {
    ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), ObModIds::OB_STORE_ROW_LOCK_CHECKER));
    ObMemtable *write_memtable = nullptr;
    ObTableIterParam param;
    ObTableAccessContext context;
    const uint64_t table_id = relative_table.get_table_id();

    if (OB_FAIL(prepare_memtable(relative_table, store_ctx, write_memtable))) {
      LOG_WARN("prepare write memtable fail", K(ret), K(relative_table));
    } else if (OB_FAIL(prepare_param_ctx(allocator, relative_table, store_ctx, param, context))) {
      LOG_WARN("prepare param context fail, ", K(ret), K(table_id));
    } else if (OB_FAIL(write_memtable->lock(param, context, rowkey))) {
      LOG_WARN("failed to lock write memtable", K(ret), K(table_id), K(rowkey));
    }
  }
  return ret;
}


int ObTablet::check_row_locked_by_myself(
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    const blocksstable::ObDatumRowkey &rowkey,
    bool &locked)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), ObModIds::OB_STORE_ROW_LOCK_CHECKER));
  ObMemtable *write_memtable = nullptr;
  ObTableIterParam param;
  ObTableAccessContext context;
  locked = false;

  if (OB_FAIL(prepare_memtable(relative_table, store_ctx, write_memtable))) {
    LOG_WARN("prepare write memtable fail", K(ret), K(relative_table));
  } else if (OB_FAIL(prepare_param_ctx(allocator, relative_table, store_ctx, param, context))) {
    LOG_WARN("prepare param context fail, ", K(ret), K(rowkey));
  } else if (OB_TMP_FAIL(ObRowConflictHandler::check_row_locked(param, context, rowkey, true /* by_myself */))) {
    if (OB_TRY_LOCK_ROW_CONFLICT == tmp_ret) {
      locked = true;
    } else if (OB_TRANSACTION_SET_VIOLATION != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("failed to check row locked by myself", K(tmp_ret), K(rowkey));
    }
  }
  return ret;
}


int ObTablet::get_read_tables(
    const int64_t snapshot_version,
    ObTabletTableIterator &iter,
    const bool allow_no_ready_read)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(allow_to_read_())) {
    LOG_WARN("not allowed to read", K(ret), K(tablet_meta_));
  } else if (OB_UNLIKELY(!iter.is_valid() || iter.get_tablet() != this)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(iter), K(this));
  } else if (OB_FAIL(auto_get_read_tables(snapshot_version, iter, allow_no_ready_read))) {
    LOG_WARN("failed to get read tables", K(ret), K(snapshot_version), K(allow_no_ready_read));
  }

  return ret;
}

// TODO (wenjinyu.wjy) need to be moved to ObLSTableService in 4.3
int ObTablet::get_src_tablet_read_tables_(
    const int64_t snapshot_version,
    const bool allow_no_ready_read,
    ObTabletTableIterator &iter,
    bool &succ_get_src_tables)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  succ_get_src_tables = false;
  ObTabletCreateDeleteMdsUserData user_data;
  ObLSTabletService *tablet_service = nullptr;
  ObLSTabletService::AllowToReadMgr::AllowToReadInfo read_info;
  SCN max_decided_scn;
  if (OB_UNLIKELY(snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(snapshot_version));
  } else if (OB_FAIL(ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tablet status", K(ret), KPC(tablet), K(user_data));
  } else if (ObTabletStatus::TRANSFER_IN != user_data.tablet_status_) {
    ret = OB_SNAPSHOT_DISCARDED;
    LOG_WARN("tablet status is not match, need retry", K(ret), K(user_data), "tablet_id", tablet_meta_.tablet_id_);
  } else if (!user_data.transfer_ls_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table type is unexpected", K(ret), K(user_data));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(user_data.transfer_ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(user_data));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(user_data));
  } else if (OB_ISNULL(tablet_service = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls tablet service should not be NULL", K(ret), KP(ls));
  } else if (OB_FAIL(tablet_service->check_allow_to_read(read_info))) {
    if (OB_REPLICA_NOT_READABLE == ret) {
      LOG_WARN("replica unreadable", K(ret), "ls_id", ls->get_ls_id(), "tablet_id", tablet_meta_.tablet_id_, K(user_data));
    } else {
      LOG_WARN("failed to check allow to read", K(ret), "ls_id", ls->get_ls_id(), "tablet_id", tablet_meta_.tablet_id_, K(user_data));
    }
  } else if (OB_FAIL(ls->get_max_decided_scn(max_decided_scn))) {
    LOG_WARN("failed to log stream get decided scn", K(ret), "ls_id", ls->get_ls_id());
  } else if (max_decided_scn < user_data.transfer_scn_) {
    ret = OB_REPLICA_NOT_READABLE;
    LOG_WARN("src ls max decided scn is smaller than transfer start scn, replica unreadable",
      K(ret), "ls_id", ls->get_ls_id(), "tablet_id", tablet_meta_.tablet_id_, K(max_decided_scn), K(user_data));
  } else if (OB_FAIL(ls->get_tablet(tablet_meta_.tablet_id_, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), "ls_id", ls->get_ls_id(), "tablet_id", tablet_meta_.tablet_id_);
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(user_data));
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(iter.table_store_iter_.transfer_src_table_store_handle_)) {
      void *meta_hdl_buf = ob_malloc(sizeof(ObStorageMetaHandle), ObMemAttr(MTL_ID(), "TransferMetaH"));
      if (OB_ISNULL(meta_hdl_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocator memory for handles", K(ret));
      } else {
        iter.table_store_iter_.transfer_src_table_store_handle_ = new (meta_hdl_buf) ObStorageMetaHandle();
      }
    }

    if (FAILEDx(iter.set_transfer_src_tablet_handle(tablet_handle))) {
      LOG_WARN("failed to set transfer src tablet handle", K(ret));
    } else if (OB_FAIL(tablet->get_read_tables_(
        snapshot_version,
        iter.table_store_iter_,
        *(iter.table_store_iter_.transfer_src_table_store_handle_),
        allow_no_ready_read))) {
      LOG_WARN("failed to get read tables from table store", K(ret), KPC(tablet));
    } else if (OB_FAIL(tablet_service->check_read_info_same(read_info))) {
      LOG_WARN("failed to check read info same", K(ret), KPC(tablet));
    } else {
      succ_get_src_tables = true;
    }
  }

  return ret;
}

int ObTablet::auto_get_read_tables(
    const int64_t snapshot_version,
    ObTabletTableIterator &iter,
    const bool allow_no_ready_read)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  iter.table_store_iter_.reset();
  bool succ_get_src_tables = false;

  if (OB_UNLIKELY(tablet_meta_.has_transfer_table())) {
    if (OB_FAIL(get_src_tablet_read_tables_(snapshot_version, allow_no_ready_read, iter, succ_get_src_tables))) {
      LOG_WARN("failed to get src ls read tables", K(ret), K(ls_id), K(tablet_id),
          K(snapshot_version), "has_transfer_table", tablet_meta_.has_transfer_table());
    } else {
#ifdef ENABLE_DEBUG_LOG
      FLOG_INFO("get read tables during transfer",K(ret), K(ls_id), K(tablet_id),
          K(snapshot_version), "has_transfer_table", tablet_meta_.has_transfer_table(),
          K(iter.table_store_iter_.table_ptr_array_));
#endif
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    bool allow_not_ready = succ_get_src_tables ? true : allow_no_ready_read;
    if (OB_FAIL(get_read_tables_(snapshot_version, iter.table_store_iter_, iter.table_store_iter_.table_store_handle_, allow_not_ready))) {
      LOG_WARN("failed to get read tables from table store", K(ret), K(ls_id), K(tablet_id));
    }
  }
  return ret;
}

int ObTablet::get_read_tables_(
    const int64_t snapshot_version,
    ObTableStoreIterator &iter,
    ObStorageMetaHandle &table_store_handle,
    const bool allow_no_ready_read)
{
  int ret = OB_SUCCESS;
  const ObTabletTableStore *table_store = nullptr;
  if (OB_UNLIKELY(!table_store_addr_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table store addr", K(ret), K_(table_store_addr));
  } else if (table_store_addr_.is_memory_object()) {
    table_store = table_store_addr_.get_ptr();
  } else {
    ObStorageMetaKey meta_key(MTL_ID(), table_store_addr_.addr_);
    const ObStorageMetaValue *value = nullptr;
    if (OB_FAIL(OB_STORE_CACHE.get_storage_meta_cache().get_meta(
                ObStorageMetaValue::MetaType::TABLE_STORE, meta_key, table_store_handle, this))) {
      LOG_WARN("get meta failed", K(ret), K(meta_key));
    } else if (OB_FAIL(table_store_handle.get_value(value))) {
      LOG_WARN("fail to get cache value", K(ret), K(table_store_handle));
    } else if (OB_FAIL(value->get_table_store(table_store))) {
      LOG_WARN("fail to get tablet store", K(ret), KPC(value));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_store->get_read_tables(
        snapshot_version, *this, iter, allow_no_ready_read))) {
      LOG_WARN("fail to get read tables", K(ret), K(iter), K(snapshot_version));
    }
  }
  return ret;
}

int ObTablet::get_read_major_sstable(
    const int64_t &major_snapshot_version,
    ObTabletTableIterator &iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(allow_to_read_())) {
    LOG_WARN("not allowed to read", K(ret), K(tablet_meta_));
  } else if (OB_UNLIKELY(!iter.is_valid() || iter.get_tablet() != this)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(iter), K(this));
  } else if (OB_FAIL(get_read_major_sstable(major_snapshot_version, *iter.table_iter()))) {
    LOG_WARN("failed to get read tables", K(ret), K(major_snapshot_version));
  }
  return ret;
}

int ObTablet::get_read_major_sstable(
    const int64_t &major_snapshot_version,
    ObTableStoreIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member()->get_read_major_sstable(
      major_snapshot_version, iter))) {
    LOG_WARN("fail to get read tables", K(ret), K(table_store_wrapper), K(iter),
        K(major_snapshot_version));
  } else if (!table_store_addr_.is_memory_object()
      && OB_FAIL(iter.set_handle(table_store_wrapper.get_meta_handle()))) {
    LOG_WARN("fail to set storage meta handle", K(ret), K_(table_store_addr), K(table_store_wrapper));
  }
  return ret;
}

int ObTablet::get_ddl_memtables(common::ObIArray<ObITable *> &ddl_memtables) const
{
  int ret = OB_SUCCESS;
  ddl_memtables.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < ddl_kv_count_; ++i) {
    if (OB_ISNULL(ddl_kvs_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ddl mem table", K(ret), K(i), KPC(this));
    } else if (OB_FAIL(ddl_memtables.push_back(ddl_kvs_[i]))) {
      LOG_WARN("failed to push back ddl memtables", K(ret));
    }
  }
  return ret;
}

int ObTablet::get_all_sstables(ObTableStoreIterator &iter) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(inner_get_all_sstables(iter))) {
    LOG_WARN("fail to get all sstable", K(ret));
  }
  return ret;
}

int ObTablet::get_all_tables(ObTableStoreIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObSEArray<storage::ObITable *, MAX_MEMSTORE_CNT> memtables;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(inner_get_all_sstables(iter))) {
    LOG_WARN("fail to get all sstable", K(ret));
  } else if (OB_FAIL(get_memtables(memtables, true))) {
    LOG_WARN("fail to get memtables", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < memtables.count(); ++i) {
      if (OB_FAIL(iter.add_table(memtables.at(i)))) {
        LOG_WARN("fail to add memtable to iter", K(ret), K(memtables), K(iter));
      }
    }
  }
  return ret;
}

int ObTablet::inner_get_all_sstables(ObTableStoreIterator &iter) const
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member()->get_all_sstable(iter))) {
    LOG_WARN("fail to get all sstables", K(ret), K(table_store_wrapper));
  } else if (!table_store_addr_.is_memory_object()
      && OB_FAIL(iter.set_handle(table_store_wrapper.get_meta_handle()))) {
    LOG_WARN("fail to set storage meta handle", K(ret), K_(table_store_addr), K(table_store_wrapper));
  }
  return ret;
}

int ObTablet::get_sstables_size(int64_t &used_size, const bool ignore_shared_block) const
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;
  bool multi_version = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_all_sstables(table_store_iter))) {
    LOG_WARN("fail to get all sstables", K(ret));
  } else {
     while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      ObSSTable *sstable = nullptr;
      ObSSTableMetaHandle sstable_meta_hdl;
      if (OB_FAIL(table_store_iter.get_next(table))) {
        if (OB_UNLIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next table from iter", K(ret), K(table_store_iter));
        }
      } else if (FALSE_IT(sstable = static_cast<ObSSTable *>(table))) {
      } else if (OB_ISNULL(table)) {
      } else if (OB_FAIL(sstable->get_meta(sstable_meta_hdl))) {
        LOG_WARN("fail to get sstable meta handle", K(ret));
      } else if (sstable->is_small_sstable() && ignore_shared_block) {
        // skip small sstables
      } else {
        if (multi_version && sstable->is_major_sstable()) {
          used_size -= sstable_meta_hdl.get_sstable_meta().get_total_use_old_macro_block_count() * sstable->get_macro_read_size();
        } else if (sstable->is_major_sstable()) {
          multi_version = true;
        }
        used_size += sstable_meta_hdl.get_sstable_meta().get_total_macro_block_count() * sstable->get_macro_read_size();
      }
    }
    if (OB_SUCC(ret) && tablet_meta_.has_next_tablet_ && OB_FAIL(
        next_tablet_guard_.get_obj()->get_sstables_size(used_size, ignore_shared_block /*whether ignore shared block*/))) {
      LOG_WARN("failed to get size of tablets on the list", K(ret), K(used_size));
    }
  }
  return ret;
}

int ObTablet::get_memtables(common::ObIArray<storage::ObITable *> &memtables, const bool need_active) const
{
  common::SpinRLockGuard guard(memtables_lock_);
  return inner_get_memtables(memtables, need_active);
}

int ObTablet::check_need_remove_old_table(
    const int64_t multi_version_start,
    bool &need_remove) const
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member()->need_remove_old_table(
      multi_version_start, need_remove))) {
    LOG_WARN("failed to check need rebuild table store", K(ret), K(multi_version_start));
  }

  return ret;
}

int ObTablet::update_upper_trans_version(ObLS &ls, bool &is_updated)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  is_updated = false;
  bool is_paused = false;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObTableStoreIterator iter(false/*is_reverse*/, false/*need_load_sstable*/);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (FALSE_IT(is_paused = false)) { // TODO(DanLing) get is_paused
  } else if (is_paused) {
    LOG_INFO("paused, cannot update trans version now", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member()->get_mini_minor_sstables(
      true/*is_ha_data_status_complete*/, iter))) {
    LOG_WARN("fail to get mini minor sstable", K(ret), K(table_store_wrapper));
  } else {
    ObITable *table = nullptr;
    while (OB_SUCC(ret) && OB_SUCC(iter.get_next(table))) {
      if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, table is nullptr", K(ret), KPC(table));
      } else {
        ObSSTable *sstable = reinterpret_cast<ObSSTable *>(table);
        if (INT64_MAX == sstable->get_upper_trans_version()) {
          int64_t max_trans_version = INT64_MAX;
          SCN tmp_scn = SCN::max_scn();
          if (OB_FAIL(ls.get_upper_trans_version_before_given_scn(sstable->get_end_scn(), tmp_scn))) {
            LOG_WARN("failed to get upper trans version before given log ts", K(ret), KPC(sstable));
          } else if (FALSE_IT(max_trans_version = tmp_scn.get_val_for_tx())) {
          } else if (0 == max_trans_version) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("max trans version should not be 0", KPC(sstable));
          } else if (INT64_MAX != max_trans_version) {
            if (OB_UNLIKELY(0 == max_trans_version)) {
              FLOG_INFO("get max_trans_version = 0, maybe all the trans have been rollbacked", K(ret), K(ls_id), K(tablet_id),
                  K(max_trans_version), KPC(sstable));
            }
            if (OB_FAIL(sstable->set_upper_trans_version(max_trans_version))) {
              LOG_WARN("failed to set_upper_trans_version", K(ret), KPC(sstable));
            } else {
              is_updated = true;
              FLOG_INFO("success to update sstable's upper trans version", K(ret), K(ls_id), K(tablet_id),
                  K(max_trans_version), KPC(sstable));
            }
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObTablet::insert_row(
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    const ObColDescIArray &col_descs,
    const ObStoreRow &row)
{
  int ret = OB_SUCCESS;
  bool b_exist = false;
  common::ObIArray<transaction::ObEncryptMetaCache> *encrypt_meta_arr = NULL;
  if (OB_UNLIKELY(!store_ctx.is_valid() || col_descs.count() <= 0 || !row.is_valid()
      || !relative_table.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(store_ctx), K(col_descs), K(row), K(ret));
  } else {
    const bool check_exists = !relative_table.is_storage_index_table()
                              || relative_table.is_unique_index();
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get rowkey columns");
    } else if (check_exists
        && OB_FAIL(rowkey_exists(relative_table, store_ctx, row.row_val_, b_exist))) {
      LOG_WARN("failed to check whether row exists", K(row), K(ret));
    } else if (OB_UNLIKELY(b_exist)) {
      ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
      LOG_WARN("rowkey already exists",  K(relative_table.get_table_id()), K(row), K(ret));
    } else if (OB_FAIL(insert_row_without_rowkey_check(relative_table, store_ctx, col_descs, row, encrypt_meta_arr))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("failed to set row", K(row), K(ret));
      }
    }
  }
  return ret;
}

int ObTablet::update_row(
    ObRelativeTable &relative_table,
    storage::ObStoreCtx &store_ctx,
    const common::ObIArray<share::schema::ObColDesc> &col_descs,
    const ObIArray<int64_t> &update_idx,
    const storage::ObStoreRow &old_row,
    const storage::ObStoreRow &new_row,
    const common::ObIArray<transaction::ObEncryptMetaCache> *encrypt_meta_arr)
{
  int ret = OB_SUCCESS;

  {
    ObStorageTableGuard guard(this, store_ctx, true);
    ObMemtable *write_memtable = nullptr;
    const transaction::ObSerializeEncryptMeta *encrypt_meta = NULL;

    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not inited", K(ret), K_(is_inited));
    } else if (OB_UNLIKELY(!store_ctx.is_valid()
        || col_descs.count() <= 0
        || !old_row.is_valid()
        || !new_row.is_valid()
        || !relative_table.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(ret), K(store_ctx),
          K(relative_table), K(col_descs), K(update_idx),
          K(old_row), K(new_row));
    } else if (OB_UNLIKELY(relative_table.get_tablet_id() != tablet_meta_.tablet_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet id doesn't match", K(ret), K(relative_table.get_tablet_id()), K(tablet_meta_.tablet_id_));
    } else if (OB_FAIL(try_update_storage_schema(relative_table.get_table_id(),
        relative_table.get_schema_version(),
        store_ctx.mvcc_acc_ctx_.get_mem_ctx()->get_query_allocator(),
        store_ctx.timeout_))) {
      LOG_WARN("fail to record table schema", K(ret));
    } else if (OB_FAIL(guard.refresh_and_protect_table(relative_table))) {
      LOG_WARN("fail to protect table", K(ret));
    } else if (OB_FAIL(prepare_memtable(relative_table, store_ctx, write_memtable))) {
      LOG_WARN("prepare write memtable fail", K(ret), K(relative_table));
#ifdef OB_BUILD_TDE_SECURITY
    // XXX we do not turn on clog encryption now
    } else if (false && NULL != encrypt_meta_arr && !encrypt_meta_arr->empty() &&
      FALSE_IT(get_encrypt_meta(relative_table.get_table_id(), encrypt_meta_arr, encrypt_meta))) {
#endif
    } else {
      ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), ObModIds::OB_STORE_ROW_EXISTER));
      ObTableIterParam param;
      ObTableAccessContext context;
      if (OB_FAIL(prepare_param_ctx(allocator, relative_table, store_ctx, param, context))) {
        LOG_WARN("prepare param ctx fail, ", K(ret));
      } else if (OB_FAIL(write_memtable->set(param, context, col_descs, update_idx, old_row, new_row, encrypt_meta))) {
        LOG_WARN("failed to set memtable, ", K(ret));
      }
    }
  }

  return ret;
}

int ObTablet::insert_row_without_rowkey_check(
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    const common::ObIArray<share::schema::ObColDesc> &col_descs,
    const storage::ObStoreRow &row,
    const common::ObIArray<transaction::ObEncryptMetaCache> *encrypt_meta_arr)
{
  int ret = OB_SUCCESS;
  {
    ObStorageTableGuard guard(this, store_ctx, true);
    ObMemtable *write_memtable = nullptr;
    const transaction::ObSerializeEncryptMeta *encrypt_meta = NULL;

    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not inited", K(ret), K_(is_inited));
    } else if (OB_UNLIKELY(!store_ctx.is_valid()
        || col_descs.count() <= 0
        || !row.is_valid()
        || !relative_table.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(ret), K(store_ctx), K(relative_table),
          K(col_descs), K(row));
    } else if (OB_UNLIKELY(relative_table.get_tablet_id() != tablet_meta_.tablet_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet id doesn't match", K(ret), K(relative_table.get_tablet_id()), K(tablet_meta_.tablet_id_));
    } else if (OB_FAIL(try_update_storage_schema(relative_table.get_table_id(),
        relative_table.get_schema_version(),
        store_ctx.mvcc_acc_ctx_.get_mem_ctx()->get_query_allocator(),
        store_ctx.timeout_))) {
      LOG_WARN("fail to record table schema", K(ret));
    } else if (OB_FAIL(guard.refresh_and_protect_table(relative_table))) {
      LOG_WARN("fail to protect table", K(ret));
    } else if (OB_FAIL(prepare_memtable(relative_table, store_ctx, write_memtable))) {
      LOG_WARN("prepare write memtable fail", K(ret), K(relative_table));
#ifdef OB_BUILD_TDE_SECURITY
    // XXX we do not turn on clog encryption now
    } else if (false && NULL != encrypt_meta_arr && !encrypt_meta_arr->empty() &&
      FALSE_IT(get_encrypt_meta(relative_table.get_table_id(), encrypt_meta_arr, encrypt_meta))) {
#endif
    } else {
      ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), ObModIds::OB_STORE_ROW_EXISTER));
      ObTableIterParam param;
      ObTableAccessContext context;
      if (OB_FAIL(prepare_param_ctx(allocator, relative_table, store_ctx, param, context))) {
        LOG_WARN("prepare param ctx fail, ", K(ret));
      } else if (OB_FAIL(write_memtable->set(param, context, col_descs, row, encrypt_meta))) {
        LOG_WARN("fail to set memtable", K(ret));
      }
    }
  }
  return ret;
}

int ObTablet::do_rowkey_exists(
    ObTableIterParam &param,
    ObTableAccessContext &context,
    const blocksstable::ObDatumRowkey &rowkey,
    bool &exists)
{
  int ret = OB_SUCCESS;
  ObTabletTableIterator table_iter(true/*reverse_iter*/);
  if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(rowkey));
  } else if (OB_FAIL(allow_to_read_())) {
    LOG_WARN("not allowed to read", K(ret), K(tablet_meta_));
  } else if (OB_FAIL(auto_get_read_tables(
     context.store_ctx_->mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx(), table_iter, context.query_flag_.index_invalid_))) {
    LOG_WARN("get read iterator fail", K(ret));
  } else {
    bool found = false;
    ObITable *table = nullptr;
    int64_t check_table_cnt = 0;
    while (OB_SUCC(ret) && !found) {
      if (OB_FAIL(table_iter.table_store_iter_.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next tables", K(ret));
        }
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table must not be null", K(ret), K(table_iter));
      } else if (OB_FAIL(table->exist(param, context, rowkey, exists, found))) {
        LOG_WARN("Fail to check if exist in store", K(ret), KPC(table));
      } else {
        ++check_table_cnt;
        LOG_DEBUG("rowkey_exists check", KPC(table), K(rowkey), K(exists), K(found), K(table_iter));
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      int tmp_ret = OB_SUCCESS;
      if (0 == context.store_ctx_->tablet_stat_.query_cnt_) {
        // ROWKEY IN_ROW_CACHE / NOT EXIST
      } else if (FALSE_IT(context.store_ctx_->tablet_stat_.exist_row_read_table_cnt_ = check_table_cnt)) {
      } else if (FALSE_IT(context.store_ctx_->tablet_stat_.exist_row_total_table_cnt_ = table_iter.table_store_iter_.count())) {
      } else if (MTL(ObTenantTabletScheduler *)->enable_adaptive_compaction()) {
        bool report_succ = false; /*placeholder*/
        if (OB_TMP_FAIL(MTL(ObTenantTabletStatMgr *)->report_stat(context.store_ctx_->tablet_stat_, report_succ))) {
          LOG_WARN("failed to report tablet stat", K(tmp_ret), K(stat));
        }
      }
    }

    if (OB_SUCCESS == ret && false == found) {
      exists = false;
    }
  }
  return ret;
}

int ObTablet::do_rowkeys_exist(ObTableStoreIterator &tables_iter, ObRowsInfo &rows_info, bool &exists)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(tables_iter.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument-tables_iter", K(ret), K(tables_iter.count()));
  }
  bool all_rows_found = false;
  int64_t check_table_cnt = 0;
  while (OB_SUCC(ret) && !exists && !all_rows_found) {
    ObITable *table = nullptr;
    if (OB_FAIL(tables_iter.get_next(table))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      }
      LOG_WARN("fail to get next table", K(ret), KP(table));
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the table is nullptr", K(ret));
    } else if (OB_FAIL(table->exist(rows_info, exists, all_rows_found))) {
      LOG_WARN("fail to check the existence of rows", K(ret), K(rows_info), K(exists));
    } else {
      ++check_table_cnt;
      LOG_DEBUG("rowkey exists check", K(rows_info), K(exists));
    }
  }

  if (OB_SUCC(ret)) {
    ObTabletStat tablet_stat;
    const ObTableAccessContext &access_ctx = rows_info.exist_helper_.table_access_context_;
    tablet_stat.ls_id_ = access_ctx.ls_id_.id();
    tablet_stat.tablet_id_ = access_ctx.tablet_id_.id();
    tablet_stat.query_cnt_ = 1;
    tablet_stat.exist_row_read_table_cnt_ = check_table_cnt;
    tablet_stat.exist_row_total_table_cnt_ = tables_iter.count();
    int tmp_ret = OB_SUCCESS;
    if (0 == access_ctx.table_store_stat_.exist_row_.empty_read_cnt_) {
      // ROWKEY IN_ROW_CACHE / NOT EXIST
    } else if (MTL(ObTenantTabletScheduler *)->enable_adaptive_compaction()) {
      bool report_succ = false; /*placeholder*/
      if (OB_TMP_FAIL(MTL(ObTenantTabletStatMgr *)->report_stat(tablet_stat, report_succ))) {
        LOG_WARN("failed to report tablet stat", K(tmp_ret), K(tablet_stat));
      }
    }
  }
  return ret;
}

int ObTablet::rowkey_exists(
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    const common::ObNewRow &row,
    bool &exists)
{
  int ret = OB_SUCCESS;
  const bool read_latest = true;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!store_ctx.is_valid() || !row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(store_ctx), K(row));
  } else if (OB_UNLIKELY(relative_table.get_tablet_id() != tablet_meta_.tablet_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet id doesn't match", K(ret), K(relative_table.get_tablet_id()), K(tablet_meta_.tablet_id_));
  } else if (OB_FAIL(allow_to_read_())) {
    LOG_WARN("not allowed to read", K(ret), K(tablet_meta_));
  } else {
    {
      ObStorageTableGuard guard(this, store_ctx, false);
      if (OB_FAIL(guard.refresh_and_protect_table(relative_table))) {
        LOG_WARN("fail to protect table", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObStoreRowkey rowkey;
      ObDatumRowkey datum_rowkey;
      ObDatumRowkeyHelper rowkey_helper;
      ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), ObModIds::OB_STORE_ROW_EXISTER));
      ObTableIterParam param;
      ObTableAccessContext context;

      if (OB_FAIL(rowkey.assign(row.cells_, relative_table.get_rowkey_column_num()))) {
        LOG_WARN("Failed to assign rowkey", K(ret), K(row));
      } else if (OB_FAIL(rowkey_helper.convert_datum_rowkey(rowkey.get_rowkey(), datum_rowkey))) {
        LOG_WARN("Failed to transfer datum rowkey", K(ret), K(rowkey));
      } else if (OB_FAIL(prepare_param_ctx(allocator, relative_table, store_ctx, param, context))) {
        LOG_WARN("Failed to prepare param ctx, ", K(ret), K(rowkey));
      } else if (OB_FAIL(relative_table.tablet_iter_.get_tablet()->do_rowkey_exists(
              param, context, datum_rowkey, exists))) {
        LOG_WARN("do rowkey exist fail", K(ret), K(rowkey));
      }
      LOG_DEBUG("chaser debug row", K(ret), K(row), K(rowkey));
    }
  }
  return ret;
}

int ObTablet::rowkeys_exists(
    ObStoreCtx &store_ctx,
    ObRelativeTable &relative_table,
    ObRowsInfo &rows_info,
    bool &exists)
{
  int ret = OB_SUCCESS;
  ObTabletTableIterator tables_iter(true/*reverse_iter*/);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!rows_info.is_valid() || !relative_table.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(rows_info), K(relative_table));
  } else if (OB_UNLIKELY(relative_table.get_tablet_id() != tablet_meta_.tablet_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet id doesn't match", K(ret), K(relative_table.get_tablet_id()), K(tablet_meta_.tablet_id_));
  } else if (OB_FAIL(allow_to_read_())) {
    LOG_WARN("not allowed to read", K(ret), K(tablet_meta_));
  } else {
    {
      ObStorageTableGuard guard(this, store_ctx, false);
      if (OB_FAIL(guard.refresh_and_protect_table(relative_table))) {
        LOG_WARN("fail to protect table", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(relative_table.tablet_iter_.get_tablet()->auto_get_read_tables(
              store_ctx.mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx(),
              tables_iter,
              relative_table.allow_not_ready()))) {
        LOG_WARN("get read iterator fail", K(ret));
      } else if (OB_FAIL(relative_table.tablet_iter_.get_tablet()->do_rowkeys_exist(
              tables_iter.table_store_iter_, rows_info, exists))) {
        LOG_WARN("fail to check the existence of rows", K(ret), K(rows_info), K(exists));
      }
    }
  }

  return ret;
}

int ObTablet::prepare_memtable(
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    memtable::ObMemtable *&write_memtable)
{
  int ret = OB_SUCCESS;
  write_memtable = nullptr;
  store_ctx.table_iter_ = relative_table.tablet_iter_.table_iter();
  ObITable* last_table = nullptr;
  if (OB_FAIL(relative_table.tablet_iter_.table_iter()->get_boundary_table(true, last_table))) {
    LOG_WARN("fail to get last table from iter", K(ret));
  } else if (OB_ISNULL(last_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("last table is null", K(relative_table));
  } else if (OB_UNLIKELY(!last_table->is_data_memtable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("last table is not memtable", K(ret), K(*last_table));
  } else {
    write_memtable = reinterpret_cast<ObMemtable*>(last_table);
  }
  return ret;
}

int ObTablet::choose_and_save_storage_schema(
    common::ObArenaAllocator &allocator,
    const ObStorageSchema &tablet_schema,
    const ObStorageSchema &param_schema)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const ObTabletID &tablet_id = tablet_meta_.tablet_id_;

  const ObStorageSchema *chosen_schema = nullptr;
  ObStorageSchema *tmp_storage_schema = nullptr;

  int64_t tablet_schema_version = 0;
  int64_t param_schema_version = 0;
  int64_t tablet_schema_stored_col_cnt = 0;
  int64_t param_schema_stored_col_cnt = 0;
  if (OB_UNLIKELY(!tablet_schema.is_valid()) || OB_UNLIKELY(!param_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input schema is invalid", K(ret), K(ls_id), K(tablet_id), K(tablet_schema), K(param_schema));
  } else if (FALSE_IT(tablet_schema_version = tablet_schema.schema_version_)) {
  } else if (FALSE_IT(param_schema_version = param_schema.schema_version_)) {
  } else if (OB_FAIL(tablet_schema.get_store_column_count(tablet_schema_stored_col_cnt, true/*full_col*/))) {
    LOG_WARN("failed to get stored column count from schema", KR(ret), K(ls_id), K(tablet_id), K(tablet_schema));
  } else if (OB_FAIL(param_schema.get_store_column_count(param_schema_stored_col_cnt, true/*full_col*/))) {
    LOG_WARN("failed to get stored column count from schema", KR(ret), K(ls_id), K(tablet_id), K(param_schema));
  } else if (param_schema_version >= tablet_schema_version && param_schema_stored_col_cnt >= tablet_schema_stored_col_cnt) {
    // use param schema totally
    chosen_schema = &param_schema;
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, tmp_storage_schema))) {
    LOG_WARN("failed to alloc mem for tmp storage schema", K(ret), K(param_schema), K(tablet_schema));
  } else if (OB_FAIL(tmp_storage_schema->init(allocator, param_schema, true/*column_info_simplified*/))) {
    LOG_WARN("failed to init storage schema", K(ret), K(param_schema));
    ObTablet::free_storage_schema(allocator, tmp_storage_schema);
    tmp_storage_schema = nullptr;
  } else {
    tmp_storage_schema->column_cnt_ = MAX(tablet_schema.get_column_count(), param_schema.get_column_count());
    tmp_storage_schema->store_column_cnt_ = MAX(tablet_schema_stored_col_cnt, param_schema_stored_col_cnt);
    tmp_storage_schema->schema_version_ = MAX(tablet_schema_version, param_schema_version);
    chosen_schema = tmp_storage_schema;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(chosen_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("chosen schema is unexpected null", K(ret), K(param_schema), K(tablet_schema));
  } else {
    ALLOC_AND_INIT(allocator, storage_schema_addr_, *chosen_schema, true/*skip_column_info*/);
  }
  ObTablet::free_storage_schema(allocator, tmp_storage_schema);
  return ret;
}

int ObTablet::get_meta_disk_addr(ObMetaDiskAddr &addr) const
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet_meta_.ls_id_;
  const ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  ObTabletPointer *tablet_ptr = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(tablet_ptr = static_cast<ObTabletPointer*>(pointer_hdl_.get_resource_ptr()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet pointer is null", K(ret), K(ls_id), K(tablet_id));
  } else {
    addr = tablet_ptr->get_addr();
  }

  return ret;
}

int ObTablet::assign_pointer_handle(const ObTabletPointerHandle &ptr_hdl)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pointer_hdl_.assign(ptr_hdl))) {
    LOG_WARN("assign tablet ptr fail", K(ret));
  }
  return ret;
}

int ObTablet::replay_update_storage_schema(
    const SCN &scn,
    const char *buf,
    const int64_t buf_size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  ObTabletMemtableMgr *data_memtable_mgr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (tablet_meta_.tablet_id_.is_special_merge_tablet()) {
    // do nothing
  } else if (OB_FAIL(get_tablet_memtable_mgr(data_memtable_mgr))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else if (OB_FAIL(data_memtable_mgr->get_storage_schema_recorder().replay_schema_log(scn, buf, buf_size, new_pos))) {
    LOG_WARN("storage schema recorder replay fail", K(ret), K(scn));
  } else {
    pos = new_pos;
  }
  if (OB_TIMEOUT == ret) {
    ret = OB_EAGAIN; // need retry.
  }
  return ret;
}

int ObTablet::submit_medium_compaction_clog(
    ObMediumCompactionInfo &medium_info,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObTabletMemtableMgr *data_memtable_mgr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (tablet_meta_.tablet_id_.is_special_merge_tablet()) {
    // do nothing
  } else if (OB_FAIL(get_tablet_memtable_mgr(data_memtable_mgr))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else if (OB_FAIL(data_memtable_mgr->get_medium_info_recorder().submit_medium_compaction_info(
      medium_info, allocator))) {
    LOG_WARN("medium compaction recorder submit fail", K(ret), K(medium_info));
  } else {
    LOG_DEBUG("success to submit medium compaction clog", K(medium_info));
  }
  return ret;
}

int ObTablet::replay_medium_compaction_clog(
    const share::SCN &scn,
    const char *buf,
    const int64_t buf_size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  ObTabletMemtableMgr *data_memtable_mgr = nullptr;

  if (IS_NOT_INIT) {
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(buf_size <= pos || pos < 0 || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf_size), K(pos));
  } else if (tablet_meta_.tablet_id_.is_ls_inner_tablet()) {
    // do nothing
  } else if (OB_FAIL(get_tablet_memtable_mgr(data_memtable_mgr))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else if (OB_FAIL(data_memtable_mgr->get_medium_info_recorder().replay_medium_compaction_log(scn, buf, buf_size, new_pos))) {
    LOG_WARN("medium compaction recorder replay fail", K(ret), KPC(this), K(buf_size), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

int ObTablet::get_schema_version_from_storage_schema(int64_t &schema_version) const
{
  int ret = OB_SUCCESS;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  const ObStorageSchema *storage_schema = nullptr;
  ObArenaAllocator arena_allocator(common::ObMemAttr(MTL_ID(), "TmpSchema"));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited), K(tablet_id));
  } else if (OB_FAIL(load_storage_schema(arena_allocator, storage_schema))) {
    LOG_WARN("fail to load storage schema", K(ret), K_(storage_schema_addr));
  } else {
    schema_version = storage_schema->schema_version_;
  }
  ObTablet::free_storage_schema(arena_allocator, storage_schema);
  return ret;
}

int ObTablet::get_active_memtable(ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;
  ObIMemtableMgr *memtable_mgr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_memtable_mgr(memtable_mgr))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else if (OB_FAIL(memtable_mgr->get_active_memtable(handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get active memtable for tablet", K(ret), K(*this));
    }
  }
  return ret;
}

int ObTablet::create_memtable(
    const int64_t schema_version,
    const SCN clog_checkpoint_scn,
    const bool for_replay)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  ObTimeGuard time_guard("ObTablet::create_memtable", 10 * 1000);
  common::SpinWLockGuard guard(memtables_lock_);
  time_guard.click("lock");
  const SCN new_clog_checkpoint_scn = clog_checkpoint_scn.is_min() ? tablet_meta_.clog_checkpoint_scn_ : clog_checkpoint_scn;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(schema_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema version", K(ret), K(schema_version));
  } else if (FALSE_IT(time_guard.click("prepare_memtables"))) {
  } else if (OB_FAIL(inner_create_memtable(new_clog_checkpoint_scn, schema_version, for_replay))) {
    if (OB_ENTRY_EXIST == ret) {
      ret = OB_SUCCESS;
    } else if (OB_MINOR_FREEZE_NOT_ALLOW != ret) {
      LOG_WARN("failed to create memtable", K(ret), K(clog_checkpoint_scn),
               K(schema_version), K(for_replay));
    }
  } else if (FALSE_IT(time_guard.click("inner_create_memtable"))) {
  } else if (OB_FAIL(update_memtables())) {
    LOG_WARN("failed to append new memtable to table store", K(ret), KPC(this));
    if (OB_SIZE_OVERFLOW == ret) {
      // rewrite errno to OB_EAGAIN when memtable count overflow, in case to stuck the log relpay engine
      ret = OB_EAGAIN;
    }
  } else if (FALSE_IT(time_guard.click("update_memtables"))) {
  } else {
    tablet_addr_.inc_seq();
    table_store_addr_.addr_.inc_seq();

    if (table_store_addr_.is_memory_object()) {
      ObSEArray<ObITable *, MAX_MEMSTORE_CNT> memtable_array;
      if (OB_FAIL(inner_get_memtables(memtable_array, true/*need_active*/))) {
        LOG_WARN("inner get memtables fail", K(ret), K(*this));
      } else if (OB_FAIL(table_store_addr_.get_ptr()->update_memtables(memtable_array))) {
        LOG_WARN("table store update memtables fail", K(ret), K(memtable_array));
      } else {
       time_guard.click("ts update mem");
       LOG_INFO("table store update memtable success", K(ret), K(ls_id), K(tablet_id), K_(table_store_addr), KP(this));
      }
    }
  }

  return ret;
}

int ObTablet::inner_create_memtable(
    const SCN clog_checkpoint_scn,
    const int64_t schema_version,
    const bool for_replay)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  ObIMemtableMgr *memtable_mgr = nullptr;

  if (OB_UNLIKELY(!clog_checkpoint_scn.is_valid_and_not_min()) || OB_UNLIKELY(schema_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(clog_checkpoint_scn), K(schema_version));
  } else if (OB_UNLIKELY(MAX_MEMSTORE_CNT == memtable_count_)) {
    ret = OB_MINOR_FREEZE_NOT_ALLOW;
    if (TC_REACH_TIME_INTERVAL(1000 * 1000)) {
      LOG_WARN("The memtable array in the tablet reaches the upper limit, and no more memtable can "
          "be created", K(ret), K(memtable_count_), KPC(this));
    }
  } else if (OB_FAIL(get_memtable_mgr(memtable_mgr))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else if (OB_FAIL(memtable_mgr->create_memtable(clog_checkpoint_scn, schema_version, tablet_meta_.clog_checkpoint_scn_, for_replay))) {
    if (OB_ENTRY_EXIST != ret && OB_MINOR_FREEZE_NOT_ALLOW != ret) {
      LOG_WARN("failed to create memtable for tablet", K(ret), K(ls_id), K(tablet_id),
          K(clog_checkpoint_scn), K(schema_version), K(for_replay));
    }
  } else {
    LOG_INFO("succeeded to create memtable for tablet", K(ret), K(ls_id), K(tablet_id),
        K(clog_checkpoint_scn), K(schema_version));
  }

  return ret;
}

int ObTablet::inner_get_memtables(common::ObIArray<storage::ObITable *> &memtables, const bool need_active) const
{
  int ret = OB_SUCCESS;
  memtables.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < memtable_count_; ++i) {
    if (OB_ISNULL(memtables_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("memtable must not null", K(ret), K(memtables_));
    } else if (!need_active && memtables_[i]->is_active_memtable()) {
      continue;
    } else if (OB_FAIL(memtables.push_back(memtables_[i]))) {
      LOG_WARN("failed to add memtables", K(ret), K(*this));
    }
  }
  return ret;
}

int ObTablet::rebuild_memtables(const share::SCN scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(release_memtables(scn))) {
    LOG_WARN("fail to release memtables", K(ret), K(scn));
  } else {
    reset_memtable();
    if (OB_FAIL(pull_memtables_without_ddl())) {
      LOG_WARN("fail to pull memtables without ddl", K(ret));
    } else {
      tablet_addr_.inc_seq();
      table_store_addr_.addr_.inc_seq();
      if (table_store_addr_.is_memory_object()) {
        ObSEArray<ObITable *, MAX_MEMSTORE_CNT> memtable_array;
        if (OB_FAIL(table_store_addr_.get_ptr()->clear_memtables())) {
          LOG_WARN("fail to clear memtables", K(ret));
        } else if (OB_FAIL(inner_get_memtables(memtable_array, true/*need_active*/))) {
          LOG_WARN("inner get memtables fail", K(ret), K(*this));
        } else if (OB_FAIL(table_store_addr_.get_ptr()->update_memtables(memtable_array))) {
          LOG_WARN("table store update memtables fail", K(ret), K(memtable_array));
        } else {
          LOG_INFO("table store update memtable success", KPC(this));
        }
      }
    }
  }
  return ret;
}

int ObTablet::release_memtables(const SCN scn)
{
  int ret = OB_SUCCESS;
  ObIMemtableMgr *memtable_mgr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_memtable_mgr(memtable_mgr))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else if (OB_FAIL(memtable_mgr->release_memtables(scn))) {
    LOG_WARN("failed to release memtables", K(ret), K(scn));
  }

  return ret;
}

int ObTablet::release_memtables()
{
  int ret = OB_SUCCESS;
  ObIMemtableMgr *memtable_mgr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (is_empty_shell()) {
    LOG_DEBUG("tablet is empty shell", K(ret));
  } else if (OB_FAIL(get_memtable_mgr(memtable_mgr))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else if (OB_FAIL(memtable_mgr->release_memtables())) {
    LOG_WARN("failed to release memtables", K(ret));
  }

  return ret;
}

int ObTablet::wait_release_memtables()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(wait_release_memtables_())) {
    LOG_WARN("failed to release memtables", K(ret));
  }

  return ret;
}

int ObTablet::wait_release_memtables_()
{
  int ret = OB_SUCCESS;
  ObIMemtableMgr *memtable_mgr = nullptr;

  if (OB_FAIL(get_memtable_mgr(memtable_mgr))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else {
    const int64_t start = ObTimeUtility::current_time();
    do {
      if (OB_FAIL(memtable_mgr->release_memtables())) {
        const int64_t cost_time = ObTimeUtility::current_time() - start;
        if (cost_time > 1000 * 1000) {
          if (TC_REACH_TIME_INTERVAL(1000 * 1000)) {
            LOG_WARN("failed to release memtables", K(ret), KPC(memtable_mgr));
          }
        }
      }
    } while (OB_FAIL(ret));
  }

  return ret;
}

int ObTablet::mark_mds_table_switched_to_empty_shell_()
{
  int64_t ret = OB_SUCCESS;
  mds::MdsTableHandle mds_table;

  if (OB_FAIL(inner_get_mds_table(mds_table))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get mds table", K(ret));
    }
  } else if (OB_FAIL(mds_table.mark_switched_to_empty_shell())) {
    LOG_WARN("failed to mark mds table switched to empty shell", K(ret));
  }

  return ret;
}

int ObTablet::get_memtable_mgr(ObIMemtableMgr *&memtable_mgr) const
{
  int ret = OB_SUCCESS;

  ObTabletPointer *tablet_ptr = static_cast<ObTabletPointer*>(pointer_hdl_.get_resource_ptr());
  ObMemtableMgrHandle &memtable_mgr_handle = tablet_ptr->memtable_mgr_handle_;
  if (OB_UNLIKELY(!memtable_mgr_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memtable mgr handle is invalid", K(ret), K(memtable_mgr_handle));
  } else {
    memtable_mgr = memtable_mgr_handle.get_memtable_mgr();
  }

  return ret;
}

int ObTablet::get_tablet_memtable_mgr(ObTabletMemtableMgr *&tablet_memtable_mgr) const
{
  int ret = OB_SUCCESS;
  tablet_memtable_mgr = nullptr;
  ObIMemtableMgr *memtable_mgr = nullptr;
  if (tablet_meta_.tablet_id_.is_ls_inner_tablet()) {
    // do nothing
  } else if (OB_FAIL(get_memtable_mgr(memtable_mgr))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else {
    tablet_memtable_mgr = static_cast<ObTabletMemtableMgr *>(memtable_mgr);
  }
  return ret;
}

int ObTablet::get_ddl_kv_mgr(ObDDLKvMgrHandle &ddl_kv_mgr_handle, bool try_create)
{
  int ret = OB_SUCCESS;
  ddl_kv_mgr_handle.reset();
  if (!pointer_hdl_.is_valid()) {
    if (try_create) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet pointer not valid", K(ret));
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_DEBUG("tablet pointer not valid", K(ret));
    }
  } else {
    ObTabletPointer *tablet_ptr = static_cast<ObTabletPointer*>(pointer_hdl_.get_resource_ptr());
    if (try_create) {
      if (OB_FAIL(tablet_ptr->create_ddl_kv_mgr(tablet_meta_.ls_id_, tablet_meta_.tablet_id_, ddl_kv_mgr_handle))) {
        LOG_WARN("create ddl kv mgr failed", K(ret), K(tablet_meta_));
      }
    } else {
      tablet_ptr->get_ddl_kv_mgr(ddl_kv_mgr_handle);
      if (!ddl_kv_mgr_handle.is_valid()) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_DEBUG("ddl kv mgr not exist", K(ret), K(ddl_kv_mgr_handle));
      }
    }
  }
  return ret;
}

int ObTablet::set_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  ObTabletPointer *tablet_pointer = static_cast<ObTabletPointer *>(pointer_hdl_.get_resource_ptr());
  if (OB_FAIL(tablet_pointer->set_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    LOG_WARN("set ddl kv mgr failed", K(ret));
  }
  return ret;
}

int ObTablet::remove_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  ObTabletPointer *tablet_pointer = static_cast<ObTabletPointer *>(pointer_hdl_.get_resource_ptr());
  if (OB_FAIL(tablet_pointer->remove_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    LOG_WARN("remove ddl kv mgr failed", K(ret));
  }
  return ret;
}

int ObTablet::init_shared_params(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t max_saved_schema_version, // for init storage_schema_recorder on MemtableMgr
    const int64_t max_saved_medium_scn, // for init medium_recorder on MemtableMgr
    const lib::Worker::CompatMode compat_mode,
    ObFreezer *freezer)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!pointer_hdl_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet pointer handle is invalid", K(ret), K_(pointer_hdl));
  } else {
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    ObIMemtableMgr *memtable_mgr = nullptr;

    if (OB_FAIL(get_memtable_mgr(memtable_mgr))) {
      LOG_WARN("failed to get memtable mgr", K(ret));
    } else if (OB_FAIL(memtable_mgr->init(
            tablet_id,
            ls_id,
            max_saved_schema_version,
            max_saved_medium_scn,
            compat_mode,
            log_handler_,
            freezer,
            t3m))) {
      LOG_WARN("failed to init memtable mgr", K(ret), K(tablet_id), K(ls_id), KP(freezer));
    }
  }

  return ret;
}

int ObTablet::build_read_info(common::ObArenaAllocator &allocator, const ObTablet *tablet)
{
  int ret = OB_SUCCESS;
  int64_t full_stored_col_cnt = 0;
  common::ObArenaAllocator tmp_allocator(common::ObMemAttr(MTL_ID(), "TmpSchema"));
  const ObStorageSchema *storage_schema = nullptr;
  ObSEArray<share::schema::ObColDesc, 16> cols_desc;
  tablet = (tablet == nullptr) ? this : tablet;
  if (OB_FAIL(tablet->load_storage_schema(tmp_allocator, storage_schema))) {
    LOG_WARN("fail to load storage schema", K(ret));
  } else if (OB_FAIL(storage_schema->get_mulit_version_rowkey_column_ids(cols_desc))) {
    LOG_WARN("fail to get rowkey column ids", K(ret), KPC(storage_schema));
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, rowkey_read_info_))) {
    LOG_WARN("fail to allocate and new rowkey read info", K(ret));
  } else if (OB_FAIL(storage_schema->get_store_column_count(full_stored_col_cnt, true/*full col*/))) {
    LOG_WARN("failed to get store column count", K(ret), KPC(storage_schema));
  } else if (OB_FAIL(rowkey_read_info_->init(allocator,
                                             full_stored_col_cnt,
                                             storage_schema->get_rowkey_column_num(),
                                             storage_schema->is_oracle_mode(),
                                             cols_desc))) {
    LOG_WARN("fail to init rowkey read info", K(ret), KPC(storage_schema));
  }
  ObTablet::free_storage_schema(tmp_allocator, storage_schema);
  return ret;
}

int ObTablet::try_update_start_scn()
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObTableStoreIterator iter;
  if (OB_FAIL(fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    ObSSTable *first_minor = static_cast<ObSSTable *>(
        table_store_wrapper.get_member()->get_minor_sstables().get_boundary_table(false /*first*/));
    const SCN &start_scn = OB_NOT_NULL(first_minor) ? first_minor->get_start_scn() : tablet_meta_.clog_checkpoint_scn_;
    const SCN &tablet_meta_scn = tablet_meta_.start_scn_;
    tablet_meta_.start_scn_ = start_scn;
    if (OB_UNLIKELY(start_scn < tablet_meta_scn)) {
      FLOG_INFO("tablet start scn is small than tablet meta start scn", K(start_scn), K(tablet_meta_scn), K(tablet_meta_));
    }
  }
  return ret;
}

int ObTablet::try_update_ddl_checkpoint_scn()
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObTableStoreIterator iter;
  if (OB_FAIL(fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    ObSSTable *last_ddl_sstable = static_cast<ObSSTable *>(
        table_store_wrapper.get_member()->get_ddl_sstables().get_boundary_table(true/*last*/));
    if (OB_NOT_NULL(last_ddl_sstable)) {
      const SCN &ddl_checkpoint_scn = last_ddl_sstable->get_end_scn();
      if (OB_UNLIKELY(ddl_checkpoint_scn < tablet_meta_.ddl_checkpoint_scn_)) {
        if (ddl_checkpoint_scn < tablet_meta_.ddl_start_scn_) {
          ret = OB_TASK_EXPIRED;
          LOG_INFO("ddl checkpoint scn is less than ddl start log ts, task expired", K(ret), K(ddl_checkpoint_scn), K(tablet_meta_));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected clog checkpoint scn", K(ret), K(ddl_checkpoint_scn), K(tablet_meta_));
        }
      } else {
        tablet_meta_.ddl_checkpoint_scn_ = ddl_checkpoint_scn;
      }
    }
  }
  return ret;
}

int ObTablet::try_update_table_store_flag(const ObUpdateTableStoreParam &param)
{
  int ret = OB_SUCCESS;
  if (param.update_with_major_flag_) {
    tablet_meta_.table_store_flag_.set_with_major_sstable();
  }
  return ret;
}

int ObTablet::build_migration_tablet_param(
    ObMigrationTabletParam &mig_tablet_param) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    // allocator
    mig_tablet_param.allocator_.set_attr(ObMemAttr(MTL_ID(), "MigTabletParam", ObCtxIds::DEFAULT_CTX_ID));

    mig_tablet_param.ls_id_ = tablet_meta_.ls_id_;
    mig_tablet_param.tablet_id_ = tablet_meta_.tablet_id_;
    mig_tablet_param.data_tablet_id_ = tablet_meta_.data_tablet_id_;
    mig_tablet_param.ref_tablet_id_ = tablet_meta_.ref_tablet_id_;
    mig_tablet_param.create_scn_ = tablet_meta_.create_scn_;
    mig_tablet_param.create_schema_version_ = tablet_meta_.create_schema_version_;
    mig_tablet_param.start_scn_ = tablet_meta_.start_scn_;
    mig_tablet_param.clog_checkpoint_scn_ = tablet_meta_.clog_checkpoint_scn_;
    mig_tablet_param.snapshot_version_ = tablet_meta_.snapshot_version_;
    mig_tablet_param.multi_version_start_ = tablet_meta_.multi_version_start_;
    mig_tablet_param.compat_mode_ = tablet_meta_.compat_mode_;
    mig_tablet_param.ha_status_ = tablet_meta_.ha_status_;
    mig_tablet_param.table_store_flag_ = tablet_meta_.table_store_flag_;
    mig_tablet_param.ddl_checkpoint_scn_ = tablet_meta_.ddl_checkpoint_scn_;
    mig_tablet_param.ddl_start_scn_ = tablet_meta_.ddl_start_scn_;
    mig_tablet_param.ddl_snapshot_version_ = tablet_meta_.ddl_snapshot_version_;
    mig_tablet_param.max_sync_storage_schema_version_ = tablet_meta_.max_sync_storage_schema_version_;
    mig_tablet_param.max_serialized_medium_scn_ = tablet_meta_.max_serialized_medium_scn_;
    // max_version on tablet meta is the latest serialized version
    mig_tablet_param.ddl_execution_id_ = tablet_meta_.ddl_execution_id_;
    mig_tablet_param.ddl_data_format_version_ = tablet_meta_.ddl_data_format_version_;
    mig_tablet_param.ddl_commit_scn_ = tablet_meta_.ddl_commit_scn_;
    mig_tablet_param.report_status_ = tablet_meta_.report_status_;
    mig_tablet_param.mds_checkpoint_scn_ = tablet_meta_.mds_checkpoint_scn_;
    mig_tablet_param.transfer_info_ = tablet_meta_.transfer_info_;
    mig_tablet_param.is_empty_shell_ = is_empty_shell();

    ObArenaAllocator arena_allocator(common::ObMemAttr(MTL_ID(), "BuildMigParam"));
    const ObStorageSchema *storage_schema = nullptr;
    const ObTabletAutoincSeq *tablet_autoinc_seq = nullptr;
    if (!is_empty_shell()) {
      if (OB_FAIL(load_storage_schema(arena_allocator, storage_schema))) {
        LOG_WARN("fail to load storage schema", K(ret));
      } else if (OB_FAIL(get_medium_info_list(mig_tablet_param.allocator_, mig_tablet_param.medium_info_list_))) {
        LOG_WARN("failed to get medium info list", K(ret));
      } else if (OB_FAIL(mig_tablet_param.storage_schema_.init(mig_tablet_param.allocator_, *storage_schema))) {
        LOG_WARN("failed to copy storage schema", K(ret), KPC(storage_schema));
      } else if (OB_FAIL(mig_tablet_param.mds_data_.init(mig_tablet_param.allocator_, mds_data_))) {
        LOG_WARN("failed to assign mds data", K(ret), K_(mds_data));
      }
      ObTablet::free_storage_schema(arena_allocator, storage_schema);
    } else {
      const ObTabletCreateDeleteMdsUserData &user_data = mds_data_.tablet_status_cache_;
      const int64_t serialize_size = user_data.get_serialize_size();
      int64_t pos = 0;
      char *buffer = nullptr;
      if (OB_ISNULL(buffer = static_cast<char*>(mig_tablet_param.allocator_.alloc(serialize_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(serialize_size));
      } else if (OB_FAIL(user_data.serialize(buffer, serialize_size, pos))) {
        LOG_WARN("user data serialize failed", K(ret), K(user_data));
      } else {
        mig_tablet_param.mds_data_.tablet_status_committed_kv_.v_.user_data_.assign_ptr(buffer, serialize_size);
      }
    }
  }

  return ret;
}

int ObTablet::build_migration_sstable_param(
    const ObITable::TableKey &table_key,
    blocksstable::ObMigrationSSTableParam &mig_sstable_param) const
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObTableStoreIterator iter;
  ObSSTableMetaHandle sstable_meta_handle;
  ObSSTable *sstable = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_key));
  } else if (OB_FAIL(fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member()->get_table(
      table_store_wrapper.get_meta_handle(), table_key, handle))) {
    LOG_WARN("fail to get table from table store", K(ret), K(table_key));
  } else if (OB_FAIL(handle.get_sstable(sstable))) {
    LOG_WARN("fail to get sstable", K(ret), KPC(sstable));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), KPC(sstable));
  } else if (OB_FAIL(sstable->get_meta(sstable_meta_handle))) {
    LOG_WARN("fail to get sstable meta handle", K(ret), KPC(sstable));
  } else {
    const ObSSTableMeta &sstable_meta = sstable_meta_handle.get_sstable_meta();
    mig_sstable_param.basic_meta_ = sstable_meta.get_basic_meta();
    mig_sstable_param.table_key_ = table_key;
    mig_sstable_param.is_small_sstable_ = sstable->is_small_sstable();
    mig_sstable_param.table_key_ = sstable->get_key();

    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_meta.get_col_checksum_cnt(); ++i) {
      if (OB_FAIL(mig_sstable_param.column_checksums_.push_back(sstable_meta.get_col_checksum()[i]))) {
        LOG_WARN("fail to push back column checksum", K(ret), K(i));
      }
    }
  }

  if (OB_FAIL(ret)) {
    mig_sstable_param.reset();
  }
  return ret;
}

int ObTablet::get_ha_sstable_size(int64_t &data_size)
{
  int ret = OB_SUCCESS;
  data_size = 0;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObTableStoreIterator iter;
  bool is_ready_for_read = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member()->get_ha_tables(iter, is_ready_for_read))) {
    LOG_WARN("failed to get read tables", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      ObSSTable *sstable = nullptr;
      ObSSTableMetaHandle sstable_meta_hdl;
      if (OB_FAIL(iter.get_next(table))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get read tables", K(ret), K(*this));
        }
      } else if (table->is_memtable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get migration get memtable", K(ret), KPC(table));
      } else if (FALSE_IT(sstable = static_cast<ObSSTable *> (table))) {
      } else if (OB_FAIL(sstable->get_meta(sstable_meta_hdl))) {
        LOG_WARN("failed to get sstable meta", K(ret));
      } else {
        data_size += sstable_meta_hdl.get_sstable_meta().get_occupy_size();
      }
    }
  }
  return ret;
}

int ObTablet::fetch_tablet_autoinc_seq_cache(
    const uint64_t cache_size,
    share::ObTabletAutoincInterval &result)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "FetchAutoSeq"));
  ObTabletAutoincSeq autoinc_seq;
  uint64_t auto_inc_seqvalue = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_autoinc_seq(allocator, share::SCN::max_scn(), autoinc_seq))) {
    LOG_WARN("fail to get latest autoinc seq", K(ret));
  } else if (OB_FAIL(autoinc_seq.get_autoinc_seq_value(auto_inc_seqvalue))) {
    LOG_WARN("failed to get autoinc seq value", K(ret), K(autoinc_seq));
  } else {
    const uint64_t interval_start = auto_inc_seqvalue;
    const uint64_t interval_end = auto_inc_seqvalue + cache_size - 1;
    const uint64_t result_autoinc_seq = auto_inc_seqvalue + cache_size;
    const ObTabletID &tablet_id = tablet_meta_.tablet_id_;
    SCN scn = SCN::min_scn();
    if (OB_FAIL(autoinc_seq.set_autoinc_seq_value(allocator, result_autoinc_seq))) {
      LOG_WARN("failed to set autoinc seq value", K(ret), K(result_autoinc_seq));
    } else if (OB_FAIL(write_sync_tablet_seq_log(autoinc_seq, scn))) {
      LOG_WARN("fail to write sync tablet seq log", K(ret));
    } else {
      result.start_ = interval_start;
      result.end_ = interval_end;
      result.tablet_id_ = tablet_id;
    }
  }
  return ret;
}

// MIN { ls min_reserved_snapshot, freeze_info, all_acquired_snapshot}
int ObTablet::get_kept_multi_version_start(
    ObLS &ls,
    const ObTablet &tablet,
    int64_t &multi_version_start)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  multi_version_start = 0;
  int64_t last_major_snapshot_version = 0;
  int64_t min_reserved_snapshot = 0;
  int64_t min_medium_snapshot = INT64_MAX;
  int64_t ls_min_reserved_snapshot = INT64_MAX;
  const share::ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletTableStore *table_store = nullptr;
  common::ObArenaAllocator arena_allocator(common::ObMemAttr(MTL_ID(), "reader"));

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member(table_store))) {
    LOG_WARN("fail to get table store", K(ret), K(table_store_wrapper));
  } else if (0 != table_store->get_major_sstables().count()) {
    last_major_snapshot_version = table_store->get_major_sstables().get_boundary_table(true/*last*/)->get_snapshot_version();
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(multi_version_start = tablet.get_multi_version_start())) {
  } else if (OB_FAIL(MTL(ObTenantFreezeInfoMgr*)->get_min_reserved_snapshot(
      tablet_id, last_major_snapshot_version, min_reserved_snapshot))) {
    LOG_WARN("failed to get multi version from freeze info mgr", K(ret), K(tablet_id));
  } else if (!tablet.is_ls_inner_tablet()) {
    ObTabletMediumInfoReader medium_info_reader(tablet);
    if (OB_FAIL(medium_info_reader.init(arena_allocator))) {
      LOG_WARN("failed to init medium info reader", K(ret));
    } else if (OB_FAIL(medium_info_reader.get_min_medium_snapshot(min_medium_snapshot))) {
      LOG_WARN("failed to get min medium snapshot", K(ret), K(tablet));
    }
  }

  // for compat, if receive ls_reserved_snapshot clog, should consider ls.get_min_reserved_snapshot()
  if (ls.get_min_reserved_snapshot() > 0) {
    ls_min_reserved_snapshot = ls.get_min_reserved_snapshot();
  }
  if (OB_SUCC(ret)) {
    const int64_t old_min_reserved_snapshot = min_reserved_snapshot;
    min_reserved_snapshot = common::min(
        ls_min_reserved_snapshot,
        common::min(min_reserved_snapshot, min_medium_snapshot));
    multi_version_start = MIN(MAX(min_reserved_snapshot, multi_version_start), tablet.get_snapshot_version());

    const int64_t current_time = common::ObTimeUtility::fast_current_time() * 1000; // needs ns here.
    if (current_time - multi_version_start > 120 * 60 * 1000 * 1000L /*2 hour*/) {
      if (REACH_TENANT_TIME_INTERVAL(10 * 1000 * 1000L /*10s*/)) {
        LOG_INFO("tablet multi version start not advance for a long time", K(ret), K(ls_id), K(tablet_id),
                K(multi_version_start), K(old_min_reserved_snapshot), K(min_medium_snapshot),
                K(ls_min_reserved_snapshot), "old_tablet_multi_version_start", tablet.get_multi_version_start(),
                "old_tablet_snaphsot_version", tablet.get_snapshot_version(), K(tablet));
      }
    }
  }
  LOG_DEBUG("get multi version start", K(ret), K(ls_id), K(tablet_id),
      K(multi_version_start), K(min_reserved_snapshot), K(min_medium_snapshot),
      K(ls_min_reserved_snapshot), K(last_major_snapshot_version));
  return ret;
}

int ObTablet::write_sync_tablet_seq_log(ObTabletAutoincSeq &autoinc_seq,
                                        share::SCN &scn)
{
  int ret = OB_SUCCESS;
  const int64_t WAIT_TIME = 1000; // 1ms
  const int64_t SYNC_TABLET_SEQ_LOG_TIMEOUT = 1000L * 1000L * 30L; // 30s
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  const enum ObReplayBarrierType replay_barrier_type = logservice::ObReplayBarrierType::NO_NEED_BARRIER;
  ObLogBaseHeader base_header(ObLogBaseType::TABLET_SEQ_SYNC_LOG_BASE_TYPE, replay_barrier_type);
  ObSyncTabletSeqLog log;
  // NOTICE: ObLogBaseHeader & ObSyncTabletSeqLog should have fixed serialize size!
  const int64_t buffer_size = base_header.get_serialize_size() + log.get_serialize_size();
  char buffer[buffer_size];
  int64_t retry_cnt = 0;
  int64_t pos = 0;
  ObSyncTabletSeqMdsLogCb *cb = nullptr;
  ObLogHandler *log_handler = get_log_handler();
  palf::LSN lsn;
  const bool need_nonblock= false;
  const SCN ref_scn = SCN::min_scn();
  uint64_t new_autoinc_seq = 0;
  if (OB_FAIL(autoinc_seq.get_autoinc_seq_value(new_autoinc_seq))) {
    LOG_WARN("failed to get autoinc seq value", K(ret));
  } else if (OB_FAIL(log.init(tablet_id, new_autoinc_seq))) {
    LOG_WARN("fail to init SyncTabletSeqLog", K(tablet_id), K(new_autoinc_seq));
  } else if (OB_ISNULL(cb = op_alloc(ObSyncTabletSeqMdsLogCb))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(base_header.serialize(buffer, buffer_size, pos))) {
    LOG_WARN("failed to serialize log base header", K(ret));
  } else if (OB_FAIL(log.serialize(buffer, buffer_size, pos))) {
    LOG_WARN("fail to serialize sync tablet seq log", K(ret));
  } else if (OB_FAIL(cb->init(tablet_meta_.ls_id_, tablet_id, static_cast<int64_t>(new_autoinc_seq)))) {
    LOG_WARN("failed to init cb", K(ret), K(tablet_meta_));
  } else if (OB_FAIL(set<ObTabletAutoincSeq>(std::move(autoinc_seq), cb->get_mds_ctx()))) {
    LOG_WARN("failed to set mds", K(ret));
  } else if (OB_FAIL(log_handler->append(buffer,
                                         buffer_size,
                                         ref_scn,
                                         need_nonblock,
                                         cb,
                                         lsn,
                                         scn))) {
    LOG_WARN("fail to submit sync tablet seq log", K(ret), K(buffer_size));
    cb->on_failure();
  } else {
    // wait until majority
    bool wait_timeout = false;
    int64_t start_time = ObTimeUtility::fast_current_time();
    while (!cb->is_finished() && !wait_timeout) {
      ob_usleep(WAIT_TIME);
      retry_cnt++;
      if (retry_cnt % 1000 == 0) {
        if (ObTimeUtility::fast_current_time() - start_time > SYNC_TABLET_SEQ_LOG_TIMEOUT) {
          wait_timeout = true;
        }
        LOG_WARN("submit sync tablet seq log wait too much time", K(retry_cnt), K(wait_timeout));
      }
    }
    if (wait_timeout) {
      ret = OB_TIMEOUT;
      LOG_WARN("submit sync tablet seq log timeout", K(ret));
    } else if (cb->is_failed()) {
      ret = cb->get_ret_code();
      LOG_WARN("submit sync tablet seq log failed", K(ret));
    } else {
      int64_t wait_time = ObTimeUtility::fast_current_time() - start_time;
      LOG_INFO("submit sync tablet seq log succeed", K(ret), K(ls_id), K(tablet_id),
          K(new_autoinc_seq), K(lsn), K(scn), K(wait_time));
    }
    if (nullptr != cb) {
      cb->try_release();
      cb = nullptr;
    }
  }
  if (OB_FAIL(ret) && nullptr != cb) {
    op_free(cb);
    cb = nullptr;
  }
  return ret;
}

int ObTablet::update_tablet_autoinc_seq(const uint64_t autoinc_seq)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "UpdAutoincSeq"));
  ObTabletAutoincSeq curr_autoinc_seq;
  uint64_t curr_auto_inc_seqvalue;
  SCN scn;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_autoinc_seq(allocator, share::SCN::max_scn(), curr_autoinc_seq))) {
    LOG_WARN("fail to get latest autoinc seq", K(ret));
  } else if (OB_FAIL(curr_autoinc_seq.get_autoinc_seq_value(curr_auto_inc_seqvalue))) {
    LOG_WARN("failed to get autoinc seq value", K(ret));
  } else if (autoinc_seq > curr_auto_inc_seqvalue) {
    if (OB_FAIL(curr_autoinc_seq.set_autoinc_seq_value(allocator, autoinc_seq))) {
      LOG_WARN("failed to set autoinc seq value", K(ret), K(autoinc_seq));
    } else if (OB_FAIL(write_sync_tablet_seq_log(curr_autoinc_seq, scn))) {
      LOG_WARN("fail to write sync tablet seq log", K(ret));
    }
  }
  return ret;
}

int ObTablet::start_ddl_if_need()
{
  int ret = OB_SUCCESS;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  if (!tablet_meta_.ddl_start_scn_.is_valid_and_not_min()) {
    LOG_DEBUG("no need to start ddl kv manager", K(ret), K(tablet_meta_));
  } else if (OB_FAIL(get_ddl_kv_mgr(ddl_kv_mgr_handle, true/*try_create*/))) {
    LOG_WARN("create ddl kv mgr failed", K(ret));
  } else {
    ObLS *ls = nullptr;
    ObLSService *ls_service = nullptr;
    ObLSHandle ls_handle;
    ObITable::TableKey table_key;
    table_key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    table_key.tablet_id_ = tablet_meta_.tablet_id_;
    table_key.version_range_.base_version_ = 0;
    table_key.version_range_.snapshot_version_ = tablet_meta_.ddl_snapshot_version_;
    const SCN &start_scn = tablet_meta_.ddl_start_scn_;
    if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(tablet_meta_.ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
      LOG_WARN("failed to get ls", K(ret));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", K(ret), KP(ls));
    } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->ddl_start(*ls,
                                                       *this,
                                                       table_key,
                                                       start_scn,
                                                       tablet_meta_.ddl_data_format_version_,
                                                       tablet_meta_.ddl_execution_id_,
                                                       tablet_meta_.ddl_checkpoint_scn_))) {
      LOG_WARN("start ddl kv manager failed", K(ret), K(table_key), K(tablet_meta_));
    }
  }
  return ret;
}

int ObTablet::check_schema_version_elapsed(
    const int64_t schema_version,
    const bool need_wait_trans_end,
    int64_t &max_commit_version,
    transaction::ObTransID &pending_tx_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  ObMultiVersionSchemaService *schema_service = MTL(ObTenantSchemaService*)->get_schema_service();
  SCN scn;
  SCN max_commit_scn;
  int64_t tenant_refreshed_schema_version = 0;
  int64_t refreshed_schema_ts = 0;
  int64_t refreshed_schema_version = 0;
  max_commit_version = 0L;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTablet has not been inited", K(ret));
  } else if (OB_UNLIKELY(schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(schema_version));
  } else if (!need_wait_trans_end) {
    // obtain_snapshot of offline ddl don't need to wait trans end.
    transaction::ObTransService *txs = MTL(transaction::ObTransService*);
    if (OB_FAIL(txs->get_max_commit_version(max_commit_scn))) {
      LOG_WARN("fail to get max commit version", K(ret));
    } else if (OB_UNLIKELY(!max_commit_scn.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, scn is invalid", K(ret), K(max_commit_scn));
    } else {
      max_commit_version = max_commit_scn.get_val_for_tx();
    }
  } else {
    if (OB_FAIL(get_ddl_info(refreshed_schema_version, refreshed_schema_ts))) {
      LOG_WARN("get ddl info failed", K(ret));
    } else if (refreshed_schema_version >= schema_version) {
      // schema version already refreshed
    } else if (OB_FAIL(schema_service->get_tenant_refreshed_schema_version(tenant_id, tenant_refreshed_schema_version))) {
      LOG_WARN("get tenant refreshed schema version failed", K(ret));
    } else if (tenant_refreshed_schema_version < schema_version) {
      ret = OB_EAGAIN;
      LOG_WARN("current schema version not latest, need retry", K(ret), K(schema_version), K(tenant_refreshed_schema_version));
    } else if (OB_FAIL(replay_schema_version_change_log(schema_version))) {
      LOG_WARN("set schema change version clog failed", K(ret), K(schema_version));
    } else if (OB_FAIL(write_tablet_schema_version_change_clog(schema_version, scn))) {
      LOG_WARN("write partition schema version change clog error", K(ret), K(schema_version));
      // override ret
      ret = OB_EAGAIN;
    } else if (OB_FAIL(update_ddl_info(schema_version, scn, refreshed_schema_ts))) {
      LOG_WARN("update ddl info failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      transaction::ObTransService *txs = MTL(transaction::ObTransService*);
      ObLSService *ls_service = MTL(ObLSService*);
      ObLSHandle ls_handle;
      if (OB_FAIL(ls_service->get_ls(tablet_meta_.ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
        LOG_WARN("failed to get ls", K(ret), "ls_id", tablet_meta_.ls_id_);
      } else if (OB_FAIL(ls_handle.get_ls()->check_modify_schema_elapsed(tablet_id, schema_version, pending_tx_id))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("check schema version elapsed failed", K(ret), K(ls_id), K(tablet_id), K(schema_version));
        } else {
          LOG_INFO("check schema version elapsed again", K(ret), K(ls_id), K(tablet_id), K(schema_version), K(refreshed_schema_ts));
        }
      } else if (OB_FAIL(txs->get_max_commit_version(max_commit_scn))) {
        LOG_WARN("fail to get max commit version", K(ret));
      } else {
        max_commit_version = max_commit_scn.get_val_for_tx();
        LOG_INFO("check wait trans end", K(ret), K(ls_id), K(tablet_id), K(max_commit_version), K(max_commit_scn), K(refreshed_schema_ts));
      }
    }
  }
  return ret;
}

int ObTablet::write_tablet_schema_version_change_clog(
    const int64_t schema_version,
    SCN &scn)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  ObTabletSchemaVersionChangeLog log;
  if (OB_FAIL(log.init(tablet_id, schema_version))) {
    LOG_WARN("fail to init tablet schema version change log", K(ret), K(tablet_id), K(schema_version));
  } else {
    const int64_t CHECK_SCHEMA_VERSION_CHANGE_LOG_US = 1000;
    const int64_t CHECK_SCHEMA_VERSION_CHANGE_LOG_TIMEOUT = 1000L * 1000L * 30L; // 30s
    const enum ObReplayBarrierType replay_barrier_type = ObReplayBarrierType::STRICT_BARRIER;
    ObLogBaseHeader base_header(ObLogBaseType::DDL_LOG_BASE_TYPE, replay_barrier_type);
    ObDDLClogHeader ddl_header(ObDDLClogType::DDL_TABLET_SCHEMA_VERSION_CHANGE_LOG);
    const int64_t buffer_size = base_header.get_serialize_size() + ddl_header.get_serialize_size()
                              + log.get_serialize_size();
    char buffer[buffer_size];
    int64_t retry_cnt = 0;
    int64_t pos = 0;
    ObDDLClogCb *cb = nullptr;
    ObLogHandler *log_handler = get_log_handler();

    palf::LSN lsn;
    const bool need_nonblock= false;
    SCN ref_scn;
    ref_scn.set_min();
    scn.reset();

    if (OB_ISNULL(cb = op_alloc(ObDDLClogCb))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else if (OB_FAIL(base_header.serialize(buffer, buffer_size, pos))) {
      LOG_WARN("failed to serialize log base header", K(ret));
    } else if (OB_FAIL(ddl_header.serialize(buffer, buffer_size, pos))) {
      LOG_WARN("fail to seriaize sync tablet seq log", K(ret));
    } else if (OB_FAIL(log.serialize(buffer, buffer_size, pos))) {
      LOG_WARN("fail to seriaize schema version change log", K(ret));
    } else if (OB_FAIL(log_handler->append(buffer,
                                           buffer_size,
                                           ref_scn,
                                           need_nonblock,
                                           cb,
                                           lsn,
                                           scn))) {
      LOG_WARN("fail to submit schema version change log", K(ret), K(buffer_size));
    } else {
      ObDDLClogCb *tmp_cb = cb;
      cb = nullptr;
      // wait unti majority
      bool wait_timeout = false;
      int64_t start_time = ObTimeUtility::fast_current_time();
      while (!tmp_cb->is_finished() && !wait_timeout) {
        ob_usleep(CHECK_SCHEMA_VERSION_CHANGE_LOG_US);
        retry_cnt++;
        if (retry_cnt % 1000 == 0) {
          if (ObTimeUtility::fast_current_time() - start_time > CHECK_SCHEMA_VERSION_CHANGE_LOG_TIMEOUT) {
            wait_timeout = true;
          }
          LOG_WARN_RET(OB_ERR_TOO_MUCH_TIME, "submit schema version change log wait too much time", K(retry_cnt), K(wait_timeout));
        }
      }
      if (wait_timeout) {
        ret = OB_TIMEOUT;
        LOG_WARN("submit schema version change log timeout", K(ret), K(ls_id), K(tablet_id));
      } else if (tmp_cb->is_failed()) {
        ret = OB_NOT_MASTER;
        LOG_WARN("submit schema version change log failed", K(ret), K(ls_id), K(tablet_id));
      } else {
        LOG_INFO("submit schema version change log succeed", K(ret), K(ls_id), K(tablet_id), K(schema_version));
      }
      tmp_cb->try_release(); // release the memory no matter succ or not
    }
    if (nullptr != cb) {
      op_free(cb);
      cb = nullptr;
    }
  }
  return ret;
}

int ObTablet::replay_schema_version_change_log(const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObTableHandleV2, 8> table_handle_array;
  ObIMemtableMgr *memtable_mgr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(get_memtable_mgr(memtable_mgr))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else if (OB_FAIL(memtable_mgr->get_all_memtables(table_handle_array))) {
    LOG_WARN("failed to get memtables", K(ret));
  } else {
    memtable::ObMemtable *memtable = nullptr;
    const int64_t table_num = table_handle_array.count();
    if (0 == table_num) {
      // no memtable, no need to replay schema version change
    } else if (!table_handle_array[table_num - 1].is_valid()) {
      ret = OB_ERR_SYS;
      LOG_WARN("latest memtable is invalid", K(ret));
    } else if (OB_FAIL(table_handle_array[table_num - 1].get_data_memtable(memtable))) {
      LOG_WARN("fail to get memtable", K(ret));
    } else if (OB_ISNULL(memtable)) {
      ret = OB_ERR_SYS;
      LOG_WARN("memtable is null", K(ret), KP(memtable));
    } else if (OB_FAIL(memtable->replay_schema_version_change_log(schema_version))) {
      LOG_WARN("fail to replay schema version change log", K(ret), K(schema_version));
    }
  }

  return ret;
}

int ObTablet::get_tablet_report_info(
    const int64_t snapshot_version,
    common::ObIArray<int64_t> &column_checksums,
    int64_t &data_size,
    int64_t &required_size,
    const bool need_checksums)
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletTableStore *table_store = nullptr;
  column_checksums.reset();
  data_size = 0;
  required_size = 0;
  const ObSSTable *main_major = nullptr;
  ObSSTableMetaHandle main_major_meta_hdl;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member(table_store))) {
    LOG_WARN("fail to get table store", K(ret), K(table_store_wrapper));
  } else if (table_store->get_major_sstables().empty()) {
    ret = OB_TABLET_NOT_EXIST;
    LOG_INFO("no major sstables in this tablet, cannot report", K(ret));
  } else if (FALSE_IT(main_major = static_cast<ObSSTable *>(table_store->get_major_sstables().get_boundary_table(true)))) {
  } else if (OB_UNLIKELY(nullptr == main_major || snapshot_version != main_major->get_snapshot_version())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get unexpected null major", K(ret));
  } else if (OB_FAIL(main_major->get_meta(main_major_meta_hdl))) {
    LOG_WARN("failed to get sstable meta handle", K(ret));
  } else if (need_checksums) {
    for (int64_t i = 0; OB_SUCC(ret) && i < main_major_meta_hdl.get_sstable_meta().get_col_checksum_cnt(); ++i) {
      if (OB_FAIL(column_checksums.push_back(main_major_meta_hdl.get_sstable_meta().get_col_checksum()[i]))) {
        LOG_WARN("fail to push back column checksum", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    data_size = main_major_meta_hdl.get_sstable_meta().get_basic_meta().occupy_size_;
    const int64_t macro_block_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
    ObITable *table = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_store->get_major_sstables().count(); ++i) {
      table = table_store->get_major_sstables().at(i);
      if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, table is nullptr", K(ret), KPC(table));
      } else {
        ObSSTable *sstable = reinterpret_cast<ObSSTable *>(table);
        ObSSTableMetaHandle major_meta_hdl;
        if (OB_FAIL(sstable->get_meta(major_meta_hdl))) {
          LOG_WARN("fail to get major sstable meta", K(ret));
        } else if (0 == i) {
          required_size += (major_meta_hdl.get_sstable_meta().get_total_macro_block_count()) * macro_block_size;
        } else {
          required_size +=
              (major_meta_hdl.get_sstable_meta().get_total_macro_block_count()
                  - major_meta_hdl.get_sstable_meta().get_total_use_old_macro_block_count())
              * macro_block_size;
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObTablet::get_ddl_sstables(ObTableStoreIterator &table_store_iter) const
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletTableStore *table_store = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member(table_store))) {
    LOG_WARN("fail to get table store", K(ret), K(table_store_wrapper));
  } else if (OB_FAIL(table_store->get_ddl_sstables(table_store_iter))) {
    LOG_WARN("fail to get read tables", K(ret));
  } else if (!table_store_addr_.is_memory_object()
      && OB_FAIL(table_store_iter.set_handle(table_store_wrapper.get_meta_handle()))) {
    LOG_WARN("fail to set storage meta handle", K(ret), K_(table_store_addr), K(table_store_wrapper));
  }
  return ret;
}

int ObTablet::get_mini_minor_sstables(ObTableStoreIterator &table_store_iter) const
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member()->get_mini_minor_sstables(
      tablet_meta_.ha_status_.is_data_status_complete(), table_store_iter))) {
    LOG_WARN("fail to get ddl sstable handles", K(ret));
  } else if (!table_store_addr_.is_memory_object()
      && OB_FAIL(table_store_iter.set_handle(table_store_wrapper.get_meta_handle()))) {
    LOG_WARN("fail to set storage meta handle", K(ret), K_(table_store_addr), K(table_store_wrapper));
  }
  return ret;
}

int ObTablet::get_table(const ObITable::TableKey &table_key, ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member()->get_table(
      table_store_wrapper.get_meta_handle(), table_key, handle))) {
    LOG_WARN("fail to get ddl sstable handles", K(ret));
  }
  return ret;
}

int ObTablet::get_recycle_version(const int64_t multi_version_start, int64_t &recycle_version) const
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member()->get_recycle_version(
      multi_version_start, recycle_version))) {
    LOG_WARN("fail to get ddl sstable handles", K(ret));
  }
  return ret;
}

int ObTablet::get_ha_tables(
    ObTableStoreIterator &iter,
    bool &is_ready_for_read)
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member()->get_ha_tables(iter, is_ready_for_read))) {
    LOG_WARN("fail to get ha tables", K(ret));
  } else if (!table_store_addr_.is_memory_object()
      && OB_FAIL(iter.set_handle(table_store_wrapper.get_meta_handle()))) {
    LOG_WARN("fail to set storage meta handle", K(ret), K_(table_store_addr), K(table_store_wrapper));
  }

  return ret;
}

int ObTablet::update_ddl_info(
    const int64_t schema_version,
    const SCN &scn,
    int64_t &schema_refreshed_ts)
{
  int ret = OB_SUCCESS;
  ObTabletPointer *tablet_ptr = static_cast<ObTabletPointer*>(pointer_hdl_.get_resource_ptr());
  if (OB_FAIL(tablet_ptr->ddl_info_.update(schema_version, scn, schema_refreshed_ts))) {
    LOG_WARN("fail to update ddl info", K(ret), K(schema_version), K(scn));
  }
  return ret;
}

int ObTablet::get_ddl_info(int64_t &schema_version, int64_t &schema_refreshed_ts) const
{
  int ret = OB_SUCCESS;
  ObTabletPointer *tablet_ptr = static_cast<ObTabletPointer*>(pointer_hdl_.get_resource_ptr());
  if (OB_FAIL(tablet_ptr->ddl_info_.get(schema_version, schema_refreshed_ts))) {
    LOG_WARN("fail to update ddl info", K(ret));
  }
  return ret;
}

int ObTablet::get_rec_log_scn(SCN &rec_scn) {
  int ret = OB_SUCCESS;
  rec_scn = SCN::max_scn();
  ObTableHandleV2 handle;
  memtable::ObMemtable *mt = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(memtable_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memtable_mgr is NULL", KR(ret), KPC(this));
  } else if (OB_FAIL(memtable_mgr_->get_first_nonempty_memtable(handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get first memtable", KR(ret), K(handle));
    }
  } else if (OB_FAIL(handle.get_data_memtable(mt))) {
    LOG_WARN("fail to get data memtables", KR(ret), K(handle));
  } else if (OB_ISNULL(mt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mt is NULL", KR(ret), K(handle));
  } else {
    rec_scn = mt->get_rec_scn();
  }
  return ret;
}

int ObTablet::get_mds_table_rec_log_scn(SCN &rec_scn)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  mds::MdsTableHandle mds_table;
  rec_scn = SCN::max_scn();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (is_ls_inner_tablet()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("inner tablet does not have mds table", K(ret));
  } else if (OB_FAIL(inner_get_mds_table(mds_table))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_DEBUG("mds_table does not exist", K(ret), K(ls_id), K(tablet_id));
    } else {
      LOG_WARN("failed to get mds table", K(ret), K(ls_id), K(tablet_id));
    }
  } else if (OB_FAIL(mds_table.get_rec_scn(rec_scn))) {
    LOG_WARN("failed to get mds table rec scn", K(ret));
  } else if (!rec_scn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid scn from mds table", K(ret));
  }
  return ret;
}

int ObTablet::mds_table_flush(const share::SCN &recycle_scn)
{
  int ret = OB_SUCCESS;
  mds::MdsTableHandle mds_table;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (is_ls_inner_tablet()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("inner tablet does not have mds table", K(ret));
  } else if (OB_FAIL(inner_get_mds_table(mds_table))) {
    LOG_WARN("failed to get mds table", K(ret));
  } else if (OB_FAIL(mds_table.flush(recycle_scn))) {
    LOG_WARN("failed to flush mds table", KR(ret), KPC(this));
  }
  return ret;
}

int ObTablet::get_storage_schema_for_transfer_in(
    common::ObArenaAllocator &allocator,
    ObStorageSchema &storage_schema) const
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  const ObStorageSchema *tablet_storage_schema = nullptr;
  ObIMemtableMgr *memtable_mgr = nullptr;
  ObArray<ObTableHandleV2> memtables;
  int64_t max_column_cnt_in_memtable = 0;
  int64_t max_schema_version_in_memtable = 0;
  int64_t store_column_cnt_in_schema = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(tablet_id.is_ls_inner_tablet())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported to get storage schema for ls inner tablet", KR(ret), K(tablet_id));
  } else if (OB_ISNULL(memtable_mgr = get_memtable_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memtable mgr should not be NULL", K(ret), KP(memtable_mgr));
  } else if (OB_FAIL(memtable_mgr->get_all_memtables(memtables))) {
    LOG_WARN("failed to get all memtables", K(ret), KPC(this));
  } else if (OB_FAIL(load_storage_schema(allocator, tablet_storage_schema))) {
    LOG_WARN("fail to load storage schema", K(ret), K_(storage_schema_addr));
  } else if (OB_FAIL(tablet_storage_schema->get_store_column_count(store_column_cnt_in_schema, true/*full_col*/))) {
    LOG_WARN("failed to get store column count", K(ret), K(store_column_cnt_in_schema));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < memtables.count(); ++i) {
      ObITable *table = memtables.at(i).get_table();
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table in tables_handle is invalid", K(ret), KP(table));
      } else if (OB_FAIL(static_cast<memtable::ObMemtable *>(table)->get_schema_info(
          store_column_cnt_in_schema,
          max_schema_version_in_memtable, max_column_cnt_in_memtable))) {
        LOG_WARN("failed to get schema info from memtable", KR(ret), KPC(table));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(storage_schema.init(allocator, *tablet_storage_schema))) {
    LOG_WARN("failed to init storage schema", K(ret), K(ls_id), K(tablet_id), KPC(tablet_storage_schema));
  } else {
    int64_t old_column_cnt = storage_schema.get_column_count();
    int64_t old_schema_version = storage_schema.get_schema_version();
    storage_schema.update_column_cnt(max_column_cnt_in_memtable);
    storage_schema.schema_version_ = MAX(old_schema_version, max_schema_version_in_memtable);
    LOG_INFO("succeeded to get storage schema from transfer source tablet", K(ret), K(storage_schema), K(max_column_cnt_in_memtable),
        K(max_schema_version_in_memtable), K(old_column_cnt), K(store_column_cnt_in_schema), K(old_schema_version));
  }
  ObTablet::free_storage_schema(allocator, tablet_storage_schema);
  return ret;
}

int ObTablet::check_and_set_initial_state()
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet_meta_.ls_id_;
  const ObTabletID &tablet_id = tablet_meta_.tablet_id_;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    // for normal tablet(except ls inner tablet), if mds_checkpoint_scn equals initial SCN(value is 1),
    // it means all kinds of mds data(including tablet status) has never been dumped to disk,
    // then we think that this tablet is in initial state
    bool initial_state = true;
    if (is_ls_inner_tablet()) {
      initial_state = false;
    } else {
      initial_state = (tablet_meta_.mds_checkpoint_scn_ == ObTabletMeta::INIT_CLOG_CHECKPOINT_SCN);
    }

    if (initial_state) {
      // do nothing
    } else if (OB_FAIL(set_initial_state(false/*initial_state*/))) {
      LOG_WARN("failed to set initial state", K(ret));
    } else {
      LOG_DEBUG("set initial state to false", K(ret), K(ls_id), K(tablet_id));
    }
  }

  return ret;
}

int ObTablet::check_medium_list() const
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  if (tablet_meta_.ha_status_.is_none()) {
    ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
    ObITable *last_major = nullptr;
    if (OB_FAIL(fetch_table_store(table_store_wrapper))) {
      LOG_WARN("fail to fetch table store", K(ret));
    } else if (OB_NOT_NULL(last_major = table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/))) {
      ObArenaAllocator arena_allocator("check_medium", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      const ObTabletDumpedMediumInfo *dumped_list = nullptr;
      if (OB_FAIL(ObTabletMdsData::load_medium_info_list(arena_allocator, mds_data_.medium_info_list_, dumped_list))) {
        LOG_WARN("failed to load medium info list", K(ret), K(ls_id), K(tablet_id), K(mds_data_));
      } else if (OB_FAIL(ObMediumListChecker::validate_medium_info_list(
          mds_data_.extra_medium_info_,
          nullptr == dumped_list ? nullptr : &dumped_list->medium_info_list_,
          last_major->get_snapshot_version()))) {
        LOG_WARN("fail to validate medium info list", K(ret), K(ls_id), K(tablet_id), K(mds_data_), KPC(dumped_list), KPC(last_major));
      }
      ObTabletMdsData::free_medium_info_list(arena_allocator, dumped_list);
    }
  } else {
    LOG_INFO("skip check medium list for non empty ha_status", KR(ret), K(ls_id), K(tablet_id), "ha_status", tablet_meta_.ha_status_);
  }
  return ret;
}

int ObTablet::get_finish_medium_scn(int64_t &finish_medium_scn) const
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObTabletTableStore *table_store = table_store_wrapper.get_member();
    ObITable *last_major = table_store->get_major_sstables().get_boundary_table(true/*last*/);
    if (nullptr == last_major) {
      finish_medium_scn = 0;
    } else {
      finish_medium_scn = last_major->get_snapshot_version();
    }
  }

  return ret;
}

int ObTablet::set_memtable_clog_checkpoint_scn(
    const ObMigrationTabletParam *tablet_meta)
{
  int ret = OB_SUCCESS;
  ObIMemtableMgr *memtable_mgr = nullptr;
  ObTableHandleV2 handle;
  memtable::ObMemtable *memtable = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(tablet_meta)) {
    // no need to set memtable clog checkpoint ts
  } else if (tablet_meta->clog_checkpoint_scn_ <= tablet_meta_.clog_checkpoint_scn_) {
    // do nothing
  } else if (OB_FAIL(get_memtable_mgr(memtable_mgr))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else if (is_ls_inner_tablet()) {
    if (OB_UNLIKELY(memtable_mgr->has_memtable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls inner tablet should not have memtable", K(ret), KPC(tablet_meta));
    }
  } else if (OB_FAIL(memtable_mgr->get_boundary_memtable(handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get boundary memtable for tablet", K(ret), KPC(this), KPC(tablet_meta));
    }
  } else if (OB_FAIL(handle.get_data_memtable(memtable))) {
    LOG_WARN("failed to get memtable", K(ret), K(handle));
  } else if (OB_ISNULL(memtable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null memtable", K(ret), KPC(memtable));
  } else if (OB_FAIL(memtable->set_migration_clog_checkpoint_scn(tablet_meta->clog_checkpoint_scn_))) {
    LOG_WARN("failed to set migration clog checkpoint ts", K(ret), K(handle), KPC(this));
  }

  return ret;
}

int ObTablet::get_medium_info_list(
    common::ObArenaAllocator &allocator,
    compaction::ObMediumCompactionInfoList &medium_info_list) const
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(load_medium_info_list(allocator,
      mds_data_.medium_info_list_,
      mds_data_.extra_medium_info_,
      medium_info_list))) {
    LOG_WARN("load medium info list failed", K(ret));
  }

  return ret;
}

int ObTablet::prepare_param_ctx(
    ObIAllocator &allocator,
    ObRelativeTable &relative_table,
    ObStoreCtx &ctx,
    ObTableIterParam &param,
    ObTableAccessContext &context)
{
  int ret = OB_SUCCESS;
  ObVersionRange trans_version_range;
  const bool read_latest = true;
  ObQueryFlag query_flag;


  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = EXIST_READ_SNAPSHOT_VERSION;
  query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;
  query_flag.read_latest_ = read_latest & ObQueryFlag::OBSF_MASK_READ_LATEST;
  if (relative_table.is_storage_index_table()) {
    query_flag.index_invalid_ = !relative_table.can_read_index();
  }

  param.table_id_ = relative_table.get_table_id();
  param.tablet_id_ = tablet_meta_.tablet_id_;
  param.read_info_ = rowkey_read_info_;

  if (OB_FAIL(context.init(query_flag, ctx, allocator, trans_version_range))) {
    LOG_WARN("Fail to init access context", K(ret));
  }
  return ret;
}

int ObTablet::build_transfer_tablet_param(
    const share::ObLSID &dest_ls_id,
    ObMigrationTabletParam &mig_tablet_param)
{
  int ret = OB_SUCCESS;
  mig_tablet_param.reset();
  // allocator
  mig_tablet_param.allocator_.set_attr(ObMemAttr(MTL_ID(), "MigTabletParam", ObCtxIds::DEFAULT_CTX_ID));

  ObTabletCreateDeleteMdsUserData user_data;
  share::SCN max_data_scn;
  ObTabletMdsData mds_table_data;
  ObTabletMdsData new_mds_data;
  bool unused_committed_flag = false;
  int64_t finish_medium_scn = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (!dest_ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build transfer tablet param get invalid argument", K(ret), K(dest_ls_id));
  } else if (!tablet_meta_.ha_status_.is_data_status_complete()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet meta data status is incompleted, unexpected", K(ret), KPC(this), K(tablet_meta_));
  } else if (tablet_meta_.ref_tablet_id_.is_valid()) {
    //ref tablet id is unused now. So it should be invalid
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref tablet id is valid, unexpected", K(ret), KPC(this), K(tablet_meta_));
  } else if (OB_FAIL(ObITabletMdsInterface::get_latest_tablet_status(user_data, unused_committed_flag))) {
    LOG_WARN("failed to get lastest tablet status", K(ret), KPC(this), K(tablet_meta_));
  } else if (OB_FAIL(get_storage_schema_for_transfer_in(mig_tablet_param.allocator_, mig_tablet_param.storage_schema_))) {
    LOG_WARN("failed to get storage schema", K(ret), KPC(this));
  } else if (OB_FAIL(get_medium_info_list(mig_tablet_param.allocator_, mig_tablet_param.medium_info_list_))) {
    LOG_WARN("failed to get_medium_info_list", K(ret), KPC(this));
  } else {
    //TODO(lingchuan) need split it into slog, mds data, ddl data
    mig_tablet_param.ls_id_ = dest_ls_id;
    mig_tablet_param.tablet_id_ = tablet_meta_.tablet_id_;
    mig_tablet_param.data_tablet_id_ = tablet_meta_.data_tablet_id_;
    mig_tablet_param.ref_tablet_id_ = tablet_meta_.ref_tablet_id_;
    mig_tablet_param.create_scn_ = tablet_meta_.create_scn_;
    mig_tablet_param.create_schema_version_ = tablet_meta_.create_schema_version_;
    mig_tablet_param.start_scn_ = tablet_meta_.start_scn_;
    mig_tablet_param.clog_checkpoint_scn_ = user_data.transfer_scn_;
    mig_tablet_param.snapshot_version_ = 1;
    mig_tablet_param.multi_version_start_ = 1;
    mig_tablet_param.compat_mode_ = tablet_meta_.compat_mode_;
    mig_tablet_param.ha_status_ = tablet_meta_.ha_status_;
    mig_tablet_param.table_store_flag_ = tablet_meta_.table_store_flag_;
    // Due to the need to wait for ddl merge in the doing phase of transfer, the ddl checkpoint scn of src_tablet may be pushed up after the transfer in tablet is created.
    mig_tablet_param.ddl_checkpoint_scn_ = user_data.transfer_scn_;
    mig_tablet_param.ddl_start_scn_ = tablet_meta_.ddl_start_scn_;
    mig_tablet_param.ddl_snapshot_version_ = tablet_meta_.ddl_snapshot_version_;
    mig_tablet_param.max_sync_storage_schema_version_ = mig_tablet_param.storage_schema_.schema_version_;
    mig_tablet_param.ddl_execution_id_ = tablet_meta_.ddl_execution_id_;
    mig_tablet_param.ddl_data_format_version_ = tablet_meta_.ddl_data_format_version_;
    mig_tablet_param.mds_checkpoint_scn_ = user_data.transfer_scn_;
    mig_tablet_param.report_status_.reset();

    const int64_t transfer_seq = tablet_meta_.transfer_info_.transfer_seq_ + 1;

    if (OB_FAIL(mig_tablet_param.transfer_info_.init(tablet_meta_.ls_id_, user_data.transfer_scn_, transfer_seq))) {
      LOG_WARN("failed to init transfer info", K(ret), K(tablet_meta_), K(user_data));
    } else if (OB_FAIL(read_mds_table(mig_tablet_param.allocator_, mds_table_data, false))) {
      if (OB_EMPTY_RESULT == ret) {
        // do nothing
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to read mds table", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_finish_medium_scn(finish_medium_scn))) {
      LOG_WARN("failed to get finish medium scn", K(ret));
    } else if (OB_FAIL(new_mds_data.init_for_mds_table_dump(mig_tablet_param.allocator_, mds_table_data, mds_data_, finish_medium_scn))) {
      LOG_WARN("failed to init new mds data", K(ret), K(finish_medium_scn));
    } else if (OB_FAIL(build_transfer_in_tablet_status_(user_data, new_mds_data, mig_tablet_param.allocator_))) {
      LOG_WARN("failed to build transfer in tablet status", K(new_mds_data), K(mds_data_), KPC(this));
    } else if (OB_FAIL(mig_tablet_param.mds_data_.init(mig_tablet_param.allocator_, new_mds_data))) {
      LOG_WARN("failed to assign mds data", K(ret), K(new_mds_data), K(mds_data_), K(mds_table_data));
    } else {
      LOG_INFO("succeed build_transfer_tablet_param_mds", K(user_data), K(max_data_scn),
          K(new_mds_data), K(mds_table_data), K(mds_data_), K(mig_tablet_param), KPC(this));
    }
  }
  return ret;
}

int64_t ObTablet::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
      // do nothing
  } else {
    J_OBJ_START();
    J_NAME("ObTablet");
    J_COLON();
    J_KV(KP(this),
         K_(wash_score),
         K_(ref_cnt),
         K_(version),
         K_(length),
         K_(tablet_addr),
         KP_(allocator),
         K_(tablet_meta),
         K_(table_store_addr),
         K_(storage_schema_addr),
         K_(next_tablet_guard),
         K_(pointer_hdl),
         KP_(next_tablet),
         KP_(memtable_mgr),
         KP_(log_handler),
         KPC_(rowkey_read_info),
         K_(mds_data),
         K_(hold_ref_cnt),
         K_(is_inited),
         K_(memtable_count));
    J_COMMA();
    BUF_PRINTF("memtables");
    J_COLON();
    J_ARRAY_START();
    for (int64_t i = 0; i < MAX_MEMSTORE_CNT; ++i) {
      if (i > 0) {
        J_COMMA();
      }
      BUF_PRINTO(OB_P(memtables_[i]));
    }
    J_ARRAY_END();
    J_OBJ_END();
  }
  return pos;
}

int ObTablet::refresh_memtable_and_update_seq(const uint64_t seq)
{
  int ret = OB_SUCCESS;
  if (table_store_addr_.is_memory_object()) {
    table_store_addr_.get_ptr()->clear_memtables();
  }
  reset_memtable();
  if (OB_FAIL(pull_memtables_without_ddl())) {
    LOG_WARN("fail to pull memtables", K(ret), KPC(this));
  } else {
    tablet_addr_.set_seq(seq);
    table_store_addr_.addr_.set_seq(seq);
    if (table_store_addr_.is_memory_object()) {
      ObSEArray<ObITable *, MAX_MEMSTORE_CNT> memtable_array;
      if (OB_FAIL(inner_get_memtables(memtable_array, true/*need_active*/))) {
        LOG_WARN("inner get memtables fail", K(ret), K(*this));
      } else if (OB_FAIL(table_store_addr_.get_ptr()->update_memtables(memtable_array))) {
        LOG_WARN("table store update memtables fail", K(ret), K(memtable_array));
      } else {
       LOG_INFO("table store update memtable success", KPC(table_store_addr_.get_ptr()), KP(this));
      }
    }
  }
  return ret;
}

int ObTablet::pull_memtables(ObArenaAllocator &allocator, ObITable **&ddl_kvs_addr, int64_t &ddl_kv_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pull_memtables_without_ddl())) {
    LOG_WARN("fail to pull memtables without ddl", K(ret));
  } else if (OB_FAIL(pull_ddl_memtables(allocator, ddl_kvs_addr, ddl_kv_count))) {
    LOG_WARN("failed to pull ddl memtables", K(ret));
  }
  return ret;
}

int ObTablet::pull_memtables_without_ddl()
{
  int ret = OB_SUCCESS;
  ObTableHandleArray memtable_handles;

  if (OB_ISNULL(memtable_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memtable_mgr_ is null", K(ret));
  } else if (!memtable_mgr_->has_memtable()) {
    LOG_TRACE("no memtable in memtable mgr", K(ret));
  } else if (OB_FAIL(memtable_mgr_->get_all_memtables(memtable_handles))) {
    LOG_WARN("failed to get all memtables from memtable_mgr", K(ret));
  } else {
    int64_t start_snapshot_version = get_snapshot_version();
    const SCN& clog_checkpoint_scn = tablet_meta_.clog_checkpoint_scn_;
    int64_t start_pos = -1;

    for (int64_t i = 0; OB_SUCC(ret) && i < memtable_handles.count(); ++i) {
      memtable::ObIMemtable *table = static_cast<memtable::ObIMemtable*>(memtable_handles.at(i).get_table());
      if (OB_ISNULL(table) || !table->is_memtable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table must not null and must be memtable", K(ret), K(table));
      } else if (table->is_resident_memtable()) { // Single full resident memtable will be available always
        LOG_INFO("is_resident_memtable will be pulled always", K(table->get_key().tablet_id_.id()));
        start_pos = i;
        break;
      } else if (table->get_end_scn() == clog_checkpoint_scn) {
        if (table->get_snapshot_version() > start_snapshot_version) {
          start_pos = i;
          break;
        }
      } else if (table->get_end_scn() > clog_checkpoint_scn) {
        start_pos = i;
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (start_pos < 0 || start_pos >= memtable_handles.count()) {
      // all memtables need to be released
      reset_memtable();
    } else if (OB_FAIL(build_memtable(memtable_handles, start_pos))) {
      LOG_WARN("failed to build table store memtables", K(ret), K(memtable_handles), K(start_pos), K(*this));
    }
  }
  return ret;
}

int ObTablet::build_transfer_in_tablet_status_(
    const ObTabletCreateDeleteMdsUserData &user_data,
    ObTabletMdsData &mds_data,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData new_user_data;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(new_user_data.assign(user_data))) {
    LOG_WARN("assign user data failed", K(ret), K_(is_inited));
  } else {
    new_user_data.tablet_status_ = ObTabletStatus::TRANSFER_IN;
    new_user_data.transfer_ls_id_ = tablet_meta_.ls_id_;
    new_user_data.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_IN;

    const int64_t length = user_data.get_serialize_size();
    char *buffer = static_cast<char*>(allocator.alloc(length));
    int64_t pos = 0;
    if (OB_ISNULL(buffer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(length));
    } else if (OB_FAIL(new_user_data.serialize(buffer, length, pos))) {
      LOG_WARN("failed to serialize user data", K(ret));
    } else {
      mds::MdsDumpKV *&uncommitted_kv = mds_data.tablet_status_.uncommitted_kv_.ptr_;
      const mds::MdsDumpKV *committed_kv = mds_data.tablet_status_.committed_kv_.ptr_;
      if (nullptr == uncommitted_kv) {
        if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, uncommitted_kv))) {
          LOG_WARN("failed to alloc and new", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (nullptr == committed_kv) {
        // do nothing, no need to copy
      } else if (OB_FAIL(uncommitted_kv->assign(*committed_kv, allocator))) {
        LOG_WARN("failed to copy committed kv to uncommitted kv", K(ret));
      } else {
        mds::MdsDumpNode &node = uncommitted_kv->v_;
        node.allocator_ = &allocator;
        node.user_data_.assign(buffer, length);
        mds_data.tablet_status_.committed_kv_.get_ptr()->reset();
      }
    }
  }
  return ret;
}

int ObTablet::update_memtables()
{
  int ret = OB_SUCCESS;
  ObTableHandleArray inc_memtables;
  ObIMemtableMgr *memtable_mgr;

  if (OB_FAIL(get_memtable_mgr(memtable_mgr))) {
    LOG_WARN("failed to get memtable_mgr", K(ret));
  } else if (!memtable_mgr->has_memtable()) {
    LOG_INFO("no memtable in memtable mgr", K(ret), "ls_id", tablet_meta_.ls_id_, "tablet_id", tablet_meta_.tablet_id_);
  } else if (OB_FAIL(memtable_mgr->get_all_memtables(inc_memtables))) {
    LOG_WARN("failed to get all memtables from memtable_mgr", K(ret));
  } else if (is_ls_inner_tablet() && OB_FAIL(rebuild_memtable(inc_memtables))) {
    LOG_ERROR("failed to rebuild table store memtables for ls inner tablet", K(ret), K(inc_memtables), KPC(this));
  } else if (!is_ls_inner_tablet() && memtable_count_ > 0 && OB_FAIL(rebuild_memtable(inc_memtables))) {
    LOG_ERROR("failed to rebuild table store memtables for normal tablet when current memtable exists", K(ret), K(inc_memtables), KPC(this));
  } else if (!is_ls_inner_tablet() && memtable_count_ == 0 && OB_FAIL(rebuild_memtable(tablet_meta_.clog_checkpoint_scn_, inc_memtables))) {
    LOG_ERROR("failed to rebuild table store memtables for normal tablet when current memtable does not exist", K(ret),
        "clog_checkpoint_scn", tablet_meta_.clog_checkpoint_scn_,
        K(inc_memtables), KPC(this));
  }
  LOG_DEBUG("update memtables", K(ret), K(inc_memtables));
  return ret;
}

int ObTablet::inner_get_mds_table(mds::MdsTableHandle &mds_table, bool not_exist_create) const
{
  int ret = OB_SUCCESS;
  ObTabletPointer *tablet_ptr = nullptr;
  if (OB_ISNULL(tablet_ptr = static_cast<ObTabletPointer*>(pointer_hdl_.get_resource_ptr()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet pointer is null", K(ret), KPC(this));
  } else if (OB_FAIL(tablet_ptr->get_mds_table(mds_table, not_exist_create))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get mds table", K(ret));
    }
  }
  return ret;
}

int ObTablet::build_memtable(common::ObIArray<ObTableHandleV2> &handle_array, const int64_t start_pos)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(start_pos < 0 || start_pos >= handle_array.count() || handle_array.count() > MAX_MEMSTORE_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(start_pos), K(handle_array));
  }

  ObITable *table = nullptr;
  for (int64_t i = start_pos; OB_SUCC(ret) && i < handle_array.count(); ++i) {
    memtable::ObMemtable *memtable = nullptr;
    table = handle_array.at(i).get_table();
    if (OB_UNLIKELY(nullptr == table || !table->is_memtable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table must be memtable", K(ret), K(i), KPC(table));
    } else if (FALSE_IT(memtable = reinterpret_cast<memtable::ObMemtable *>(table))) {
    } else if (memtable->is_empty()) {
      FLOG_INFO("Empty memtable discarded", KPC(memtable));
    } else if (OB_FAIL(add_memtable(memtable))) {
      LOG_WARN("failed to add to memtables", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW == ret)) {
      LOG_DEBUG("memtable is full while building memtable", K(ret), KPC(table), K(memtable_count_));
    } else {
      reset_memtable();
    }
  }
  return ret;
}

int ObTablet::read_mds_table(common::ObIAllocator &allocator,
                             ObTabletMdsData &mds_data,
                             const bool for_flush,
                             const int64_t mds_construct_sequence)
{
  TIMEGUARD_INIT(STORAGE, 10_ms);
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  mds_data.reset();
  mds::MdsTableHandle mds_table_handle;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (CLICK_FAIL(mds_data.init_for_first_creation(allocator))) {
    LOG_WARN("failed to init mds data", K(ret));
  } else if (CLICK_FAIL(inner_get_mds_table(mds_table_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_EMPTY_RESULT;
      LOG_INFO("mds table does not exist, may be released", K(ret), K(ls_id), K(tablet_id));
    } else {
      LOG_WARN("failed to get mds table", K(ret), K(ls_id), K(tablet_id));
    }
  } else {
    ObTabletDumpMdsNodeOperator op(mds_data, allocator);
    if (CLICK_FAIL(mds_table_handle.for_each_unit_from_small_key_to_big_from_old_node_to_new_to_dump(op, mds_construct_sequence, for_flush))) {
      LOG_WARN("failed to traverse mds table", K(ret), K(ls_id), K(tablet_id));
    } else if (!op.dumped()) {
      ret = OB_EMPTY_RESULT;
      LOG_INFO("read nothing from mds table", K(ret), K(ls_id), K(tablet_id));
    }
  }

  return ret;
}

int ObTablet::read_mds_table_medium_info_list(
    common::ObIAllocator &allocator,
    ObTabletDumpedMediumInfo &medium_info_list) const
{
  TIMEGUARD_INIT(STORAGE, 10_ms);
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  medium_info_list.reset();
  mds::MdsTableHandle mds_table_handle;

  if (CLICK_FAIL(medium_info_list.init_for_first_creation(allocator))) {
    LOG_WARN("failed to init medium info list", K(ret));
  } else if (CLICK_FAIL(inner_get_mds_table(mds_table_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_EMPTY_RESULT;
      LOG_DEBUG("mds table does not exist, may be released", K(ret), K(ls_id), K(tablet_id));
    } else {
      LOG_WARN("failed to get mds table", K(ret), K(ls_id), K(tablet_id));
    }
  } else {
    ObTabletMediumInfoNodeOperator op(medium_info_list, allocator);
    if (CLICK_FAIL(mds_table_handle.for_each_unit_from_small_key_to_big_from_old_node_to_new_to_dump(op, 0, false/*for_flush*/))) {
      LOG_WARN("failed to traverse mds table", K(ret), K(ls_id), K(tablet_id));
    } else if (!op.dumped()) {
      ret = OB_EMPTY_RESULT;
      LOG_DEBUG("read nothing from mds table", K(ret), K(ls_id), K(tablet_id));
    }
  }

  return ret;
}

int ObTablet::notify_mds_table_flush_ret(
    const share::SCN &flush_scn,
    const int flush_ret)
{
  TIMEGUARD_INIT(STORAGE, 10_ms);
  int ret = OB_SUCCESS;
  mds::MdsTableHandle mds_table;
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (is_ls_inner_tablet()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("inner tablet does not have mds table", K(ret), K(ls_id), K(tablet_id));
  } else if (CLICK_FAIL(inner_get_mds_table(mds_table))) {
    LOG_WARN("failed to get mds table", K(ret), K(ls_id), K(tablet_id));
  } else if (CLICK() && !mds_table.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mds table is null", K(ret), K(ls_id), K(tablet_id));
  } else {
    mds_table.on_flush(flush_scn, flush_ret);
  }

  return ret;
}

int ObTablet::rebuild_memtable(common::ObIArray<ObTableHandleV2> &handle_array)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObTablet isn't inited", K(ret), KPC(this), K(handle_array));
  } else {
    int last_idx = memtable_count_ > 0 ? memtable_count_ - 1 : 0;
    ObITable *last_memtable = memtables_[last_idx];
    share::SCN end_scn = (NULL == last_memtable) ? share::SCN() : last_memtable->get_end_scn();

    LOG_DEBUG("before rebuild memtable", K(memtable_count_), K(last_idx), KP(last_memtable), K(end_scn), K(handle_array));
    for (int64_t i = 0; OB_SUCC(ret) && i < handle_array.count(); ++i) {
      memtable::ObMemtable *memtable = nullptr;
      ObITable *table = handle_array.at(i).get_table();
      if (OB_UNLIKELY(nullptr == table || !table->is_memtable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table must be memtable", K(ret), K(i), KPC(table));
      } else if (FALSE_IT(memtable = static_cast<memtable::ObMemtable *>(table))) {
      } else if (memtable->is_empty()) {
        FLOG_INFO("Empty memtable discarded", KPC(memtable));
      } else if (table->get_end_scn() < end_scn) {
      } else if (exist_memtable_with_end_scn(table, end_scn)) {
        FLOG_INFO("duplicated memtable with same end_scn discarded", KPC(table), K(end_scn));
      } else if (OB_FAIL(add_memtable(memtable))) {
        LOG_WARN("failed to add memtable to curr memtables", K(ret), KPC(this));
      } else {
        LOG_INFO("succeed to add memtable", K(ret), KPC(memtable));
      }
    }
    LOG_DEBUG("after rebuild memtable", K(memtable_count_), K(last_idx), KP(last_memtable), K(end_scn), K(handle_array));
  }
  return ret;
}

int ObTablet::rebuild_memtable(
    const share::SCN &clog_checkpoint_scn,
    common::ObIArray<ObTableHandleV2> &handle_array)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObMemtableArray not inited", K(ret), KPC(this), K(handle_array));
  } else if (OB_UNLIKELY(0 != memtable_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current memtable array is not empty", K(ret), K(clog_checkpoint_scn), K(memtable_count_));
  } else {
    // use clog checkpoint scn to filter memtable handle array
    for (int64_t i = 0; OB_SUCC(ret) && i < handle_array.count(); ++i) {
      memtable::ObMemtable *memtable = nullptr;
      ObITable *table = handle_array.at(i).get_table();
      if (OB_UNLIKELY(nullptr == table || !table->is_memtable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table must be memtable", K(ret), K(i), KPC(table));
      } else if (FALSE_IT(memtable = static_cast<memtable::ObMemtable *>(table))) {
      } else if (memtable->is_empty()) {
        FLOG_INFO("Empty memtable discarded", K(ret), KPC(memtable));
      } else if (table->get_end_scn() <= clog_checkpoint_scn) {
        FLOG_INFO("memtable end scn no greater than clog checkpoint scn, should be discarded", K(ret),
            "end_scn", table->get_end_scn(), K(clog_checkpoint_scn));
      } else if (OB_FAIL(add_memtable(memtable))) {
        LOG_WARN("failed to add memtable", K(ret), KPC(memtable));
      }
    }
  }
  return ret;
}

int ObTablet::add_memtable(memtable::ObMemtable* const table)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), KPC(table));
  } else if (MAX_MEMSTORE_CNT == memtable_count_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_DEBUG("memtable is full", K(ret), KPC(table), K(memtable_count_));
  } else {
    memtables_[memtable_count_]=table;
    memtable_count_++;
    table->inc_ref();
  }
  return ret;
}

bool ObTablet::exist_memtable_with_end_scn(const ObITable *table, const SCN &end_scn)
{
  // when frozen memtable's log was not committed, its right boundary is open (end_scn == MAX)
  // the right boundary would be refined asynchronuously
  // we need to make sure duplicate memtable was not added to tablet,
  // and ensure active memtable could be added to tablet
  bool is_exist = false;
  if (table->get_end_scn() == end_scn && memtable_count_ >= 1) {
    for (int64_t i = memtable_count_ - 1; i >= 0 ; --i) {
      const ObIMemtable *memtable = memtables_[i];
      if (memtable == table) {
        is_exist = true;
        break;
      }
    }
  }
  return is_exist;
}

int ObTablet::assign_memtables(memtable::ObIMemtable * const * memtables, const int64_t memtable_count)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(memtables) || OB_UNLIKELY(memtable_count < 0 || 0 != memtable_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid memtable argument", K(ret), KP(memtables), K(memtable_count));
  } else {
    MEMSET(memtables_, 0, sizeof(memtable::ObIMemtable*) * MAX_MEMSTORE_CNT);
    // deep copy memtables to tablet.memtables_ and inc ref
    for (int64_t i = 0; OB_SUCC(ret) && i < memtable_count; ++i) {
      memtable::ObIMemtable * memtable = memtables[i];
      if (OB_ISNULL(memtable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null memtable ptr", K(ret), K(i), KP(memtables));
      } else {
        memtables_[i] = memtable;
        memtable->inc_ref();
        ++memtable_count_;
      }
    }
  }

  return ret;
}

int ObTablet::assign_ddl_kvs(ObITable * const *ddl_kvs, const int64_t ddl_kv_count)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_NOT_NULL(ddl_kvs) && ddl_kv_count == 0) ||
      OB_UNLIKELY(OB_ISNULL(ddl_kvs) && ddl_kv_count > 0) ||
      OB_UNLIKELY(ddl_kv_count < 0 || 0 != ddl_kv_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ddl_kv argument", K(ret), KP(ddl_kvs), K(ddl_kv_count));
  } else {
    // deep copy ddl_kvs to tablet.ddl_kvs_ and inc ref
    for (int64_t i = 0; OB_SUCC(ret) && i < ddl_kv_count; ++i) {
      ObITable *ddl_kv = ddl_kvs[i];
      if (OB_ISNULL(ddl_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ddl_kvs", K(ret), K(i), KP(ddl_kvs));
      } else {
        ddl_kvs_[i] = ddl_kv;
        ddl_kv->inc_ref();
        ++ddl_kv_count_;
      }
    }
  }

  return ret;
}

void ObTablet::reset_memtable()
{
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  for(int i = 0; i < MAX_MEMSTORE_CNT; ++i) {
    if (OB_NOT_NULL(memtables_[i])) {
      const ObITable::TableType table_type = memtables_[i]->get_key().table_type_;
      const int64_t ref_cnt = memtables_[i]->dec_ref();
      if (0 == ref_cnt) {
        t3m->push_table_into_gc_queue(memtables_[i], table_type);
      }
    }
    memtables_[i] = nullptr;
  }
  memtable_count_ = 0;
}

int ObTablet::clear_memtables_on_table_store() // be careful to call this func
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(allocator_) || OB_ISNULL(table_store_addr_.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("clear_memtables_on_table_store can only be called by full memory tablet",
        K(ret), KP(allocator_), K(table_store_addr_));
  } else {
    table_store_addr_.get_ptr()->clear_memtables();
    reset_memtable();
  }
  return ret;
}

int ObTablet::get_restore_status(ObTabletRestoreStatus::STATUS &restore_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FALSE_IT(tablet_meta_.ha_status_.get_restore_status(restore_status))) {
  } else if (!ObTabletRestoreStatus::is_valid(restore_status)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("restore status is invalid", KR(ret), K(restore_status));
  }

  return ret;
}

int ObTablet::get_mds_table_handle_(mds::MdsTableHandle &handle,
                                    const bool create_if_not_exist) const
{
  int ret = OB_SUCCESS;
  if (is_ls_inner_tablet()) {
    ret = OB_ENTRY_NOT_EXIST;// will continue read mds_data on tablet
    LOG_TRACE("there is no mds table on ls inner tablet yet", KR(ret));
  } else if (OB_FAIL(inner_get_mds_table(handle, create_if_not_exist))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("inner get mds table failed", KR(ret), "ls_id", tablet_meta_.ls_id_, "tablet_id", tablet_meta_.tablet_id_);
    } else if (REACH_TENANT_TIME_INTERVAL(10_s)) {
      LOG_TRACE("inner get mds table failed", KR(ret), "ls_id", tablet_meta_.ls_id_, "tablet_id", tablet_meta_.tablet_id_);
    }
  }
  return ret;
}

int ObTablet::pull_ddl_memtables(ObArenaAllocator &allocator, ObITable **&ddl_kvs_addr, int64_t &ddl_kv_count)
{
  int ret = OB_SUCCESS;
  ObArray<ObITable *> ddl_memtables;
  ObDDLKvMgrHandle kv_mgr_handle;
  bool has_ddl_kv = false;
  ObTablesHandleArray ddl_kvs_handle;
  if (OB_UNLIKELY(0 != ddl_kv_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ddl kv count when pull ddl memtables", K(ret), K(ddl_kv_count), KPC(this));
  } else if (OB_FAIL(get_ddl_kv_mgr(kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get ddl kv mgr failed", K(ret), KPC(this));
    } else {
      LOG_TRACE("there is no ddl kv mgr in this tablet", K(ret));
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(kv_mgr_handle.get_obj()->get_ddl_kvs_for_query(*this, ddl_kvs_handle))) {
    LOG_WARN("failed to get all ddl freeze kvs", K(ret));
  } else {
    ObITable *temp_ddl_kvs;
    if (ddl_kvs_handle.get_count() > 0) {
      ddl_kvs_addr = static_cast<ObITable**>(allocator.alloc(sizeof(ObITable*) * DDL_KV_ARRAY_SIZE));
      if (OB_ISNULL(ddl_kvs_addr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for ddl_kvs_addr", K(ret), K(ddl_kvs_handle.get_count()));
      }
    }
    SCN ddl_checkpoint_scn = get_tablet_meta().ddl_checkpoint_scn_;
    for (int64_t i = 0; OB_SUCC(ret) && i < ddl_kvs_handle.get_count(); ++i) {
      ObDDLKV *ddl_kv = static_cast<ObDDLKV *>(ddl_kvs_handle.get_table(i));
      if (OB_ISNULL(ddl_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get ddl kv failed", K(ret), K(i));
      } else if (ddl_kv->is_closed()) {
        // skip, because closed meanns ddl dump sstable created
      } else if (ddl_kv->get_freeze_scn() > ddl_checkpoint_scn) {
        if (OB_FAIL(ddl_kv->prepare_sstable(false/*need_check*/))) {
          LOG_WARN("prepare sstable failed", K(ret));
        } else if (OB_UNLIKELY(ddl_kv_count >= DDL_KV_ARRAY_SIZE)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ddl kv count overflow", K(ret), K(i), K(ddl_kv_count), K(ddl_kvs_handle));
        } else {
          ddl_kvs_addr[ddl_kv_count] = ddl_kv;
          ddl_kv->inc_ref();
          ++ddl_kv_count;
        }
      }
    }
  }
  if (ddl_kv_count == 0) {
    // In the above for loop, ddl_kvs_addr's assignment can be skipped (e.g. ddl_kv->is_closed()).
    ddl_kvs_addr = nullptr;
  }
  if (OB_SUCC(ret) && OB_ISNULL(ddl_kvs_addr) && ddl_kv_count > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value on ddl_kvs_addr and ddl_kv_count", KP(ddl_kvs_addr), K(ddl_kv_count));
  }
  return ret;
}

void ObTablet::reset_ddl_memtables()
{
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  for(int i = 0; i < ddl_kv_count_; ++i) {
    ObITable *ddl_kv = ddl_kvs_[i];
    if (OB_NOT_NULL(ddl_kv)) {
      const ObITable::TableType table_type = ddl_kv->get_key().table_type_;
      const int64_t ref_cnt = ddl_kv->dec_ref();
      if (0 == ref_cnt) {
        t3m->push_table_into_gc_queue(ddl_kv, table_type);
      }
    }
    ddl_kvs_[i] = nullptr;
  }
  ddl_kvs_ = nullptr;
  ddl_kv_count_ = 0;
}

int ObTablet::set_initial_state(const bool initial_state)
{
  int ret = OB_SUCCESS;
  ObTabletPointer *tablet_ptr = static_cast<ObTabletPointer*>(pointer_hdl_.get_resource_ptr());

  if (OB_ISNULL(tablet_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet pointer is null", K(ret));
  } else {
    tablet_ptr->set_initial_state(initial_state);
  }

  return ret;
}

int ObTablet::load_medium_info_list(
    common::ObArenaAllocator &allocator,
    const ObTabletComplexAddr<oceanbase::storage::ObTabletDumpedMediumInfo> &complex_addr,
    const compaction::ObExtraMediumInfo &extra_info,
    compaction::ObMediumCompactionInfoList &medium_info_list)
{
  int ret = OB_SUCCESS;
  const ObTabletDumpedMediumInfo *list = nullptr;

  if (OB_FAIL(ObTabletMdsData::load_medium_info_list(allocator, complex_addr, list))) {
    LOG_WARN("failed to load medium info list", K(ret), K(complex_addr));
  } else if (OB_FAIL(medium_info_list.init(allocator, extra_info, list))) {
    LOG_WARN("failed to init", K(ret));
  }

  ObTabletMdsData::free_medium_info_list(allocator, list);

  return ret;
}

int ObTablet::get_fused_medium_info_list(
    common::ObArenaAllocator &allocator,
    ObTabletFullMemoryMdsData &mds_data)
{
  int ret = OB_SUCCESS;
  mds_data.reset();
  ObTabletMdsData mds_table_data;
  int64_t finish_medium_scn = 0;
  ObTabletMdsData new_mds_data;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(read_mds_table(allocator, mds_table_data, false))) {
    if (OB_EMPTY_RESULT == ret) {
      // do nothing
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to read mds table", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_finish_medium_scn(finish_medium_scn))) {
    LOG_WARN("failed to get finish medium scn", K(ret));
  } else if (OB_FAIL(new_mds_data.init_for_mds_table_dump(allocator, mds_table_data, mds_data_, finish_medium_scn))) {
    LOG_WARN("failed to init new mds data", K(ret), K(finish_medium_scn));
  } else if (OB_FAIL(validate_medium_info_list(finish_medium_scn, new_mds_data))) {
    LOG_WARN("failed to validate medium info list", K(ret), K(finish_medium_scn));
  } else if (OB_FAIL(mds_data.init(allocator, new_mds_data))) {
    LOG_WARN("failed to assign mds data", K(ret), K(new_mds_data), K(mds_data_), K(mds_table_data));
  }
  return ret;
}

int ObTablet::validate_medium_info_list(
    const int64_t finish_medium_scn,
    const ObTabletMdsData &mds_data) const
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  const ObTabletDumpedMediumInfo *medium_info_list = mds_data.medium_info_list_.ptr_;
  const ObExtraMediumInfo &extra_info = mds_data.extra_medium_info_;

  if (mds_data.medium_info_list_.is_none_object()) {
    LOG_INFO("medium info list addr is none, no need to validate", K(ret), K(ls_id), K(tablet_id), K(finish_medium_scn), "medium_info_list_complex_addr", mds_data.medium_info_list_);
  } else if (OB_ISNULL(medium_info_list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("medium info list is null", K(ret), K(ls_id), K(tablet_id), K(finish_medium_scn), KP(medium_info_list));
  } else if (OB_FAIL(ObMediumListChecker::validate_medium_info_list(extra_info, &medium_info_list->medium_info_list_, finish_medium_scn))) {
    LOG_WARN("failed to validate medium info list", KR(ret), K(ls_id), K(tablet_id), K(mds_data), K(finish_medium_scn));
  }
  return ret;
}

int ObTablet::convert_to_mds_dump_kv(
    common::ObIAllocator &allocator,
    const share::ObTabletAutoincSeq &auto_inc_seq,
    mds::MdsDumpKV &kv)
{
  int ret = OB_SUCCESS;
  mds::MdsDumpKey &key = kv.k_;
  mds::MdsDumpNode &node = kv.v_;

  key.mds_table_id_ = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
  key.mds_unit_id_ = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, share::ObTabletAutoincSeq>>::value;
  key.allocator_ = &allocator;
  // no need to serialize dummy key
  key.key_.reset();

  node.mds_table_id_ = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
  node.mds_unit_id_ = mds::TupleTypeIdx<mds::NormalMdsTable, share::ObTabletAutoincSeq>::value;
  node.status_.union_.field_.node_type_ = mds::MdsNodeType::SET;
  node.status_.union_.field_.writer_type_ = mds::WriterType::AUTO_INC_SEQ;
  node.status_.union_.field_.state_ = mds::TwoPhaseCommitState::ON_COMMIT;

  node.allocator_ = &allocator;
  node.writer_id_ = 0;
  //node->seq_no_ = ;
  node.redo_scn_ = share::SCN::minus(share::SCN::max_scn(), 1);
  node.end_scn_ = share::SCN::invalid_scn();
  node.trans_version_ = share::SCN::minus(share::SCN::max_scn(), 1);

  // serialize
  const int64_t size = auto_inc_seq.get_serialize_size();
  char *buffer = static_cast<char*>(allocator.alloc(size));
  int64_t pos = 0;
  if (OB_ISNULL(buffer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(size));
  } else if (OB_FAIL(auto_inc_seq.serialize(buffer, size, pos))) {
    LOG_WARN("failed to serialize auto inc seq", K(ret), K(auto_inc_seq));
  } else {
    node.user_data_.assign(buffer, size);
  }

  if (OB_FAIL(ret)) {
    if (nullptr != buffer) {
      allocator.free(buffer);
    }
  }

  return ret;
}

int ObTablet::convert_to_mds_dump_kv(
    common::ObIAllocator &allocator,
    const compaction::ObMediumCompactionInfo &info,
    mds::MdsDumpKV &kv)
{
  int ret = OB_NOT_IMPLEMENT;

  return ret;
}

int ObTablet::get_tablet_status_uncommitted_mds_dump_kv(
    common::ObIAllocator &allocator,
    const mds::MdsDumpKV *&kv)
{
  int ret = OB_SUCCESS;
  const ObTabletComplexAddr<mds::MdsDumpKV> &complex_addr = mds_data_.tablet_status_.uncommitted_kv_;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(ObTabletMdsData::load_mds_dump_kv(allocator, complex_addr, kv))) {
    LOG_WARN("failed to load mds dump kv", K(ret));
  }

  return ret;
}

int ObTablet::get_tablet_status_committed_mds_dump_kv(
    common::ObIAllocator &allocator,
    const mds::MdsDumpKV *&kv)
{
  int ret = OB_SUCCESS;
  const ObTabletComplexAddr<mds::MdsDumpKV> &complex_addr = mds_data_.tablet_status_.committed_kv_;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(ObTabletMdsData::load_mds_dump_kv(allocator, complex_addr, kv))) {
    LOG_WARN("failed to load mds dump kv", K(ret));
  }

  return ret;
}

int ObTablet::get_aux_tablet_info_uncommitted_mds_dump_kv(
    common::ObIAllocator &allocator,
    const mds::MdsDumpKV *&kv)
{
  int ret = OB_SUCCESS;
  const ObTabletComplexAddr<mds::MdsDumpKV> &complex_addr = mds_data_.aux_tablet_info_.uncommitted_kv_;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(ObTabletMdsData::load_mds_dump_kv(allocator, complex_addr, kv))) {
    LOG_WARN("failed to load mds dump kv", K(ret));
  }

  return ret;
}

int ObTablet::get_aux_tablet_info_committed_mds_dump_kv(
    common::ObIAllocator &allocator,
    const mds::MdsDumpKV *&kv)
{
  int ret = OB_SUCCESS;
  const ObTabletComplexAddr<mds::MdsDumpKV> &complex_addr = mds_data_.aux_tablet_info_.committed_kv_;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(ObTabletMdsData::load_mds_dump_kv(allocator, complex_addr, kv))) {
    LOG_WARN("failed to load mds dump kv", K(ret));
  }

  return ret;
}

int ObTablet::get_auto_inc_seq_mds_dump_kv(
    common::ObIAllocator &allocator,
    mds::MdsDumpKV *&kv)
{
  int ret = OB_SUCCESS;
  kv = nullptr;
  const ObTabletComplexAddr<share::ObTabletAutoincSeq> &complex_addr = mds_data_.auto_inc_seq_;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (complex_addr.is_none_object()) {
    // do nothing
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, kv))) {
    LOG_WARN("failed to alloc and new", K(ret));
  } else {
    const share::ObTabletAutoincSeq *auto_inc_seq = nullptr;
    if (OB_FAIL(ObTabletMdsData::load_auto_inc_seq(allocator, complex_addr, auto_inc_seq))) {
      LOG_WARN("failed to load auto inc seq", K(ret), K(complex_addr));
    } else if (nullptr == auto_inc_seq) {
      // do nothing
      if (nullptr != kv) {
        allocator.free(kv);
      }
    } else if (OB_FAIL(convert_to_mds_dump_kv(allocator, *auto_inc_seq, *kv))) {
      LOG_WARN("failed to convert to mds dump kv", K(ret));
    }

    if (OB_FAIL(ret)) {
      if (nullptr != kv) {
        allocator.free(kv);
      }
    }
  }

  return ret;
}

int ObTablet::get_medium_info_mds_dump_kv_by_key(
    common::ObIAllocator &allocator,
    const compaction::ObMediumCompactionInfoKey &key,
    mds::MdsDumpKV *&kv)
{
  int ret = OB_NOT_IMPLEMENT;

  return ret;
}

int ObTablet::get_medium_info_mds_dump_kv(
    common::ObIAllocator &allocator,
    const int64_t idx,
    mds::MdsDumpKV *&kv)
{
  int ret = OB_NOT_IMPLEMENT;

  return ret;
}

int ObTablet::check_new_mds_with_cache(
    const int64_t snapshot_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = get_tablet_meta().tablet_id_;
  const ObLSID &ls_id = get_tablet_meta().ls_id_;
  bool r_valid = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    {
      SpinRLockGuard guard(mds_cache_lock_);
      if (tablet_status_cache_.is_valid()) {
        if (OB_FAIL(ObTabletCreateDeleteHelper::check_read_snapshot_by_commit_version(
            *this, tablet_status_cache_.get_create_commit_version(), tablet_status_cache_.get_delete_commit_version(),
            snapshot_version, tablet_status_cache_.get_tablet_status()))) {
          LOG_WARN("failed to check read snapshot by commit version", K(ret), K(snapshot_version), K(tablet_status_cache_));
        }
        r_valid = true;
      }
    }
    if (OB_SUCC(ret) && !r_valid) {
      SpinWLockGuard guard(mds_cache_lock_);
      if (tablet_status_cache_.is_valid()) {
        if (OB_FAIL(ObTabletCreateDeleteHelper::check_read_snapshot_by_commit_version(
            *this, tablet_status_cache_.get_create_commit_version(), tablet_status_cache_.get_delete_commit_version(),
            snapshot_version, tablet_status_cache_.get_tablet_status()))) {
          LOG_WARN("failed to check read snapshot by commit version", K(ret), K(snapshot_version), K(tablet_status_cache_));
        }
      } else if (OB_FAIL(ObTabletCreateDeleteHelper::check_status_for_new_mds(*this, snapshot_version, timeout, tablet_status_cache_))) {
        if (OB_TABLET_NOT_EXIST != ret) {
          LOG_WARN("failed to check status for new mds", KR(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(timeout));
        }
      }
    }
  }

  return ret;
}

int ObTablet::check_tablet_status_for_read_all_committed()
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = get_tablet_meta().tablet_id_;
  const ObLSID &ls_id = get_tablet_meta().ls_id_;
  ObTabletCreateDeleteMdsUserData user_data;
  // first make sure tablet is in any committed state
  // then check if it is empty shell
  if (OB_FAIL(get_tablet_status(share::SCN::max_scn(), user_data, 0/*timeout*/))) {
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_WARN("tablet creation has not been committed, or has been roll backed", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_ERR_SHARED_LOCK_CONFLICT == ret) {
      bool is_committed = false;
      if (OB_FAIL(get_latest_tablet_status(user_data, is_committed))) {
        if (OB_EMPTY_RESULT == ret) {
          ret = OB_TABLET_NOT_EXIST;
          LOG_WARN("tablet creation has no been committed, or has been roll backed", K(ret), K(ls_id), K(tablet_id));
        }
      } else if (!is_committed) {
        if (transaction::ObTransVersion::INVALID_TRANS_VERSION == user_data.create_commit_version_) {
          ret = OB_TABLET_NOT_EXIST;
          LOG_WARN("create commit version is invalid", K(ret), K(ls_id), K(tablet_id), K(user_data));
        }
      }
    } else {
      LOG_WARN("failed to get tablet status", K(ret), K(ls_id), K(tablet_id));
    }
  } else if (OB_UNLIKELY(is_empty_shell())) {
    ret = OB_TABLET_NOT_EXIST;
    LOG_WARN("tablet become empty shell", K(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

int ObTablet::set_tablet_status(
    const ObTabletCreateDeleteMdsUserData &tablet_status,
    mds::MdsCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    SpinWLockGuard guard(mds_cache_lock_);
    if (OB_FAIL(ObITabletMdsInterface::set(tablet_status, ctx))) {
      LOG_WARN("failed to set mds data", K(ret));
    } else {
      tablet_status_cache_.reset();
    }
  }
  return ret;
}

int ObTablet::replay_set_tablet_status(
    const share::SCN &scn,
    const ObTabletCreateDeleteMdsUserData &tablet_status,
    mds::MdsCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    SpinWLockGuard guard(mds_cache_lock_);
    if (OB_FAIL(ObITabletMdsInterface::replay(tablet_status, ctx, scn))) {
      LOG_WARN("failed to replay mds data", K(ret));
    } else {
      tablet_status_cache_.reset();
    }
  }
  return ret;
}

int ObTablet::check_schema_version_with_cache(
    const int64_t schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = get_tablet_meta().tablet_id_;
  bool r_valid = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(timeout < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(timeout));
  } else {
    {
      SpinRLockGuard guard(mds_cache_lock_);
      if (ddl_data_cache_.is_valid()) {
        if (OB_FAIL(check_schema_version(schema_version))) {
          LOG_WARN("fail to check schema version", K(ret));
        }
        r_valid = true;
      }
    }

    if (OB_SUCC(ret) && !r_valid) {
      SpinWLockGuard guard(mds_cache_lock_);
      if (ddl_data_cache_.is_valid()) {
        if (OB_FAIL(check_schema_version(schema_version))) {
          LOG_WARN("fail to check schema version", K(ret));
        }
      } else {
        ObTabletBindingMdsUserData tmp_ddl_data;
        if (OB_FAIL(ObITabletMdsInterface::get_ddl_data(share::SCN::max_scn(), tmp_ddl_data, timeout))) {
          LOG_WARN("failed to get snapshot", KR(ret), K(timeout));
        } else if (FALSE_IT(ddl_data_cache_.set_value(tmp_ddl_data))) {
        } else if (OB_FAIL(check_schema_version(schema_version))) {
          LOG_WARN("fail to check schema version", K(ret), K(ddl_data_cache_));
        } else {
          LOG_INFO("refresh ddl data cache", K(ret), K(tablet_meta_.ls_id_), K(tablet_id), K(ddl_data_cache_),
              K(schema_version), K(timeout), KP(this));
        }
      }
    }
  }

  return ret;
}

int ObTablet::check_schema_version(int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(schema_version < ddl_data_cache_.get_schema_version())) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("use stale schema before ddl", K(ret), K(get_tablet_meta().tablet_id_),
             K(ddl_data_cache_.get_schema_version()), K(schema_version));
  }
  return ret;
}

int ObTablet::check_snapshot_readable_with_cache(
    const int64_t snapshot_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = get_tablet_meta().tablet_id_;
  bool r_valid = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(timeout < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(timeout));
  } else {
    {
      SpinRLockGuard guard(mds_cache_lock_);
      if (ddl_data_cache_.is_valid()) {
        if (OB_FAIL(check_snapshot_readable(snapshot_version))) {
          LOG_WARN("fail to check schema version", K(ret));
        }
        r_valid = true;
      }
    }

    if (OB_SUCC(ret) && !r_valid) {
      SpinWLockGuard guard(mds_cache_lock_);
      if (ddl_data_cache_.is_valid()) {
        if (OB_FAIL(check_snapshot_readable(snapshot_version))) {
          LOG_WARN("fail to check snapshot version", K(ret));
        }
      } else {
        ObTabletBindingMdsUserData tmp_ddl_data;
        if (OB_FAIL(ObITabletMdsInterface::get_ddl_data(share::SCN::max_scn(), tmp_ddl_data, timeout))) {
          LOG_WARN("failed to get snapshot", KR(ret), K(timeout));
        } else if (FALSE_IT(ddl_data_cache_.set_value(tmp_ddl_data))) {
        } else if (OB_FAIL(check_snapshot_readable(snapshot_version))) {
          LOG_WARN("fail to check snapshot version", K(ret), K(ddl_data_cache_));
        } else {
          LOG_INFO("refresh ddl data cache", K(ret), K(tablet_meta_.ls_id_), K(tablet_id), K(ddl_data_cache_),
              K(snapshot_version), K(timeout), KP(this));
        }
      }
    }
  }

  return ret;
}

int ObTablet::check_snapshot_readable(int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  if (OB_UNLIKELY(ddl_data_cache_.is_redefined() && snapshot_version >= ddl_data_cache_.get_snapshot_version())) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("read data after ddl, need to retry on new tablet", K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K_(ddl_data_cache));
  } else if (OB_UNLIKELY(!ddl_data_cache_.is_redefined() && snapshot_version < ddl_data_cache_.get_snapshot_version())) {
    ret = OB_SNAPSHOT_DISCARDED;
    LOG_WARN("read data before ddl", K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K_(ddl_data_cache));
  }
  return ret;
}

int ObTablet::check_transfer_seq_equal(const ObTablet &old_tablet, const int64_t transfer_seq)
{
  int ret = OB_SUCCESS;
  if (old_tablet.get_tablet_meta().transfer_info_.transfer_seq_ != transfer_seq) {
    ret = OB_TABLET_TRANSFER_SEQ_NOT_MATCH;
    LOG_WARN("old tablet transfer seq not eq with new transfer seq",
        "old_tablet_meta", old_tablet.get_tablet_meta(), K(transfer_seq));
  }
  return ret;
}

int ObTablet::set_ddl_info(
    const ObTabletBindingMdsUserData &ddl_info,
    mds::MdsCtx &ctx,
    const int64_t lock_timeout_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    SpinWLockGuard guard(mds_cache_lock_);
    if (OB_FAIL(ObITabletMdsInterface::set(ddl_info, ctx, lock_timeout_us))) {
      LOG_WARN("failed to set ddl info", K(ret));
    } else {
      ddl_data_cache_.reset();
    }
  }
  return ret;
}

int ObTablet::replay_set_ddl_info(
    const share::SCN &scn,
    const ObTabletBindingMdsUserData &ddl_info,
    mds::MdsCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    SpinWLockGuard guard(mds_cache_lock_);
    if (OB_FAIL(ObITabletMdsInterface::replay(ddl_info, ctx, scn))) {
      LOG_WARN("failed to replay set ddl info", K(ret));
    } else {
      ddl_data_cache_.reset();
    }
  }
  return ret;
}

bool ObTablet::is_empty_shell() const
{
  return table_store_addr_.addr_.is_none();
}

bool ObTablet::is_data_complete() const
{
  return !is_empty_shell()
      && !tablet_meta_.has_transfer_table()
      && tablet_meta_.ha_status_.is_data_status_complete();
}

int ObTablet::check_valid(const bool ignore_ha_status) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    ret = inner_check_valid(ignore_ha_status);
  }
  return ret;
}

// only check storage_schema & medium_list when ha_status is none
int ObTablet::inner_check_valid(const bool ignore_ha_status) const
{
  int ret = OB_SUCCESS;
  const bool need_check_ha = tablet_meta_.ha_status_.is_none() && !ignore_ha_status;
  if (need_check_ha && OB_FAIL(check_medium_list())) {
    LOG_WARN("failed to check medium list", K(ret), KPC(this));
  } else if (OB_FAIL(check_sstable_column_checksum())) {
    LOG_WARN("failed to check sstable column checksum", K(ret), KPC(this));
  }
  return ret;
}

int ObTablet::set_frozen_for_all_memtables()
{
  int ret = OB_SUCCESS;
  ObIMemtableMgr *memtable_mgr = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_memtable_mgr(memtable_mgr))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else if (OB_FAIL(memtable_mgr->set_frozen_for_all_memtables())){
    LOG_WARN("failed to set_frozen_for_all_memtables", K(ret));
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase
