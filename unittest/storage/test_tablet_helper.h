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

#ifndef OCEANBASE_STORAGE_TEST_TABLET_HELPER
#define OCEANBASE_STORAGE_TEST_TABLET_HELPER

#define private public
#define protected public

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_rpc_struct.h"
#include "share/scn.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/tablet/ob_tablet_slog_helper.h"
#include "storage/init_basic_struct.h"

namespace oceanbase
{
namespace storage
{
class TestTabletHelper
{
public:
  static void prepare_sstable_param(
      const common::ObTabletID &tablet_id,
      const share::schema::ObTableSchema &table_schema,
      ObTabletCreateSSTableParam &param);
  static int create_tablet(
      ObLSHandle &ls_handle,
      const common::ObTabletID &tablet_id,
      const share::schema::ObTableSchema &table_schema,
      common::ObArenaAllocator &allocator,
      const ObTabletStatus::Status tablet_status,
      const share::SCN &create_commit_scn,
      ObTabletHandle &handle);
  static int create_tablet(
      ObLSHandle &ls_handle,
      const common::ObTabletID &tablet_id,
      const share::schema::ObTableSchema &table_schema,
      common::ObArenaAllocator &allocator,
      const ObTabletStatus::Status tablet_status = ObTabletStatus::NORMAL,
      const share::SCN &create_commit_scn = share::SCN::min_scn());
  static int remove_tablet(
      const ObLSHandle &ls_handle,
      const common::ObTabletID &tablet_id);
};

inline void TestTabletHelper::prepare_sstable_param(
    const common::ObTabletID &tablet_id,
    const share::schema::ObTableSchema &table_schema,
    ObTabletCreateSSTableParam &param)
{
  const int64_t multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  param.table_key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  param.table_key_.tablet_id_ = tablet_id;
  param.table_key_.version_range_.base_version_ = ObVersionRange::MIN_VERSION;
  param.table_key_.version_range_.snapshot_version_ = 1;
  param.schema_version_ = table_schema.get_schema_version();
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = table_schema.get_progressive_merge_round();
  param.progressive_merge_step_ = 0;
  param.table_mode_ = table_schema.get_table_mode_struct();
  param.index_type_ = table_schema.get_index_type();
  param.rowkey_column_cnt_ = table_schema.get_rowkey_column_num()
            + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  param.root_block_addr_.set_none_addr();
  param.data_block_macro_meta_addr_.set_none_addr();
  param.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.data_index_tree_height_ = 0;
  param.index_blocks_cnt_ = 0;
  param.data_blocks_cnt_ = 0;
  param.micro_block_cnt_ = 0;
  param.use_old_macro_block_count_ = 0;
  param.column_cnt_ = table_schema.get_column_count() + multi_version_col_cnt;
  param.data_checksum_ = 0;
  param.occupy_size_ = 0;
  param.ddl_scn_.set_min();
  param.filled_tx_scn_.set_min();
  param.original_size_ = 0;
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.encrypt_id_ = 0;
  param.master_key_id_ = 0;
}

inline int TestTabletHelper::create_tablet(
    ObLSHandle &ls_handle,
    const common::ObTabletID &tablet_id,
    const share::schema::ObTableSchema &table_schema,
    common::ObArenaAllocator &allocator,
    const ObTabletStatus::Status tablet_status,
    const share::SCN &create_commit_scn,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObLSTabletService *ls_tablet_svr = ls_handle.get_ls()->get_tablet_svr();

  ObTabletCreateSSTableParam param;
  prepare_sstable_param(tablet_id, table_schema, param);
  ObSSTable *sstable = nullptr;
  if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, sstable))) {
    STORAGE_LOG(WARN, "fail to alloc and new", K(ret));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param, allocator, *sstable))) {
    STORAGE_LOG(WARN, "failed to acquire sstable", K(ret));
  } else if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_))) {
    STORAGE_LOG(WARN, "fill column checksum failed", K(ret), K(param));
  } else {
    const int64_t snapshot_version = 1;
    const share::ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
    ObFreezer *freezer = ls_handle.get_ls()->get_freezer();
    ObTabletTableStoreFlag store_flag;
    const lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
    ObTabletHandle tablet_handle;
    const ObTabletMapKey key(ls_id, tablet_id);
    if (OB_FAIL(t3m->create_msd_tablet(WashTabletPriority::WTP_HIGH, key, ls_handle, tablet_handle))) {
      STORAGE_LOG(WARN, "t3m acquire tablet failed", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(tablet_handle.get_obj()->init_for_first_time_creation(
        *tablet_handle.get_allocator(),
        ls_id, tablet_id, tablet_id, share::SCN::base_scn(),
        snapshot_version, table_schema, compat_mode, store_flag, sstable, freezer))){
      STORAGE_LOG(WARN, "failed to init tablet", K(ret), K(ls_id), K(tablet_id));
    } else if (ObTabletStatus::Status::MAX != tablet_status) {
      ObTabletCreateDeleteMdsUserData data;
      data.tablet_status_ = tablet_status;
      data.create_commit_scn_ = create_commit_scn;
      data.create_commit_version_ = create_commit_scn.get_val_for_tx();
      if (tablet_status == ObTabletStatus::Status::DELETED) {
        data.delete_commit_scn_ = SCN::base_scn();
      }

      ObTabletComplexAddr<mds::MdsDumpKV> &tablet_status_committed_kv = tablet_handle.get_obj()->mds_data_.tablet_status_.committed_kv_;
      const int64_t data_serialize_size = data.get_serialize_size();
      int64_t pos = 0;
      char *buf = static_cast<char *>(t3m->full_tablet_creator_.get_allocator().alloc(data_serialize_size));
      if (OB_FAIL(data.serialize(buf, data_serialize_size, pos))) {
        STORAGE_LOG(WARN, "data serialize failed", K(ret), K(data_serialize_size), K(pos));
      } else if (ObTabletObjLoadHelper::alloc_and_new(*tablet_handle.get_allocator(), tablet_status_committed_kv.ptr_)) {
        STORAGE_LOG(WARN, "failed to alloc and new", K(ret));
      } else {
        tablet_status_committed_kv.ptr_->v_.user_data_.assign(buf, data_serialize_size);
      }
    }

    if (FAILEDx(t3m->compare_and_swap_tablet(key, tablet_handle, tablet_handle))) {
      STORAGE_LOG(WARN, "failed to compare and swap tablet", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(ls_tablet_svr->tablet_id_set_.set(tablet_id))){
      STORAGE_LOG(WARN, "set tablet id failed", K(ret), K(tablet_id));
    } else {
      handle = tablet_handle;
    }
  }
  return ret;
}

inline int TestTabletHelper::create_tablet(
    ObLSHandle &ls_handle,
    const common::ObTabletID &tablet_id,
    const share::schema::ObTableSchema &table_schema,
    common::ObArenaAllocator &allocator,
    const ObTabletStatus::Status tablet_status,
    const share::SCN &create_commit_scn)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;

  if (OB_FAIL(create_tablet(ls_handle, tablet_id, table_schema, allocator, tablet_status, create_commit_scn, tablet_handle))) {
    STORAGE_LOG(WARN, "failed to create tablet", K(ret), K(tablet_id));
  }

  return ret;
}

inline int TestTabletHelper::remove_tablet(const ObLSHandle &ls_handle, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  ObTabletHandle tablet_handle;
  ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle);
  const share::ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTabletCreateDeleteMdsUserData data;
  ObTabletStatus status(ObTabletStatus::DELETING);
  data.tablet_status_ = status;
  const int64_t data_serialize_size = data.get_serialize_size();
  int64_t pos = 0;
  char *buf = reinterpret_cast<char *>(t3m->full_tablet_creator_.get_allocator().alloc(data_serialize_size));
  if (OB_FAIL(data.serialize(buf, data_serialize_size, pos))) {
    STORAGE_LOG(WARN, "data serialize failed", K(ret), K(data_serialize_size), K(pos));
  } else {
    tablet_handle.get_obj()->mds_data_.tablet_status_.committed_kv_.get_ptr()->v_.user_data_.assign(buf, data_serialize_size);
    ObMetaDiskAddr disk_addr;
    disk_addr.set_mem_addr(0, sizeof(ObTablet));
    if(OB_FAIL(t3m->compare_and_swap_tablet(
            ObTabletMapKey(ls_id, tablet_id), tablet_handle, tablet_handle))) {
      STORAGE_LOG(WARN, "failed to compare and swap tablet", K(ret), K(ls_id), K(tablet_id), K(disk_addr));
    }
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_TEST_TABLET_HELPER
