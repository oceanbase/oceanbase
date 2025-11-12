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

#include "storage/multi_data_source/ob_tablet_mds_merge_ctx.h"
#include "storage/tablet/ob_mds_schema_helper.h"
#include "storage/multi_data_source/ob_mds_minor_compaction_filter.h"

#define USING_LOG_PREFIX MDS

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::compaction;

namespace oceanbase
{
namespace storage
{
ObTabletMdsMinorMergeCtx::ObTabletMdsMinorMergeCtx(ObTabletMergeDagParam &param, ObArenaAllocator &allocator)
    : ObTabletExeMergeCtx(param, allocator)
{
}

int ObTabletMdsMinorMergeCtx::prepare_schema()
{
  int ret = OB_SUCCESS;
  static_param_.schema_ = ObMdsSchemaHelper::get_instance().get_storage_schema();
  return ret;
}

int ObTabletMdsMinorMergeCtx::prepare_index_tree()
{
  return ObBasicTabletMergeCtx::build_index_tree(
      merge_info_,
      ObMdsSchemaHelper::get_instance().get_rowkey_read_info());
}

void ObTabletMdsMinorMergeCtx::free_schema()
{
  static_param_.schema_ = nullptr;
}

int ObTabletMdsMinorMergeCtx::get_merge_tables(ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_tables_by_key(get_merge_table_result))) {
    LOG_WARN("failed to get tables by key", KR(ret), "param", get_dag_param(), KPC(merge_dag_));
  } else {
    int tmp_ret = OB_SUCCESS;
    filter_ctx_.compaction_filter_ = NULL;
    ObICompactionFilter *compaction_filter = NULL;
    if (OB_TMP_FAIL(prepare_compaction_filter(mem_ctx_.get_allocator(), *get_tablet(), static_param_.get_ls_id(), compaction_filter))) {
      LOG_WARN("prepare compaction filter fail", K(tmp_ret));
    } else {
      filter_ctx_.compaction_filter_ = compaction_filter;
    }
  }
  return ret;
}

int ObTabletMdsMinorMergeCtx::update_tablet(ObTabletHandle &new_tablet_handle)
{
  int ret = OB_SUCCESS;
  const ObSSTable *sstable = nullptr;
  if (OB_FAIL(create_sstable(sstable))) {
    LOG_WARN("failed to create sstable", KR(ret), "dag_param", get_dag_param());
  } else {
    ObUpdateTableStoreParam mds_param(static_param_.version_range_.snapshot_version_,
                                      1/*multi_version_start*/,
                                      ObMdsSchemaHelper::get_instance().get_storage_schema(),
                                      get_ls_rebuild_seq(),
                                      sstable,
                                      false/*allow_duplicate_sstable*/);
    if (OB_FAIL(mds_param.init_with_compaction_info(
      ObCompactionTableStoreParam(get_merge_type(), sstable->get_end_scn()/*clog_checkpoint_scn*/, false/*need_report*/, false/*has_truncate_info*/)))) {
      LOG_WARN("failed to init with compaction info", KR(ret));
    } else if (OB_FAIL(get_ls()->update_tablet_table_store(get_tablet_id(), mds_param, new_tablet_handle))) {
      LOG_WARN("failed to update tablet table store", K(ret), K(mds_param), K(new_tablet_handle));
      CTX_SET_DIAGNOSE_LOCATION(*this);
    } else {
      LOG_INFO("success to update tablet table store with mds table", K(sstable), K(new_tablet_handle));
      time_guard_click(ObStorageCompactionTimeGuard::UPDATE_TABLET);
    }
  }
  return ret;
}

ObTabletCrossLSMdsMinorMergeCtx::ObTabletCrossLSMdsMinorMergeCtx(ObTabletMergeDagParam &param, ObArenaAllocator &allocator)
  : ObTabletMergeCtx(param, allocator)
{
}

int ObTabletCrossLSMdsMinorMergeCtx::prepare_schema()
{
  int ret = OB_SUCCESS;
  static_param_.schema_ = ObMdsSchemaHelper::get_instance().get_storage_schema();
  return ret;
}

int ObTabletCrossLSMdsMinorMergeCtx::prepare_index_tree()
{
  return ObBasicTabletMergeCtx::build_index_tree(
      merge_info_,
      ObMdsSchemaHelper::get_instance().get_rowkey_read_info());
}

void ObTabletCrossLSMdsMinorMergeCtx::free_schema()
{
  static_param_.schema_ = nullptr;
}

int ObTabletCrossLSMdsMinorMergeCtx::get_merge_tables(ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  get_merge_table_result.reset();
  const ObMergeType merge_type = static_param_.get_merge_type();
  ObTablet *tablet = nullptr;

  if (!tablet_handle_.is_valid() || static_param_.tables_handle_.empty()) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet cross ls mds minor merge ctx do not init", K(ret), K(static_param_));
  } else if (OB_UNLIKELY(!is_mds_minor_merge(merge_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), "merge_type", merge_type_to_str(merge_type));
  } else if (OB_FAIL(prepare_compaction_filter())) {
    LOG_WARN("fail to prepare compaction filter", K(ret));
  } else if (OB_FAIL(ObTabletCrossLSMdsMinorMergeCtxHelper::get_merge_tables(merge_type,
      static_param_.tables_handle_, tablet_handle_, get_merge_table_result))) {
    LOG_WARN("failed to get merge tables", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletCrossLSMdsMinorMergeCtx::update_tablet(ObTabletHandle &new_tablet_handle)
{
  int ret = OB_NOT_SUPPORTED;
  // only called in finish merge task, such as ObTabletMergeFinishTask/ObCOMergeFinishTask
  LOG_WARN("cross ls minor merge should not update tablet", K(ret), KPC(this));
  return ret;
}

int ObTabletCrossLSMdsMinorMergeCtx::prepare_compaction_filter()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  if (OB_ISNULL(buf = mem_ctx_.alloc(sizeof(ObCrossLSMdsMinorFilter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), "size", sizeof(ObCrossLSMdsMinorFilter));
  } else {
    ObCrossLSMdsMinorFilter *filter = new (buf) ObCrossLSMdsMinorFilter();
    filter_ctx_.compaction_filter_ = filter;
    FLOG_INFO("success to init mds compaction filter", K(ret));
  }

  return ret;
}

int ObTabletCrossLSMdsMinorMergeCtx::prepare_merge_tables(
    const common::ObIArray<ObTableHandleV2> &table_handle_array)
{
  int ret = OB_SUCCESS;
  if (!static_param_.tables_handle_.empty()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet cross ls mds minor merge ctx init twice", K(ret), K(static_param_));
  } else if (table_handle_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prepare merge tables get invalid argument", K(ret), K(table_handle_array));
  } else if (OB_FAIL(prepare_compaction_filter())) {
    LOG_WARN("failed to prepare compaction filter", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_handle_array.count(); ++i) {
      const ObTableHandleV2 &table_handle = table_handle_array.at(i);
      if (OB_FAIL(static_param_.tables_handle_.add_table(table_handle))) {
        LOG_WARN("failed to add table into array", K(ret), K(table_handle));
      }
    }
  }
  return ret;
}

int ObTabletMdsMinorMergeCtx::prepare_compaction_filter(ObIAllocator &allocator, ObTablet &tablet, const share::ObLSID &ls_id, ObICompactionFilter *&filter)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMdsMinorFilter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), "size", sizeof(ObMdsMinorFilter));
  } else {
    // prepare mds compaction filter
    ObMdsMinorFilter *compaction_filter = new (buf) ObMdsMinorFilter();
    const int64_t last_major_snapshot = tablet.get_last_major_snapshot_version();
    const int64_t multi_version_start = tablet.get_multi_version_start();
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(compaction_filter->init(last_major_snapshot, multi_version_start, ls_id))) {
      LOG_WARN("failed to init mds compaction_filter", K(tmp_ret), K(last_major_snapshot), K(multi_version_start));
    } else {
      filter = compaction_filter;
      FLOG_INFO("success to init mds compaction filter", K(tmp_ret), K(last_major_snapshot), K(multi_version_start));
    }

    if (OB_TMP_FAIL(tmp_ret)) {
      if (OB_NOT_NULL(buf)) {
        allocator.free(buf);
        buf = nullptr;
      }
    }
  }
  return ret;
}

int ObTabletCrossLSMdsMinorMergeCtxHelper::get_merge_tables(
    const ObMergeType merge_type,
    const ObTablesHandleArray &tables_handle,
    const ObTabletHandle &tablet_handle,
    ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  get_merge_table_result.reset();
  const ObTablet *tablet = nullptr;

  if (!tablet_handle.is_valid() || tables_handle.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet cross ls mds minor merge ctx get merge tables get invalid argument",
        K(ret), K(merge_type), K(tables_handle), K(tablet_handle));
  } else if (OB_UNLIKELY(!is_mds_minor_merge(merge_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), "merge_type", merge_type_to_str(merge_type));
  } else if (OB_FAIL(get_merge_table_result.handle_.assign(tables_handle))) {
    LOG_WARN("failed to assign table handle array", K(ret), K(tables_handle));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_handle));
  } else if (OB_FAIL(tablet->get_private_transfer_epoch(get_merge_table_result.private_transfer_epoch_))) {
    LOG_WARN("failed to get private transfer epoch", K(ret), KPC(tablet));
  } else {
    get_merge_table_result.scn_range_.start_scn_ = tables_handle.get_table(0)->get_start_scn();
    get_merge_table_result.rec_scn_ = tables_handle.get_table(0)->get_rec_scn();
    get_merge_table_result.scn_range_.end_scn_ = tables_handle.get_table(tables_handle.get_count() - 1)->get_end_scn();
    get_merge_table_result.version_range_.snapshot_version_ = tablet->get_snapshot_version();
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
ObSSTabletCrossLSMdsMinorMergeCtx::ObSSTabletCrossLSMdsMinorMergeCtx(
    compaction::ObTabletMergeDagParam &param, common::ObArenaAllocator &allocator)
  : ObTabletSSMinorMergeCtx(param, allocator)
{
}

ObSSTabletCrossLSMdsMinorMergeCtx::~ObSSTabletCrossLSMdsMinorMergeCtx()
{
}

int ObSSTabletCrossLSMdsMinorMergeCtx::get_merge_tables(ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  get_merge_table_result.reset();
  const ObMergeType merge_type = static_param_.get_merge_type();
  ObTablet *tablet = nullptr;

  if (!tablet_handle_.is_valid() || static_param_.tables_handle_.empty()) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet cross ls mds minor merge ctx do not init", K(ret), K(static_param_));
  } else if (OB_UNLIKELY(!is_mds_minor_merge(merge_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), "merge_type", merge_type_to_str(merge_type));
  } else if (OB_FAIL(ObTabletCrossLSMdsMinorMergeCtxHelper::get_merge_tables(merge_type,
      static_param_.tables_handle_, tablet_handle_, get_merge_table_result))) {
    LOG_WARN("failed to get merge tables", K(ret), K(merge_type));
  }
  return ret;
}

int ObSSTabletCrossLSMdsMinorMergeCtx::update_tablet(ObTabletHandle &new_tablet_handle)
{
  int ret = OB_NOT_SUPPORTED;
  // only called in finish merge task, such as ObTabletMergeFinishTask/ObCOMergeFinishTask
  LOG_WARN("cross ls minor merge should not update tablet", K(ret), KPC(this));
  return ret;
}

int ObSSTabletCrossLSMdsMinorMergeCtx::prepare_merge_tables(
    const common::ObIArray<ObTableHandleV2> &table_handle_array)
{
  int ret = OB_SUCCESS;
  if (!static_param_.tables_handle_.empty()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet cross ls mds minor merge ctx init twice", K(ret), K(static_param_));
  } else if (table_handle_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prepare merge tables get invalid argument", K(ret), K(table_handle_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_handle_array.count(); ++i) {
      const ObTableHandleV2 &table_handle = table_handle_array.at(i);
      if (OB_FAIL(static_param_.tables_handle_.add_table(table_handle))) {
        LOG_WARN("failed to add table into array", K(ret), K(table_handle));
      }
    }
  }
  return ret;
}

int ObSSTabletCrossLSMdsMinorMergeCtx::prepare_compaction_filter()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  if (OB_ISNULL(buf = mem_ctx_.alloc(sizeof(ObCrossLSMdsMinorFilter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), "size", sizeof(ObCrossLSMdsMinorFilter));
  } else {
    ObCrossLSMdsMinorFilter *filter = new (buf) ObCrossLSMdsMinorFilter();
    filter_ctx_.compaction_filter_ = filter;
    FLOG_INFO("success to init mds compaction filter", K(ret));
  }

  return ret;
}

int ObSSTabletCrossLSMdsMinorMergeCtx::build_sstable(
    ObTableHandleV2 &table_handle,
    uint64_t &op_id)
{
  int ret = OB_SUCCESS;
  const ObSSTable *sstable = NULL;
  table_handle.reset();
  op_id = 0;
  if (OB_FAIL(create_sstable(sstable))) {
    LOG_WARN("failed to create sstable", KR(ret), "dag_param", get_dag_param());
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("build sstable should not be NULL", K(ret), KPC(this));
  } else if (OB_FAIL(get_minor_op_id_(op_id))) {
    LOG_WARN("failed to get op id", K(ret), KPC(this));
  } else {
    table_handle = merged_table_handle_;
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(fail_add_minor_op_())) {
      LOG_WARN("fail add minor op fail", K(tmp_ret), K(ret));
    }
  }

  if (FAILEDx(finish_add_minor_op_())) {
    LOG_WARN("finish add minor op fail", K(ret));
  }
  LOG_INFO("build sstable", K(ret), KPC(sstable), KPC(this), K(op_id));
  return ret;
}
#endif

} // namespace storage
} // namespace oceanbase
