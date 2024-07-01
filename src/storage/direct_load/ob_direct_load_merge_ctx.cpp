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

#include "storage/direct_load/ob_direct_load_merge_ctx.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "storage/direct_load/ob_direct_load_external_table.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_block.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable.h"
#include "storage/direct_load/ob_direct_load_range_splitter.h"
#include "storage/direct_load/ob_direct_load_partition_del_lob_task.h"
#include "storage/direct_load/ob_direct_load_partition_merge_task.h"
#include "storage/direct_load/ob_direct_load_partition_rescan_task.h"
#include "storage/direct_load/ob_direct_load_tmp_file.h"
#include "storage/direct_load/ob_direct_load_external_multi_partition_table.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;
using namespace common;
using namespace share;
using namespace table;
using namespace sql;
using namespace observer;

/**
 * ObDirectLoadMergeParam
 */

ObDirectLoadMergeParam::ObDirectLoadMergeParam()
  : table_id_(OB_INVALID_ID),
    lob_meta_table_id_(OB_INVALID_ID),
    target_table_id_(OB_INVALID_ID),
    rowkey_column_num_(0),
    store_column_count_(0),
    fill_cg_thread_cnt_(0),
    lob_column_idxs_(nullptr),
    datum_utils_(nullptr),
    col_descs_(nullptr),
    lob_meta_datum_utils_(nullptr),
    lob_meta_col_descs_(nullptr),
    is_heap_table_(false),
    is_fast_heap_table_(false),
    is_incremental_(false),
    insert_mode_(ObDirectLoadInsertMode::INVALID_INSERT_MODE),
    insert_table_ctx_(nullptr),
    dml_row_handler_(nullptr),
    file_mgr_(nullptr)
{
}

ObDirectLoadMergeParam::~ObDirectLoadMergeParam()
{
}

bool ObDirectLoadMergeParam::is_valid() const
{
  return OB_INVALID_ID != table_id_ && OB_INVALID_ID != target_table_id_ &&
         0 < rowkey_column_num_ && 0 < store_column_count_ && nullptr != lob_column_idxs_ &&
         table_data_desc_.is_valid() && nullptr != datum_utils_ && nullptr != col_descs_ &&
         lob_id_table_data_desc_.is_valid() && nullptr != lob_meta_datum_utils_ && nullptr != lob_meta_col_descs_ &&
         ObDirectLoadInsertMode::is_type_valid(insert_mode_) &&
         (ObDirectLoadInsertMode::INC_REPLACE == insert_mode_ ? is_incremental_ : true) &&
         (ObDirectLoadInsertMode::OVERWRITE == insert_mode_ ? !is_incremental_ : true) &&
         nullptr != insert_table_ctx_ && nullptr != dml_row_handler_ && nullptr != file_mgr_;
}

/**
 * ObDirectLoadMergeCtx
 */

ObDirectLoadMergeCtx::ObDirectLoadMergeCtx()
  : allocator_("TLD_MergeCtx"), ctx_(nullptr), is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  tablet_merge_ctx_array_.set_tenant_id(MTL_ID());
}

ObDirectLoadMergeCtx::~ObDirectLoadMergeCtx()
{
  for (int64_t i = 0; i < tablet_merge_ctx_array_.count(); ++i) {
    ObDirectLoadTabletMergeCtx *tablet_ctx = tablet_merge_ctx_array_.at(i);
    tablet_ctx->~ObDirectLoadTabletMergeCtx();
    allocator_.free(tablet_ctx);
  }
  tablet_merge_ctx_array_.reset();
  FOREACH(iter, table_builder_map_)
  {
    ObIDirectLoadPartitionTableBuilder *table_builder = iter->second;
    table_builder->~ObIDirectLoadPartitionTableBuilder();
    allocator_.free(table_builder);
  }
  table_builder_map_.destroy();
}

int ObDirectLoadMergeCtx::init(ObTableLoadTableCtx *ctx,
                               const ObDirectLoadMergeParam &param,
                               const ObIArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMerger init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == ctx
                         || !param.is_valid()
                         || ls_partition_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(ctx), K(param), K(ls_partition_ids));
  } else if (OB_FAIL(table_builder_map_.create(1024, "TLD_TblBd", "TLD_TblBd", MTL_ID()))) {
    LOG_WARN("fail to create table_builder_map_", KR(ret));
  } else {
    ctx_ = ctx;
    param_ = param;
    if (OB_FAIL(create_all_tablet_ctxs(ls_partition_ids))) {
      LOG_WARN("fail to create all tablet ctxs", KR(ret));
    } else {
      lib::ob_sort(tablet_merge_ctx_array_.begin(), tablet_merge_ctx_array_.end(),
                [](const ObDirectLoadTabletMergeCtx *lhs, const ObDirectLoadTabletMergeCtx *rhs) {
                  return lhs->get_tablet_id().compare(rhs->get_tablet_id()) < 0;
                });
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMergeCtx::get_table_builder(ObIDirectLoadPartitionTableBuilder *&table_builder)
{
  int ret = OB_SUCCESS;
  table_builder = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else {
    const int64_t part_id = get_tid_cache();
    if (OB_FAIL(table_builder_map_.get_refactored(part_id, table_builder))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get table builder", KR(ret), K(part_id));
      } else {
        ret = OB_SUCCESS;
        ObDirectLoadExternalMultiPartitionTableBuildParam builder_param;
        builder_param.table_data_desc_ = param_.table_data_desc_;
        builder_param.datum_utils_ = param_.datum_utils_;
        builder_param.file_mgr_ = param_.file_mgr_;
        builder_param.extra_buf_ = reinterpret_cast<char *>(1);     // unuse, delete in future
        builder_param.extra_buf_size_ = 4096;                       // unuse, delete in future
        builder_param.table_data_desc_.rowkey_column_num_ = 1;      // only a column of lobid
        builder_param.table_data_desc_.column_count_ = 1;           // only a column of lobid
        {
          ObMutexGuard guard(mutex_);
          // use mutext to avoid thread unsafe allocator;
          ObDirectLoadExternalMultiPartitionTableBuilder *new_builder = nullptr;
          if (OB_ISNULL(new_builder = OB_NEWx(
              ObDirectLoadExternalMultiPartitionTableBuilder, &allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to new ObDirectLoadExternalMultiPartitionTableBuilder", KR(ret));
          } else if (OB_FAIL(new_builder->init(builder_param))) {
            LOG_WARN("fail to init new_builder", KR(ret));
          } else if (OB_FAIL(table_builder_map_.set_refactored(part_id, new_builder))) {
            LOG_WARN("fail to set table_builder_map_", KR(ret), K(part_id));
          } else {
            table_builder = new_builder;
          }
          if (OB_FAIL(ret)) {
            if (nullptr != new_builder) {
              new_builder->~ObDirectLoadExternalMultiPartitionTableBuilder();
              allocator_.free(new_builder);
              new_builder = nullptr;
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObDirectLoadMergeCtx::close_table_builder()
{
  int ret = OB_SUCCESS;
  FOREACH_X(item, table_builder_map_, OB_SUCC(ret)) {
    if (item->second != nullptr) {
      if (OB_FAIL(item->second->close())) {
        LOG_WARN("fail to close table", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadMergeCtx::create_all_tablet_ctxs(
  const ObIArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_partition_ids.count(); ++i) {
    const ObTableLoadLSIdAndPartitionId &ls_partition_id = ls_partition_ids.at(i);
    ObDirectLoadTabletMergeCtx *partition_ctx = nullptr;
    if (OB_ISNULL(partition_ctx = OB_NEWx(ObDirectLoadTabletMergeCtx, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadTabletMergeCtx", KR(ret));
    } else if (OB_FAIL(partition_ctx->init(this, ctx_, param_, ls_partition_id))) {
      LOG_WARN("fail to init tablet ctx", KR(ret), K(param_), K(ls_partition_id));
    } else if (OB_FAIL(tablet_merge_ctx_array_.push_back(partition_ctx))) {
      LOG_WARN("fail to push back", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != partition_ctx) {
        partition_ctx->~ObDirectLoadTabletMergeCtx();
        allocator_.free(partition_ctx);
        partition_ctx = nullptr;
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadTabletMergeCtx
 */

ObDirectLoadTabletMergeCtx::ObDirectLoadTabletMergeCtx()
  : allocator_("TLD_MegTbtCtx"),
    merge_ctx_(nullptr),
    ctx_(nullptr),
    task_finish_count_(0),
    rescan_task_finish_count_(0),
    del_lob_task_finish_count_(0),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  sstable_array_.set_tenant_id(MTL_ID());
  multiple_sstable_array_.set_tenant_id(MTL_ID());
  multiple_heap_table_array_.set_tenant_id(MTL_ID());
  del_lob_multiple_sstable_array_.set_tenant_id(MTL_ID());
  range_array_.set_tenant_id(MTL_ID());
  del_lob_range_array_.set_tenant_id(MTL_ID());
  task_array_.set_tenant_id(MTL_ID());
  rescan_task_array_.set_tenant_id(MTL_ID());
  del_lob_task_array_.set_tenant_id(MTL_ID());
}

ObDirectLoadTabletMergeCtx::~ObDirectLoadTabletMergeCtx()
{
  merge_ctx_ = nullptr;
  ctx_ = nullptr;
  for (int64_t i = 0; i < task_array_.count(); ++i) {
    ObDirectLoadPartitionMergeTask *task = task_array_.at(i);
    task->~ObDirectLoadPartitionMergeTask();
    allocator_.free(task);
  }
  for (int64_t i = 0; i < rescan_task_array_.count(); ++i) {
    ObDirectLoadPartitionRescanTask *task = rescan_task_array_.at(i);
    task->~ObDirectLoadPartitionRescanTask();
    allocator_.free(task);
  }
  for (int64_t i = 0; i < del_lob_task_array_.count(); ++i) {
    ObDirectLoadPartitionDelLobTask *task = del_lob_task_array_.at(i);
    task->~ObDirectLoadPartitionDelLobTask();
    allocator_.free(task);
  }
  task_array_.reset();
  rescan_task_array_.reset();
  del_lob_task_array_.reset();
}

int ObDirectLoadTabletMergeCtx::init(ObDirectLoadMergeCtx *merge_ctx,
                                     ObTableLoadTableCtx *ctx,
                                     const ObDirectLoadMergeParam &param,
                                     const ObTableLoadLSIdAndPartitionId &ls_partition_id)

{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadTabletMergeCtx init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == merge_ctx || nullptr == ctx || !param.is_valid() ||
                         !ls_partition_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(merge_ctx), KP(ctx), K(param), K(ls_partition_id));
  } else {
    const ObLSID &ls_id = ls_partition_id.ls_id_;
    const ObTabletID &tablet_id = ls_partition_id.part_tablet_id_.tablet_id_;
    ObTabletID lob_meta_tablet_id;
    if (OB_INVALID_ID != param.lob_meta_table_id_) {
      ObLSService *ls_service = nullptr;
      ObLSHandle ls_handle;
      ObTabletHandle tablet_handle;
      ObTabletBindingMdsUserData ddl_data;
      if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
      } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
        LOG_WARN("failed to get log stream", K(ret));
      } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tablet_handle,
                                                  ObMDSGetTabletMode::READ_ALL_COMMITED))) {
        LOG_WARN("get tablet handle failed", K(ret));
      } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_data(SCN::max_scn(), ddl_data))) {
        LOG_WARN("get ddl data failed", K(ret));
      } else {
        lob_meta_tablet_id = ddl_data.lob_meta_tablet_id_;
      }
    }
    if (OB_SUCC(ret)) {
      ObDirectLoadOriginTableCreateParam origin_table_param;
      origin_table_param.table_id_ = param.table_id_;
      origin_table_param.ls_id_ = ls_id;
      origin_table_param.tablet_id_ = tablet_id;
      origin_table_param.tx_id_ = param.trans_param_.tx_id_;
      origin_table_param.tx_seq_ = param.trans_param_.tx_seq_;
      if (OB_FAIL(origin_table_.init(origin_table_param))) {
        LOG_WARN("fail to init origin table", KR(ret));
      }
    }
    if (OB_SUCC(ret) && OB_INVALID_ID != param.lob_meta_table_id_) {
      ObDirectLoadOriginTableCreateParam lob_meta_origin_table_param;
      lob_meta_origin_table_param.table_id_ = param.lob_meta_table_id_;
      lob_meta_origin_table_param.ls_id_ = ls_id;
      lob_meta_origin_table_param.tablet_id_ = lob_meta_tablet_id;
      lob_meta_origin_table_param.tx_id_ = param.trans_param_.tx_id_;
      lob_meta_origin_table_param.tx_seq_ = param.trans_param_.tx_seq_;
      if (OB_FAIL(lob_meta_origin_table_.init(lob_meta_origin_table_param))) {
        LOG_WARN("fail to init lob meta origin table", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      merge_ctx_ = merge_ctx;
      ctx_ = ctx;
      param_ = param;
      tablet_id_ = tablet_id;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::init_sstable_array(
  const ObIArray<ObIDirectLoadPartitionTable *> &table_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_array.count(); ++i) {
    ObDirectLoadSSTable *sstable = nullptr;
    if (OB_ISNULL(sstable = dynamic_cast<ObDirectLoadSSTable *>(table_array.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table", KR(ret), K(i), K(table_array));
    } else if (OB_FAIL(sstable_array_.push_back(sstable))) {
      LOG_WARN("fail to push back sstable", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::init_multiple_sstable_array(
  const ObIArray<ObIDirectLoadPartitionTable *> &table_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_array.count(); ++i) {
    ObDirectLoadMultipleSSTable *sstable = nullptr;
    if (OB_ISNULL(sstable = dynamic_cast<ObDirectLoadMultipleSSTable *>(table_array.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table", KR(ret), K(i), K(table_array));
    } else if (OB_FAIL(multiple_sstable_array_.push_back(sstable))) {
      LOG_WARN("fail to push back multiple sstable", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::init_multiple_heap_table_array(
  const ObIArray<ObIDirectLoadPartitionTable *> &table_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_array.count(); ++i) {
    ObDirectLoadMultipleHeapTable *heap_table = nullptr;
    if (OB_ISNULL(heap_table = dynamic_cast<ObDirectLoadMultipleHeapTable *>(table_array.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table", KR(ret), K(i), K(table_array));
    } else if (OB_FAIL(multiple_heap_table_array_.push_back(heap_table))) {
      LOG_WARN("fail to push back multiple heap table", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_merge_task(
  const ObIArray<ObIDirectLoadPartitionTable *> &table_array,
  const ObIArray<ObColDesc> &col_descs,
  int64_t max_parallel_degree,
  bool is_multiple_mode)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(max_parallel_degree <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(max_parallel_degree));
  } else {
    if (table_array.empty()) {
      if (OB_FAIL(build_empty_data_merge_task(col_descs, max_parallel_degree))) {
        LOG_WARN("fail to build empty data merge task", KR(ret));
      }
    } else if (!param_.is_heap_table_) {
      if (!is_multiple_mode) {
        // 有主键表不排序也写成multiple sstable
        if (OB_FAIL(build_pk_table_multiple_merge_task(table_array, col_descs, max_parallel_degree))) {
          LOG_WARN("fail to build pk table merge task", KR(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("This should never be reached", KR(ret));
      }
    } else {
      if (!is_multiple_mode) {
        if (!param_.is_fast_heap_table_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("This should never be reached", KR(ret));
        } else if (OB_FAIL(build_empty_data_merge_task(col_descs, max_parallel_degree))) {
          LOG_WARN("fail to build empty table merge task", KR(ret));
        }
      } else {
        if (OB_FAIL(
              build_heap_table_multiple_merge_task(table_array, col_descs, max_parallel_degree))) {
          LOG_WARN("fail to build heap table multiple merge task", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_empty_data_merge_task(const ObIArray<ObColDesc> &col_descs,
                                                            int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  // only existing data, construct task by split range
  ObDirectLoadMergeRangeSplitter range_splitter;
  if (OB_FAIL(range_splitter.init(
      (merge_with_origin_data() ? &origin_table_ : nullptr),
      sstable_array_,
      param_.datum_utils_,
      col_descs))) {
    LOG_WARN("fail to init range splitter", KR(ret));
  } else if (OB_FAIL(range_splitter.split_range(range_array_, max_parallel_degree, allocator_))) {
    LOG_WARN("fail to split range", KR(ret));
  } else if (OB_UNLIKELY(range_array_.count() > max_parallel_degree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected range count", KR(ret), K(max_parallel_degree), K(range_array_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < range_array_.count(); ++i) {
    const ObDatumRange &range = range_array_.at(i);
    ObDirectLoadPartitionRangeMergeTask *merge_task = nullptr;
    if (OB_ISNULL(merge_task = OB_NEWx(ObDirectLoadPartitionRangeMergeTask, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadPartitionRangeMergeTask", KR(ret));
    } else if (OB_FAIL(merge_task->init(ctx_, param_, this, &origin_table_, sstable_array_, range, i))) {
      LOG_WARN("fail to init merge task", KR(ret));
    } else if (OB_FAIL(task_array_.push_back(merge_task))) {
      LOG_WARN("fail to push back merge task", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != merge_task) {
        merge_task->~ObDirectLoadPartitionRangeMergeTask();
        allocator_.free(merge_task);
        merge_task = nullptr;
      }
    }
  }

  return ret;
}

int ObDirectLoadTabletMergeCtx::build_pk_table_multiple_merge_task(
  const ObIArray<ObIDirectLoadPartitionTable *> &table_array,
  const ObIArray<ObColDesc> &col_descs,
  int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_multiple_sstable_array(table_array))) {
    LOG_WARN("fail to init multiple sstable array", KR(ret));
  }
  // split range
  if (OB_SUCC(ret)) {
    ObDirectLoadMultipleMergeTabletRangeSplitter range_splitter;
    if (OB_FAIL(range_splitter.init(
          tablet_id_,
          (merge_with_origin_data() ? &origin_table_ : nullptr),
          multiple_sstable_array_,
          param_.table_data_desc_,
          param_.datum_utils_,
          col_descs))) {
      LOG_WARN("fail to init range splitter", KR(ret));
    } else if (OB_FAIL(range_splitter.split_range(range_array_, max_parallel_degree, allocator_))) {
      LOG_WARN("fail to split range", KR(ret));
    } else if (OB_UNLIKELY(range_array_.count() > max_parallel_degree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected range count", KR(ret), K(max_parallel_degree), K(range_array_.count()));
    }
  }
  // construct task per range
  for (int64_t i = 0; OB_SUCC(ret) && i < range_array_.count(); ++i) {
    const ObDatumRange &range = range_array_.at(i);
    ObDirectLoadPartitionRangeMultipleMergeTask *merge_task = nullptr;
    if (OB_ISNULL(merge_task =
                    OB_NEWx(ObDirectLoadPartitionRangeMultipleMergeTask, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadPartitionRangeMultipleMergeTask", KR(ret));
    } else if (OB_FAIL(merge_task->init(ctx_, param_, this, &origin_table_, multiple_sstable_array_,
                                        range, i))) {
      LOG_WARN("fail to init merge task", KR(ret));
    } else if (OB_FAIL(task_array_.push_back(merge_task))) {
      LOG_WARN("fail to push back merge task", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != merge_task) {
        merge_task->~ObDirectLoadPartitionRangeMultipleMergeTask();
        allocator_.free(merge_task);
        merge_task = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_merge_task_for_multiple_pk_table(
  const ObIArray<ObDirectLoadMultipleSSTable *> &multiple_sstable_array,
  ObDirectLoadMultipleMergeRangeSplitter &range_splitter,
  int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(max_parallel_degree <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(max_parallel_degree));
  } else {
    if (OB_FAIL(multiple_sstable_array_.assign(multiple_sstable_array))) {
      LOG_WARN("fail to assign multiple sstable array", KR(ret));
    } else if (OB_FAIL(range_splitter.split_range(
        tablet_id_,
        (merge_with_origin_data() ? &origin_table_ : nullptr),
        max_parallel_degree,
        range_array_,
        allocator_))) {
      LOG_WARN("fail to split range", KR(ret));
    } else if (OB_UNLIKELY(range_array_.count() > max_parallel_degree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected range count", KR(ret), K(max_parallel_degree), K(range_array_.count()));
    }
    // construct task per range
    for (int64_t i = 0; OB_SUCC(ret) && i < range_array_.count(); ++i) {
      const ObDatumRange &range = range_array_.at(i);
      ObDirectLoadPartitionRangeMultipleMergeTask *merge_task = nullptr;
      if (OB_ISNULL(merge_task =
                      OB_NEWx(ObDirectLoadPartitionRangeMultipleMergeTask, (&allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObDirectLoadPartitionRangeMultipleMergeTask", KR(ret));
      } else if (OB_FAIL(merge_task->init(ctx_, param_, this, &origin_table_, multiple_sstable_array_,
                                          range, i))) {
        LOG_WARN("fail to init merge task", KR(ret));
      } else if (OB_FAIL(task_array_.push_back(merge_task))) {
        LOG_WARN("fail to push back merge task", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != merge_task) {
          merge_task->~ObDirectLoadPartitionRangeMultipleMergeTask();
          allocator_.free(merge_task);
          merge_task = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_heap_table_multiple_merge_task(
  const ObIArray<ObIDirectLoadPartitionTable *> &table_array,
  const ObIArray<ObColDesc> &col_descs,
  int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  int64_t parallel_idx = 0;
  if (OB_FAIL(init_multiple_heap_table_array(table_array))) {
    LOG_WARN("fail to init multiple heap table array", KR(ret));
  }
  // for existing data, construct task by split range
  if (OB_SUCC(ret)) {
    ObDirectLoadMergeRangeSplitter range_splitter;
    if (OB_FAIL(range_splitter.init(
        (merge_with_origin_data() ? &origin_table_ : nullptr),
        sstable_array_,
        param_.datum_utils_,
        col_descs))) {
      LOG_WARN("fail to init range splitter", KR(ret));
    } else if (OB_FAIL(range_splitter.split_range(range_array_, max_parallel_degree, allocator_))) {
      LOG_WARN("fail to split range", KR(ret));
    } else if (OB_UNLIKELY(range_array_.count() > max_parallel_degree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected range count", KR(ret), K(max_parallel_degree), K(range_array_.count()));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < range_array_.count(); ++i) {
    const ObDatumRange &range = range_array_.at(i);
    ObDirectLoadPartitionRangeMergeTask *merge_task = nullptr;
    if (OB_ISNULL(merge_task = OB_NEWx(ObDirectLoadPartitionRangeMergeTask, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadPartitionRangeMergeTask", KR(ret));
    } else if (OB_FAIL(merge_task->init(ctx_, param_, this, &origin_table_, sstable_array_, range,
                                        parallel_idx++))) {
      LOG_WARN("fail to init merge task", KR(ret));
    } else if (OB_FAIL(task_array_.push_back(merge_task))) {
      LOG_WARN("fail to push back merge task", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != merge_task) {
        merge_task->~ObDirectLoadPartitionRangeMergeTask();
        allocator_.free(merge_task);
        merge_task = nullptr;
      }
    }
    if (OB_SUCC(ret)) {
      // for imported data, construct task by multiple heap table
      for (int64_t i = 0; OB_SUCC(ret) && !param_.is_fast_heap_table_ && i < multiple_heap_table_array_.count(); ++i) {
        ObDirectLoadMultipleHeapTable *heap_table = multiple_heap_table_array_.at(i);
        ObDirectLoadPartitionHeapTableMultipleMergeTask *merge_task = nullptr;
        int64_t row_count = 0;
        ObTabletCacheInterval pk_interval;
        if (OB_FAIL(heap_table->get_tablet_row_count(tablet_id_, param_.table_data_desc_, row_count))) {
          LOG_WARN("fail to get tablet row count", KR(ret), K(tablet_id_));
        } else if (0 == row_count) {
          // ignore
        } else if (OB_FAIL(get_autoincrement_value(row_count, pk_interval))) {
          LOG_WARN("fail to get autoincrement value", KR(ret), K(row_count));
        } else if (OB_ISNULL(merge_task = OB_NEWx(ObDirectLoadPartitionHeapTableMultipleMergeTask, (&allocator_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new ObDirectLoadPartitionHeapTableMultipleMergeTask", KR(ret));
        } else if (OB_FAIL(merge_task->init(ctx_, param_, this, heap_table, pk_interval, parallel_idx++))) {
          LOG_WARN("fail to init merge task", KR(ret));
        } else if (OB_FAIL(task_array_.push_back(merge_task))) {
          LOG_WARN("fail to push back merge task", KR(ret));
        }
        if (OB_FAIL(ret)) {
          if (nullptr != merge_task) {
            merge_task->~ObDirectLoadPartitionHeapTableMultipleMergeTask();
            allocator_.free(merge_task);
            merge_task = nullptr;
          }
        }
      }
    }
  }

  return ret;
}

int ObDirectLoadTabletMergeCtx::build_aggregate_merge_task_for_multiple_heap_table(
  const ObIArray<ObIDirectLoadPartitionTable *> &table_array)
{
  int ret = OB_SUCCESS;
  int64_t total_row_count = 0;
  ObTabletCacheInterval pk_interval;
  ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask *merge_task = nullptr;
  if (OB_FAIL(init_multiple_heap_table_array(table_array))) {
    LOG_WARN("fail to init multiple heap table array", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < multiple_heap_table_array_.count(); ++i) {
    ObDirectLoadMultipleHeapTable *heap_table = multiple_heap_table_array_.at(i);
    int64_t row_count = 0;
    if (OB_FAIL(heap_table->get_tablet_row_count(tablet_id_, param_.table_data_desc_, row_count))) {
      LOG_WARN("fail to get tablet row count", KR(ret), K(tablet_id_));
    } else {
      total_row_count += row_count;
    }
  }
  if (OB_SUCC(ret)) {
    if (total_row_count > 0 && OB_FAIL(get_autoincrement_value(total_row_count, pk_interval))) {
      LOG_WARN("fail to get autoincrement value", KR(ret), K(total_row_count));
    } else if (OB_ISNULL(merge_task =
                           OB_NEWx(ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask,
                                   (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask", KR(ret));
    } else if (OB_FAIL(merge_task->init(ctx_, param_, this, &origin_table_, multiple_heap_table_array_,
                                        pk_interval))) {
      LOG_WARN("fail to init merge task", KR(ret));
    } else if (OB_FAIL(task_array_.push_back(merge_task))) {
      LOG_WARN("fail to push back merge task", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != merge_task) {
      merge_task->~ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask();
      allocator_.free(merge_task);
      merge_task = nullptr;
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_rescan_task(int64_t thread_count)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < thread_count; ++i) {
    ObDirectLoadPartitionRescanTask *rescan_task = nullptr;
    if (OB_ISNULL(rescan_task = OB_NEWx(ObDirectLoadPartitionRescanTask,
                                              (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadPartitionRescanTask", KR(ret));
    } else if (OB_FAIL(rescan_task->init(param_, this, thread_count, i))) {
      LOG_WARN("fail to init merge task", KR(ret));
    } else if (OB_FAIL(rescan_task_array_.push_back(rescan_task))) {
      LOG_WARN("fail to push back merge task", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != rescan_task) {
        rescan_task->~ObDirectLoadPartitionRescanTask();
        allocator_.free(rescan_task);
        rescan_task = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::build_del_lob_task(
    const common::ObIArray<ObDirectLoadMultipleSSTable *> &multiple_sstable_array,
    ObDirectLoadMultipleMergeRangeSplitter &range_splitter,
    const int64_t max_parallel_degree)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(max_parallel_degree <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(max_parallel_degree));
  } else {
    if (OB_FAIL(del_lob_multiple_sstable_array_.assign(multiple_sstable_array))) {
      LOG_WARN("fail to assign multiple sstable array", KR(ret));
    } else if (OB_FAIL(range_splitter.split_range(tablet_id_,
                                                  nullptr /*origin_table*/,
                                                  max_parallel_degree,
                                                  del_lob_range_array_,
                                                  allocator_))) {
      LOG_WARN("fail to split range", KR(ret));
    } else if (OB_UNLIKELY(del_lob_range_array_.count() > max_parallel_degree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected range count", KR(ret), K(max_parallel_degree), K(del_lob_range_array_.count()));
    }
    // construct task per range
    for (int64_t i = 0; OB_SUCC(ret) && i < del_lob_range_array_.count(); ++i) {
      const ObDatumRange &range = del_lob_range_array_.at(i);
      ObDirectLoadPartitionDelLobTask *del_lob_task = nullptr;
      if (OB_ISNULL(del_lob_task =
                      OB_NEWx(ObDirectLoadPartitionDelLobTask, (&allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObDirectLoadPartitionDelLobTask", KR(ret));
      } else if (OB_FAIL(del_lob_task->init(param_,
                                            this,
                                            &lob_meta_origin_table_,
                                            del_lob_multiple_sstable_array_,
                                            range,
                                            i))) {
        LOG_WARN("fail to init del lob task", KR(ret));
      } else if (OB_FAIL(del_lob_task_array_.push_back(del_lob_task))) {
        LOG_WARN("fail to push back del lob task", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != del_lob_task) {
          del_lob_task->~ObDirectLoadPartitionDelLobTask();
          allocator_.free(del_lob_task);
          del_lob_task = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::get_autoincrement_value(uint64_t count,
                                                        ObTabletCacheInterval &interval)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(count));
  } else {
    const uint64_t tenant_id = MTL_ID();
    ObTabletAutoincrementService &auto_inc = ObTabletAutoincrementService::get_instance();
    interval.tablet_id_ = tablet_id_;
    interval.cache_size_ = count;
    if (OB_FAIL(auto_inc.get_tablet_cache_interval(tenant_id, interval))) {
      LOG_WARN("fail to get tablet cache interval", K(ret), K(tenant_id), K_(tablet_id));
    } else if (OB_UNLIKELY(count > interval.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected autoincrement value count", K(ret), K(count), K(interval));
    }
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::inc_finish_count(bool &is_ready)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else {
    const int64_t finish_count = ATOMIC_AAF(&task_finish_count_, 1);
    is_ready = (finish_count >= task_array_.count());
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::inc_rescan_finish_count(bool &is_ready)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else {
    const int64_t finish_count = ATOMIC_AAF(&rescan_task_finish_count_, 1);
    is_ready = (finish_count >= rescan_task_array_.count());
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::inc_del_lob_finish_count(bool &is_ready)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else {
    const int64_t finish_count = ATOMIC_AAF(&del_lob_task_finish_count_, 1);
    is_ready = (finish_count >= del_lob_task_array_.count());
  }
  return ret;
}

int ObDirectLoadTabletMergeCtx::get_table_builder(ObIDirectLoadPartitionTableBuilder *&table_builder)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else if (OB_FAIL(merge_ctx_->get_table_builder(table_builder))) {
    LOG_WARN("fail to get table builder", KR(ret));
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase
