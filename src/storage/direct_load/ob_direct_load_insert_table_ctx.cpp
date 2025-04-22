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

#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_stat_item.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace table;
using namespace sql;
using namespace observer;

/**
 * ObDirectLoadInsertTableParam
 */

ObDirectLoadInsertTableParam::ObDirectLoadInsertTableParam()
  : table_id_(OB_INVALID_ID),
    dest_table_id_(OB_INVALID_ID),
    schema_version_(0),
    snapshot_version_(0),
    execution_id_(0),
    ddl_task_id_(0),
    data_version_(0),
    session_cnt_(0),
    rowkey_column_count_(0),
    column_count_(0),
    is_partitioned_table_(false),
    is_heap_table_(false),
    online_opt_stat_gather_(false),
    col_descs_(nullptr),
    cmp_funcs_(nullptr)
{
}

ObDirectLoadInsertTableParam::~ObDirectLoadInsertTableParam()
{
}

bool ObDirectLoadInsertTableParam::is_valid() const
{
  return OB_INVALID_ID != table_id_
            && OB_INVALID_ID != dest_table_id_
            && schema_version_ >= 0
            && snapshot_version_ >= 0
            && rowkey_column_count_ > 0
            && column_count_ > 0
            && (column_count_ >= rowkey_column_count_)
            && nullptr != col_descs_
            && nullptr != cmp_funcs_;
}

/**
 * ObDirectLoadInsertTableContext
 */

ObDirectLoadInsertTableContext::ObDirectLoadInsertTableContext()
  : allocator_("TLD_SqlStat"),
    safe_allocator_(allocator_),
    tablet_count_(0),
    tablet_finish_count_(0),
    table_row_count_(0),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadInsertTableContext::~ObDirectLoadInsertTableContext()
{
  int ret = OB_SUCCESS;
  if (0 != ddl_ctrl_.context_id_) {
    ObSSTableInsertManager &sstable_insert_mgr = ObSSTableInsertManager::get_instance();
    if (OB_FAIL(sstable_insert_mgr.finish_table_context(ddl_ctrl_.context_id_, false))) {
      LOG_WARN("fail to finish table context", KR(ret), K_(ddl_ctrl));
    }
    ddl_ctrl_.context_id_ = 0;
  }
  if (sql_stat_map_.created()) {
    FOREACH(iter, sql_stat_map_)
    {
      ObTableLoadSqlStatistics *sql_statistics = iter->second;
      sql_statistics->~ObTableLoadSqlStatistics();
      allocator_.free(sql_statistics);
    }
    sql_stat_map_.destroy();
  }
}

int ObDirectLoadInsertTableContext::init(
  const ObDirectLoadInsertTableParam &param,
  const ObIArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadInsertTableContext init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid() || ls_partition_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param), K(ls_partition_ids));
  } else {
    ObSSTableInsertManager &sstable_insert_mgr = ObSSTableInsertManager::get_instance();
    ObSSTableInsertTableParam table_insert_param;
    table_insert_param.dest_table_id_ = param.table_id_;
    table_insert_param.snapshot_version_ = 0;
    table_insert_param.schema_version_ = param.schema_version_;
    table_insert_param.task_cnt_ = 1;
    table_insert_param.write_major_ = true;
    table_insert_param.execution_id_ = param.execution_id_;
    table_insert_param.ddl_task_id_ = param.ddl_task_id_;
    table_insert_param.data_format_version_ = param.data_version_;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_partition_ids.count(); ++i) {
      const ObTableLoadLSIdAndPartitionId &ls_partition_id = ls_partition_ids.at(i);
      if (OB_FAIL(table_insert_param.ls_tablet_ids_.push_back(
            std::make_pair(ls_partition_id.ls_id_, ls_partition_id.part_tablet_id_.tablet_id_)))) {
        LOG_WARN("fail to push back ls tablet id", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sstable_insert_mgr.create_table_context(table_insert_param,
                                                               ddl_ctrl_.context_id_))) {
      LOG_WARN("fail to create table context", KR(ret), K(table_insert_param));
    } else if (param.online_opt_stat_gather_ &&
               OB_FAIL(sql_stat_map_.create(1024, "TLD_SqlStatMap", "TLD_SqlStatMap", MTL_ID()))) {
      LOG_WARN("fail to create sql stat map", KR(ret));
    } else {
      param_ = param;
      tablet_count_ = ls_partition_ids.count();
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadInsertTableContext::add_sstable_slice(const ObTabletID &tablet_id,
                                                      const ObMacroDataSeq &start_seq,
                                                      ObNewRowIterator &iter,
                                                      int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSSTableInsertManager &sstable_insert_mgr = ObSSTableInsertManager::get_instance();
  ObSSTableInsertTabletParam tablet_insert_param;
  tablet_insert_param.context_id_ = ddl_ctrl_.context_id_;
  tablet_insert_param.table_id_ = param_.table_id_;
  tablet_insert_param.tablet_id_ = tablet_id;
  tablet_insert_param.write_major_ = true;
  tablet_insert_param.task_cnt_ = 1;
  tablet_insert_param.schema_version_ = param_.schema_version_;
  tablet_insert_param.snapshot_version_ = param_.snapshot_version_;
  tablet_insert_param.execution_id_ = param_.execution_id_;
  tablet_insert_param.ddl_task_id_ = param_.ddl_task_id_;
  if (OB_FAIL(sstable_insert_mgr.update_table_tablet_context(ddl_ctrl_.context_id_, tablet_id,
                                                             param_.snapshot_version_))) {
    LOG_WARN("fail to update table context", KR(ret), K_(ddl_ctrl), K(tablet_id));
  } else if (OB_FAIL(sstable_insert_mgr.add_sstable_slice(tablet_insert_param, start_seq, iter,
                                                          affected_rows))) {
    LOG_WARN("fail to add sstable slice", KR(ret));
  }

  if (OB_NOT_MASTER == ret) { //incase write start log -4038, todo jianming write start log at very beginning
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObDirectLoadInsertTableContext::construct_sstable_slice_writer(
  const ObTabletID &tablet_id, const ObMacroDataSeq &start_seq,
  ObSSTableInsertSliceWriter *&slice_writer, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else {
    ObSSTableInsertManager &sstable_insert_mgr = ObSSTableInsertManager::get_instance();
    ObSSTableInsertTabletParam tablet_insert_param;
    tablet_insert_param.context_id_ = ddl_ctrl_.context_id_;
    tablet_insert_param.table_id_ = param_.table_id_;
    tablet_insert_param.tablet_id_ = tablet_id;
    tablet_insert_param.write_major_ = true;
    tablet_insert_param.task_cnt_ = 1;
    tablet_insert_param.schema_version_ = param_.schema_version_;
    tablet_insert_param.snapshot_version_ = param_.snapshot_version_;
    tablet_insert_param.execution_id_ = param_.execution_id_;
    tablet_insert_param.ddl_task_id_ = param_.ddl_task_id_;
    if (OB_FAIL(sstable_insert_mgr.update_table_tablet_context(ddl_ctrl_.context_id_, tablet_id,
                                                               param_.snapshot_version_))) {
      LOG_WARN("fail to update table context", KR(ret), K_(ddl_ctrl), K(tablet_id));
    } else if (OB_FAIL(sstable_insert_mgr.construct_sstable_slice_writer(
                 tablet_insert_param, start_seq, slice_writer, allocator))) {
      LOG_WARN("fail to construct sstable slice writer", KR(ret));
    }
  }

  if (OB_NOT_MASTER == ret) { //incase write start log -4038, todo jianming write start log at very beginning
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObDirectLoadInsertTableContext::notify_tablet_finish(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else {
    ObSSTableInsertManager &sstable_insert_mgr = ObSSTableInsertManager::get_instance();
    int64_t tablet_finish_count = 0;
    if (OB_FAIL(sstable_insert_mgr.notify_tablet_end(ddl_ctrl_.context_id_, tablet_id))) {
      LOG_WARN("fail to notify tablet end", KR(ret), K_(ddl_ctrl), K(tablet_id));
    } else if (FALSE_IT(tablet_finish_count = ATOMIC_AAF(&tablet_finish_count_, 1))) {
    } else if (OB_FAIL(sstable_insert_mgr.finish_ready_tablets(ddl_ctrl_.context_id_,
                                                               tablet_finish_count))) {
      LOG_WARN("fail to finish ready tablets", KR(ret), K_(ddl_ctrl), K(tablet_finish_count));
    }
  }
  return ret;
}

int ObDirectLoadInsertTableContext::commit(ObTableLoadSqlStatistics &sql_statistics)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(tablet_finish_count_ != tablet_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected finished tablet count", KR(ret), K(tablet_count_),
             K(tablet_finish_count_));
  } else {
    ObSSTableInsertManager &sstable_insert_mgr = ObSSTableInsertManager::get_instance();
    if (OB_FAIL(sstable_insert_mgr.finish_table_context(ddl_ctrl_.context_id_, true))) {
      LOG_WARN("fail to finish table context", KR(ret), K_(ddl_ctrl));
    } else if (FALSE_IT(ddl_ctrl_.context_id_ = 0)) {
    } else if (param_.online_opt_stat_gather_ && OB_FAIL(collect_sql_statistics(sql_statistics))) {
      LOG_WARN("fail to collect sql stats", KR(ret));
    }
  }
  return ret;
}

int64_t ObDirectLoadInsertTableContext::get_sql_stat_column_count() const
{
  int64_t column_count = 0;
  if (!param_.is_heap_table_) {
    column_count = param_.column_count_;
  } else {
    column_count = param_.column_count_ - param_.rowkey_column_count_;
  }
  return column_count;
}

int ObDirectLoadInsertTableContext::get_sql_statistics(ObTableLoadSqlStatistics *&sql_statistics)
{
  int ret = OB_SUCCESS;
  sql_statistics = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param_.online_opt_stat_gather_)) {
  } else {
    const int64_t tid = gettid();
    if (OB_FAIL(sql_stat_map_.get_refactored(tid, sql_statistics))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get sql stat", KR(ret), K(tid));
      } else {
        ret = OB_SUCCESS;
        const int64_t column_count = get_sql_stat_column_count();
        if (OB_ISNULL(sql_statistics = OB_NEWx(ObTableLoadSqlStatistics, (&safe_allocator_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new ObTableLoadSqlStatistics", KR(ret));
        } else if (OB_FAIL(sql_statistics->create(column_count))) {
          LOG_WARN("fail to create sql staticstics", KR(ret));
        } else if (OB_FAIL(sql_stat_map_.set_refactored(tid, sql_statistics))) {
          LOG_WARN("fail to push back", KR(ret));
        }
        if (OB_FAIL(ret)) {
          if (nullptr != sql_statistics) {
            sql_statistics->~ObTableLoadSqlStatistics();
            safe_allocator_.free(sql_statistics);
            sql_statistics = nullptr;
          }
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertTableContext::update_sql_statistics(
  ObTableLoadSqlStatistics &sql_statistics, const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  const int64_t extra_rowkey_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertDataTableContext not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param_.online_opt_stat_gather_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not gather sql stat", KR(ret), K(param_));
  } else if (OB_UNLIKELY(datum_row.get_column_count() != param_.column_count_ + extra_rowkey_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid datum row", KR(ret), K(param_), K(datum_row));
  } else {
    ObOptOSGColumnStat *col_stat = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_.column_count_; i++) {
      if (i < param_.rowkey_column_count_ && param_.is_heap_table_) {
        // ignore heap table hidden pk
      } else {
        const int64_t datum_idx = i < param_.rowkey_column_count_ ? i : i + extra_rowkey_cnt;
        const int64_t col_stat_idx = param_.is_heap_table_ ? i - 1 : i;
        const ObStorageDatum &datum = datum_row.storage_datums_[datum_idx];
        const ObCmpFunc &cmp_func = param_.cmp_funcs_->at(i).get_cmp_func();
        const ObColDesc &col_desc = param_.col_descs_->at(i);
        const bool is_valid =
          ObColumnStatParam::is_valid_opt_col_type(col_desc.col_type_.get_type(), true);
        if (is_valid) {
          if (OB_FAIL(sql_statistics.get_col_stat(col_stat_idx, col_stat))) {
            LOG_WARN("fail to get col stat", KR(ret), K(col_stat_idx));
          } else if (OB_ISNULL(col_stat)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col stat is null", KR(ret), K(col_stat_idx));
          } else if (OB_FAIL(col_stat->update_column_stat_info(&datum, col_desc.col_type_,
                                                               cmp_func.cmp_func_))) {
            LOG_WARN("fail to merge obj", KR(ret), K(i), K(col_desc), K(datum), KP(col_stat));
          }
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertTableContext::collect_sql_statistics(ObTableLoadSqlStatistics &sql_statistics)
{
  int ret = OB_SUCCESS;
  const int64_t column_count = get_sql_stat_column_count();
  sql_statistics.reset();
  if (OB_FAIL(sql_statistics.create(column_count))) {
    LOG_WARN("fail to create sql stat", KR(ret), K(column_count));
  } else {
    const StatLevel stat_level = TABLE_LEVEL;
    const int64_t partition_id = !param_.is_partitioned_table_ ? param_.dest_table_id_ : -1;
    int64_t table_avg_len = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      ObOptOSGColumnStat *osg_col_stat = nullptr;
      const ObColDesc &col_desc = param_.col_descs_->at(!param_.is_heap_table_ ? i : i + 1);
      if (OB_FAIL(sql_statistics.get_col_stat(i, osg_col_stat))) {
        LOG_WARN("fail to get col stat", KR(ret), K(i));
      }
      FOREACH_X(iter, sql_stat_map_, OB_SUCC(ret))
      {
        const int64_t part_id = iter->first;
        ObTableLoadSqlStatistics *part_sql_statistics = iter->second;
        ObOptOSGColumnStat *part_osg_col_stat = nullptr;
        if (OB_ISNULL(part_sql_statistics)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected sql stat is null", KR(ret), K(part_id));
        } else if (OB_FAIL(part_sql_statistics->get_col_stat(i, part_osg_col_stat))) {
          LOG_WARN("fail to get col stat", KR(ret), K(i));
        } else if (OB_FAIL(osg_col_stat->merge_column_stat(*part_osg_col_stat))) {
          LOG_WARN("fail to merge column stat", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        osg_col_stat->col_stat_->calc_avg_len();
        osg_col_stat->col_stat_->set_table_id(param_.dest_table_id_);
        osg_col_stat->col_stat_->set_partition_id(partition_id);
        osg_col_stat->col_stat_->set_stat_level(stat_level);
        osg_col_stat->col_stat_->set_column_id(col_desc.col_id_);
        osg_col_stat->col_stat_->set_num_distinct(
          ObGlobalNdvEval::get_ndv_from_llc(osg_col_stat->col_stat_->get_llc_bitmap()));
        if (OB_FAIL(osg_col_stat->set_min_max_datum_to_obj())) {
          LOG_WARN("failed to set min max datum to obj", K(ret));
        } else {
          table_avg_len += osg_col_stat->col_stat_->get_avg_len();
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObOptTableStat *table_stat = nullptr;
      if (OB_FAIL(sql_statistics.get_table_stat(0, table_stat))) {
        LOG_WARN("fail to get table stat", KR(ret));
      } else {
        table_stat->set_table_id(param_.dest_table_id_);
        table_stat->set_partition_id(partition_id);
        table_stat->set_object_type(stat_level);
        table_stat->set_row_count(table_row_count_);
        table_stat->set_avg_row_size(table_avg_len);
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
