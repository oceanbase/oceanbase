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
#include "observer/table_load/ob_table_load_schema.h"

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
    online_opt_stat_gather_(false),
    is_heap_table_(false),
    ls_partition_ids_()
{
  ls_partition_ids_.set_attr(ObMemAttr(MTL_ID(), "DLITP_ids"));
}

ObDirectLoadInsertTableParam::~ObDirectLoadInsertTableParam()
{
}

bool ObDirectLoadInsertTableParam::is_valid() const
{
  return OB_INVALID_ID != table_id_ && OB_INVALID_ID != dest_table_id_ && schema_version_ >= 0 &&
         snapshot_version_ >= 0 && ls_partition_ids_.count() > 0;
}

int ObDirectLoadInsertTableParam::assign(const ObDirectLoadInsertTableParam &other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  dest_table_id_ = other.dest_table_id_;
  schema_version_ = other.schema_version_;
  snapshot_version_ = other.snapshot_version_;
  data_version_ = other.data_version_;
  session_cnt_ = other.session_cnt_;
  rowkey_column_count_ = other.rowkey_column_count_;
  column_count_ = other.column_count_;
  online_opt_stat_gather_ = other.online_opt_stat_gather_;
  is_heap_table_ = other.is_heap_table_;
  col_descs_ = other.col_descs_;
  cmp_funcs_ = other.cmp_funcs_;
  if (OB_FAIL(ls_partition_ids_.assign(other.ls_partition_ids_))) {
    LOG_WARN("fail to assign ls tablet ids", KR(ret));
  }
  return ret;
}

/**
 * ObDirectLoadInsertTableContext
 */

ObDirectLoadInsertTableContext::ObDirectLoadInsertTableContext()
  : allocator_("TLD_SqlStat"), safe_allocator_(allocator_), tablet_finish_count_(0), table_row_count_(0), is_inited_(false)
{
}

ObDirectLoadInsertTableContext::~ObDirectLoadInsertTableContext()
{
  reset();
}

void ObDirectLoadInsertTableContext::reset()
{
  int ret = OB_SUCCESS;
  if (0 != ddl_ctrl_.context_id_) {
    ObSSTableInsertManager &sstable_insert_mgr = ObSSTableInsertManager::get_instance();
    if (OB_FAIL(sstable_insert_mgr.finish_table_context(ddl_ctrl_.context_id_, false))) {
      LOG_WARN("fail to finish table context", KR(ret), K_(ddl_ctrl));
    }
    ddl_ctrl_.context_id_ = 0;
  }
  for (decltype(sql_stat_map_)::const_iterator iter = sql_stat_map_.begin(); iter != sql_stat_map_.end(); ++ iter) {
    ObTableLoadSqlStatistics *sql_statistics = iter->second;
    if (sql_statistics != nullptr) {
      sql_statistics->~ObTableLoadSqlStatistics();
      safe_allocator_.free(sql_statistics);
    }
  }
  sql_stat_map_.clear();
  is_inited_ = false;
}

int ObDirectLoadInsertTableContext::init(const ObDirectLoadInsertTableParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadInsertTableContext init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
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
    for (int64_t i = 0; i < param.ls_partition_ids_.count(); ++i) {
      const ObTableLoadLSIdAndPartitionId &ls_partition_id = param.ls_partition_ids_.at(i);
      if (OB_FAIL(table_insert_param.ls_tablet_ids_.push_back(
            std::make_pair(ls_partition_id.ls_id_, ls_partition_id.part_tablet_id_.tablet_id_)))) {
        LOG_WARN("fail to push back ls tablet id", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(param_.assign(param))) {
      LOG_WARN("fail to assign param", KR(ret));
    } else if (OB_FAIL(sstable_insert_mgr.create_table_context(table_insert_param,
                                                               ddl_ctrl_.context_id_))) {
      LOG_WARN("fail to create table context", KR(ret), K(table_insert_param));
    } else if (param_.online_opt_stat_gather_ && OB_FAIL(init_sql_statistics())) {
      LOG_WARN("fail to init sql statistics", KR(ret));
    } else {
      is_inited_ = true;
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}


int ObDirectLoadInsertTableContext::get_sql_stat(table::ObTableLoadSqlStatistics *&sql_statistics)
{
  int ret = OB_SUCCESS;
  int64_t tid = gettid();
  sql_statistics = nullptr;
  ret = sql_stat_map_.get_refactored(tid, sql_statistics);
  if (ret == OB_HASH_NOT_EXIST) {
    ret = OB_SUCCESS;
    ObOptTableStat *table_stat = nullptr;
    if (OB_ISNULL(sql_statistics = OB_NEWx(ObTableLoadSqlStatistics, (&safe_allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadSqlStatistics", KR(ret));
    } else if (OB_FAIL(sql_statistics->allocate_table_stat(table_stat))) {
      LOG_WARN("fail to allocate table stat", KR(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < param_.column_count_; ++j) {
        ObOptOSGColumnStat *osg_col_stat = nullptr;
        if (OB_FAIL(sql_statistics->allocate_col_stat(osg_col_stat))) {
          LOG_WARN("fail to allocate col stat", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql_stat_map_.set_refactored(tid, sql_statistics))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
      if (nullptr != sql_statistics) {
        sql_statistics->~ObTableLoadSqlStatistics();
        safe_allocator_.free(sql_statistics);
        sql_statistics = nullptr;
      }
    }
  } else if (ret != OB_SUCCESS) {
    LOG_WARN("fail to get item from map", KR(ret), K(tid));
  }
  return ret;
}


int ObDirectLoadInsertTableContext::init_sql_statistics()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(sql_stat_map_.create(1024, "TLD_SqlStatMap", "TLD_SqlStatMap", tenant_id))) {
    LOG_WARN("fail to init map", KR(ret));
  }
  return ret;
}

int ObDirectLoadInsertTableContext::collect_sql_statistics(ObTableLoadSqlStatistics &sql_statistics)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadTabletMergeCtx not init", KR(ret), KP(this));
  } else if (OB_FAIL(ObTableLoadSchema::get_table_schema(tenant_id, param_.dest_table_id_,
                                                         schema_guard, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(param_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else {
    sql_statistics.reset();
    int64_t table_row_cnt = table_row_count_;
    int64_t table_avg_len = 0;
    int64_t col_cnt = param_.column_count_;
    uint64_t partition_id = -1;
    ObOptTableStat *table_stat = nullptr;
    StatLevel stat_level = TABLE_LEVEL;
    if (table_schema->get_part_level() == PARTITION_LEVEL_ZERO) {
      partition_id = param_.dest_table_id_;
    }
    if (OB_FAIL(sql_statistics.allocate_table_stat(table_stat))) {
      LOG_WARN("fail to allocate table stat", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
        int64_t col_id = param_.is_heap_table_ ? i + 1 : i;
        ObOptOSGColumnStat *osg_col_stat = nullptr;
        if (OB_FAIL(sql_statistics.allocate_col_stat(osg_col_stat))) {
          LOG_WARN("fail to allocate table stat", KR(ret));
        }
        // scan session_sql_ctx_array
        for (decltype(sql_stat_map_)::const_iterator iter = sql_stat_map_.begin(); iter != sql_stat_map_.end(); ++ iter) {
          if (iter->second == nullptr) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sql_stat in map should not be null", KR(ret));
          } else {
            ObOptOSGColumnStat *tmp_col_stat =
              iter->second->get_col_stat_array().at(i);
            if (OB_FAIL(osg_col_stat->merge_column_stat(*tmp_col_stat))) {
              LOG_WARN("fail to merge column stat", KR(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          osg_col_stat->col_stat_->calc_avg_len();
          table_avg_len += osg_col_stat->col_stat_->get_avg_len();
          osg_col_stat->col_stat_->set_table_id(param_.dest_table_id_);
          osg_col_stat->col_stat_->set_partition_id(partition_id);
          osg_col_stat->col_stat_->set_stat_level(stat_level);
          osg_col_stat->col_stat_->set_column_id(param_.col_descs_->at(col_id).col_id_);
          osg_col_stat->col_stat_->set_num_distinct(
            ObGlobalNdvEval::get_ndv_from_llc(osg_col_stat->col_stat_->get_llc_bitmap()));
          if (OB_FAIL(osg_col_stat->set_min_max_datum_to_obj())) {
            LOG_WARN("failed to set min max datum to obj", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        table_stat->set_table_id(param_.dest_table_id_);
        table_stat->set_partition_id(partition_id);
        table_stat->set_object_type(stat_level);
        table_stat->set_row_count(table_row_cnt);
        table_stat->set_avg_row_size(table_avg_len);
      }
    }
  }
  return ret;
}

int ObDirectLoadInsertTableContext::collect_obj(const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  const int64_t extra_rowkey_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  if (param_.online_opt_stat_gather_) {
    ObTableLoadSqlStatistics *sql_stat = nullptr;
    if (OB_FAIL(get_sql_stat(sql_stat))) {
      LOG_WARN("fail to get sql stat", KR(ret));
    } else {
      if (param_.is_heap_table_ ) {
        for (int64_t i = 0; OB_SUCC(ret) && i < param_.column_count_; i++) {
          const ObStorageDatum &datum = datum_row.storage_datums_[i + extra_rowkey_cnt + 1];
          const ObColDesc &col_desc = param_.col_descs_->at(i + 1);
          const ObCmpFunc &cmp_func = param_.cmp_funcs_->at(i + 1).get_cmp_func();
          ObOptOSGColumnStat *col_stat = sql_stat->get_col_stat_array().at(i);
          bool is_valid = ObColumnStatParam::is_valid_opt_col_type(col_desc.col_type_.get_type());
          if (col_stat != nullptr && is_valid) {
            if (OB_FAIL(col_stat->update_column_stat_info(&datum, col_desc.col_type_, cmp_func.cmp_func_))) {
              LOG_WARN("Failed to merge obj", K(ret), KP(col_stat));
            }
          }
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < param_.rowkey_column_count_; i++) {
          const ObStorageDatum &datum = datum_row.storage_datums_[i];
          const ObColDesc &col_desc = param_.col_descs_->at(i);
          const ObCmpFunc &cmp_func = param_.cmp_funcs_->at(i).get_cmp_func();
          ObOptOSGColumnStat *col_stat = sql_stat->get_col_stat_array().at(i);
          bool is_valid = ObColumnStatParam::is_valid_opt_col_type(col_desc.col_type_.get_type());
          if (col_stat != nullptr && is_valid) {
            if (OB_FAIL(col_stat->update_column_stat_info(&datum, col_desc.col_type_, cmp_func.cmp_func_))) {
              LOG_WARN("Failed to merge obj", K(ret), KP(col_stat));
            }
          }
        }
        for (int64_t i = param_.rowkey_column_count_; OB_SUCC(ret) && i < param_.column_count_; i++) {
          const ObStorageDatum &datum = datum_row.storage_datums_[i + extra_rowkey_cnt];
          const ObColDesc &col_desc = param_.col_descs_->at(i);
          const ObCmpFunc &cmp_func = param_.cmp_funcs_->at(i).get_cmp_func();
          ObOptOSGColumnStat *col_stat = sql_stat->get_col_stat_array().at(i);
          bool is_valid = ObColumnStatParam::is_valid_opt_col_type(col_desc.col_type_.get_type());
          if (col_stat != nullptr && is_valid) {
            if (OB_FAIL(col_stat->update_column_stat_info(&datum, col_desc.col_type_, cmp_func.cmp_func_))) {
              LOG_WARN("Failed to merge obj", K(ret), KP(col_stat));
            }
          }
        }
      }
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
  } else if (OB_UNLIKELY(tablet_finish_count_ != param_.ls_partition_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected finished tablet count", KR(ret), K(tablet_finish_count_),
             K(param_.ls_partition_ids_.count()));
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

} // namespace storage
} // namespace oceanbase
