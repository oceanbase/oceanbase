//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX RS_COMPACTION
#include "share/compaction/ob_table_ckm_items.h"
#include "lib/utility/ob_tracepoint.h"
#include "rootserver/freeze/ob_major_merge_progress_util.h"
namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace compaction
{
int ObSortColumnIdArray::build(
  const uint64_t tenant_id,
  const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColDesc, ObTabletReplicaReportColumnMeta::DEFAULT_COLUMN_CNT> column_descs;
  int64_t col_cnt = 0;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("array is inited", KR(ret), KPC(this));
  } else if (OB_FAIL(table_schema.get_multi_version_column_descs(column_descs))) {
    LOG_WARN("fail to get multi version column descs", KR(ret), K(table_schema));
  } else if (FALSE_IT(col_cnt = column_descs.count())) {
  } else if (OB_UNLIKELY(0 == col_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty input column desc array", KR(ret), K(column_descs));
  } else if (col_cnt > BUILD_HASH_MAP_TABLET_CNT_THRESHOLD) { // build hash map
    ret = build_hash_map(tenant_id, column_descs);
  } else {
    array_.set_attr(ObMemAttr(tenant_id, "SortColIdArr"));
    ret = build_sort_array(column_descs);
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else {
    reset();
  }
  return ret;
}

int32_t ObSortColumnIdArray::get_func_from_map(
  ObSortColumnIdArray &sort_array,
  const int64_t column_id,
  int64_t &input_array_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sort_array.map_.get_refactored(column_id, input_array_idx))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("failed to get column id from map", KR(ret), K(column_id));
    }
  }
  return ret;
}

int ObSortColumnIdArray::build_hash_map(
  const uint64_t tenant_id,
  const ObIArray<ObColDesc> &column_descs)
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = column_descs.count();
  if (OB_FAIL(map_.create(col_cnt, "RSCompColId", "RSCompColId", tenant_id))) {
    LOG_WARN("failed to create hash map", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && (i < col_cnt); ++i) {
    if (OB_FAIL(map_.set_refactored(column_descs.at(i).col_id_, i))) {
      LOG_WARN("failed to push back item", KR(ret), K(i), "column_id", column_descs.at(i).col_id_);
    }
  } // end of for
  if (OB_SUCC(ret)) {
    get_func_ = get_func_from_map;
  }
  return ret;
}

int32_t ObSortColumnIdArray::get_func_from_array(
  ObSortColumnIdArray &sort_array,
  const int64_t column_id,
  int64_t &input_array_idx)
{
  int ret = OB_SUCCESS;
  const int64_t array_idx =
      std::lower_bound(sort_array.array_.begin(), sort_array.array_.end(),
                       ObColumnIdToIdx(column_id)) - sort_array.array_.begin();
  if ((sort_array.array_.count() != array_idx) &&
      (sort_array.array_[array_idx].column_id_ == column_id)) {
    input_array_idx = sort_array.array_[array_idx].idx_;
  } else {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObSortColumnIdArray::build_sort_array(
  const ObIArray<ObColDesc> &column_descs)
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = column_descs.count();
  if (OB_FAIL(array_.reserve(col_cnt))) {  // build array
    LOG_WARN("failed to reserve", KR(ret), K(col_cnt));
  } else {
    ObColumnIdToIdx item;
    for (int64_t i = 0; OB_SUCC(ret) && (i < col_cnt); ++i) {
      item.column_id_ = column_descs.at(i).col_id_;
      item.idx_ = i;
      if (OB_FAIL(array_.push_back(item))) {
        LOG_WARN("failed to push back item", KR(ret), K(item));
      }
    } // end of for
    if (OB_SUCC(ret)) {
      lib::ob_sort(array_.begin(), array_.end());
      LOG_TRACE("success to sort array", KR(ret), K(array_));
      get_func_ = get_func_from_array;
    }
  }
  return ret;
}

void ObSortColumnIdArray::reset()
{
  is_inited_ = false;
  build_map_flag_ = false;
  array_.reset();
  if (map_.created()) {
    map_.destroy();
  }
}

ObTableCkmItems::VALIDATE_CKM_FUNC ObTableCkmItems::validate_ckm_func[FUNC_CNT] = {
    ObTableCkmItems::validate_tablet_column_ckm,
    ObTableCkmItems::validate_column_ckm_sum
};

ObTableCkmItems::ObTableCkmItems(const uint64_t tenant_id)
  : is_inited_(false),
    is_fts_index_(false),
    tenant_id_(tenant_id),
    table_id_(0),
    row_count_(0),
    table_schema_(nullptr),
    tablet_pairs_(),
    ckm_items_(),
    sort_col_id_array_(),
    ckm_sum_array_()
{
  ckm_items_.set_attr(ObMemAttr(tenant_id, "TableCkmItems"));
  ckm_sum_array_.set_attr(ObMemAttr(tenant_id, "TableCkmItems"));
}

ObTableCkmItems::~ObTableCkmItems()
{
  clear();
}

int ObTableCkmItems::build(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const schema::ObSimpleTableSchemaV2 &simple_schema,
    const ObArray<share::ObTabletLSPair> &input_tablet_pairs,
    const ObArray<share::ObTabletReplicaChecksumItem> &input_ckm_items)
{
  int ret = OB_SUCCESS;
  const int64_t table_id = simple_schema.get_table_id();
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("is inited before", KR(ret), KPC(this));
  } else if (OB_FAIL(tablet_pairs_.assign(input_tablet_pairs))) {
    LOG_WARN("failed to assgin tablet ls pair array", KR(ret), K(input_tablet_pairs));
  } else if (OB_FAIL(ckm_items_.assign(input_ckm_items))) {
    LOG_WARN("failed to assgin tablet replica ckm array", KR(ret), K(input_ckm_items));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema_))) {
    LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(table_id));
  } else if ((!simple_schema.is_index_table() || simple_schema.is_fts_or_multivalue_index())
      && OB_FAIL(sort_col_id_array_.build(tenant_id_, *table_schema_))) {
    LOG_WARN("failed to build column id array for data table", KR(ret), KPC_(table_schema));
  } else {
    table_id_ = simple_schema.get_table_id();
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObTableCkmItems::build(
    const uint64_t table_id,
    const share::SCN &compaction_scn,
    common::ObMySQLProxy &sql_proxy,
    schema::ObSchemaGetterGuard &schema_guard,
    const compaction::ObTabletLSPairCache &tablet_ls_pair_cache)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 64> tablet_id_array;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("is inited before", KR(ret), KPC(this));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema_))) {
    LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema_)) {
    // table schemas are changed, and index_table or data_table does not exist
    // in new table schemas. no need to check index column checksum.
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is null", KR(ret), K(table_id), KP_(table_schema));
  } else if (OB_FAIL(table_schema_->get_tablet_ids(tablet_id_array))) {
    LOG_WARN("fail to get tablet_ids from table schema", KR(ret), KPC(table_schema_));
  } else if (OB_FAIL(tablet_ls_pair_cache.get_tablet_ls_pairs(table_id, tablet_id_array, tablet_pairs_))) {
    LOG_WARN("failed to get tablet ls pairs", KR(ret), K_(tenant_id), K(table_id), K(tablet_id_array));
  } else if (OB_FAIL(ObTabletReplicaChecksumOperator::get_tablet_replica_checksum_items(
                             tenant_id_, sql_proxy,
                             compaction_scn, tablet_pairs_,
                             ckm_items_))) {
    LOG_WARN("failed to get table column checksum items", KR(ret));
  } else if ((!table_schema_->is_index_table() || table_schema_->is_fts_or_multivalue_index())
      && OB_FAIL(sort_col_id_array_.build(tenant_id_, *table_schema_))) {
    LOG_WARN("failed to build column id array for data table", KR(ret), KPC(table_schema_));
  } else {
    table_id_ = table_id;
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObTableCkmItems::build_column_ckm_sum_array(
  const SCN &compaction_scn,
  const share::schema::ObTableSchema &table_schema,
  int64_t &row_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t items_cnt = ckm_items_.count();
  if (OB_UNLIKELY(ckm_items_.empty() || tablet_pairs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("checksum items or tablet pairs are empty", KR(ret), K_(ckm_items), K_(tablet_pairs));
  } else if (!ckm_sum_array_.empty()) {
    LOG_INFO("use cached ckm array", KR(ret), K_(row_count), K_(ckm_sum_array), K(compaction_scn));
  } else {
    row_count_ = 0;
    ckm_sum_array_.reuse();

    const int64_t column_checksums_cnt = ckm_items_.at(0).column_meta_.column_checksums_.count();
    uint64_t pre_tablet_id = OB_INVALID_ID;
    if (OB_FAIL(ckm_sum_array_.reserve(column_checksums_cnt))) {
      LOG_WARN("failed to reserve tablet column checksum array", KR(ret));
    }
    // items are order by tablet_id
    int64_t pair_idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && (i < items_cnt); ++i) {
      const ObTabletReplicaChecksumItem &cur_item = ckm_items_.at(i);
      LOG_TRACE("build_column_ckm_sum_array", KR(ret), K(i), K(cur_item), K(compaction_scn), K(row_cnt));
      if (OB_UNLIKELY(cur_item.column_meta_.column_checksums_.count() != column_checksums_cnt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column ckm count is unexpected", KR(ret), K(cur_item), K(column_checksums_cnt));
      } else if (cur_item.compaction_scn_ == compaction_scn) {
        const ObTabletReplicaReportColumnMeta &cur_column_meta = cur_item.column_meta_;
        if (pre_tablet_id == OB_INVALID_ID) { // first ckm item
          for (int64_t j = 0; OB_SUCC(ret) && (j < column_checksums_cnt); ++j) {
            if (OB_FAIL(ckm_sum_array_.push_back(cur_column_meta.column_checksums_.at(j)))) {
              LOG_WARN("failed to push back column ckm", KR(ret), K(j), K(cur_column_meta));
            }
          } // end of for
          row_count_ += cur_item.row_count_;
        } else if (cur_item.tablet_id_.id() != pre_tablet_id) { // start new tablet
          for (int64_t j = 0; j < column_checksums_cnt; ++j) {
            ckm_sum_array_.at(j) += cur_column_meta.column_checksums_.at(j);
          } // end of for
          row_count_ += cur_item.row_count_;
        }

        if (OB_FAIL(ret)) {
        } else if (OB_UNLIKELY(cur_item.tablet_id_.id() != pre_tablet_id // meet new tablet
            && (pair_idx >= tablet_pairs_.count() || tablet_pairs_.at(pair_idx++).get_tablet_id() != cur_item.tablet_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet pair and ckm items are mismatch", KR(ret), K(i), K(cur_item), K(tablet_pairs_), K(pair_idx));
        } else {
          pre_tablet_id = cur_item.tablet_id_.id();
        }
      } else {
        ret = OB_ITEM_NOT_MATCH;
        LOG_WARN("compaction scn mismtach", KR(ret), K(cur_item), K(compaction_scn));
      }
    } // end of for
  }
  if (OB_SUCC(ret)) {
    row_cnt = row_count_;
  } else {
    row_count_ = 0;
    ckm_sum_array_.reuse();
  }
  return ret;
}

#define RECORD_CKM_ERROR_INFO(tablet_array_idx, is_global_index) \
  ckm_error_info.tenant_id_ = data_ckm.tenant_id_; \
  ckm_error_info.is_global_index_ = is_global_index; \
  ckm_error_info.frozen_scn_ = compaction_scn; \
  ckm_error_info.data_table_id_ = data_table_schema->get_table_id(); \
  ckm_error_info.index_table_id_ = index_table_schema->get_table_id(); \
  if (tablet_array_idx >= 0 && tablet_array_idx < data_ckm.tablet_pairs_.count()) { \
    ckm_error_info.data_tablet_id_ = data_ckm.tablet_pairs_.at(tablet_array_idx).get_tablet_id(); \
    ckm_error_info.index_tablet_id_ = index_ckm.tablet_pairs_.at(tablet_array_idx).get_tablet_id(); \
  }

int ObTableCkmItems::validate_column_ckm_sum(
    const share::SCN &compaction_scn,
    common::ObMySQLProxy &sql_proxy,
    ObTableCkmItems &data_ckm,
    ObTableCkmItems &index_ckm)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t check_cnt = 0;
  ObColumnChecksumErrorInfo ckm_error_info;
  int64_t data_row_cnt = 0;
  int64_t index_row_cnt = 0;
  const schema::ObTableSchema *data_table_schema = data_ckm.table_schema_;
  const schema::ObTableSchema *index_table_schema = index_ckm.table_schema_;
  if (OB_UNLIKELY(!index_ckm.is_fts_index_
    && (nullptr == data_table_schema || nullptr == index_table_schema
      || data_table_schema->get_table_id() != index_table_schema->get_data_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table and index table should not validate column checksum", KR(ret), KPC(data_table_schema),
      KPC(index_table_schema));
  } else if (OB_FAIL(data_ckm.build_column_ckm_sum_array(compaction_scn, *data_table_schema, data_row_cnt))) {
    LOG_WARN("failed to build column ckm sum map for data table", KR(ret));
  } else if (OB_FAIL(index_ckm.build_column_ckm_sum_array(compaction_scn, *index_table_schema, index_row_cnt))) {
    LOG_WARN("failed to build column ckm sum map for index table", KR(ret));
  } else if (OB_UNLIKELY(data_row_cnt != index_row_cnt)) {
    ret = OB_CHECKSUM_ERROR;
    LOG_ERROR("sum row count in data & global index is not equal", KR(ret), K(data_row_cnt), K(index_row_cnt));
  } else if (OB_FAIL(compare_ckm_by_column_ids(
                 data_ckm,
                 index_ckm,
                 *data_table_schema,
                 *index_table_schema,
                 data_ckm.ckm_sum_array_,
                 index_ckm.ckm_sum_array_,
                 ckm_error_info))) {
    if (OB_CHECKSUM_ERROR != ret) {
      LOG_WARN("failed to compare column checksum for global index", KR(ret),
               K(ckm_error_info), K(data_ckm.ckm_items_),
               K(index_ckm.ckm_items_));
    }
  }
  if (OB_CHECKSUM_ERROR == ret) {
    RECORD_CKM_ERROR_INFO(OB_INVALID_INDEX /*array_idx*/, true/*is_global_index*/);
    LOG_ERROR("failed to compare column checksum", KR(ret), K(ckm_error_info),
      K(data_ckm.ckm_items_), K(index_ckm.ckm_items_));
    if (OB_TMP_FAIL(ObColumnChecksumErrorOperator::insert_column_checksum_err_info(sql_proxy, data_ckm.tenant_id_,
        ckm_error_info))) {
      LOG_WARN("fail to insert global index column checksum error info", KR(tmp_ret), K(ckm_error_info));
    }
  }
  return ret;
}

int64_t ObTableCkmItems::get_replica_checksum_idx(
    const int64_t last_tablet_idx,
    const ObTabletID &tablet_id) const
{
  int64_t ret_idx = -1;
  for (int64_t i = last_tablet_idx + 1; i < ckm_items_.count(); ++i) {
    if (ckm_items_.at(i).tablet_id_ == tablet_id) {
      ret_idx = i;
      break;
    }
  }
  return ret_idx;
}

int ObTableCkmItems::validate_tablet_column_ckm(
    const share::SCN &compaction_scn,
    common::ObMySQLProxy &sql_proxy,
    ObTableCkmItems &data_ckm,
    ObTableCkmItems &index_ckm)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t check_cnt = 0;
  ObColumnChecksumErrorInfo ckm_error_info;
  const schema::ObTableSchema *data_table_schema = data_ckm.table_schema_;
  const schema::ObTableSchema *index_table_schema = index_ckm.table_schema_;
  if (OB_UNLIKELY(!index_ckm.is_fts_index_
    && (nullptr == data_table_schema || nullptr == index_table_schema
    || data_table_schema->get_table_id() != index_table_schema->get_data_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table and index table should not validate column checksum", KR(ret), KPC(data_table_schema),
      KPC(index_table_schema));
  } else if (OB_UNLIKELY(index_ckm.tablet_pairs_.count() != data_ckm.tablet_pairs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet ls pairs count are not equal", KR(ret), K(data_ckm), K(index_ckm),
      K(data_ckm.tablet_pairs_), K(index_ckm.tablet_pairs_));
  } else {
    int64_t data_replica_ckm_idx = -1;
    int64_t index_replica_ckm_idx = -1;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < index_ckm.tablet_pairs_.count(); ++idx) {
      if (OB_UNLIKELY(-1 == (data_replica_ckm_idx = data_ckm.get_replica_checksum_idx(
        data_replica_ckm_idx/*last_tablet_idx*/,
        data_ckm.tablet_pairs_.at(idx).get_tablet_id())))) {
        LOG_WARN("failed to find replica_checksum idx of data tablet", KR(ret), K(data_replica_ckm_idx), K(idx),
          "tablet_id", data_ckm.tablet_pairs_.at(idx).get_tablet_id(), K(data_ckm.tablet_pairs_));
      } else if (OB_UNLIKELY(-1 == (index_replica_ckm_idx = index_ckm.get_replica_checksum_idx(
        index_replica_ckm_idx/*last_tablet_idx*/,
        index_ckm.tablet_pairs_.at(idx).get_tablet_id())))) {
        LOG_WARN("failed to find replica_checksum idx of index tablet", KR(ret), K(index_replica_ckm_idx), K(idx),
          "tablet_id", index_ckm.tablet_pairs_.at(idx).get_tablet_id(), K(index_ckm.tablet_pairs_));
      } else {
        const ObTabletReplicaChecksumItem &data_replica_ckm = data_ckm.ckm_items_.at(data_replica_ckm_idx);
        const ObTabletReplicaChecksumItem &index_replica_ckm = index_ckm.ckm_items_.at(index_replica_ckm_idx);
        if (OB_UNLIKELY(data_ckm.tablet_pairs_.at(idx).get_tablet_id() != data_replica_ckm.tablet_id_
          || index_ckm.tablet_pairs_.at(idx).get_tablet_id() != index_replica_ckm.tablet_id_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet id & replica checksum is not match", KR(ret), K(idx),
              "data_tablet", data_ckm.tablet_pairs_.at(idx), K(data_replica_ckm),
              "index_tablet", index_ckm.tablet_pairs_.at(idx), K(index_replica_ckm));
        } else if (OB_UNLIKELY(data_replica_ckm.compaction_scn_ != compaction_scn
          || index_replica_ckm.compaction_scn_ != compaction_scn)) {
          ret = OB_ITEM_NOT_MATCH;
          LOG_WARN("compaction scan is not match, no need to validate", KR(ret), K(compaction_scn), K(data_replica_ckm), K(index_replica_ckm));
        } else if (OB_UNLIKELY(data_replica_ckm.row_count_ != index_replica_ckm.row_count_)) {
          ret = OB_CHECKSUM_ERROR;
          LOG_ERROR("tablet row count in data & local index is not equal", KR(ret),
            "data_row_cnt", data_replica_ckm.row_count_,
            "index_row_cnt", index_replica_ckm.row_count_);
        } else if (OB_FAIL(compare_ckm_by_column_ids(
            data_ckm,
            index_ckm,
            *data_table_schema,
            *index_table_schema,
            data_replica_ckm.column_meta_.column_checksums_,
            index_replica_ckm.column_meta_.column_checksums_,
            ckm_error_info))) {
          if (OB_CHECKSUM_ERROR != ret) {
            LOG_WARN("failed to compare column checksum", KR(ret),
              "data_tablet", data_ckm.tablet_pairs_.at(idx), K(data_replica_ckm),
              "index_tablet", index_ckm.tablet_pairs_.at(idx), K(index_replica_ckm),
              K(data_ckm.ckm_items_), K(index_ckm.ckm_items_));
          }
        }
#ifdef ERRSIM
        if (OB_SUCC(ret)) {
          ret = OB_E(EventTable::EN_RS_USER_INDEX_CHECKSUM_ERROR) OB_SUCCESS;
          if (OB_FAIL(ret)) {
            STORAGE_LOG(INFO, "ERRSIM EN_RS_USER_INDEX_CHECKSUM_ERROR", K(ret));
            ret = OB_CHECKSUM_ERROR;
          }
        }
#endif
        if (OB_CHECKSUM_ERROR == ret) {
          RECORD_CKM_ERROR_INFO(idx, false/*is_global_index*/);
          LOG_ERROR("failed to compare column checksum", KR(ret), K(ckm_error_info),
            "data_tablet", data_ckm.tablet_pairs_.at(idx), "data_row_cnt", data_replica_ckm.row_count_, K(data_replica_ckm),
            "index_tablet", index_ckm.tablet_pairs_.at(idx), "index_row_cnt", index_replica_ckm.row_count_, K(index_replica_ckm),
            K(data_ckm.ckm_items_), K(index_ckm.ckm_items_));
          if (OB_TMP_FAIL(ObColumnChecksumErrorOperator::insert_column_checksum_err_info(sql_proxy, data_ckm.tenant_id_,
            ckm_error_info))) {
            LOG_WARN("fail to insert global index column checksum error info", KR(tmp_ret), K(ckm_error_info));
          }
        }
      }
    } // end of for
  }
  return ret;
}

int ObTableCkmItems::compare_ckm_by_column_ids(
    ObTableCkmItems &data_ckm,
    ObTableCkmItems &index_ckm,
    const schema::ObTableSchema &data_table_schema,
    const schema::ObTableSchema &index_table_schema,
    const ObIArray<int64_t> &data_replica_col_ckm_array,
    const ObIArray<int64_t> &index_replica_col_ckm_array,
    ObColumnChecksumErrorInfo &ckm_error_info)
{
  int ret = OB_SUCCESS;
  int64_t data_array_idx = 0;
  ObArray<ObColDesc> index_column_descs;
  const ObColumnSchemaV2 *data_column_schema = nullptr;
  const ObColumnSchemaV2 *index_column_schema = nullptr;
  if (OB_FAIL(index_table_schema.get_multi_version_column_descs(index_column_descs))) {
    LOG_WARN("fail to get multi version column descs", KR(ret), K(index_table_schema));
  }
  for (int64_t idx = 0; OB_SUCC(ret) && idx < index_column_descs.count(); ++idx) {
    const int64_t column_id = index_column_descs.at(idx).col_id_;
    // index_column_id -> array index in data table schema
    if ((column_id == OB_HIDDEN_TRANS_VERSION_COLUMN_ID) || (column_id == OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID)) {
      // there not exists a promise: these two hidden columns checksums in data table and index table are equal
      // when data table update normal cols, not all index tables need to update
      // thus, we skip them.
    } else {
      data_column_schema = data_table_schema.get_column_schema(column_id);
      if (OB_ISNULL(data_column_schema)) {
        if (OB_ISNULL(index_column_schema = index_table_schema.get_column_schema(column_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index column schema is unexpected null", KR(ret));
        } else if (index_ckm.is_fts_index_ || index_column_schema->is_shadow_column()) {
          // some column in fts index is not exist in data table
          // shadow column only exists in index table
          LOG_TRACE("column do not need to compare checksum", K(column_id), KPC(index_column_schema),
            K(index_column_schema->is_shadow_column()));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index column schema not found in data table", KR(ret), K(column_id), KPC(index_column_schema));
        }
      } else if (!data_column_schema->is_column_stored_in_sstable()) {
        // virtual column/ROWID fake column only tag in data table
        LOG_TRACE("column do not need to compare checksum", KPC(data_column_schema), K(data_column_schema->is_column_stored_in_sstable()));
      } else if (OB_UNLIKELY(!data_ckm.sort_col_id_array_.is_inited())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sort col id is unexpected invalid", KR(ret), K(data_ckm));
      } else if (OB_FAIL(ObSortColumnIdArray::get_array_idx_by_column_id(data_ckm.sort_col_id_array_, column_id, data_array_idx))) {
        LOG_WARN("failed to get array idx from data ckm", KR(ret), K(idx), K(column_id));
      } else if (OB_UNLIKELY(data_array_idx < 0 || data_array_idx >= data_replica_col_ckm_array.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("array idx is invalid", KR(ret), K(data_array_idx), K(data_ckm));
      } else if (data_replica_col_ckm_array.at(data_array_idx) != index_replica_col_ckm_array.at(idx)) {
        ret = OB_CHECKSUM_ERROR;
        ckm_error_info.column_id_ = column_id;
        ckm_error_info.data_column_checksum_ = data_replica_col_ckm_array.at(data_array_idx);
        ckm_error_info.index_column_checksum_ = index_replica_col_ckm_array.at(idx);
        LOG_ERROR("failed to compare column checksum", KR(ret), K(column_id),
          K(data_array_idx), "data_column_checksum", data_replica_col_ckm_array.at(data_array_idx), K(data_replica_col_ckm_array),
          "index_array_idx", idx, "index_column_checksum", index_replica_col_ckm_array.at(idx), K(index_replica_col_ckm_array));
      }
    }
  } // end of for
  return ret;
}

void ObTableCkmItems::clear()
{
  if (is_inited_) {
    reset();
  }
}

void ObTableCkmItems::reset()
{
  is_inited_ = false;
  is_fts_index_ = false;
  table_id_ = 0;
  row_count_ = 0;
  table_schema_ = NULL;
  tablet_pairs_.reset();
  ckm_items_.reset();
  sort_col_id_array_.reset();
  ckm_sum_array_.reset();
}

} // namespace compaction
} // namespace oceanbase
