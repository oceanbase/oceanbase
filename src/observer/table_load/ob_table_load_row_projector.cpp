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

#include "observer/table_load/ob_table_load_row_projector.h"
#include "lib/oblog/ob_log_module.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_table_schema.h"
#include "storage/blocksstable/ob_batch_datum_rows.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"
#include "storage/ob_i_store.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;
using namespace common;
using namespace share::schema;
using namespace storage;

ObTableLoadRowProjector::ObTableLoadRowProjector()
 : src_column_num_(0), dest_column_num_(0), is_inited_(false)
{
}

ObTableLoadRowProjector::~ObTableLoadRowProjector()
{
  col_projector_.reset();
  tablet_projector_.destroy();
  dest_tablet_id_to_part_id_map_.destroy();
}

int ObTableLoadRowProjector::init(const ObTableSchema *src_table_schema,
                                  const ObTableSchema *dest_table_schema)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadProjector init twice", KR(ret));
  } else if (OB_ISNULL(src_table_schema) || OB_ISNULL(dest_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(src_table_schema), KP(dest_table_schema));
  } else if (OB_FAIL(tablet_projector_.create(1024, "TLD_ITP", "TLD_ITP", MTL_ID()))) {
    LOG_WARN("fail to create tablet projector", KR(ret));
  } else if (OB_FAIL(dest_tablet_id_to_part_id_map_.create(1024, "TLD_ITP", "TLD_ITP", MTL_ID()))) {
    LOG_WARN("fail to create index tablet id to part id map", KR(ret));
  } else if (OB_FAIL(build_projector(src_table_schema, dest_table_schema))) {
    LOG_WARN("fail to build projector", KR(ret), KPC(src_table_schema), KPC(dest_table_schema));
  } else if (OB_FAIL(src_table_schema->get_store_column_count(src_column_num_))) {
    LOG_WARN("fail to get store column count", KR(ret));
  } else {
    dest_column_num_ = col_projector_.count();
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadRowProjector::get_dest_tablet_id_and_part_id_by_src_tablet_id(
  const ObTabletID &src_tablet_id, ObTabletID &dest_tablet_id, ObObjectID &part_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadProjector not init", KR(ret));
  } else if (OB_FAIL(tablet_projector_.get_refactored(src_tablet_id, dest_tablet_id))) {
    LOG_WARN("fail to get index tablet id", KR(ret), K(src_tablet_id));
  } else if (OB_FAIL(dest_tablet_id_to_part_id_map_.get_refactored(dest_tablet_id, part_id))) {
    LOG_WARN("fail to get index tablet id", KR(ret), K(dest_tablet_id));
  }
  return ret;
}

int ObTableLoadRowProjector::get_dest_tablet_id(const ObTabletID &src_tablet_id,
                                                ObTabletID &dest_tablet_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadIndexTableProjector not init", KR(ret));
  } else if (OB_FAIL(tablet_projector_.get_refactored(src_tablet_id, dest_tablet_id))) {
    LOG_WARN("fail to get index tablet id", KR(ret), K(src_tablet_id));
  }
  return ret;
}

int ObTableLoadRowProjector::build_projector(const ObTableSchema *src_table_schema,
                                             const ObTableSchema *dest_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_table_schema) || OB_ISNULL(dest_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(src_table_schema), KP(dest_table_schema));
  } else if (OB_FAIL(build_row_projector(src_table_schema, dest_table_schema))) {
    LOG_WARN("fail to build row projector", KR(ret));
  } else if (OB_FAIL(build_tablet_projector(src_table_schema, dest_table_schema))) {
    LOG_WARN("fail to build tablet projector", KR(ret));
  }
  return ret;
}

int ObTableLoadRowProjector::build_tablet_projector(
  const ObTableSchema *src_table_schema,
  const ObTableSchema *dest_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_table_schema) || OB_ISNULL(dest_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(src_table_schema), KP(dest_table_schema));
  } else {
    ObArray<ObTabletID> tablet_ids;
    if (OB_FAIL(src_table_schema->get_tablet_ids(tablet_ids))) {
      LOG_WARN("fail to get tablet ids", KR(ret));
    } else {
      int64_t src_part_idx = OB_INVALID_ID;
      int64_t src_subpart_idx = OB_INVALID_ID;
      ObObjectID dest_part_id = OB_INVALID_ID;
      ObObjectID dest_subpart_id = OB_INVALID_ID;
      ObTabletID dest_tablet_id(ObTabletID::INVALID_TABLET_ID);
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
        if (src_table_schema->is_partitioned_table() &&
            OB_FAIL(src_table_schema->get_part_idx_by_tablet(tablet_ids.at(i), src_part_idx,
                                                             src_subpart_idx))) {
          LOG_WARN("fail to get part idx by tablet", KR(ret), K(tablet_ids.at(i)));
        } else if (OB_FAIL(dest_table_schema->get_part_id_and_tablet_id_by_idx(
                     src_part_idx, src_subpart_idx, dest_part_id, dest_subpart_id,
                     dest_tablet_id))) {
          LOG_WARN("fail to get index tablet id", KR(ret), K(src_part_idx), K(src_subpart_idx));
        } else if (OB_FAIL(tablet_projector_.set_refactored(tablet_ids.at(i), dest_tablet_id))) {
          LOG_WARN("fail to add tablet projector", KR(ret), K(dest_tablet_id), K(tablet_ids.at(i)));
        } else if (OB_FAIL(
                     dest_tablet_id_to_part_id_map_.set_refactored(dest_tablet_id, dest_part_id))) {
          LOG_WARN("fail to add index tablet id to part id map", KR(ret), K(dest_tablet_id),
                   K(dest_part_id));
        }
      }
    }
  }
  return ret;
}

int ObTableLoadRowProjector::project_row(const ObDirectLoadDatumRow &src_datum_row,
                                         ObDirectLoadDatumRow &dest_datum_row) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(src_datum_row.get_column_count() != src_column_num_ ||
                  dest_datum_row.get_column_count() != dest_column_num_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src_datum_row), K(dest_datum_row), K(src_column_num_),
             K(dest_column_num_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_projector_.size(); ++i) {
    const int64_t column_idx = col_projector_.at(i);
    dest_datum_row.storage_datums_[i] = src_datum_row.storage_datums_[column_idx];
  }
  return ret;
}

int ObTableLoadRowProjector::project_row(const ObDatumRow &src_datum_row,
                                         const int64_t src_rowkey_column_num,
                                         ObDirectLoadDatumRow &dest_datum_row) const
{
  int ret = OB_SUCCESS;
  const int64_t extra_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  if (OB_UNLIKELY(src_datum_row.get_column_count() != src_column_num_ + extra_col_cnt ||
                  dest_datum_row.get_column_count() != dest_column_num_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src_datum_row), K(dest_datum_row), K(src_column_num_),
             K(dest_column_num_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_projector_.size(); ++i) {
    int64_t column_idx = col_projector_.at(i);
    if (column_idx >= src_rowkey_column_num) {
      column_idx += extra_col_cnt;
    }
    dest_datum_row.storage_datums_[i] = src_datum_row.storage_datums_[column_idx];
  }
  return ret;
}

int ObTableLoadRowProjector::project_row(const blocksstable::ObBatchDatumRows &src_datum_rows,
                                         const int64_t row_idx,
                                         const int64_t src_rowkey_column_num,
                                         storage::ObDirectLoadDatumRow &dest_datum_row) const
{
  int ret = OB_SUCCESS;
  const int64_t extra_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  if (OB_UNLIKELY(src_datum_rows.get_column_count() != src_column_num_ + extra_col_cnt ||
                  row_idx < 0 || dest_datum_row.get_column_count() != dest_column_num_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src_datum_rows), K(row_idx), K(dest_datum_row),
             K(src_column_num_), K(dest_column_num_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_projector_.size(); ++i) {
    int64_t column_idx = col_projector_.at(i);
    if (column_idx >= src_rowkey_column_num) {
      column_idx += extra_col_cnt;
    }
    ObIVector *vector = src_datum_rows.vectors_.at(column_idx);
    if (OB_FAIL(ObDirectLoadVectorUtils::to_datum(vector,
                                                  row_idx,
                                                  dest_datum_row.storage_datums_[i]))) {
      LOG_WARN("fail to get datum", KR(ret));
    }
  }
  return ret;
}

/**
 * ObTableLoadMainToIndexProjector
 */

ObTableLoadMainToIndexProjector::ObTableLoadMainToIndexProjector()
  : src_rowkey_column_num_(0)
{
}

ObTableLoadMainToIndexProjector::~ObTableLoadMainToIndexProjector()
{
}

int ObTableLoadMainToIndexProjector::build_row_projector(
  const ObTableSchema *src_table_schema,
  const ObTableSchema *dest_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_table_schema) || OB_ISNULL(dest_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(src_table_schema), KP(dest_table_schema));
  } else {
    src_rowkey_column_num_ = src_table_schema->get_rowkey_column_num();
    ObArray<ObColDesc> main_column_descs;
    ObArray<ObColDesc> index_column_descs;
    if (OB_FAIL(src_table_schema->get_column_ids(main_column_descs, true))) {
      LOG_WARN("fail to get column ids", KR(ret));
    } else if (OB_FAIL(dest_table_schema->get_column_ids(index_column_descs, true))) {
      LOG_WARN("fail to get column ids", KR(ret));
    } else {
      FOREACH_X(iter, index_column_descs, OB_SUCC(ret))
      {
        ObColDesc index_col_desc = *iter;
        for (int64_t i = 0; OB_SUCC(ret) && i < main_column_descs.count(); i++) {
          if (index_col_desc.col_id_ == main_column_descs.at(i).col_id_) {
            if (OB_FAIL(col_projector_.push_back(i))) {
              LOG_WARN("fail to push back", KR(ret), K(i));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadMainToIndexProjector::projector(const ObTabletID &src_tablet_id,
                                               const ObDirectLoadDatumRow &src_datum_row,
                                               ObTabletID &dest_tablet_id,
                                               ObDirectLoadDatumRow &dest_datum_row) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMainToIndexProjector not init", KR(ret));
  } else if (OB_FAIL(tablet_projector_.get_refactored(src_tablet_id, dest_tablet_id))) {
    LOG_WARN("fail to get index id", KR(ret), K(src_tablet_id));
  } else if (OB_FAIL(project_row(src_datum_row, dest_datum_row))) {
    LOG_WARN("fail to project row", KR(ret));
  }
  return ret;
}

int ObTableLoadMainToIndexProjector::projector(const ObTabletID &src_tablet_id,
                                               const ObDatumRow &src_datum_row,
                                               ObTabletID &dest_tablet_id,
                                               ObDirectLoadDatumRow &dest_datum_row) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMainToIndexProjector not init", KR(ret));
  } else if (OB_FAIL(tablet_projector_.get_refactored(src_tablet_id, dest_tablet_id))) {
    LOG_WARN("fail to get index id", KR(ret), K(src_tablet_id));
  } else if (OB_FAIL(project_row(src_datum_row, src_rowkey_column_num_, dest_datum_row))) {
    LOG_WARN("fail to project row", KR(ret));
  }
  return ret;
}

int ObTableLoadMainToIndexProjector::projector(const ObBatchDatumRows &src_datum_rows,
                                               const int64_t row_idx,
                                               ObDirectLoadDatumRow &dest_datum_row) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMainToIndexProjector not init", KR(ret));
  } else if (OB_UNLIKELY(dest_datum_row.count_ != dest_column_num_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(dest_datum_row.count_), K(dest_column_num_));
  } else if (OB_FAIL(project_row(src_datum_rows, row_idx, src_rowkey_column_num_, dest_datum_row))) {
    LOG_WARN("fail to project row", KR(ret));
  }
  return ret;
}

/**
 * ObTableLoadMainToUniqueIndexProjector
 */

ObTableLoadMainToUniqueIndexProjector::ObTableLoadMainToUniqueIndexProjector()
  : src_rowkey_column_num_(0), dest_rowkey_cnt_(0), dest_spk_cnt_(0), dest_index_rowkey_cnt_(0)
{
}

ObTableLoadMainToUniqueIndexProjector::~ObTableLoadMainToUniqueIndexProjector() {}

int ObTableLoadMainToUniqueIndexProjector::build_row_projector(
  const ObTableSchema *src_table_schema,
  const ObTableSchema *dest_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_table_schema) || OB_ISNULL(dest_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(src_table_schema), KP(dest_table_schema));
  } else {
    src_rowkey_column_num_ = src_table_schema->get_rowkey_column_num();
    dest_rowkey_cnt_ = dest_table_schema->get_rowkey_info().get_size();
    dest_spk_cnt_ = dest_table_schema->get_shadow_rowkey_info().get_size();
    dest_index_rowkey_cnt_ = dest_rowkey_cnt_ - dest_spk_cnt_;
    ObArray<ObColDesc> src_column_descs;
    ObArray<ObColDesc> dest_column_descs;
    if (OB_FAIL(src_table_schema->get_column_ids(src_column_descs, true))) {
      LOG_WARN("fail to get column ids", KR(ret));
    } else if (OB_FAIL(dest_table_schema->get_column_ids(dest_column_descs))) {
      LOG_WARN("fail to get column ids", KR(ret));
    } else {
      FOREACH_X(iter, dest_column_descs, OB_SUCC(ret))
      {
        ObColDesc dest_col_desc = *iter;
        for (int64_t i = 0; OB_SUCC(ret) && i < src_column_descs.count(); i++) {
          if ((is_shadow_column(dest_col_desc.col_id_)
                 ? dest_col_desc.col_id_ - OB_MIN_SHADOW_COLUMN_ID
                 : dest_col_desc.col_id_) == src_column_descs.at(i).col_id_) {
            if (OB_FAIL(col_projector_.push_back(i))) {
              LOG_WARN("fail to push back", KR(ret), K(i));
            }
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadMainToUniqueIndexProjector::projector(const ObTabletID &src_tablet_id,
                                                     const ObDirectLoadDatumRow &src_datum_row,
                                                     ObTabletID &dest_tablet_id,
                                                     ObDirectLoadDatumRow &dest_datum_row) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMainToUniqueIndexProjector not init", KR(ret));
  } else if (OB_FAIL(tablet_projector_.get_refactored(src_tablet_id, dest_tablet_id))) {
    LOG_WARN("fail to get index id", KR(ret), K(src_tablet_id));
  } else if (OB_FAIL(project_row(src_datum_row, dest_datum_row))) {
    LOG_WARN("fail to project row", KR(ret));
  } else {
    shadow_columns(dest_datum_row);
  }
  return ret;
}

int ObTableLoadMainToUniqueIndexProjector::projector(const ObTabletID &src_tablet_id,
                                                     const ObDatumRow &src_datum_row,
                                                     ObTabletID &dest_tablet_id,
                                                     ObDirectLoadDatumRow &dest_datum_row) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMainToUniqueIndexProjector not init", KR(ret));
  } else if (OB_FAIL(tablet_projector_.get_refactored(src_tablet_id, dest_tablet_id))) {
    LOG_WARN("fail to get index id", KR(ret), K(src_tablet_id));
  } else if (OB_FAIL(project_row(src_datum_row, src_rowkey_column_num_, dest_datum_row))) {
    LOG_WARN("fail to project row", KR(ret));
  } else {
    shadow_columns(dest_datum_row);
  }
  return ret;
}

int ObTableLoadMainToUniqueIndexProjector::projector(const ObBatchDatumRows &src_datum_rows,
                                                     const int64_t row_idx,
                                                     ObDirectLoadDatumRow &dest_datum_row) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMainToUniqueIndexProjector not init", KR(ret));
  } else if (OB_FAIL(project_row(src_datum_rows, row_idx, src_rowkey_column_num_, dest_datum_row))) {
    LOG_WARN("fail to project row", KR(ret));
  } else {
    shadow_columns(dest_datum_row);
  }
  return ret;
}

void ObTableLoadMainToUniqueIndexProjector::shadow_columns(ObDirectLoadDatumRow &datum_row) const
{
  bool need_shadow_columns = false;
  if (lib::is_mysql_mode()) {
    // compatible with mysql: contain null value in unique index key,
    // need to fill shadow pk with the real pk value
    bool rowkey_has_null = false;
    for (int64_t i = 0; !rowkey_has_null && i < dest_index_rowkey_cnt_; i++) {
      rowkey_has_null = datum_row.storage_datums_[i].is_null();
    }
    need_shadow_columns = rowkey_has_null;
  } else {
    // compatible with Oracle: only all unique index keys are null value
    // need to fill shadow pk with the real pk value
    bool is_rowkey_all_null = true;
    for (int64_t i = 0; is_rowkey_all_null && i < dest_index_rowkey_cnt_; i++) {
      is_rowkey_all_null = datum_row.storage_datums_[i].is_null();
    }
    need_shadow_columns = is_rowkey_all_null;
  }
  if (!need_shadow_columns) {
    for (int64_t i = 0; i < dest_spk_cnt_; ++i) {
      int64_t spk_idx = dest_index_rowkey_cnt_ + i;
      datum_row.storage_datums_[spk_idx].set_null();
    }
  }
}

/**
 * ObTableLoadUniqueIndexToMainRowkeyProjector
 */

int ObTableLoadUniqueIndexToMainRowkeyProjector::build_row_projector(
  const ObTableSchema *src_table_schema,
  const ObTableSchema *dest_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_table_schema) || OB_ISNULL(dest_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(src_table_schema), KP(dest_table_schema));
  } else {
    ObArray<ObColDesc> src_column_descs;
    ObArray<ObColDesc> dest_column_descs;
    if (OB_FAIL(src_table_schema->get_column_ids(src_column_descs, true))) {
      LOG_WARN("fail to get column ids", KR(ret));
    } else if (OB_FAIL(dest_table_schema->get_rowkey_column_ids(dest_column_descs))) {
      LOG_WARN("fail to get column ids", KR(ret));
    } else {
      FOREACH_X(iter, dest_column_descs, OB_SUCC(ret))
      {
        ObColDesc dest_col_desc = *iter;
        for (int64_t i = 0; OB_SUCC(ret) && i < src_column_descs.count(); i++) {
          if (dest_col_desc.col_id_ == src_column_descs.at(i).col_id_) {
            if (OB_FAIL(col_projector_.push_back(i))) {
              LOG_WARN("fail to push back", KR(ret), K(i));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadUniqueIndexToMainRowkeyProjector::projector(const ObTabletID &src_tablet_id,
                                                           const ObDirectLoadDatumRow &src_datum_row,
                                                           ObTabletID &dest_tablet_id,
                                                           ObDirectLoadDatumRow &dest_datum_row) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadUniqueIndexToMainRowkeyProjector not init", KR(ret));
  } else if (OB_FAIL(tablet_projector_.get_refactored(src_tablet_id, dest_tablet_id))) {
    LOG_WARN("fail to get index id", KR(ret), K(src_tablet_id));
  } else if (OB_FAIL(project_row(src_datum_row, dest_datum_row))) {
    LOG_WARN("fail to project row", KR(ret));
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
