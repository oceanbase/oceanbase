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
#include "observer/table_load/ob_table_load_index_table_projector.h"
#include "lib/oblog/ob_log_module.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ob_i_store.h"

namespace oceanbase
{
namespace observer
{
ObTableLoadIndexTableProjector::~ObTableLoadIndexTableProjector()
{
  row_projector_.reset();
  tablet_projector_.destroy();
  index_tablet_id_to_part_id_map_.destroy();
}

int ObTableLoadIndexTableProjector::init(const share::schema::ObTableSchema *data_table_schema,
                                         const share::schema::ObTableSchema *index_table_schema)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadIndexTableProjector init twice", KR(ret));
  } else if (OB_FAIL(tablet_projector_.create(1024, "TLD_ITP", "TLD_ITP", MTL_ID()))) {
    LOG_WARN("fail to create tablet projector", KR(ret));
  } else if (OB_FAIL(
               index_tablet_id_to_part_id_map_.create(1024, "TLD_ITP", "TLD_ITP", MTL_ID()))) {
    LOG_WARN("fail to create index tablet id to part id map", KR(ret));
  } else if (OB_FAIL(build_projector(data_table_schema, index_table_schema))) {
    LOG_WARN("fail to build projector", KR(ret), KPC(data_table_schema), KPC(index_table_schema));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadIndexTableProjector::get_index_tablet_id_and_part_id_by_data_tablet_id(
  const ObTabletID &data_tablet_id, ObTabletID &index_tablet_id, ObObjectID &part_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadIndexTableProjector not init", KR(ret));
  } else if (OB_FAIL(tablet_projector_.get_refactored(data_tablet_id, index_tablet_id))) {
    LOG_WARN("fail to get index tablet id", KR(ret), K(data_tablet_id));
  } else if (OB_FAIL(index_tablet_id_to_part_id_map_.get_refactored(index_tablet_id, part_id))) {
    LOG_WARN("fail to get index tablet id", KR(ret), K(index_tablet_id));
  }
  return ret;
}

int ObTableLoadIndexTableProjector::build_projector(
  const share::schema::ObTableSchema *data_table_schema,
  const share::schema::ObTableSchema *index_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_table_schema) || OB_ISNULL(index_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(data_table_schema), KP(index_table_schema));
  } else if (OB_FAIL(build_row_projector(data_table_schema, index_table_schema))) {
    LOG_WARN("fail to build row projector", KR(ret));
  } else if (OB_FAIL(build_tablet_projector(data_table_schema, index_table_schema))) {
    LOG_WARN("fail to build tablet projector", KR(ret));
  } else {
    column_num_ = index_table_schema->get_column_count();
    main_table_rowkey_column_num_ = data_table_schema->get_rowkey_column_num();
  }
  return ret;
}

int ObTableLoadIndexTableProjector::build_row_projector(
  const share::schema::ObTableSchema *data_table_schema,
  const share::schema::ObTableSchema *index_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_table_schema) || OB_ISNULL(index_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(data_table_schema), KP(index_table_schema));
  } else {
    common::ObArray<share::schema::ObColDesc> main_column_descs;
    common::ObArray<share::schema::ObColDesc> index_column_descs;
    if (OB_FAIL(data_table_schema->get_column_ids(main_column_descs, false))) {
      LOG_WARN("fail to get column ids", KR(ret));
    } else if (OB_FAIL(index_table_schema->get_column_ids(index_column_descs, false))) {
      LOG_WARN("fail to get column ids", KR(ret));
    } else {
      FOREACH_X(iter, index_column_descs, OB_SUCC(ret)) {
        share::schema::ObColDesc index_col_desc = *iter;
        for (int64_t i = 0; OB_SUCC(ret) && i < main_column_descs.count(); i++) {
          if (index_col_desc.col_id_ == main_column_descs.at(i).col_id_) {
            if (OB_FAIL(row_projector_.push_back(i))) {
              LOG_WARN("fail to push back", KR(ret), K(i));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadIndexTableProjector::build_tablet_projector(
  const share::schema::ObTableSchema *data_table_schema,
  const share::schema::ObTableSchema *index_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_table_schema) || OB_ISNULL(index_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(data_table_schema), KP(index_table_schema));
  } else {
    ObArray<ObTabletID> tablet_ids;
    if (OB_FAIL(data_table_schema->get_tablet_ids(tablet_ids))) {
      LOG_WARN("fail to get tablet ids", KR(ret));
    } else {
      int64_t main_part_idx = OB_INVALID_ID;
      int64_t main_subpart_idx = OB_INVALID_ID;
      ObObjectID index_part_id = OB_INVALID_ID;
      ObObjectID index_subpart_id = OB_INVALID_ID;
      ObTabletID index_tablet_id(ObTabletID::INVALID_TABLET_ID);
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
        if (data_table_schema->is_partitioned_table() &&
            OB_FAIL(data_table_schema->get_part_idx_by_tablet(tablet_ids.at(i), main_part_idx,
                                                              main_subpart_idx))) {
          LOG_WARN("fail to get part idx by tablet", KR(ret), K(tablet_ids.at(i)));
        } else if (OB_FAIL(index_table_schema->get_part_id_and_tablet_id_by_idx(
                     main_part_idx, main_subpart_idx, index_part_id, index_subpart_id,
                     index_tablet_id))) {
          LOG_WARN("fail to get index tablet id", KR(ret), K(main_part_idx), K(main_subpart_idx));
        } else if (OB_FAIL(tablet_projector_.set_refactored(tablet_ids.at(i), index_tablet_id))) {
          LOG_WARN("fail to add tablet projector", KR(ret), K(index_tablet_id),
                   K(tablet_ids.at(i)));
        } else if (OB_FAIL(index_tablet_id_to_part_id_map_.set_refactored(index_tablet_id,
                                                                          index_part_id))) {
          LOG_WARN("fail to add index tablet id to part id map", KR(ret), K(index_tablet_id),
                   K(index_part_id));
        }
      }
    }
  }
  return ret;
}

int ObTableLoadIndexTableProjector::projector(const ObTabletID &data_tablet_id,
                                              const blocksstable::ObDatumRow &origin_datum_row,
                                              const bool &have_multiversion_col,
                                              ObTabletID &index_tablet_id,
                                              blocksstable::ObDatumRow &out_datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadIndexTableProjector not init", KR(ret));
  } else if (OB_FAIL(out_datum_row.init(column_num_))) {
    LOG_WARN("fail to init index datum row", KR(ret));
  } else if (OB_FAIL(tablet_projector_.get_refactored(data_tablet_id, index_tablet_id))) {
    LOG_WARN("fail to get index id", KR(ret), K(data_tablet_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_projector_.size(); i++) {
      if (have_multiversion_col && row_projector_.at(i) >= main_table_rowkey_column_num_) {
        if (row_projector_.at(i) + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt() >=
            origin_datum_row.get_column_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get datum", KR(ret), K(row_projector_.at(i)), K(origin_datum_row));
        } else {
          out_datum_row.storage_datums_[i] =
            origin_datum_row
              .storage_datums_[row_projector_.at(i) +
                               storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()];
        }
      } else {
        if (row_projector_.at(i) >= origin_datum_row.get_column_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get datum", KR(ret), K(row_projector_.at(i)), K(origin_datum_row));
        } else {
          out_datum_row.storage_datums_[i] = origin_datum_row.storage_datums_[row_projector_.at(i)];
        }
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase