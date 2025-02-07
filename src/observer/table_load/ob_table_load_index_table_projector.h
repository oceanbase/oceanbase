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

 #pragma once
#include "lib/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "ob_tablet_id.h"
#include "share/schema/ob_table_schema.h"
#include "storage/blocksstable/ob_batch_datum_rows.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadIndexTableProjector
{
public:
  ObTableLoadIndexTableProjector()
    : column_num_(0), main_table_rowkey_column_num_(0), is_inited_(false)
  {
  }
  ~ObTableLoadIndexTableProjector();
  int init(const share::schema::ObTableSchema *data_table_schema, const share::schema::ObTableSchema *index_table_schema);
  int init_datum_row(blocksstable::ObDatumRow &datum_row);
  int projector(const ObTabletID &data_tablet_id, const blocksstable::ObDatumRow &origin_datum_row,
                const bool &have_multiversion_col, ObTabletID &index_tablet_id,
                blocksstable::ObDatumRow &out_datum_row);
  int projector(const blocksstable::ObBatchDatumRows &data_datum_rows, // contain multi version cols
                const int64_t row_idx,
                const bool has_multi_version_cols,
                blocksstable::ObDatumRow &index_datum_row);
  int get_index_tablet_id_and_part_id_by_data_tablet_id(const ObTabletID &data_tablet_id, ObTabletID &index_tablet_id, ObObjectID &part_id);
  int get_index_tablet_id(const ObTabletID &data_tablet_id, ObTabletID &index_tablet_id);
private:
  int build_projector(const share::schema::ObTableSchema *data_table_schema, const share::schema::ObTableSchema *index_table_schema);
  int build_tablet_projector(const share::schema::ObTableSchema *data_table_schema, const share::schema::ObTableSchema *index_table_schema);
  int build_row_projector(const share::schema::ObTableSchema *data_table_schema, const share::schema::ObTableSchema *index_table_schema);
private:
  common::ObArray<uint64_t> row_projector_;
  common::hash::ObHashMap<ObTabletID, ObTabletID, common::hash::NoPthreadDefendMode> tablet_projector_;//main tablet id -> index tablet id
  common::hash::ObHashMap<ObTabletID, ObObjectID> index_tablet_id_to_part_id_map_;
  int64_t column_num_;
  int64_t main_table_rowkey_column_num_;
  bool is_inited_;
};

}
}