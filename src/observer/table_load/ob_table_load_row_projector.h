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

#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/ob_define.h"
#include "ob_tablet_id.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
} // namespace schema
} // namespace share
namespace blocksstable
{
class ObDatumRow;
class ObBatchDatumRows;
} // namespace blocksstable
namespace storage
{
class ObDirectLoadDatumRow;
} // namespace storage
namespace observer
{
class ObTableLoadRowProjector
{
public:
  ObTableLoadRowProjector();
  virtual ~ObTableLoadRowProjector();
  int init(const share::schema::ObTableSchema *src_table_schema,
           const share::schema::ObTableSchema *dest_table_schema);
  virtual int projector(const ObTabletID &src_tablet_id,
                        const storage::ObDirectLoadDatumRow &src_datum_row,
                        ObTabletID &dest_tablet_id,
                        storage::ObDirectLoadDatumRow &dest_datum_row) const = 0;
  virtual int projector(const ObTabletID &src_tablet_id,
                        const blocksstable::ObDatumRow &src_datum_row,
                        ObTabletID &dest_tablet_id,
                        storage::ObDirectLoadDatumRow &dest_datum_row) const = 0;
  virtual int projector(const blocksstable::ObBatchDatumRows &src_datum_rows,
                        const int64_t row_idx,
                        storage::ObDirectLoadDatumRow &dest_datum_row) const = 0;
  int get_dest_tablet_id_and_part_id_by_src_tablet_id(const ObTabletID &src_tablet_id,
                                                      ObTabletID &dest_tablet_id,
                                                      ObObjectID &part_id) const;

  int get_dest_tablet_id(const ObTabletID &src_tablet_id, ObTabletID &dest_tablet_id);
  int64_t get_src_column_num() const { return src_column_num_; }
  int64_t get_dest_column_num() const { return dest_column_num_; }

protected:
  int build_projector(const share::schema::ObTableSchema *src_table_schema,
                      const share::schema::ObTableSchema *dest_table_schema);
  int build_tablet_projector(const share::schema::ObTableSchema *src_table_schema,
                             const share::schema::ObTableSchema *dest_table_schema);
  virtual int build_row_projector(const share::schema::ObTableSchema *src_table_schema,
                                  const share::schema::ObTableSchema *dest_table_schema) = 0;

  int project_row(const storage::ObDirectLoadDatumRow &src_datum_row,
                  storage::ObDirectLoadDatumRow &dest_datum_row) const;
  int project_row(const blocksstable::ObDatumRow &src_datum_row,
                  const int64_t src_rowkey_column_num,
                  storage::ObDirectLoadDatumRow &dest_datum_row) const;
  int project_row(const blocksstable::ObBatchDatumRows &src_datum_rows,
                  const int64_t row_idx,
                  const int64_t src_rowkey_column_num,
                  storage::ObDirectLoadDatumRow &dest_datum_row) const;

protected:
  common::ObArray<int64_t> col_projector_;
  common::hash::ObHashMap<ObTabletID, ObTabletID, common::hash::NoPthreadDefendMode>
    tablet_projector_;
  common::hash::ObHashMap<ObTabletID, ObObjectID, common::hash::NoPthreadDefendMode>
    dest_tablet_id_to_part_id_map_;
  int64_t src_column_num_;
  int64_t dest_column_num_;
  bool is_inited_;
};

class ObTableLoadMainToIndexProjector : public ObTableLoadRowProjector
{
public:
  ObTableLoadMainToIndexProjector();
  virtual ~ObTableLoadMainToIndexProjector();
  int projector(const ObTabletID &src_tablet_id,
                const storage::ObDirectLoadDatumRow &src_datum_row,
                ObTabletID &dest_tablet_id,
                storage::ObDirectLoadDatumRow &dest_datum_row) const override;
  int projector(const ObTabletID &src_tablet_id,
                const blocksstable::ObDatumRow &src_datum_row,
                ObTabletID &dest_tablet_id,
                storage::ObDirectLoadDatumRow &dest_datum_row) const override;
  int projector(const blocksstable::ObBatchDatumRows &src_datum_rows,
                const int64_t row_idx,
                storage::ObDirectLoadDatumRow &dest_datum_row) const override;
private:
  int build_row_projector(const share::schema::ObTableSchema *src_table_schema,
                          const share::schema::ObTableSchema *dest_table_schema) override;
private:
  int64_t src_rowkey_column_num_;
};

class ObTableLoadMainToUniqueIndexProjector : public ObTableLoadRowProjector
{
public:
  ObTableLoadMainToUniqueIndexProjector();
  virtual ~ObTableLoadMainToUniqueIndexProjector();
  int projector(const ObTabletID &src_tablet_id,
                const storage::ObDirectLoadDatumRow &src_datum_row,
                ObTabletID &dest_tablet_id,
                storage::ObDirectLoadDatumRow &dest_datum_row) const override;
  int projector(const ObTabletID &src_tablet_id,
                const blocksstable::ObDatumRow &src_datum_row,
                ObTabletID &dest_tablet_id,
                storage::ObDirectLoadDatumRow &dest_datum_row) const override;
  int projector(const blocksstable::ObBatchDatumRows &src_datum_rows,
                const int64_t row_idx,
                storage::ObDirectLoadDatumRow &dest_datum_row) const override;
private:
  int build_row_projector(const share::schema::ObTableSchema *src_table_schema,
                          const share::schema::ObTableSchema *dest_table_schema) override;
  void shadow_columns(storage::ObDirectLoadDatumRow &datum_row) const;
private:
  int64_t src_rowkey_column_num_;
  int64_t dest_rowkey_cnt_;
  int64_t dest_spk_cnt_;
  int64_t dest_index_rowkey_cnt_;
};

class ObTableLoadUniqueIndexToMainRowkeyProjector : public ObTableLoadRowProjector
{
public:
  ObTableLoadUniqueIndexToMainRowkeyProjector() = default;
  virtual ~ObTableLoadUniqueIndexToMainRowkeyProjector() = default;
  int projector(const ObTabletID &src_tablet_id,
                const storage::ObDirectLoadDatumRow &src_datum_row,
                ObTabletID &dest_tablet_id,
                storage::ObDirectLoadDatumRow &dest_datum_row) const override;
  int projector(const ObTabletID &src_tablet_id,
                const blocksstable::ObDatumRow &src_datum_row,
                ObTabletID &dest_tablet_id,
                storage::ObDirectLoadDatumRow &dest_datum_row) const override
  {
    return OB_ERR_UNEXPECTED;
  }
  int projector(const blocksstable::ObBatchDatumRows &src_datum_rows,
                const int64_t row_idx,
                storage::ObDirectLoadDatumRow &dest_datum_row) const override
  {
    return OB_ERR_UNEXPECTED;
  }
private:
  int build_row_projector(const share::schema::ObTableSchema *src_table_schema,
                          const share::schema::ObTableSchema *dest_table_schema) override;
};

} // namespace observer
} // namespace oceanbase
