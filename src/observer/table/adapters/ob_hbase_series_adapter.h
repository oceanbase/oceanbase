/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_HBASE_SERIES_ADAPTER_H
#define _OB_HBASE_SERIES_ADAPTER_H

#include "ob_i_adapter.h"
#include "ob_series_adapter_iter.h"

namespace oceanbase
{
namespace table
{

struct QVUpdateOp
{
  ObString value;
  QVUpdateOp(ObString value) : value(value) {}
  void operator()(hash::HashMapPair<ObString, ObString> &entry) { entry.second = value; }
};

struct KTNode {
  KTNode(const ObString &key, int64_t time, ObTabletID tablet_id) : key_(key), time_(time), tablet_id_(tablet_id) {}
  KTNode() : key_(), time_(0), tablet_id_() {}
  // uint64_t hash() const;
  int hash(uint64_t &res) const;
  bool operator==(const KTNode &other) const
  {
    return (other.key_.case_compare(key_) == 0) && (other.time_ == time_);
  }

  TO_STRING_KV(K(key_), K(time_), K(tablet_id_));

  ObString key_;
  int64_t time_;
  ObTabletID tablet_id_;
};


typedef common::hash::ObHashMap<ObString, ObString, common::hash::NoPthreadDefendMode> QVMap;
typedef common::hash::ObHashMap<KTNode, QVMap *, common::hash::NoPthreadDefendMode> KTAggMap;

class ObHSeriesAdapter : public ObIHbaseAdapter
{
public:
  ObHSeriesAdapter(): table_query_(nullptr) {}
  virtual ~ObHSeriesAdapter() {
    release_map();
  }

  virtual int multi_put(ObTableExecCtx &ctx, const ObIArray<const ObITableEntity *> &cells) override;
  virtual int put(ObTableExecCtx &ctx, const ObITableEntity &cell) override;
  virtual int put(ObTableCtx &ctx, const ObHCfRows &rows) override;
  virtual int del(ObTableExecCtx &ctx, const ObITableEntity &cell) override;
  virtual int scan(ObIAllocator &alloc, ObTableExecCtx &ctx, const ObTableQuery &query, ObHbaseICellIter *&iter) override;
  int convert_normal_to_series(const ObITableEntity &cell, ObITableEntity &series_cell);
  int convert_normal_to_series(const ObIArray<const ObITableEntity *> &cells,
                               ObIArray<const ObITableEntity *> &series_cells,
                               ObIArray<common::ObTabletID> &real_tablet_ids);
private:
  int save_and_adjust_range(ObHbaseSeriesCellIter *&iter, ObIAllocator &alloc);
  int construct_series_value(ObIAllocator &allocator, ObJsonNode &json, ObObj &value_obj);

  int construct_query(ObTableExecCtx &ctx, const ObITableEntity &entity, ObTableQuery &table_query);
  int del_and_insert(common::ObIAllocator &alloc,
                     ObJsonNode &json_node,
                     ObTableCtx &del_ctx,
                     ObTableCtx &ins_ctx,
                     ObNewRow &json_cell);
  int get_query_iter(ObTableExecCtx &ctx,
                     const ObITableEntity &entity,
                     ObTableCtx &scan_ctx,
                     ObTableApiRowIterator &tb_row_iter);
  int get_normal_rowkey(const ObITableEntity &normal_cell,
                        ObObj &rowkey_obj,
                        ObObj &qualifier_obj,
                        ObObj &timestamp_obj);
  int add_series_rowkey(ObITableEntity &series_cell,
                        const ObObj &rowkey_obj,
                        const ObObj &timestamp_obj,
                        const ObObj &seq_obj);
  int release_map();

private:
  KTAggMap kt_agg_map_;
  ObTableQuery *table_query_;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHSeriesAdapter);
};

} // end of namespace table
} // end of namespace oceanbase

#endif
