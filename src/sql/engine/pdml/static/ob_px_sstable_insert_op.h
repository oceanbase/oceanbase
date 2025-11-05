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

#ifndef OCEANBASE_SQL_PX_OB_PX_SSTABLE_INSERT_OP_H
#define OCEANBASE_SQL_PX_OB_PX_SSTABLE_INSERT_OP_H

#include "sql/engine/pdml/static/ob_px_multi_part_insert_op.h"
#include "share/ob_tablet_autoincrement_param.h"

namespace oceanbase
{
namespace storage
{
class ObSSTableInsertRowIterator;
typedef std::pair<share::ObLSID, common::ObTabletID> LSTabletIDPair;
struct ObInsertMonitor;
struct ObTabletSliceParam;
}

namespace sql
{
class ObPxMultiPartSSTableInsertOpInput : public ObPxMultiPartModifyOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxMultiPartSSTableInsertOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObPxMultiPartModifyOpInput(ctx, spec)
  {}
  int init(ObTaskInfo &task_info) override
  {
    return ObPxMultiPartModifyOpInput::init(task_info);
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartSSTableInsertOpInput);
};

class ObPxMultiPartSSTableInsertSpec : public ObPxMultiPartInsertSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxMultiPartSSTableInsertSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObPxMultiPartInsertSpec(alloc, type), flashback_query_expr_(nullptr),
      regenerate_heap_table_pk_(false)
  {}
  int get_snapshot_version(ObEvalCtx &eval_ctx, int64_t &snapshot_version) const;
public:
  ObExpr *flashback_query_expr_;
  bool regenerate_heap_table_pk_;
  int64_t ddl_slice_id_idx_; // record idx of exprs for ddl slice id
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartSSTableInsertSpec);
};

class ObPxMultiPartSSTableInsertOp : public ObPxMultiPartInsertOp
{
public:
  ObPxMultiPartSSTableInsertOp(ObExecContext &exec_ctx,
                               const ObOpSpec &spec,
                               ObOpInput *input)
    : ObPxMultiPartInsertOp(exec_ctx, spec, input),
      allocator_("SSTABLE_INS"),
      participants_(),
      tablet_store_map_(),
      tablet_seq_caches_(),
      curr_tablet_store_iter_(),
      curr_tablet_idx_(-1),
      count_rows_finish_(false),
      is_all_partition_finished_(false),
      curr_part_idx_(0),
      snapshot_version_(0),
      is_partitioned_table_(false),
      table_all_slice_count_(0),
      autoinc_range_interval_(0),
      is_vec_data_complement_(false),
      is_vec_gen_vid_(false)
  {}
  virtual ~ObPxMultiPartSSTableInsertOp() { destroy(); }
  const ObPxMultiPartSSTableInsertSpec &get_spec() const;
  virtual int inner_open() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override;
  int get_next_row_with_cache();
  int get_tablet_info_from_row(
      const ObExprPtrIArray &row,
      common::ObTabletID &tablet_id,
      storage::ObTabletSliceParam *tablet_slice_param = nullptr);
private:
  struct ObLSTabletIDPairCmp final
  {
    public:
      ObLSTabletIDPairCmp() { }
      OB_INLINE bool operator() (const LSTabletIDPair &left, const LSTabletIDPair &right)
      {
        if (left.second == right.second) {
          return left.first < right.first;
        } else {
          return left.second < right.second;
        }
      }
  };
private:
  int get_all_rows_and_count();
  int create_tablet_store(common::ObTabletID &tablet_id, ObChunkDatumStore *&tablet_store);
  int build_table_slice_info();
  int update_sqc_global_autoinc_value();
  bool need_count_rows() const { return (MY_SPEC.regenerate_heap_table_pk_ || is_vec_gen_vid_) && !count_rows_finish_; }
  int get_next_tablet_id(common::ObTabletID &tablet_id);
  int setup_vector_index_monitor(const share::schema::ObTableSchema *table_schema, 
                                 storage::ObInsertMonitor &insert_monitor);
private:
  friend class storage::ObSSTableInsertRowIterator;
  static const uint64_t MAP_HASH_BUCKET_NUM = 1543L;
  static const uint64_t TABLET_STORE_MEM_LIMIT = 16 * 1024; // 16 KB
  typedef common::hash::ObHashMap<common::ObTabletID, ObChunkDatumStore*, common::hash::NoPthreadDefendMode> TabletStoreMap;
  common::ObArenaAllocator allocator_;
  common::ObArray<LSTabletIDPair> participants_;
  TabletStoreMap tablet_store_map_;
  ObArray<share::ObTabletCacheInterval> tablet_seq_caches_;
  ObChunkDatumStore::Iterator curr_tablet_store_iter_;
  int64_t curr_tablet_idx_;
  bool count_rows_finish_;
  bool is_all_partition_finished_;
  int64_t curr_part_idx_;
  int64_t snapshot_version_; // ddl snapshot version.
  bool is_partitioned_table_;
  int64_t table_all_slice_count_;
  // record the count of slices before the current tablet
  common::hash::ObHashMap<int64_t, int64_t> tablet_pre_slice_count_map_;
  int64_t autoinc_range_interval_;
  // vector index
  bool is_vec_data_complement_;
  bool is_vec_gen_vid_;
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartSSTableInsertOp);
};

}// end namespace sql
}// end namespace oceanbase


#endif//OCEANBASE_SQL_PX_OB_PX_SSTABLE_INSERT_OP_H
