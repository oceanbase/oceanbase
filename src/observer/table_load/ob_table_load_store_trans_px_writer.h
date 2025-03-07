/**
 * Copyright (c) 2024 OceanBase
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

#include "common/ob_tablet_id.h"
#include "lib/hash/ob_hashset.h"
#include "storage/direct_load/ob_direct_load_batch_row_buffer.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_row_iterator.h"
#include "observer/table_load/ob_table_load_pre_sort_writer.h"
#include "src/share/table/ob_table_load_row_array.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadStoreCtx;
class ObTableLoadStoreTrans;
class ObTableLoadTransStoreWriter;
class ObTableLoadPreSortWriter;

class ObTableLoadStoreTransPXWriter
{
public:
  ObTableLoadStoreTransPXWriter();
  ~ObTableLoadStoreTransPXWriter();

  void reset();
  int init(ObTableLoadStoreCtx *store_ctx,
           ObTableLoadStoreTrans *trans,
           ObTableLoadTransStoreWriter *writer);
  int prepare_write(const common::ObIArray<uint64_t> &column_ids,
                    const bool is_vectorized,
                    const bool use_rich_format);
  // vectorized 2.0
  int write_vector(common::ObIVector *tablet_id_vector,
                   const ObIArray<common::ObIVector *> &vectors,
                   const sql::ObBatchRows &batch_rows,
                   int64_t &affected_rows);
  // vectorized 1.0
  int write_batch(const common::ObDatumVector &tablet_id_datum_vector,
                  const common::ObIArray<common::ObDatumVector> &datum_vectors,
                  const sql::ObBatchRows &batch_rows,
                  int64_t &affected_rows);
  // non-vectorized
  int write_row(const common::ObTabletID &tablet_id,
                const blocksstable::ObDatumRow &row);
  int close();
  bool is_inited() const { return is_inited_; }

  TO_STRING_KV(KP_(store_ctx),
               KP_(trans),
               KP_(writer),
               K_(is_table_without_pk),
               K_(column_count),
               K_(store_column_count),
               KP_(non_partitioned_tablet_id_vector),
               KP_(pre_sort_writer),
               KP_(batch_ctx),
               K_(row_count),
               K_(last_check_status_cycle),
               K_(is_vectorized),
               K_(use_rich_format),
               K_(can_write),
               K_(is_inited));

private:
  const static int64_t CHECK_STATUS_CYCLE = 10000;
  int check_columns(const common::ObIArray<uint64_t> &column_ids);
  int init_batch_ctx(const bool is_vectorized, const bool use_rich_format);
  int init_tablet_id_set();
  int check_tablet(const ObTabletID &tablet_id);
  int check_tablet(common::ObIVector *vector, const sql::ObBatchRows &batch_rows);
  int check_tablet(const common::ObDatumVector &datum_vector, const sql::ObBatchRows &batch_rows);
  int check_status();
  int flush_buffer();
  int flush_batch(common::ObIVector *tablet_id_vector,
                  const ObIArray<common::ObIVector *> &vectors,
                  const sql::ObBatchRows &batch_rows,
                  int64_t &affected_rows);

private:
  typedef common::hash::ObHashSet<ObTabletID, common::hash::NoPthreadDefendMode> TabletIDSet;

  // 旁路导入在以下场景, 可能会对数据进行修改, 导致vector中数据指针地址被修改
  //  * lob列
  //  * ObDASUtils::reshape_vector_value
  // 后续SQL对数据进行投影时会访问非法内存
  // 因此, 我们需要对vector做一次浅拷贝, 来保证SQL的vector中数据指针地址不发生变化
  //
  // 目前测试现象:
  //  * SQL会重置数据指针地址 (没有得到所有场景都会重置的保证)
  //  * px吐出来的数据是可以直接写存储的格式, 也就是说已经做过了ObDASUtils::reshape_vector_value里的处理
  //    这个应该是SQL层保证的, 这样就可以优化掉DDL路径中的reshape操作
  //
  // 采取保守方案, 保留浅拷贝和reshape
  struct BatchCtx
  {
  public:
    BatchCtx()
      : max_batch_size_(0),
        tablet_id_batch_vector_(nullptr),
        tablet_id_const_vector_(nullptr),
        col_fixed_(nullptr),
        col_lob_storage_(nullptr)
    {
    }
    ~BatchCtx() {}
  public:
    int64_t max_batch_size_;
    // non-vectorized : VEC_FIXED
    // vectorized 1.0 : VEC_UNIFORM
    // vectorized 2.0 : nullptr
    ObIVector *tablet_id_batch_vector_;
    // non-vectorized : nullptr
    // vectorized 1.0 : VEC_UNIFORM_CONST
    // vectorized 2.0 : nullptr
    ObIVector *tablet_id_const_vector_;
    // for non-vectorized
    storage::ObDirectLoadRowFlag row_flag_;
    storage::ObDirectLoadDatumRow datum_row_;
    storage::ObDirectLoadBatchRowBuffer batch_buffer_;
    // for vectorized
    // shallow copy unfixed cols
    sql::ObBitVector *col_fixed_;
    // expand const vector for lob storage cols
    sql::ObBitVector *col_lob_storage_;
    // vectorized 1.0 : VEC_UNIFORM
    // vectorized 2.0 : nullptr(fixed), VEC_DISCRETE(unfixed)
    ObArray<ObIVector *> batch_vectors_;
    // vectorized 1.0 : VEC_UNIFORM_CONST
    // vectorized 2.0 : nullptr(fixed), VEC_UNIFORM_CONST(unfixed)
    ObArray<ObIVector *> const_vectors_;
    ObArray<ObIVector *> append_vectors_;
    // for write
    ObArray<ObIVector *> heap_vectors_;
    sql::ObBatchRows brs_;
  };

private:
  ObArenaAllocator allocator_;
  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadStoreTrans *trans_;
  ObTableLoadTransStoreWriter *writer_;
  bool is_table_without_pk_;
  // number of columns received from px
  int64_t column_count_;
  // number of columns write to direct load
  int64_t store_column_count_;
  ObIVector *non_partitioned_tablet_id_vector_;
  TabletIDSet tablet_id_set_;
  ObTableLoadPreSortWriter *pre_sort_writer_;
  BatchCtx *batch_ctx_;
  int64_t row_count_;
  int64_t last_check_status_cycle_;
  bool is_vectorized_;
  bool use_rich_format_;
  bool can_write_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
