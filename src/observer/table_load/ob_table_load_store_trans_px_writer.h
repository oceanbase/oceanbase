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
#include "observer/table_load/ob_table_load_pre_sort_writer.h"
#include "observer/table_load/ob_table_load_px_batch_rows.h"
#include "src/share/table/ob_table_load_row_array.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_row_iterator.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadVector;
} // namespace storage
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
  int init(ObTableLoadStoreCtx *store_ctx, ObTableLoadStoreTrans *trans,
           ObTableLoadTransStoreWriter *writer);
  int prepare_write(const common::ObIArray<uint64_t> &column_ids,
                    const bool is_vectorized,
                    const bool use_rich_format);
  // vectorized 2.0
  int write_vector(common::ObIVector *tablet_id_vector,
                   const ObIArray<common::ObIVector *> &vectors,
                   const sql::ObBatchRows &brs,
                   int64_t &affected_rows);
  // vectorized 1.0
  int write_batch(const common::ObDatumVector &tablet_id_datum_vector,
                  const common::ObIArray<common::ObDatumVector> &datum_vectors,
                  const sql::ObBatchRows &brs,
                  int64_t &affected_rows);
  // non-vectorized
  int write_row(const common::ObTabletID &tablet_id,
                const blocksstable::ObDatumRow &row);
  int close();
  bool is_inited() const { return is_inited_; }

  TO_STRING_KV(KP_(store_ctx),
               KP_(trans),
               KP_(writer),
               K_(column_count),
               K_(single_tablet_id),
               KP_(single_tablet_id_vector),
               KP_(pre_sort_writer),
               KP_(batch_ctx),
               K_(row_count),
               K_(last_check_status_cycle),
               K_(is_single_part),
               K_(is_vectorized),
               K_(use_rich_format),
               K_(can_write),
               K_(is_inited));

private:
  const static int64_t CHECK_STATUS_CYCLE = 10000;
  static inline void make_selector(const sql::ObBatchRows &brs, uint16_t *selector, int64_t &size);
  int check_columns(const common::ObIArray<uint64_t> &column_ids);
  int check_tablets(ObDirectLoadVector *tablet_id_vector, const int64_t size);
  int init_batch_ctx(const bool is_vectorized, const bool use_rich_format);
  int check_status();
  int flush_buffer_if_need();
  int flush_buffer();

private:
  static const int64_t DEFAULT_MAX_BYTES_SIZE = 2LL << 20; // 2M

  struct BatchCtx
  {
  public:
    BatchCtx()
      : max_batch_size_(0),
        max_bytes_size_(0),
        tablet_id_vector_(nullptr),
        selector_(nullptr),
        tablet_offsets_(nullptr)
    {
    }
    ~BatchCtx()
    {
      if (nullptr != tablet_id_vector_) {
        tablet_id_vector_->~ObDirectLoadVector();
        tablet_id_vector_ = nullptr;
      }
    }

  public:
    int64_t max_batch_size_;
    int64_t max_bytes_size_;
    ObDirectLoadVector *tablet_id_vector_;
    storage::ObDirectLoadRowFlag row_flag_;
    storage::ObDirectLoadDatumRow datum_row_;
    ObTableLoadPXBatchRows batch_rows_;
    uint16_t *selector_;
    uint16_t *tablet_offsets_;
  };

private:
  ObArenaAllocator allocator_;
  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadStoreTrans *trans_;
  ObTableLoadTransStoreWriter *writer_;
  // number of columns received from px
  int64_t column_count_;
  ObTabletID single_tablet_id_;
  ObIVector *single_tablet_id_vector_;
  ObTableLoadPreSortWriter *pre_sort_writer_;
  BatchCtx *batch_ctx_;
  int64_t row_count_;
  int64_t last_check_status_cycle_;
  bool is_single_part_;
  bool is_vectorized_;
  bool use_rich_format_;
  bool can_write_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
