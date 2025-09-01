/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SKIP_INDEX_FILTER_EXECUTOR_H
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SKIP_INDEX_FILTER_EXECUTOR_H

#include "share/schema/ob_table_param.h"
#include "sql/engine/ob_bit_vector.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "storage/blocksstable/index_block/ob_agg_row_struct.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObMinMaxFilterParam {
  ObMinMaxFilterParam() : null_count_(), min_datum_(),
                          max_datum_(), is_min_prefix_(false),
                          is_max_prefix_(false) {}
  ObMinMaxFilterParam(blocksstable::ObStorageDatum &null_count,
                   blocksstable::ObStorageDatum &min_datum,
                   blocksstable::ObStorageDatum &max_datum,
                   bool is_min_prefix,
                   bool is_max_prefix) : null_count_(null_count), min_datum_(min_datum),
                                         max_datum_(max_datum), is_min_prefix_(is_min_prefix),
                                         is_max_prefix_(is_max_prefix) {}
  ObMinMaxFilterParam(blocksstable::ObStorageDatum &null_count,
                   blocksstable::ObStorageDatum &min_datum,
                   blocksstable::ObStorageDatum &max_datum)
                   : null_count_(null_count), min_datum_(min_datum),
                     max_datum_(max_datum), is_min_prefix_(false),
                     is_max_prefix_(false) {}

  OB_INLINE void set_uncertain()
  {
    // reset datum ptr to local buffer and set datum as null (uncertain)
    null_count_.reuse();
    min_datum_.reuse();
    max_datum_.reuse();
    null_count_.set_null();
    min_datum_.set_null();
    max_datum_.set_null();
  }
  OB_INLINE bool is_uncertain() const
  {
    const bool no_agg_data = null_count_.is_null() && min_datum_.is_null() && max_datum_.is_null();
    const bool has_prefix = is_min_prefix_ || is_max_prefix_; // enable filter with min/max prefix in subsequent versions
    return no_agg_data || has_prefix;
  }
  blocksstable::ObStorageDatum null_count_;
  blocksstable::ObStorageDatum min_datum_;
  blocksstable::ObStorageDatum max_datum_;
  bool is_min_prefix_;
  bool is_max_prefix_;
};
class ObAggRowReader;
class ObSkipIndexFilterExecutor final
{
public:
  ObSkipIndexFilterExecutor()
      : agg_row_reader_(), meta_(), skip_bit_(nullptr), allocator_(nullptr), is_inited_(false) {}
  ~ObSkipIndexFilterExecutor() { reset(); }
  void reset()
  {
    agg_row_reader_.reset();
    if (OB_NOT_NULL(allocator_)) {
      if (OB_NOT_NULL(skip_bit_)) {
        allocator_->free(skip_bit_);
        skip_bit_ = nullptr;
      }
    }
    allocator_ = nullptr;
    is_inited_ = false;
  }
  int init(const int64_t batch_size, common::ObIAllocator *allocator);
  int falsifiable_pushdown_filter(const uint32_t col_idx,
                                  const ObObjMeta &obj_meta,
                                  const ObSkipIndexType index_type,
                                  const ObMicroIndexInfo &index_info,
                                  sql::ObPhysicalFilterExecutor &filter,
                                  common::ObIAllocator &allocator,
                                  const bool use_vectorize);
  int falsifiable_pushdown_filter(const uint32_t col_idx,
                                  const ObSkipIndexType index_type,
                                  const int64_t row_count,
                                  ObMinMaxFilterParam &param,
                                  sql::ObPhysicalFilterExecutor &filter,
                                  const bool use_vectorize);

private:
  int filter_on_min_max(const uint32_t col_idx,
                        const uint64_t row_count,
                        const ObMinMaxFilterParam &param,
                        sql::ObWhiteFilterExecutor &filter);

  int read_aggregate_data(const uint32_t col_idx,
                   common::ObIAllocator &allocator,
                   const share::schema::ObColumnParam *col_param,
                   const ObObjMeta &obj_meta,
                   const bool is_padding_mode,
                   ObMinMaxFilterParam &param);
  int pad_column(const ObObjMeta &obj_meta,
                 const share::schema::ObColumnParam *col_param,
                 const bool is_padding_mode,
                 common::ObIAllocator &padding_alloc,
                 blocksstable::ObStorageDatum &datum);

  // *_operator args are the same
  int eq_operator(const sql::ObWhiteFilterExecutor &filter,
                  const common::ObDatum &min_datum,
                  const bool &is_min_prefix,
                  const common::ObDatum &max_datum,
                  const bool &is_max_prefix,
                  sql::ObBoolMask &fal_desc);

  int ne_operator(const sql::ObWhiteFilterExecutor &filter,
                  const common::ObDatum &min_datum,
                  const bool &is_min_prefix,
                  const common::ObDatum &max_datum,
                  const bool &is_max_prefix,
                  sql::ObBoolMask &fal_desc);
  int gt_operator(const sql::ObWhiteFilterExecutor &filter,
                  const common::ObDatum &min_datum,
                  const bool &is_min_prefix,
                  const common::ObDatum &max_datum,
                  const bool &is_max_prefix,
                  sql::ObBoolMask &fal_desc);

  int ge_operator(const sql::ObWhiteFilterExecutor &filter,
                  const common::ObDatum &min_datum,
                  const bool &is_min_prefix,
                  const common::ObDatum &max_datum,
                  const bool &is_max_prefix,
                  sql::ObBoolMask &fal_desc);

  int lt_operator(const sql::ObWhiteFilterExecutor &filter,
                  const common::ObDatum &min_datum,
                  const bool &is_min_prefix,
                  const common::ObDatum &max_datum,
                  const bool &is_max_prefix,
                  sql::ObBoolMask &fal_desc);

  int le_operator(const sql::ObWhiteFilterExecutor &filter,
                  const common::ObDatum &min_datum,
                  const bool &is_min_prefix,
                  const common::ObDatum &max_datum,
                  const bool &is_max_prefix,
                  sql::ObBoolMask &fal_desc);

  int bt_operator(const sql::ObWhiteFilterExecutor &filter,
                  const common::ObDatum &min_datum,
                  const bool &is_min_prefix,
                  const common::ObDatum &max_datum,
                  const bool &is_max_prefix,
                  sql::ObBoolMask &fal_desc);

  int in_operator(const sql::ObWhiteFilterExecutor &filter,
                  const common::ObDatum &min_datum,
                  const bool &is_min_prefix,
                  const common::ObDatum &max_datum,
                  const bool &is_max_prefix,
                  sql::ObBoolMask &fal_desc);

  int black_filter_on_min_max(const uint32_t col_idx,
                              const uint64_t row_count,
                              ObMinMaxFilterParam &param,
                              sql::ObBlackFilterExecutor &filter,
                              const bool use_vectorize);
private:
  ObAggRowReader agg_row_reader_;
  ObSkipIndexColMeta meta_;
  sql::ObBitVector *skip_bit_;      // to be compatible with the black filter filter() method
  common::ObIAllocator *allocator_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSkipIndexFilterExecutor);
};

} // end namespace blocksstable
} // end namespace oceanbase
#endif
