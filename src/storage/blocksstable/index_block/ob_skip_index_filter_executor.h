/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SKIP_INDEX_FILTER_EXECUTOR_H
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SKIP_INDEX_FILTER_EXECUTOR_H

#include "share/schema/ob_table_param.h"
#include "sql/engine/ob_bit_vector.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
//#include "sql/engine/table/ob_external_table_pushdown_filter.h"
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
    is_min_prefix_ = false;
    is_max_prefix_ = false;
  }
  OB_INLINE bool is_uncertain() const
  {
    return null_count_.is_null() && min_datum_.is_null() && max_datum_.is_null();
  }
  TO_STRING_KV(K_(null_count), K_(min_datum), K_(max_datum), K_(is_min_prefix), K_(is_max_prefix));
  blocksstable::ObStorageDatum null_count_;
  blocksstable::ObStorageDatum min_datum_;
  blocksstable::ObStorageDatum max_datum_;
  bool is_min_prefix_;
  bool is_max_prefix_;
};

struct ObSkipIndexCmpRes {
public:
  ObSkipIndexCmpRes() : cmp_res_(0), is_certain_(false) {}
  void reset() { is_certain_ = false; cmp_res_ = 0; }
  OB_INLINE void set_certain() { is_certain_ = true; }
  OB_INLINE void set_uncertain() { is_certain_ = false; }
  OB_INLINE bool is_gt() const { return is_certain_ && cmp_res_ > 0; }
  OB_INLINE bool is_lt() const { return is_certain_ && cmp_res_ < 0; }
  OB_INLINE bool is_le() const { return is_certain_ && cmp_res_ <= 0; }
  OB_INLINE bool is_ge() const { return is_certain_ && cmp_res_ >= 0; }
  OB_INLINE bool is_eq() const { return is_certain_ && cmp_res_ == 0; }
  TO_STRING_KV(K_(is_certain), K_(cmp_res));
  int cmp_res_;
  bool is_certain_;
};

/**
 * @brief This struct is used for mock min and max value for ora_rowscn column.
 *        And supply extra param used for skip index filter.
 */
struct ObSkipIndexExtraParam
{
  ObSkipIndexExtraParam() : min_scn_(-1), max_scn_(-1), row_count_(-1) {}

  ObSkipIndexExtraParam(const int64_t min_scn, const int64_t max_scn, const int64_t row_count)
      : min_scn_(min_scn), max_scn_(max_scn), row_count_(row_count)
  {
  }

  OB_INLINE uint64_t get_mock_row_count() const { return row_count_ < 0 ? UINT64_MAX : static_cast<uint64_t>(row_count_); }

  TO_STRING_KV(K_(min_scn), K_(max_scn), K_(row_count));

  int64_t min_scn_;   // negative value means not set
  int64_t max_scn_;   // negative value means not set
  int64_t row_count_; // negative value means not set
};

class ObAggRowReader;
class ObSkipIndexFilterExecutor final
{
public:
  ObSkipIndexFilterExecutor()
      : agg_row_reader_(), skip_bit_(nullptr), allocator_(nullptr), is_inited_(false) {}
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
                                  sql::ObPushdownFilterExecutor &filter,
                                  common::ObIAllocator &allocator,
                                  const bool use_vectorize,
                                  const ObITableReadInfo *read_info = nullptr);
  int falsifiable_pushdown_filter(const uint32_t col_idx,
                                  const ObCollationType &cs_type,
                                  const ObSkipIndexType index_type,
                                  const int64_t row_count,
                                  ObMinMaxFilterParam &param,
                                  sql::ObPushdownFilterExecutor &filter,
                                  const bool use_vectorize);

  static int apply_filter_on_min_max(const ObMinMaxFilterParam &param,
                                     const sql::ObWhiteFilterExecutor &filter,
                                     sql::ObBoolMask &fal_desc,
                                     const uint64_t row_count = UINT64_MAX);

  template <typename AggRowReader>
  static int read_aggregate_data(AggRowReader &agg_row_reader,
                                 const uint32_t col_idx,
                                 const ObObjMeta &obj_meta,
                                 const bool is_padding_mode,
                                 ObMinMaxFilterParam &param,
                                 const ObColumnParam *col_param = nullptr,
                                 ObIAllocator *allocator = nullptr,
                                 const bool is_scn_column = false,
                                 const ObSkipIndexExtraParam &extra_param = ObSkipIndexExtraParam());

private:
  static void fix_and_mock_trans_version_datum(ObMinMaxFilterParam &param,
                                               const ObSkipIndexExtraParam &extra_param);

  int filter_on_min_max(const uint64_t row_count,
                        const ObMinMaxFilterParam &param,
                        sql::ObWhiteFilterExecutor &filter);

  int read_aggregate_data(const uint32_t col_idx,
                          common::ObIAllocator &allocator,
                          const share::schema::ObColumnParam *col_param,
                          const ObObjMeta &obj_meta,
                          const bool is_padding_mode,
                          ObMinMaxFilterParam &param,
                          const ObMicroIndexInfo &index_info,
                          const ObITableReadInfo *read_info = nullptr);

  static int compare_with_prefix(const sql::ObWhiteFilterExecutor &filter,
                                 const common::ObDatum &datum,
                                 const common::ObDatum &filter_datum,
                                 ObSkipIndexCmpRes &res);

  static int compare_for_pad_charset(const sql::ObWhiteFilterExecutor &filter,
                                     const common::ObDatum &skip_datum,
                                     const bool is_prefix,
                                     const common::ObDatum &filter_datum,
                                     ObSkipIndexCmpRes &res);

  static int compare_for_non_pad_charset(const sql::ObWhiteFilterExecutor &filter,
                                         const common::ObDatum &skip_datum,
                                         const bool is_prefix,
                                         const common::ObDatum &filter_datum,
                                         ObSkipIndexCmpRes &res);

  static int compare(const sql::ObWhiteFilterExecutor &filter,
                     const common::ObDatum &skip_datum,
                     const bool is_prefix,
                     const bool compare_min_value,
                     const common::ObDatum &filter_datum,
                     ObSkipIndexCmpRes &res);

  static int pad_column(const ObObjMeta &obj_meta,
                        const ObColumnParam *col_param,
                        const bool is_padding_mode,
                        ObIAllocator *padding_alloc,
                        ObStorageDatum &datum);

  static int eq_operator(const sql::ObWhiteFilterExecutor &filter,
                         const common::ObDatum &min_datum,
                         const bool &is_min_prefix,
                         const common::ObDatum &max_datum,
                         const bool &is_max_prefix,
                         sql::ObBoolMask &fal_desc);

  static int ne_operator(const sql::ObWhiteFilterExecutor &filter,
                         const common::ObDatum &min_datum,
                         const bool &is_min_prefix,
                         const common::ObDatum &max_datum,
                         const bool &is_max_prefix,
                         sql::ObBoolMask &fal_desc);

  static int gt_operator(const sql::ObWhiteFilterExecutor &filter,
                         const common::ObDatum &min_datum,
                         const bool &is_min_prefix,
                         const common::ObDatum &max_datum,
                         const bool &is_max_prefix,
                         sql::ObBoolMask &fal_desc);

  static int ge_operator(const sql::ObWhiteFilterExecutor &filter,
                         const common::ObDatum &min_datum,
                         const bool &is_min_prefix,
                         const common::ObDatum &max_datum,
                         const bool &is_max_prefix,
                         sql::ObBoolMask &fal_desc);

  static int lt_operator(const sql::ObWhiteFilterExecutor &filter,
                         const common::ObDatum &min_datum,
                         const bool &is_min_prefix,
                         const common::ObDatum &max_datum,
                         const bool &is_max_prefix,
                         sql::ObBoolMask &fal_desc);

  static int le_operator(const sql::ObWhiteFilterExecutor &filter,
                         const common::ObDatum &min_datum,
                         const bool &is_min_prefix,
                         const common::ObDatum &max_datum,
                         const bool &is_max_prefix,
                         sql::ObBoolMask &fal_desc);

  static int bt_operator(const sql::ObWhiteFilterExecutor &filter,
                         const common::ObDatum &min_datum,
                         const bool &is_min_prefix,
                         const common::ObDatum &max_datum,
                         const bool &is_max_prefix,
                         sql::ObBoolMask &fal_desc);

  static int in_operator(const sql::ObWhiteFilterExecutor &filter,
                         const common::ObDatum &min_datum,
                         const bool &is_min_prefix,
                         const common::ObDatum &max_datum,
                         const bool &is_max_prefix,
                         sql::ObBoolMask &fal_desc);

  int black_filter_on_min_max(const uint32_t col_idx,
                              const uint64_t row_count,
                              ObMinMaxFilterParam &param,
                              sql::ObBlackFilterExecutor &filter,
                              const bool use_vectorize,
                              const ObCollationType &cs_type);
private:
  ObAggRowReader agg_row_reader_;
  sql::ObBitVector *skip_bit_;      // to be compatible with the black filter filter() method
  common::ObIAllocator *allocator_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSkipIndexFilterExecutor);
};

} // end namespace blocksstable
} // end namespace oceanbase
#endif
