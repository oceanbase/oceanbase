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

#define USING_LOG_PREFIX STORAGE

#include "ob_skip_index_sortedness.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;

int ObSkipIndexSortedness::init(const ObSSTable &sstable,
                                const ObTableSchema &schema,
                                const ObITableReadInfo *read_info,
                                const uint64_t sample_count,
                                const int64_t column_id,
                                common::ObIArray<uint64_t> &res_sample_counts)
{
  int ret = OB_SUCCESS;

  // used for get micro_block_count
  ObSSTableMetaHandle sstable_meta_handle;
  uint64_t tmp_res_sample_count = 0;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Double init for skip index sortedness", KR(ret));
  } else if (OB_UNLIKELY(!sstable.is_valid() || !schema.is_valid() || OB_ISNULL(read_info)
                         || schema.get_column_idx(column_id) < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(sstable), K(schema), K(sample_count));
  } else if (OB_FAIL(sstable.get_meta(sstable_meta_handle))) {
    LOG_WARN("Fail to get sstable meta handle", KR(ret));
  } else if (OB_FALSE_IT(micro_block_count_
                         = sstable_meta_handle.get_sstable_meta().get_data_micro_block_count())) {
  } else if (micro_block_count_ > 0 && OB_FAIL(tree_cursor_.init(sstable, allocator_, read_info))) {
    LOG_WARN("Fail to init tree cursor", KR(ret));
  } else if (OB_FAIL(init_col_idx_in_storage(sstable, schema, column_id))) {
    LOG_WARN("Fail to to init col_idx_in_storage", KR(ret), K(schema), K(column_id));
  } else if (OB_FAIL(init_compare_func(schema, column_id))) {
    LOG_WARN("Fail to init compare function", KR(ret), K(schema), K(column_id));
  } else if (OB_FAIL(init_sample_count(micro_block_count_, sample_count))) {
    LOG_WARN("Fail to init sample_count");
  } else if (OB_FAIL(res_sample_counts.push_back(sample_count_))) {
    LOG_WARN("Fail to push back", K(ret));
  } else {
    is_inited_ = true;

    LOG_DEBUG("Skip Index Init", K(sample_count_), K(micro_block_count_), K(col_idx_in_storage_));
  }

  return ret;
}

int ObSkipIndexSortedness::sample_and_calc(double &sortedness)
{
  int ret = OB_SUCCESS;

  ObArray<ObMinMaxDatum> min_max_datums; // min_max_datums only contains non-null datum min/max pair
  int64_t null_count = 0;                // null min/max datum count in sample process

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", KR(ret));
  } else if (0 == micro_block_count_) {
    // micro_block_count is 0, skip data sampling and set sortedness to 0
    sortedness = 0;
  } else if (OB_FAIL(sample_data(min_max_datums, null_count))) {
    LOG_WARN("Fail to sample data", KR(ret), KPC(this));
  } else if (OB_FAIL(calc_sortedness(min_max_datums, null_count, sortedness))) {
    LOG_WARN("Fail to calc sortedness", KR(ret), KPC(this));
  }

  return ret;
}

void ObSkipIndexSortedness::reset()
{
  is_inited_ = false;

  micro_block_count_ = 0;
  sample_count_ = 0;
  col_idx_in_storage_ = -1;
  tree_cursor_.reset();
  allocator_.reset();
}

int ObSkipIndexSortedness::init_col_idx_in_storage(const ObSSTable &sstable,
                                                   const ObTableSchema &schema,
                                                   const int64_t column_id)
{
  int ret = OB_SUCCESS;

  // the storage order of the skip index is the same as that of mvcc_col_desc
  common::ObSEArray<ObColDesc, 32> cols_desc;

  if (sstable.is_cg_sstable()) {
    col_idx_in_storage_ = 0;
  } else if (OB_FAIL(schema.get_multi_version_column_descs(cols_desc))) {
    LOG_WARN("Fail to to get cols_desc", KR(ret));
  } else {
    col_idx_in_storage_ = -1;
    for (int i = 0; i < cols_desc.count(); i++) {
      if (cols_desc.at(i).col_id_ == column_id) {
        col_idx_in_storage_ = i;
        break;
      }
    }
    if (OB_UNLIKELY(col_idx_in_storage_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Fail to calc col_idx_in_storage", KR(ret), K(cols_desc), K(column_id));
    } else {
      LOG_DEBUG("calc col_idx_in_storage_", K(cols_desc), K(col_idx_in_storage_), K(schema));
    }
  }

  return ret;
}

int ObSkipIndexSortedness::init_compare_func(const ObTableSchema &schema, const int64_t column_id)
{
  int ret = OB_SUCCESS;

  const ObColumnSchemaV2 *column_schema = nullptr;

  if (OB_ISNULL(column_schema = schema.get_column_schema(column_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(schema), K(column_id));
  } else {
    ObObjMeta col_type = column_schema->get_meta_type();
    const ObScale scale = col_type.is_decimal_int() ? column_schema->get_accuracy().get_scale() : col_type.get_scale();
    const ObPrecision precision
        = col_type.is_decimal_int() ? column_schema->get_accuracy().get_precision() : PRECISION_UNKNOWN_YET;
    sql::ObExprBasicFuncs *basic_func
        = ObDatumFuncs::get_basic_func(col_type.get_type(),
                                       col_type.get_collation_type(),
                                       scale,
                                       lib::is_oracle_mode(),
                                       is_lob_storage(col_type.get_type()),
                                       precision);

    if (nullptr == basic_func || nullptr == basic_func->null_last_cmp_) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("Fail to get compare function", KR(ret));
    } else {
      ObCmpFunc cmp_func;
      cmp_func.cmp_func_ = basic_func->null_last_cmp_;
      cmp_func_ = ObStorageDatumCmpFunc(cmp_func);
      LOG_DEBUG("col desc type", K(col_type));
    }
  }

  return ret;
}

int ObSkipIndexSortedness::init_sample_count(const int64_t micro_block_count,
                                             const uint64_t sample_count)
{
  int ret = 0;
  sample_count_ = sample_count;
  // sample_count should in [MIN_SAMPLE_COUNT, MAX_SAMPLE_COUNT] and [0, mirco_block_count]
  if (sample_count_ > MAX_SAMPLE_COUNT || sample_count_ > micro_block_count) {
    sample_count_ = min(MAX_SAMPLE_COUNT, micro_block_count);
    LOG_DEBUG("Too large sample count, limit it to upper limit", K(sample_count_));
  } else if (sample_count_ < MIN_SAMPLE_COUNT) {
    sample_count_ = min(MIN_SAMPLE_COUNT, micro_block_count);
    LOG_DEBUG("Too small sample count, limit it to lower limit", K(sample_count_));
  }
  return ret;
}

int ObSkipIndexSortedness::sample_min_max_datum(ObMinMaxDatum &min_max_datum)
{
  int ret = OB_SUCCESS;

  const ObSkipIndexColMeta min_meta(col_idx_in_storage_, ObSkipIndexColType::SK_IDX_MIN);
  const ObSkipIndexColMeta max_meta(col_idx_in_storage_, ObSkipIndexColType::SK_IDX_MAX);
  ObStorageDatum tmp_datum;

  const ObIndexBlockRowParser *idx_row_parser = nullptr;
  const char *agg_row_buf = nullptr;
  int64_t agg_row_size = 0;
  ObAggRowReader reader;

  if (OB_FAIL(tree_cursor_.get_idx_parser(idx_row_parser))) {
    LOG_WARN("Fail to get index row parser", KR(ret));
  } else if (OB_FAIL(idx_row_parser->get_agg_row(agg_row_buf, agg_row_size))) {
    LOG_WARN("Fail to get agg row", KR(ret), KPC(this));
    ret = OB_SUCCESS;
    min_max_datum.min_.set_null();
    min_max_datum.max_.set_null();
  } else if (OB_FAIL(reader.init(agg_row_buf, agg_row_size))) {
    LOG_WARN("Fail to init agg row reader", KR(ret));
  } else if (OB_FAIL(reader.read(min_meta, tmp_datum))) {
    LOG_WARN("Fail to read skip index min datum", KR(ret));
  } else if (OB_FAIL(min_max_datum.min_.deep_copy(tmp_datum, allocator_))) {
    LOG_WARN("Fail to deep copy min datum", KR(ret), K(tmp_datum));
  } else if (OB_FAIL(reader.read(max_meta, tmp_datum))) {
    LOG_WARN("Fail to read skip index max datum", KR(ret));
  } else if (OB_FAIL(min_max_datum.max_.deep_copy(tmp_datum, allocator_))) {
    LOG_WARN("Fail to deep copy max datum", KR(ret), K(tmp_datum));
  }

  return ret;
}

int ObSkipIndexSortedness::sample_data(ObArray<ObMinMaxDatum> &datums, int64_t &null_count)
{
  int ret = OB_SUCCESS;

  bool is_beyond_range = false;
  ObMinMaxDatum tmp_min_max_datum;

  // Step 1: Drill down to the leaf level
  if (OB_FAIL(tree_cursor_.drill_down(ObDatumRowkey::MIN_ROWKEY,
                                      ObIndexBlockTreeCursor::LEAF,
                                      is_beyond_range))) {
    LOG_WARN("Fail to drill down to leaf level", KR(ret));
  } else if (OB_UNLIKELY(is_beyond_range)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to drill down to leaf level", KR(ret));
  } else if (OB_FAIL(datums.reserve(sample_count_))) {
    LOG_WARN("Fail to reserve sample data array", KR(ret), K(sample_count_));
  } else {
    // Step 2. Sample data
    const double sample_step = max(1.0, 1.0 * micro_block_count_ / sample_count_);
    int64_t curr_index = 0;
    null_count = 0;

    while (OB_SUCC(ret) && datums.count() + null_count < sample_count_) {
      if (OB_FAIL(sample_min_max_datum(tmp_min_max_datum))) {
        LOG_WARN("Fail to sample skip index min/max datum", KR(ret), K(curr_index));
      } else if (tmp_min_max_datum.min_.is_null() || tmp_min_max_datum.max_.is_null()) {
        null_count++;
      } else if (OB_FAIL(datums.push_back(tmp_min_max_datum))) {
        LOG_WARN("Fail to push back to sample datums");
      } else {
        LOG_DEBUG("Sample skip index min max data", K(curr_index), K(tmp_min_max_datum));
      }

      if (OB_SUCC(ret)) {
        ret = tree_cursor_.move_forward_micro(sample_step);
        curr_index += sample_step;
      }
    }

    if (OB_SUCC(ret) || OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      tree_cursor_.reset();
      LOG_DEBUG("Sample success", K(datums.count()), K(sample_step));
    } else {
      LOG_WARN("Fail to sample data", KR(ret), K(datums.count()));
    }
  }

  return ret;
}

int ObSkipIndexSortedness::calc_sortedness(ObArray<ObMinMaxDatum> &datums,
                                           const int64_t null_count,
                                           double &sortedness)
{
  int ret = OB_SUCCESS;

  if (datums.count() == 0) {
    sortedness = 0;
    LOG_INFO("All data in skip index is null", KPC(this), K(null_count));
  } else if (datums.count() == 1) {
    sortedness = 1;
  } else {
    double ascending = 0, descending = 0;
    if (OB_FAIL(merge_sort_count_inversion(datums, ascending, descending))) {
      LOG_WARN("Fail to count inversion", KR(ret));
    } else {
      int64_t non_null_count = datums.count();
      int64_t all_count = non_null_count + null_count;
      sortedness = max(ascending, descending);
      sortedness += 1.0 * non_null_count * null_count + (1.0 * null_count * (null_count - 1)) / 2;
      sortedness /= 1.0 * all_count * (all_count - 1) / 2;

      LOG_DEBUG("Calc sortedness success",
               K(ascending),
               K(descending),
               K(non_null_count),
               K(null_count),
               K(sortedness));
    }
  }

  return ret;
}

int ObSkipIndexSortedness::brute_force_count_inversion(ObArray<ObMinMaxDatum> &datums,
                                                       double &ascending,
                                                       double &descending)
{
  int ret = OB_SUCCESS;

  ascending = 0;
  descending = 0;
  for (int i = 0; OB_SUCC(ret) && i < datums.count() - 1; i++) {
    for (int j = i + 1; OB_SUCC(ret) && j < datums.count(); j++) {
      int cmp_ret;
      if (OB_FAIL(cmp_func_.compare(datums[i].max_, datums[j].min_, cmp_ret))) {
        LOG_WARN("Fail to compare skip index min and max datum",
                 KR(ret),
                 K(datums[i].max_),
                 K(datums[j].min_));
      } else if (cmp_ret <= 0) {
        // datums[i].max <= datums[j].min
        ascending += FULL_INVERSION_VALUE;
      } else if (OB_FAIL(cmp_func_.compare(datums[j].max_, datums[i].min_, cmp_ret))) {
        LOG_WARN("Fail to compare skip index min and max datum",
                 KR(ret),
                 K(datums[i].min_),
                 K(datums[j].max_));
      } else if (cmp_ret < 0) {
        // datums[j].max < datums[i].min
        descending += FULL_INVERSION_VALUE;
      } else if (OB_FAIL(cmp_func_.compare(datums[i].min_, datums[j].min_, cmp_ret))) {
        LOG_WARN("Fail to compare skip index min and max datum",
                 KR(ret),
                 K(datums[i].min_),
                 K(datums[j].min_));
      } else if (cmp_ret <= 0) {
        // datums[i].min <= datums[j].min < datums[i].max
        if (OB_FAIL(cmp_func_.compare(datums[i].max_, datums[j].max_, cmp_ret))) {
          LOG_WARN("Fail to compare skip index min and max datum",
                   KR(ret),
                   K(datums[i].max_),
                   K(datums[j].max_));
        } else if (cmp_ret < 0) {
          // datums[i].min <= datums[j].min < datums[i].max < datums[j].max
          ascending += MEDIUM_INVERSION_VALUE;
        }
      } else {
        // datums[j].min < datums[i].min <= datums[j].max
        if (OB_FAIL(cmp_func_.compare(datums[i].max_, datums[j].max_, cmp_ret))) {
          LOG_WARN("Fail to compare skip index min and max datum",
                   KR(ret),
                   K(datums[i].max_),
                   K(datums[j].max_));
        } else if (cmp_ret < 0) {
          // datums[j].min < datums[i].min < datums[i].max < datums[j].max
          ascending += LOW_INVERSION_VALUE;
          descending += LOW_INVERSION_VALUE;
        } else {
          // datums[j].min < datums[i].min < datums[j].max < datums[i].max
          descending += MEDIUM_INVERSION_VALUE;
        }
      }
    }
  }

  return ret;
}

int ObSkipIndexSortedness::discrete_datum(ObArray<ObMinMaxDatum> &origin_datums,
                                          ObMinMaxDatumDiscrete *&discrete_datums,
                                          int64_t &max_discrete_idx)
{
  int ret = OB_SUCCESS;
  ObArray<DatumWithIdx> datum_to_sort;
  max_discrete_idx = 1;
  if (OB_FAIL(datum_to_sort.prepare_allocate(origin_datums.count() * 2))) {
    LOG_WARN("Fail to reserve datum discrete array", KR(ret), K(origin_datums.count()));
  } else {
    for (int i = 0; i < origin_datums.count(); i++) {
      datum_to_sort[i * 2].datum_ = origin_datums[i].min_;
      datum_to_sort[i * 2].idx_ = i * 2;
      datum_to_sort[i * 2 + 1].datum_ = origin_datums[i].max_;
      datum_to_sort[i * 2 + 1].idx_ = i * 2 + 1;
    }

    int sort_ret = 0;
    bool has_equal_datum = false; // if no equal datum, we can skip compare adjacent datum
    SortComparator cmp(cmp_func_, sort_ret, has_equal_datum);
    lib::ob_sort(datum_to_sort.begin(),
                 datum_to_sort.end(),
                 cmp);

    if (OB_FAIL(sort_ret)) {
      LOG_WARN("Fail to sort datum", KR(ret));
    } else if (OB_ISNULL(discrete_datums = static_cast<ObMinMaxDatumDiscrete *>(allocator_.alloc(
                             sizeof(ObMinMaxDatumDiscrete) * origin_datums.count())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to reserve datum discrete array", KR(ret), K(origin_datums.count()));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < datum_to_sort.count(); i++) {
        int64_t idx_in_origin_datums = datum_to_sort[i].idx_ / 2;
        bool is_max = datum_to_sort[i].idx_ & 1;

        int cmp_ret = 0;
        if (i == 0) {
          // skip compare
        } else if (!has_equal_datum) {
          // skip compare, but need to inc discrete index
          max_discrete_idx++;
        } else if (OB_FAIL(cmp_func_.compare(datum_to_sort[i - 1].datum_,
                                             datum_to_sort[i].datum_,
                                             cmp_ret))) {
          LOG_WARN("Fail to compare datum",
                   KR(ret),
                   K(datum_to_sort[i - 1].datum_),
                   K(datum_to_sort[i].datum_));
        } else if (cmp_ret != 0) {
          max_discrete_idx++;
        }

        if (OB_SUCC(ret)) {
          if (is_max) {
            discrete_datums[idx_in_origin_datums].max_ = max_discrete_idx;
          } else {
            discrete_datums[idx_in_origin_datums].min_ = max_discrete_idx;
          }
        }
      }
    }
  }

  return ret;
}

int ObSkipIndexSortedness::merge_sort_count_inversion(ObArray<ObMinMaxDatum> &datums,
                                                      double &ascending,
                                                      double &descending)
{
  int ret = OB_SUCCESS;

  ObMinMaxDatumDiscrete *discrete_datums = nullptr;
  ObMinMaxDatumDiscrete *tmp_buffer = nullptr;
  ObBinaryIndexTree bitree;
  int64_t max_discrete_idx = 0;

  ascending = 0;
  descending = 0;

  // sort and discrete the datums
  // for example, datums: [10, 6, 99999999]
  //    the discrete idx: [2, 1, 3]
  // then, we always use the discrete idx to do fast compare between datums
  if (OB_FAIL(discrete_datum(datums, discrete_datums, max_discrete_idx))) {
    LOG_WARN("Fail to discrete datum", KR(ret));
  } else if (OB_ISNULL(tmp_buffer = static_cast<ObMinMaxDatumDiscrete *>(
                           allocator_.alloc(sizeof(ObMinMaxDatumDiscrete) * datums.count())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory", KR(ret));
  } else if (OB_FAIL(bitree.init(max_discrete_idx))) {
    LOG_WARN("Fail to init binary index tree", KR(ret));
  } else if (OB_FAIL(merge_sort_count_inversion_impl(discrete_datums,
                                                     datums.count(),
                                                     tmp_buffer,
                                                     bitree,
                                                     ascending,
                                                     descending))) {
    LOG_WARN("Fail to count inversion use mergesort", KR(ret));
  }

  return ret;
}

int ObSkipIndexSortedness::merge_sort_count_inversion_impl(ObMinMaxDatumDiscrete *datums,
                                                           const int64_t len,
                                                           ObMinMaxDatumDiscrete *tmp_buffer,
                                                           ObBinaryIndexTree &bitree,
                                                           double &ascending,
                                                           double &descending)
{
  int ret = OB_SUCCESS;

  if (len < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to count inversion using merge sort", KR(ret), K(len));
  } else if (1 == len) {
    ascending = 0;
    descending = 0;
  } else {
    double left_ascending = 0, right_ascending = 0;
    double left_descending = 0, right_descending = 0;
    int64_t mid = len >> 1;
    if (OB_FAIL(merge_sort_count_inversion_impl(datums,
                                                mid,
                                                tmp_buffer,
                                                bitree,
                                                left_ascending,
                                                left_descending))) {
      LOG_WARN("Fail to count inversion for left range", KR(ret), K(mid));
    } else if (OB_FAIL(merge_sort_count_inversion_impl(datums + mid,
                                                       len - mid,
                                                       tmp_buffer,
                                                       bitree,
                                                       right_ascending,
                                                       right_descending))) {
      LOG_WARN("Fail to count inversion for right range", KR(ret), K(mid), K(len));
    } else {
      // count inversions
      ascending = left_ascending + right_ascending;
      descending = left_descending + right_descending;

      // merge left and right
      int64_t i = 0, j = mid, write_idx = 0;
      while (OB_SUCC(ret) && (i < mid || j < len)) {
        if (i < mid && (j >= len || datums[i].min_ < datums[j].min_)) {
          if (OB_FAIL(bitree.add(datums[i].max_, 1))) {
            LOG_WARN("Fail to add val in binary index tree", KR(ret));
          } else {
            tmp_buffer[write_idx++] = datums[i++];
          }
        } else if (j < len && (i >= mid || datums[i].min_ >= datums[j].min_)) {
          int64_t j_max_large_than_i_max_sum = 0;
          int64_t j_min_large_than_i_max_sum = 0;
          if (OB_FAIL(bitree.get_sum(datums[j].max_, j_max_large_than_i_max_sum))) {
            LOG_WARN("Fail to get sum in binary index tree", KR(ret));
          } else if (OB_FAIL(bitree.get_sum(datums[j].min_, j_min_large_than_i_max_sum))) {
            LOG_WARN("Fail to get sum in binary index tree", KR(ret));
          } else {
            ObMinMaxDatumDiscrete tmp_datum;
            tmp_datum.min_ = datums[j].max_;
            int64_t j_max_less_than_i_min = datums + mid
                                            - std::lower_bound(datums + i,
                                                               datums + mid,
                                                               tmp_datum,
                                                               [&](const ObMinMaxDatumDiscrete &a,
                                                                   const ObMinMaxDatumDiscrete &b) {
                                                                 return a.min_ < b.min_;
                                                               });

            tmp_buffer[write_idx++] = datums[j++];

            ascending += j_min_large_than_i_max_sum * FULL_INVERSION_VALUE
                         + (j_max_large_than_i_max_sum - j_min_large_than_i_max_sum)
                               * MEDIUM_INVERSION_VALUE
                         + (mid - i) * 0.5 * LOW_INVERSION_VALUE;
            descending += j_max_less_than_i_min * FULL_INVERSION_VALUE
                          + (mid - i - j_max_less_than_i_min) * 0.5
                                * (MEDIUM_INVERSION_VALUE + LOW_INVERSION_VALUE);
          }
        }
      }

      if (OB_SUCC(ret)) {
        // revert binary index tree
        for (int i = 0; OB_SUCC(ret) && i < mid; i++) {
          if (OB_FAIL(bitree.add(datums[i].max_, -1))) {
            LOG_WARN("Fail to add val in binary index tree", KR(ret));
          }
        }

        if (OB_SUCC(ret)) {
          for (int i = 0; i < write_idx; i++) {
            datums[i] = tmp_buffer[i];
          }
        }
      }
    }
  }

  return ret;
}

int ObSkipIndexSortedness::ObBinaryIndexTree::init(const int64_t size)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Double init for binary index tree", KR(ret));
  } else if (OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(size));
  } else if (OB_FAIL(counts_.prepare_allocate(size + 1, 0))) {
    LOG_WARN("Fail to reserve binary index tree array", KR(ret), K(size));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObSkipIndexSortedness::ObBinaryIndexTree::get_sum(int64_t idx, int64_t &sum)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not Init for binary index tree", KR(ret));
  } else if (OB_UNLIKELY(idx >= counts_.count() || idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("idx is larger than array size", KR(ret), K(counts_.count()), K(idx));
  } else {
    sum = 0;
    while (idx > 0) {
      sum += counts_[idx];
      idx -= lowbit(idx);
    }
  }

  return ret;
}

int ObSkipIndexSortedness::ObBinaryIndexTree::add(int64_t idx, const int32_t val)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not Init for binary index tree", KR(ret));
  } else if (OB_UNLIKELY(idx >= counts_.count() || idx <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("idx is larger than array size", KR(ret), K(idx), K(counts_.count()));
  } else {
    while (idx < counts_.count()) {
      counts_[idx] += val;
      idx += lowbit(idx);
    }
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase
