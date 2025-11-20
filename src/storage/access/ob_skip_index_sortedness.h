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

#ifndef OB_SKIP_INDEX_SORTEDNESS_H_
#define OB_SKIP_INDEX_SORTEDNESS_H_

#include "storage/blocksstable/index_block/ob_index_block_tree_cursor.h"
#include "storage/blocksstable/ob_sstable.h"

namespace oceanbase
{
namespace storage
{
struct DatumWithIdx {
  blocksstable::ObStorageDatum datum_;
  int64_t idx_;

  TO_STRING_KV(K_(datum));
};

struct SortComparator {
public:
  SortComparator(blocksstable::ObStorageDatumCmpFunc &cmp_func, int &sort_ret, bool &has_equal_datum)
      : cmp_func_(cmp_func), sort_ret_(sort_ret), has_equal_datum_(has_equal_datum)
  {}

  bool operator()(const DatumWithIdx &a, const DatumWithIdx &b) const
  {
    int ret = 0;
    int cmp_ret = 0;
    if (OB_FAIL(cmp_func_.compare(a.datum_, b.datum_, cmp_ret))) {
      sort_ret_ = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Fail to compare datum", KR(sort_ret_), K(a.datum_), K(b.datum_));
    } else if (0 == cmp_ret) {
      has_equal_datum_ = true;
    }
    return cmp_ret < 0;
  }

private:
  blocksstable::ObStorageDatumCmpFunc &cmp_func_;

  int &sort_ret_;
  bool &has_equal_datum_;
};

class ObSkipIndexSortedness
{
private:
  struct ObMinMaxDatum
  {
    blocksstable::ObStorageDatum min_;
    blocksstable::ObStorageDatum max_;

    TO_STRING_KV(K_(min), K_(max));
  };

  struct ObMinMaxDatumDiscrete
  {
    int32_t min_;
    int32_t max_;

    TO_STRING_KV(K_(min), K_(max));
  };

private:
  class ObBinaryIndexTree
  {
  public:
    ObBinaryIndexTree() : counts_(), is_inited_(false) {}

    ~ObBinaryIndexTree() {}

    int init(const int64_t size);

    int get_sum(int64_t idx, int64_t &sum);

    int add(int64_t idx, const int32_t val);

  private:
    static int32_t lowbit(int32_t x) { return x & (-x); }

  private:
    ObArray<int32_t> counts_;

    bool is_inited_;
  };

public:
  static constexpr int64_t MAX_SAMPLE_COUNT = 100000;
  static constexpr int64_t MIN_SAMPLE_COUNT = 100;

  ObSkipIndexSortedness()
      : micro_block_count_(0), sample_count_(0), col_idx_in_storage_(-1), is_inited_(false)
  {
  }

  ~ObSkipIndexSortedness() { reset(); }

  /**
   * @param sstable The sstable need to calc sortedness
   * @param schema The schema of the table used to get datum cmp func
   * @param read_info used for tree_cursor
   * @param sample_count sample count
   * @param column_id Which column to compute the sortedness
   *
   * @return OB_SUCCESS if init success
   */
  int init(const blocksstable::ObSSTable &sstable,
           const ObTableSchema &schema,
           const ObITableReadInfo *read_info,
           const uint64_t sample_count,
           const int64_t column_id,
           common::ObIArray<uint64_t> &res_sample_counts);

  /**
   * @brief sample skip index in the index block tree and calculate sortedness
   *
   * @param[out] sortedness value of the skip index sortedness which in [0, 1]
   * @return OB_SUCCESS if sample and calculate success
   */
  int sample_and_calc(double &sortedness);

  void reset();

  TO_STRING_KV(K_(micro_block_count), K_(sample_count), K_(col_idx_in_storage), K_(is_inited));

private:
  int init_col_idx_in_storage(const blocksstable::ObSSTable &sstable,
                              const ObTableSchema &schema,
                              const int64_t column_id);

  int init_compare_func(const ObTableSchema &schema, const int64_t column_id);

  int init_sample_count(const int64_t micro_block_count, const uint64_t sample_count);

  int sample_data(ObArray<ObMinMaxDatum> &min_max_datums, int64_t &null_count);

  int sample_min_max_datum(ObMinMaxDatum &min_max_datum);

  int calc_sortedness(ObArray<ObMinMaxDatum> &min_max_datums,
                      const int64_t null_count,
                      double &sortedness);

  // merge sort count (3D-)inversion need use CDQ algorithm, whose time complexity is O(nlog^2n)
  // and need to implement an Binary Indexed Tree and Datum discrete
  int merge_sort_count_inversion(ObArray<ObMinMaxDatum> &min_max_datums,
                                 double &ascending,
                                 double &descending);

  int discrete_datum(ObArray<ObMinMaxDatum> &origin_datums,
                     ObMinMaxDatumDiscrete *&discrete_datums,
                     int64_t &max_discrete_idx);

  int merge_sort_count_inversion_impl(ObMinMaxDatumDiscrete *datums,
                                      const int64_t len,
                                      ObMinMaxDatumDiscrete *tmp_buffer,
                                      ObBinaryIndexTree &bitree,
                                      double &ascending,
                                      double &descending);

  int brute_force_count_inversion(ObArray<ObMinMaxDatum> &min_max_datums,
                                  double &ascending,
                                  double &descending);

private:
  static constexpr double FULL_INVERSION_VALUE = 1;
  static constexpr double MEDIUM_INVERSION_VALUE = 0.66;
  static constexpr double LOW_INVERSION_VALUE = 0.33;

  ObArenaAllocator allocator_;

  int64_t micro_block_count_;

  int64_t sample_count_;

  // the column index in storage
  // for cg_sstable, the col_idx_in_storage_ is 0
  // for row store sstable, the col_idx_in_storage should consider mvcc column
  int64_t col_idx_in_storage_;

  blocksstable::ObStorageDatumCmpFunc cmp_func_;

  blocksstable::ObIndexBlockTreeCursor tree_cursor_;

  bool is_inited_;
};

}; // namespace storage
}; // namespace oceanbase

#endif /* OB_SKIP_INDEX_SORTEDNESS_H_ */
