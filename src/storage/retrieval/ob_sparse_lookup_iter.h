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

#ifndef OB_SPARSE_LOOKUP_ITER_H_
#define OB_SPARSE_LOOKUP_ITER_H_

#include "ob_i_sparse_retrieval_iter.h"

namespace oceanbase
{
namespace storage
{

// decorator class supporting functional lookup
class ObSRLookupIter : public ObISparseRetrievalMergeIter
{
public:
  ObSRLookupIter();
  virtual ~ObSRLookupIter() {}
  virtual int get_next_row() override;
  virtual int get_next_rows(const int64_t capacity, int64_t &count) override;
  int init(
      ObSparseRetrievalMergeParam &iter_param,
      ObISparseRetrievalMergeIter &merge_iter,
      ObIAllocator &iter_allocator,
      const int64_t cache_capacity);
  virtual void reuse() override;
  virtual void reset() override;
  INHERIT_TO_STRING_KV("ObISparseRetrievalMergeIter", ObISparseRetrievalMergeIter,
      K_(cache_capacity), K_(rangekey_size));
public:
  virtual int set_hints(
      const common::ObIArray<std::pair<ObDocIdExt, int>> &virtual_rangekeys,
      const int64_t size) = 0;
protected:
  virtual int inner_init() = 0;
  virtual int load_results() = 0;
  virtual int project_results(const int64_t capacity, int64_t &count) = 0;
protected:
  ObIAllocator *iter_allocator_;
  ObSparseRetrievalMergeParam *iter_param_;
  ObISparseRetrievalMergeIter *merge_iter_;
  int64_t cache_capacity_;
  int64_t rangekey_size_;
  ObFixedArray<ObDocIdExt, ObIAllocator> cached_domain_ids_; // can be sorted or unsorted by id
  common::ObDatumCmpFuncType cmp_func_;
  void (*set_datum_func_)(ObDatum &, const ObDocIdExt &);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSRLookupIter);
};

// lookup iter for sorted results
class ObSRSortedLookupIter final : public ObSRLookupIter
{
public:
  ObSRSortedLookupIter();
  virtual ~ObSRSortedLookupIter() {}
  TO_STRING_EMPTY();
public:
  void reset() override;
  int set_hints(
      const common::ObIArray<std::pair<ObDocIdExt, int>> &virtual_rangekeys,
      const int64_t size) override;
protected:
  int inner_init() override;
  int load_results() override;
  int project_results(const int64_t capacity, int64_t &count) override;
protected:
  ObFixedArray<int64_t, ObIAllocator> reverse_hints_;
  ObFixedArray<double, ObIAllocator> cached_relevances_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSRSortedLookupIter);
};

// lookup iter for unsorted results
class ObSRHashLookupIter final : public ObSRLookupIter
{
public:
  ObSRHashLookupIter();
  virtual ~ObSRHashLookupIter() {}
  TO_STRING_EMPTY();
public:
  void reset() override;
  int set_hints(
      const common::ObIArray<std::pair<ObDocIdExt, int>> &virtual_rangekeys,
      const int64_t size) override;
protected:
  int inner_init() override;
  int load_results() override;
  int project_results(const int64_t capacity, int64_t &count) override;
protected:
  typedef hash::ObHashMap<ObDocIdExt, double> ObSRTaaTHashMap;
  ObSRTaaTHashMap hash_map_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSRHashLookupIter);
};

} // namespace storage
} // namespace oceanbase

#endif