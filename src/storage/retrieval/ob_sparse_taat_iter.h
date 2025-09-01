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

#ifndef OB_SPARSE_TAAT_ITER_H_
#define OB_SPARSE_TAAT_ITER_H_

#include "ob_i_sparse_retrieval_iter.h"
#include "sql/das/ob_das_ir_define.h"

namespace oceanbase
{
namespace storage
{

// implementation of basic TaaT query processing algorithm primitives
class ObSRTaaTIterImpl : public ObISparseRetrievalMergeIter
{
public:
  ObSRTaaTIterImpl();
  virtual ~ObSRTaaTIterImpl() {}
  virtual int get_next_row() override;
  virtual int get_next_rows(const int64_t capacity, int64_t &count) override;
  int init(
      ObSparseRetrievalMergeParam &iter_param,
      ObISparseRetrievalDimIter &dim_iter,
      ObIAllocator &iter_allocator);
  virtual void reuse() override;
  virtual void reset() override;
  INHERIT_TO_STRING_KV("ObISparseRetrievalMergeIter", ObISparseRetrievalMergeIter,
      K_(partition_cnt), K_(cur_map_idx), K_(next_clear_map_idx));
protected:
  virtual int pre_process(); // should set partition_cnt_, among other things
  virtual int init_chunk_stores();
  virtual int fill_chunk_stores();
  virtual int load_next_hash_map();
  virtual int project_results(const int64_t safe_capacity, int64_t &count);
  virtual int update_dim_iter(const int64_t dim_idx); // should return OB_ITER_END if no more dimensions
protected:
  typedef hash::ObHashMap<ObDocIdExt, double> ObSRTaaTHashMap;
  static const int64_t OB_MAX_HASHMAP_COUNT = 20;
  static const int64_t OB_HASHMAP_DEFAULT_SIZE = 1000;
  ObIAllocator *iter_allocator_;
  ObSparseRetrievalMergeParam *iter_param_;
  ObISparseRetrievalDimIter *dim_iter_;
  int64_t partition_cnt_;
  sql::ObChunkDatumStore **datum_stores_;
  sql::ObChunkDatumStore::Iterator **datum_store_iters_;
  sql::ObBitVector **skips_;
  ObSRTaaTHashMap **hash_maps_;
  ObSRTaaTHashMap::iterator *cur_map_iter_;
  int64_t cur_map_idx_;
  int64_t next_clear_map_idx_;
  ObDocIdExt cache_first_id_;
  bool are_chunk_stores_inited_;
  bool are_chunk_stores_filled_;
  void (*set_datum_func_)(ObDatum &, const ObDocIdExt &);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSRTaaTIterImpl);
};

} // namespace storage
} // namespace oceanbase

#endif