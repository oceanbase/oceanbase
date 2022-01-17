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

#ifndef __OB_SQL_PDML_OP_BATCH_ROW_CACHE_H__
#define __OB_SQL_PDML_OP_BATCH_ROW_CACHE_H__

#include "lib/container/ob_se_array.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase {
namespace common {
class ObNewRow;
class ObPartitionKey;
}  // namespace common

namespace sql {
class ObExecContext;

// row cache for one partition
class ObPDMLOpRowIterator {
public:
  friend class ObPDMLOpBatchRowCache;
  ObPDMLOpRowIterator() = default;
  virtual ~ObPDMLOpRowIterator() = default;
  // get next row of row_store_it_
  int get_next_row(const ObExprPtrIArray& row);

private:
  int init_data_source(ObChunkDatumStore& row_datum_store, ObEvalCtx* eval_ctx);

private:
  ObChunkDatumStore::Iterator row_store_it_;
  ObEvalCtx* eval_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObPDMLOpRowIterator);
};

// PDML row cache
// cache rows for partitions
class ObPDMLOpBatchRowCache final {
public:
  explicit ObPDMLOpBatchRowCache(ObEvalCtx* eval_ctx);
  ~ObPDMLOpBatchRowCache();

public:
  int init(uint64_t tenant_id, int64_t part_cnt, bool with_barrier);
  int add_row(const ObExprPtrIArray& row, int64_t part_id);

  // @desc get all part_ids in current cache
  int get_part_id_array(PartitionIdArray& arr);

  // @desc get partition rows by part_id
  int get_row_iterator(int64_t part_id, ObPDMLOpRowIterator*& iterator);

  // release rows of part_id
  int free_rows(int64_t part_id);
  // @desc no row cached
  bool empty() const;
  void reuse();
  void destroy();

private:
  int init_row_store(common::ObIAllocator& allocator, ObChunkDatumStore*& chunk_row_store);
  int create_new_bucket(int64_t part_id, ObChunkDatumStore*& row_store);

private:
  typedef common::hash::ObHashMap<int64_t, ObChunkDatumStore*, common::hash::NoPthreadDefendMode> PartitionStoreMap;
  // HashMap: part_id => chunk_datum_store_ ptr
  // dynamic add row store to map, when meet a new partition
  common::ObArenaAllocator row_allocator_;
  ObEvalCtx* eval_ctx_;
  PartitionStoreMap pstore_map_;
  ObPDMLOpRowIterator iterator_;
  int64_t cached_rows_num_;
  uint64_t tenant_id_;
  bool with_barrier_;
  DISALLOW_COPY_AND_ASSIGN(ObPDMLOpBatchRowCache);
};

class ObDMLOpDataReader {
public:
  // read one row from DML operator and get part_id
  virtual int read_row(ObExecContext& ctx, const ObExprPtrIArray*& row, int64_t& part_id) = 0;
};

class ObDMLOpDataWriter {
public:
  // write rows to storage
  virtual int write_rows(ObExecContext& ctx, common::ObPartitionKey& pkey, ObPDMLOpRowIterator& iterator) = 0;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_PDML_OP_BATCH_ROW_CACHE_H__ */
//// end of header file
