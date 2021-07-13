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

#ifndef __OB_SQL_PDML_BATCH_ROW_CACHE_H__
#define __OB_SQL_PDML_BATCH_ROW_CACHE_H__

#include "common/row/ob_row_store.h"
#include "lib/container/ob_se_array.h"
#include "sql/engine/basic/ob_chunk_row_store.h"

namespace oceanbase {
namespace common {
class ObNewRow;
class ObPartitionKey;
}  // namespace common

namespace sql {

class ObExecContext;

// Iterator of single partition
class ObPDMLRowIterator : public common::ObNewRowIterator {
public:
  friend class ObBatchRowCache;
  ObPDMLRowIterator() = default;
  virtual ~ObPDMLRowIterator() = default;

  int get_next_row(common::ObNewRow*& row) override;
  void reset() override
  {}

private:
  int init_data_source(sql::ObChunkRowStore& row_store);

private:
  sql::ObChunkRowStore::Iterator row_store_it_;
};

class ObBatchRowCache final {
public:
  ObBatchRowCache();
  ~ObBatchRowCache();

public:
  int init(uint64_t tenant_id, int64_t part_cnt, bool with_barrier);
  // @desc the offset of the partition where the part_id row is located in the location structure
  // @return if the cache is full, return OB_SIZE_OVERFLOW; if the cache is successful, return OB_SUCCESS;
  // otherwise, return other types of errors
  int add_row(const common::ObNewRow& row, int64_t part_id);

  int get_part_id_array(PartitionIdArray& arr);

  int get_row_iterator(int64_t part_id, ObPDMLRowIterator*& iterator);

  // @desc release the data and memory of the partition corresponding to part_id
  int free_rows(int64_t part_id);
  // @desc no row cached
  bool empty() const;
  void reuse();
  void destroy();

private:
  int init_row_store(common::ObIAllocator& allocator, sql::ObChunkRowStore*& chunk_row_store);
  int create_new_bucket(int64_t part_id, sql::ObChunkRowStore*& row_store);

private:
  typedef common::hash::ObHashMap<int64_t, sql::ObChunkRowStore*, common::hash::NoPthreadDefendMode> PartitionStoreMap;
  // HashMap: part_id => chunk_row_store_ ptr
  // dynamic add row store to map, when meet a new partition
  common::ObArenaAllocator row_allocator_;
  PartitionStoreMap pstore_map_;
  ObPDMLRowIterator iterator_;
  // the number of rows in the current cache, currently only supports one row of data in the cache
  int64_t cached_rows_num_;
  uint64_t tenant_id_;
  bool with_barrier_;
  DISALLOW_COPY_AND_ASSIGN(ObBatchRowCache);
};

class ObDMLDataReader {
public:
  virtual int read_row(ObExecContext& ctx, const common::ObNewRow*& row, int64_t& part_id) const = 0;
};

class ObDMLDataWriter {
public:
  virtual int write_rows(ObExecContext& ctx, common::ObPartitionKey& pkey, ObPDMLRowIterator& iterator) const = 0;
};

class ObDMLRowChecker {
public:
  // check new row, do null check and constaint check
  virtual int on_process_new_row(ObExecContext& ctx, const common::ObNewRow& new_row) const = 0;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_PDML_BATCH_ROW_CACHE_H__ */
//// end of header file
