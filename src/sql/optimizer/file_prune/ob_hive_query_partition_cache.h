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

#ifndef _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_HIVE_QUERY_PARTITION_CACHE_H
#define _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_HIVE_QUERY_PARTITION_CACHE_H

#include "common/row/ob_row.h"
#include "lib/ob_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_array.h"
#include "sql/optimizer/file_prune/ob_opt_hive_define_fwd.h"

namespace oceanbase
{
namespace sql
{

class ObNewRowWrap
{
public:
  ObNewRowWrap() : row_(nullptr)
  {}
  ObNewRowWrap(const common::ObNewRow &row) : row_(&row)
  {}
  ~ObNewRowWrap() {};
  uint64_t hash(uint64_t seed) const
  {
    uint64_t hash_val = seed;
    for (int64_t i = 0; i < row_->get_count(); i++) {
      const ObObj &obj = row_->get_cell(i);
      if (obj.is_string_type()) {
        hash_val = common::murmurhash(obj.get_string_ptr(), obj.get_string_len(), hash_val);
      } else {
        hash_val = obj.hash(hash_val);
      }
    }
    return hash_val;
  }
  ObNewRowWrap& operator=(const ObNewRowWrap &other)
  {
    row_ = other.row_;
    return *this;
  }
  bool operator==(const ObNewRowWrap &other) const { return *row_ == *other.row_; }
private:
  const common::ObNewRow *row_;
};

struct HiveTableFileCache
{
  HiveTableFileCache(common::ObIAllocator &allocator)
      : allocator_(allocator),
        partition_infos_(allocator),
        cached_files_()
  {}

  void reset();

  int init_partition_info(common::ObIArray<HivePartitionInfo*> &partition_infos);
  int get_cached_files(const common::ObIArray<int64_t> &part_ids,
                       const common::ObIArray<common::ObString> &part_paths,
                       common::ObIArray<ObHiveFileDesc> &part_files,
                       common::ObIArray<int64_t> &unhit_part_id,
                       common::ObIArray<common::ObString> &unhit_part_path);

  int add_cached_file(const int64_t part_id, common::ObIArrayWrap<ObHiveFileDesc> &part_files);

  common::ObIAllocator &allocator_;
  common::ObFixedArray<sql::HivePartitionInfo *, common::ObIAllocator> partition_infos_;
  common::hash::ObHashMap<int64_t, common::ObArray<ObHiveFileDesc>*, common::hash::NoPthreadDefendMode> cached_files_;
  common::hash::ObHashMap<ObNewRowWrap, int64_t, common::hash::NoPthreadDefendMode> partition_map_;
};

typedef common::hash::ObHashMap<uint64_t, HiveTableFileCache*, common::hash::NoPthreadDefendMode> HiveTableFileCacheMap;

class ObHiveQueryPartitionCache
{
public:
  ObHiveQueryPartitionCache() {}
  void reset();
  int get_cache_info(uint64_t table_id,
                     common::ObIAllocator &allocator,
                     HiveTableFileCache *&cache_info);
private:
  HiveTableFileCacheMap cache_map_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_HIVE_QUERY_PARTITION_CACHE_H
