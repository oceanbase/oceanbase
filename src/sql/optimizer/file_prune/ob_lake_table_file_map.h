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

#ifndef _OCEANBASE_SQL_OPTIMIZER_OB_LAKE_TABLE_FILE_MAP_H
#define _OCEANBASE_SQL_OPTIMIZER_OB_LAKE_TABLE_FILE_MAP_H

#include "ob_lake_table_fwd.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace sql
{

struct ObLakeTableFileMapKey
{
  OB_UNIS_VERSION(1);
public:
  ObLakeTableFileMapKey();
  ObLakeTableFileMapKey(uint64_t table_loc_id, common::ObTabletID tablet_id);
  int assign(const ObLakeTableFileMapKey &other);
  void reset();
  int hash(uint64_t &hash_val) const;
  bool operator== (const ObLakeTableFileMapKey &other) const;
  TO_STRING_KV(K_(table_loc_id), K_(tablet_id));

  uint64_t table_loc_id_;
  common::ObTabletID tablet_id_;
};

typedef ObFixedArray<sql::ObILakeTableFile*, common::ObIAllocator> ObLakeTableFileArray;
typedef common::hash::ObHashMap<ObLakeTableFileMapKey, ObLakeTableFileArray*, common::hash::NoPthreadDefendMode> ObLakeTableFileMap;
static const int64_t LAKE_TABLE_FILE_MAP_BUCKET_NUM = 128;

struct ObLakeTableFileDesc
{
public:
  OB_UNIS_VERSION(1);
public:
  explicit ObLakeTableFileDesc(ObIAllocator &allocator);
  void reset();
  int assign(const ObLakeTableFileDesc &other);
  bool is_empty() const { return keys_.empty(); }
  int add_lake_table_file_desc(const ObLakeTableFileMapKey &key, const ObIArray<sql::ObILakeTableFile *> *files);
  TO_STRING_KV(K_(keys), K_(offsets));
  ObIAllocator &allocator_;
  ObSEArray<ObLakeTableFileMapKey, 8> keys_;
  ObSEArray<int64_t, 8> offsets_;
  ObSEArray<sql::ObILakeTableFile*, 8> values_;
};

struct ObOdpsPartitionKey
{
  OB_UNIS_VERSION(1);
public:
  ObOdpsPartitionKey();
  ObOdpsPartitionKey(uint64_t table_ref_id, const ObString &partition_str);
  int assign(const ObOdpsPartitionKey &other);
  void reset();
  int hash(uint64_t &hash_val) const;
  bool operator== (const ObOdpsPartitionKey &other) const;
  TO_STRING_KV(K_(table_ref_id), K_(partition_str));

  uint64_t table_ref_id_;
  ObString partition_str_;
};

}
}
#endif
