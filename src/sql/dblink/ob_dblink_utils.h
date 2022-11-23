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

#ifndef OCEANBASE_SQL_OB_DBLINK_UTILS_H
#define OCEANBASE_SQL_OB_DBLINK_UTILS_H

#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace sql
{



class GenUniqueAliasName
{
public:
  GenUniqueAliasName()
    : table_id_to_alias_name_(),
      is_inited_(false), 
      alias_name_suffix_(0),
      arena_allocator_("DblinkAlias")
  {}
  ~GenUniqueAliasName()
  {
    table_id_to_alias_name_.destroy();
    arena_allocator_.reset();
  }
  int init();
  void reset();
  int get_unique_name(uint64_t table_id, ObString &alias_name);
private:
  struct CheckUnique
  {
    CheckUnique(ObString &alias_name):alias_name_(alias_name), is_unique_(true) {}
    inline int reset(ObString &alias_name)
    {
      int ret = OB_SUCCESS;
      alias_name_ = alias_name;
      is_unique_ = true;
      return ret;
    }
    inline int operator()(common::hash::HashMapPair<uint64_t, ObString> &entry)
    {
      int ret = OB_SUCCESS;
      if (entry.second == alias_name_) {
        is_unique_ = false;
      }
      return ret;
    }
    ObString &alias_name_;
    bool is_unique_;
  };
  int set_unique_name(uint64_t &table_id, ObString &alias_name);
  hash::ObHashMap<uint64_t, ObString> table_id_to_alias_name_;
  bool is_inited_;
  int alias_name_suffix_;
  ObArenaAllocator arena_allocator_;
  static const int64_t UNIQUE_NAME_BUCKETS = 64;
  static const int MAX_LENGTH_OF_SUFFIX = 8;
};

} // end of namespace sql
} // end of namespace oceanbase

#endif // OCEANBASE_SQL_OB_DBLINK_UTILS_H