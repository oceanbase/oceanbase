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

#ifndef _OB_SESSION_VAL_MAP_H
#define _OB_SESSION_VAL_MAP_H 1

#include "lib/hash/ob_hashmap.h"
#include "lib/allocator/ob_pooled_allocator.h"
#include "common/ob_string_buf.h"

namespace oceanbase
{
namespace sql
{
struct ObSessionVariable {
  void reset()
  {
    meta_.reset();
    value_.reset();
  }
  TO_STRING_KV(K_(meta), K_(value));
  common::ObObjMeta meta_; // Meta Type of the seseion variable
  common::ObObj value_; // value of the session variable
};
class ObSessionValMap
{
  typedef common::ObPooledAllocator<common::hash::HashMapTypes<common::ObString, ObSessionVariable>
      ::AllocType, common::ObWrapperAllocator>
  VarNameValMapAllocer;
public:

  typedef common::hash::ObHashMap<common::ObString,
                                  ObSessionVariable,
                                  common::hash::NoPthreadDefendMode,
                                  common::hash::hash_func<common::ObString>,
                                  common::hash::equal_to<common::ObString>,
                                  VarNameValMapAllocer,
                                  common::hash::NormalPointer,
                                  common::ObWrapperAllocator
                                  > VarNameValMap;
public:
  ObSessionValMap();
  ObSessionValMap(const int64_t block_size, const common::ObWrapperAllocator &block_allocator,
                  const int64_t tenant_id=OB_SERVER_TENANT_ID);
  virtual ~ObSessionValMap();
  // clear all user variable, keep hash table inited
  void reuse();
  // clear all memory
  void reset();
  int assign(const ObSessionValMap &other);
  VarNameValMap &get_val_map() {return map_;}
  const VarNameValMap &get_val_map() const {return map_;}
  int init(int64_t free_threshold,
           int64_t bucket_num,
           common::ObWrapperAllocator *bucket_allocator);
  int set_refactored(const common::ObString &name, const ObSessionVariable &sess_var);
  int get_refactored(const common::ObString &name, ObSessionVariable &sess_var) const;
  const ObSessionVariable *get(const common::ObString &name) const;
  int erase_refactored(const common::ObString &key, ObSessionVariable *sess_var = NULL);
  int64_t size() const {return map_.size();}
  NEED_SERIALIZE_AND_DESERIALIZE;
private:
  int free_mem();

private:
  static const int64_t SMALL_BLOCK_SIZE = 4 * 1024LL;
  common::ObSmallBlockAllocator<> block_allocator_;
  VarNameValMapAllocer var_name_val_map_allocer_;
  common::ObStringBuf str_buf1_;
  common::ObStringBuf str_buf2_;
  common::ObStringBuf *str_buf_[2];
  int32_t current_buf_index_;
  common::ObArenaAllocator bucket_allocator_;
  common::ObWrapperAllocator bucket_allocator_wrapper_;
  VarNameValMap map_;
  int64_t str_buf_free_threshold_;
  int64_t next_free_mem_point_;
};
}
}

#endif /* _OB_SESSION_VAL_MAP_H */
