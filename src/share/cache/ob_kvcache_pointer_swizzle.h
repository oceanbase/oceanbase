/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_CACHE_OB_KVCACHE_POINTER_SWIZZLE_H_
#define OCEANBASE_CACHE_OB_KVCACHE_POINTER_SWIZZLE_H_

#include "share/cache/ob_kvcache_hazard_version.h"
#include "share/cache/ob_kvcache_struct.h"

namespace oceanbase
{
namespace blocksstable
{
  class ObMicroBlockBufferHandle;
}
namespace common
{
class ObPointerSwizzleNode final
{
public:
  ObPointerSwizzleNode();
  ~ObPointerSwizzleNode();
  void operator=(const ObPointerSwizzleNode &ps_node);
  int swizzle(const blocksstable::ObMicroBlockBufferHandle &handle);
  int access_mem_ptr(blocksstable::ObMicroBlockBufferHandle &handle);
  TO_STRING_KV(K_(seq_num), KPC_(mb_handle), KP_(value), K_(node_version));
private:
  void unswizzle();
  bool load_node(ObPointerSwizzleNode &tmp_ps_node);
  void reset();
  void set(ObKVMemBlockHandle *mb_handle, const ObIKVCacheValue *value);
  bool add_handle_ref(ObKVMemBlockHandle *mb_handle, const uint32_t seq_num);
private:
  union ObNodeVersion
  {
    uint64_t value_;
    struct {
      uint64_t version_value_: 63;
      uint64_t write_flag_: 1;
    };
    ObNodeVersion(const uint64_t &value) : value_(value) {}
    ObNodeVersion(const uint64_t &version_value, const uint64_t &write_flag) : version_value_(version_value), write_flag_(write_flag) {}
    TO_STRING_KV(K_(value), K_(version_value), K_(write_flag));
  } node_version_;
  ObKVMemBlockHandle *mb_handle_;
  const ObIKVCacheValue *value_;
  int32_t seq_num_;
private:
  class ObPointerSwizzleGuard final
  {
  public:
    ObPointerSwizzleGuard(ObNodeVersion &cur_version);
    ~ObPointerSwizzleGuard();
    bool is_multi_thd_safe() { return is_multi_thd_safe_; }
  private:
    ObNodeVersion &cur_version_;
    bool is_multi_thd_safe_;
  };
};

}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_CACHE_OB_KVCACHE_POINTER_SWIZZLE_H_