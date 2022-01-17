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

#ifndef OB_TENANT_META_MEMORY_MGR_H_
#define OB_TENANT_META_MEMORY_MGR_H_

#include "lib/hash/ob_hashmap.h"

namespace oceanbase {
namespace storage {

class ObTenantMetaMemoryMgr {
public:
  int init();
  // need update when the schema_version or memory_size has been changed
  int try_update_tenant_info(const uint64_t tenant_id, const int64_t schema_version);
  static ObTenantMetaMemoryMgr& get_instance();

private:
  struct TenantInfo {
  public:
    TenantInfo() : schema_version_(0), memory_size_(0)
    {}
    TenantInfo(const int64_t schema_version, const int64_t memory_size)
        : schema_version_(schema_version), memory_size_(memory_size)
    {}
    ~TenantInfo() = default;
    bool is_valid() const
    {
      return schema_version_ > 0 && memory_size_ > 0;
    }
    TO_STRING_KV(K_(schema_version), K_(memory_size));
    int64_t schema_version_;
    int64_t memory_size_;
  };
  static const int64_t DEFAULT_BUCKET_NUM = 100;
  ObTenantMetaMemoryMgr();
  virtual ~ObTenantMetaMemoryMgr();
  void destroy();

private:
  lib::ObMutex lock_;
  // key: tenant_id
  common::hash::ObHashMap<uint64_t, TenantInfo, common::hash::NoPthreadDefendMode> tenant_meta_memory_map_;
  bool is_inited_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_TENANT_META_MEMORY_MGR_H_
