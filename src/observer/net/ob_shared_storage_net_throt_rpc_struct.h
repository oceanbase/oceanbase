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

#ifndef OCEANBASE_SHARED_STORAGE_NET_THROT_RPC_STRUCT_H_
#define OCEANBASE_SHARED_STORAGE_NET_THROT_RPC_STRUCT_H_

#include "lib/allocator/ob_malloc.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/net/ob_addr.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_define.h"
#include "share/ob_errno.h"
#include <string.h>
#include "share/io/ob_io_manager.h"
namespace oceanbase
{
namespace obrpc
{
struct ObSharedDeviceResource
{
  OB_UNIS_VERSION(1);
public:
  ObSharedDeviceResource():key_(), type_(ResourceType::ResourceTypeCnt), value_(0) {}
  ObSharedDeviceResource(const ObTrafficControl::ObStorageKey &key, ResourceType type, uint64_t value)
  {
    key_ = key;
    type_ = type;
    value_ = value;
  }
  bool operator==(const ObSharedDeviceResource &other) const
  {
    return key_ == other.key_ && type_ == other.type_ && value_ == other.value_;
  }
  bool is_valid() const
  {
    return type_ < ResourceType::ResourceTypeCnt;
  }
  TO_STRING_KV(K(key_), K(type_), K(value_));
  ObTrafficControl::ObStorageKey key_;
  ResourceType type_;
  uint64_t value_;
};

struct ObSharedDeviceResourceArray
{
  OB_UNIS_VERSION(1);
public:
  ObSharedDeviceResourceArray() {}
  ObSharedDeviceResourceArray(const ObSEArray<obrpc::ObSharedDeviceResource, 1> &array) : array_(array)
  {}
  ~ObSharedDeviceResourceArray() {}
  bool is_valid() const { return true; }
  TO_STRING_KV(K(array_));
  ObSEArray<obrpc::ObSharedDeviceResource, 1> array_;
};

struct ObSSNTKey
{
  OB_UNIS_VERSION(1);

public:
  ObSSNTKey() : addr_(), key_()
  {}
  ObSSNTKey(const ObAddr &addr, const ObTrafficControl::ObStorageKey &key) : addr_(addr), key_(key)
  {}
  ObSSNTKey(const ObSSNTKey &other)
  {
    (void)assign(other);
  }
  int assign(const ObSSNTKey &other)
  {
    int ret = OB_SUCCESS;
    if (this != &other) {
      addr_ = other.addr_;
      key_ = other.key_;
    }
    return ret;
  }
  bool is_valid() const
  {
    return addr_.is_valid();
  }
  uint64_t hash() const
  {
    uint64_t hash_val = static_cast<uint64_t>(addr_.hash());
    uint64_t hash_val_2 = static_cast<uint64_t>(key_.hash());
    hash_val = common::murmurhash(&hash_val_2, sizeof(hash_val_2), hash_val);
    return hash_val;
  }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return common::OB_SUCCESS;
  }
  bool operator==(const ObSSNTKey &other) const
  {
    return (addr_.compare(other.addr_) == 0) && key_ == other.key_;
  }
  ObSSNTKey &operator=(const ObSSNTKey &other)
  {
    if (this != &other) {
      assign(other);
    }
    return (*this);
  }
  bool operator!=(const ObSSNTKey &other) const
  {
    return !operator==(other);
  }
  TO_STRING_KV(K(addr_), K(key_));

  ObAddr addr_;
  ObTrafficControl::ObStorageKey key_;
};

struct ObSSNTResource
{
  OB_UNIS_VERSION(1);
public:
  ObSSNTResource():ops_(0), ips_(0), iops_(0), obw_(0), ibw_(0), iobw_(0), tag_(0)
  {}
  ~ObSSNTResource()
  {}
  void destroy()
  {}
  int assign(const ObSSNTResource &other)
  {
    int ret = OB_SUCCESS;
    if (this != &other) {
      this->ops_ = other.ops_;
      this->ips_ = other.ips_;
      this->iops_ = other.iops_;
      this->obw_ = other.obw_;
      this->ibw_ = other.ibw_;
      this->iobw_ = other.iobw_;
      this->tag_ = other.tag_;
    }
    return ret;
  }
  int64_t ops_;
  int64_t ips_;
  int64_t iops_;
  int64_t obw_;
  int64_t ibw_;
  int64_t iobw_;
  int64_t tag_;
  TO_STRING_KV(K(ops_), K(ips_), K(iops_), K(obw_), K(ibw_), K(iobw_), K(tag_));
};
struct ObSSNTValue
{
  OB_UNIS_VERSION(1);

public:
  ObSSNTValue() : predicted_resource_(), assigned_resource_(), expire_time_()
  {
    expire_time_ = ObTimeUtility::current_time() + 30 * 1000L * 1000L; //30s
  }
  ~ObSSNTValue()
  {}
  void destroy()
  {}
  int assign(const ObSSNTValue &other)
  {
    int ret = OB_SUCCESS;
    if (this != &other) {
      this->predicted_resource_.assign(other.predicted_resource_);
      this->assigned_resource_.assign(other.assigned_resource_);
      this->expire_time_ = other.expire_time_;
    }
    return ret;
  }
  TO_STRING_KV(K(predicted_resource_), K(assigned_resource_), K(expire_time_));

  // predicted and assigned resource:
  ObSSNTResource predicted_resource_;
  ObSSNTResource assigned_resource_;
  int64_t expire_time_;
};

struct ObEndpointInfos
{
  OB_UNIS_VERSION(1);

public:
  ObEndpointInfos() : storage_keys_(), expire_time_(0)
  {}
  ObEndpointInfos(const common::ObSEArray<ObTrafficControl::ObStorageKey, 1> &storage_keys, const int64_t expire_time)
      : storage_keys_(storage_keys), expire_time_(expire_time)
  {}
  ~ObEndpointInfos()
  {}
  bool is_valid() const
  {
    return true;
  }
  TO_STRING_KV(K(storage_keys_), K(expire_time_));
  common::ObSEArray<ObTrafficControl::ObStorageKey, 1> storage_keys_;
  int64_t expire_time_;
};
struct ObStorageKeyLimit
{
  OB_UNIS_VERSION(1);

public:
  ObStorageKeyLimit() : max_iops_(0), max_bw_(0), expire_time_(0)
  {}
  ObStorageKeyLimit(const int64_t max_iops, const int64_t max_bw, const int64_t expire_time)
      : max_iops_(max_iops), max_bw_(max_bw), expire_time_(expire_time)
  {}
  ~ObStorageKeyLimit()
  {}
  bool is_valid() const
  {
    bool ret = false;
    const int64_t current_time = ObTimeUtility::current_time();
    if (max_iops_ < 0 || max_bw_ < 0 || expire_time_ < current_time) {
    } else {
      ret = true;
    }
    return ret;
  }
  TO_STRING_KV(K(max_iops_), K(max_bw_), K(expire_time_));
  int64_t max_iops_;
  int64_t max_bw_;
  int64_t expire_time_;
};
typedef common::ObSEArray<obrpc::ObSSNTKey, 1> ObSSNTKeyArray;
typedef common::hash::ObHashMap<ObAddr, ObEndpointInfos> ObEndpointRegMap;
typedef common::hash::ObHashMap<ObSSNTKey, ObSSNTValue*> ObQuotaPlanMap;
typedef common::hash::ObHashMap<ObTrafficControl::ObStorageKey, ObQuotaPlanMap*> ObBucketThrotMap;
typedef common::hash::ObHashMap<ObTrafficControl::ObStorageKey, ObStorageKeyLimit> ObStorageKeyLimitMap;

struct ObSSNTEndpointArg
{
  OB_UNIS_VERSION(1);

public:
  ObSSNTEndpointArg():addr_(),storage_keys_(), expire_time_(0)
  {
  }
  ObSSNTEndpointArg(ObAddr addr, common::ObSEArray<ObTrafficControl::ObStorageKey, 1> &storage_keys, const int64_t expire_time)
  {
    addr_ = addr;
    storage_keys_ = storage_keys;
    expire_time_ = expire_time;
  }
  ~ObSSNTEndpointArg()
  {}
  bool is_valid() const
  {
    return true;
  }
  TO_STRING_KV(K(addr_), K(storage_keys_), K(expire_time_));
  ObAddr addr_;
  common::ObSEArray<ObTrafficControl::ObStorageKey, 1> storage_keys_;
  int64_t expire_time_;
};

struct ObSSNTSetRes
{
  OB_UNIS_VERSION(1);

public:
  ObSSNTSetRes() : res_(common::OB_ERROR)
  {}
  ~ObSSNTSetRes()
  {}
  int assign(const ObSSNTSetRes &other);
  TO_STRING_KV(K_(res));

  int res_;
};

}  // namespace obrpc
}  // namespace oceanbase
#endif /* OCEANBASE_SHARED_STORAGE_NET_THROT_RPC_STRUCT_H_ */
