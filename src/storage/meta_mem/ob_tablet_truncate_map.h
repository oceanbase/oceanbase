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

#ifndef OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_MAP_
#define OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_MAP_

#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/hash/ob_hashmap.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"

namespace oceanbase
{
namespace storage
{
class ObMetaDiskAddr;
class ObTabletMapKey;
class ObTablet;
class ObTabletHandle;
class ObWaitingFreeTruncatedTabletList;
class ObPendingTruncateTabletMap;
enum class ObTabletPoolType : uint8_t;
// thread safe
class ObWaitingFreeTruncatedTabletList final
{
public:
  ObWaitingFreeTruncatedTabletList();
  int init(const int64_t bucket_num, const uint64_t tenant_id);
  /// @brief: should gurantee that addr is disked
  int set(const ObTabletMapKey &key, const ObTabletHandle &tablet_hdl);
  /// @brief: return OB_SUCCESS when key do not exists.
  int try_remove(const ObTabletMapKey &key);
  void destroy();

private:
  typedef common::hash::ObHashMap<ObTabletMapKey, ObTabletHandle> WaitingFreeTabletMap;
  typedef WaitingFreeTabletMap::const_iterator ConstMapIter;

private:
  mutable common::SpinRWLock spin_lock_;
  WaitingFreeTabletMap map_; // guarded by spin_lock_
  bool is_inited_;
};

/// NOTE: thread unsafe, concurrency control should be guranteed by @c t3m::bucket_lock_
class ObPendingTruncateTabletMap final
{
public:
  struct MapValue final
  {
  public:
    MapValue();
    ~MapValue() { reset(); }
    bool is_valid() const;
    int init(
      const ObTabletHandle &new_hdl,
      const ObTabletHandle &old_hdl);
    int assign(const MapValue &other);
    void reset();
    TO_STRING_KV(K_(new_handle),
                 K_(old_handle));

  public:
    ObTabletHandle new_handle_;
    // hold original tablet to prevent tablet being washed.
    ObTabletHandle old_handle_;
  };

public:
  typedef common::hash::ObHashMap<ObTabletMapKey, MapValue> PendingTruncateTabletMap;

public:
  ObPendingTruncateTabletMap();
  int init(const int64_t bucket_num, const uint64_t tenant_id);
  int set(const ObTabletMapKey &key, const MapValue &value);
  int get(const ObTabletMapKey &key, MapValue &value) const;
  int has_exist(const ObTabletMapKey &key, bool &b_ret) const;
  int remove(const ObTabletMapKey &key, MapValue &val);
  void destroy();

private:
  PendingTruncateTabletMap map_;
  bool is_inited_;
};

} // end namespace storage
} // end namespace oceanbase

#endif