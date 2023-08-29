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

#ifndef OCEANBASE_SHARE_OB_LS_LOCATION
#define OCEANBASE_SHARE_OB_LS_LOCATION


#include "lib/ob_replica_define.h"
#include "common/ob_role.h"
#include "common/ob_tablet_id.h"
#include "share/ob_define.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/ob_ls_id.h"
#include "share/restore/ob_ls_restore_status.h"
#include "lib/lock/ob_thread_cond.h"
#include "share/ob_rs_mgr.h"

namespace oceanbase
{
namespace common
{
class ObAddr;
class ObReplicaProperty;
}
namespace share
{
static inline bool is_location_service_renew_error(const int err)
{
  return err == OB_LOCATION_NOT_EXIST
      || err == OB_LS_LOCATION_NOT_EXIST
      || err == OB_LS_LOCATION_LEADER_NOT_EXIST
      || err == OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST;
}

class ObLSReplicaLocation
{
  OB_UNIS_VERSION(1);
public:
  ObLSReplicaLocation();
  virtual ~ObLSReplicaLocation() {}
  void reset();
  bool is_valid() const;
  bool operator==(const ObLSReplicaLocation &other) const;
  bool operator!=(const ObLSReplicaLocation &other) const;
  inline const common::ObAddr &get_server() const { return server_; }
  inline void set_server(const common::ObAddr &addr) { server_ = addr; }
  inline const common::ObRole &get_role() const { return role_; }
  inline int64_t get_sql_port() const { return sql_port_; }
  inline void set_sql_port(const int64_t &sql_port) { sql_port_ = sql_port; }
  inline void set_proposal_id(const int64_t proposal_id) { proposal_id_ = proposal_id; }
  inline const common::ObReplicaType &get_replica_type() const { return replica_type_; }
  inline void set_replica_type(const common::ObReplicaType &type) { replica_type_ = type; }
  inline const common::ObReplicaProperty &get_property() const { return property_; }
  inline const ObLSRestoreStatus &get_restore_status() const { return restore_status_; }
  inline int64_t get_proposal_id() const { return proposal_id_; }
  bool is_strong_leader() const { return common::is_strong_leader(role_); }
  bool is_follower() const { return common::is_follower(role_); }
  int assign(const ObLSReplicaLocation &other);
  int init(
      const common::ObAddr &server,
      const common::ObRole &role,
      const int64_t &sql_port,
      const common::ObReplicaType &replica_type,
      const common::ObReplicaProperty &property,
      const ObLSRestoreStatus &restore_status,
      const int64_t proposal_id);
  // make fake location for vtable
  int init_without_check(
      const common::ObAddr &server,
      const common::ObRole &role,
      const int64_t &sql_port,
      const common::ObReplicaType &replica_type,
      const common::ObReplicaProperty &property,
      const ObLSRestoreStatus &restore_status,
      const int64_t proposal_id);
  // set role for tenant_server in __all_virtual_proxy_schema
  void set_role(const common::ObRole &role) { role_ = role; }
  TO_STRING_KV(
      K_(server),
      K_(role),
      K_(sql_port),
      K_(replica_type),
      K_(property),
      K_(restore_status),
      K_(proposal_id));
protected:
  common::ObAddr server_;
  common::ObRole role_;
  int64_t sql_port_;
  common::ObReplicaType replica_type_;
  common::ObReplicaProperty property_; // memstore_percent is used
  ObLSRestoreStatus restore_status_;
  int64_t proposal_id_; // only leader's proposal_id_ is useful
};

class ObLSLocationCacheKey
{
  OB_UNIS_VERSION(1);
public:
  ObLSLocationCacheKey();
  ObLSLocationCacheKey(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID ls_id);
  virtual ~ObLSLocationCacheKey() {}
  int init(
       const int64_t cluster_id,
       const uint64_t tenant_id,
       const ObLSID ls_id);
  int assign(const ObLSLocationCacheKey &other);
  void reset();
  bool operator ==(const ObLSLocationCacheKey &other) const;
  bool operator !=(const ObLSLocationCacheKey &other) const;
  bool is_valid() const;
  uint64_t hash() const;
  int64_t size() const { return sizeof(*this); }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline ObLSID get_ls_id() const { return ls_id_; }
  inline int64_t get_cluster_id() const { return cluster_id_; }
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(cluster_id));
private:
  int64_t cluster_id_;
  uint64_t tenant_id_;
  ObLSID ls_id_;
};

class ObLSLeaderLocation
{
  OB_UNIS_VERSION(1);
public:
  ObLSLeaderLocation() : key_(), location_() {}
  ObLSLeaderLocation(
    const ObLSLocationCacheKey &key,
    const ObLSReplicaLocation &location)
    : key_(key), location_(location) {}
  ~ObLSLeaderLocation() {}
  int init(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID ls_id,
      const common::ObAddr &server,
      const common::ObRole &role,
      const int64_t &sql_port,
      const common::ObReplicaType &replica_type,
      const common::ObReplicaProperty &property,
      const ObLSRestoreStatus &restore_status,
      const int64_t proposal_id);
  int assign(const ObLSLeaderLocation &other);
  void reset();
  bool is_valid() const;
  const ObLSLocationCacheKey &get_key() const { return key_; }
  const ObLSReplicaLocation &get_location() const { return location_; }
  TO_STRING_KV(K_(key), K_(location));
private:
  ObLSLocationCacheKey key_;
  ObLSReplicaLocation location_;
};

class ObLSLocation : public common::ObLink
{
  OB_UNIS_VERSION(1);
public:
  typedef common::ObSEArray<ObLSReplicaLocation, OB_DEFAULT_REPLICA_NUM> ObLSReplicaLocations;
  ObLSLocation();
  explicit ObLSLocation(common::ObIAllocator &allocator);
  ~ObLSLocation();
  int64_t size() const;
  int deep_copy(const ObLSLocation &ls_location);
  int init(const int64_t cluster_id, const uint64_t tenant_id, const ObLSID &ls_id, const int64_t renew_time);
  int init_fake_location(); // make fake location for virtual table in __all_virtual_proxy_schema
  void reset();
  int assign(const ObLSLocation &ls_location);
  bool is_valid() const;
  uint64_t hash() const { return cache_key_.hash(); }
  // compare key and locations with other, ignoring the timestamp
  bool is_same_with(const ObLSLocation &other) const;
  // compare all private members with other
  bool operator==(const ObLSLocation &other) const;
  bool operator!=(const ObLSLocation &other) const;
  int add_replica_location(const ObLSReplicaLocation &replica_location);
  inline uint64_t get_tenant_id() const { return cache_key_.get_tenant_id(); }
  inline ObLSID get_ls_id() const { return cache_key_.get_ls_id(); }
  const ObLSLocationCacheKey &get_cache_key() const { return cache_key_; }
  int get_replica_count(int64_t &full_replica_cnt, int64_t &readonly_replica_cnt);
  inline const common::ObIArray<ObLSReplicaLocation> &get_replica_locations() const
  {
    return replica_locations_;
  }
  inline int64_t get_renew_time() const { return renew_time_; }
  inline void set_renew_time(const int64_t renew_time) { renew_time_ = renew_time; };
  int get_leader(common::ObAddr &leader) const;
  int get_leader(ObLSReplicaLocation &leader) const;
  void set_last_access_ts(const int64_t ts) { last_access_ts_ = ts; }
  int64_t get_last_access_ts() const { return last_access_ts_; }
  int merge_leader_from(const ObLSLocation &new_location);
  static int alloc_new_location(
      common::ObIAllocator &allocator,
      ObLSLocation *&new_location);
  static const int64_t OB_MAX_LOCATION_SERIALIZATION_SIZE = common::OB_MALLOC_BIG_BLOCK_SIZE;
  TO_STRING_KV(K_(cache_key), K_(renew_time), K_(replica_locations));
protected:
  ObLSLocationCacheKey cache_key_;
  int64_t renew_time_; // renew location by sql/rpc
  int64_t last_access_ts_;
  ObLSReplicaLocations replica_locations_;
};

class ObTabletLocation : public common::ObIKVCacheValue
{
  OB_UNIS_VERSION(1);
public:
  typedef common::ObSEArray<ObLSReplicaLocation, OB_DEFAULT_REPLICA_NUM> ObLSReplicaLocations;
  ObTabletLocation();
  explicit ObTabletLocation(common::ObIAllocator &allocator);
  virtual ~ObTabletLocation();
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;
  virtual int64_t size() const;
  int init(const uint64_t tenant_id, const ObTabletID &tablet_id, const int64_t renew_time);
  int init_fake_location(); // make fake location for virtual table in __all_virtual_proxy_schema
  void reset();
  int assign(const ObTabletLocation &ls_location);
  bool is_valid() const;
  bool operator==(const ObTabletLocation &other) const;
  bool operator!=(const ObTabletLocation &other) const;
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline void set_tenant_id(const uint64_t &tenant_id) { tenant_id_ = tenant_id; }
  inline ObTabletID get_tablet_id() const { return tablet_id_; }
  inline void set_tablet_id(const ObTabletID &tablet_id)
  {
    tablet_id_ = tablet_id;
  }
  inline void set_renew_time(const int64_t renew_time) { renew_time_ = renew_time; };
  inline int64_t get_renew_time() const { return renew_time_; }
  int get_leader(common::ObAddr &leader) const;
  int get_leader(ObLSReplicaLocation &leader) const;
  int add_replica_location(const ObLSReplicaLocation &replica_location);
  static int alloc_new_location(
      common::ObIAllocator &allocator,
      ObTabletLocation *&new_location);
  TO_STRING_KV(K_(tenant_id), K_(tablet_id), K_(renew_time), K_(replica_locations));
private:
  uint64_t tenant_id_;
  ObTabletID tablet_id_;
  int64_t renew_time_;
  ObLSReplicaLocations replica_locations_;
};

class ObTabletLSKey
{
  OB_UNIS_VERSION(1);
public:
  ObTabletLSKey() : tenant_id_(OB_INVALID_TENANT_ID), tablet_id_() {}
  ObTabletLSKey(const uint64_t tenant_id, const ObTabletID &tablet_id)
      : tenant_id_(tenant_id), tablet_id_(tablet_id) {}
  ~ObTabletLSKey() {}
  int init(const uint64_t tenant_id, const ObTabletID &tablet_id);
  void reset();
  bool is_valid() const;
  uint64_t hash() const;
  bool operator ==(const ObTabletLSKey &other) const;
  int64_t size() const { return sizeof(*this); }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline ObTabletID get_tablet_id() const { return tablet_id_; }
  TO_STRING_KV(K_(tenant_id), K_(tablet_id));
private:
  uint64_t tenant_id_;
  ObTabletID tablet_id_;
};

class ObTabletLSCache : public common::ObLink
{
  OB_UNIS_VERSION(1);
public:
  ObTabletLSCache();
  virtual ~ObTabletLSCache() {}
  int64_t size() const;
  void reset();
  int assign(const ObTabletLSCache &other);
  bool is_valid() const;
  // mapping is same with other, ignoring timestamp
  bool mapping_is_same_with(const ObTabletLSCache &other) const;
  bool operator==(const ObTabletLSCache &other) const;
  bool operator!=(const ObTabletLSCache &other) const;
  inline uint64_t get_tenant_id() const { return cache_key_.get_tenant_id(); }
  inline ObTabletID get_tablet_id() const { return cache_key_.get_tablet_id(); }
  inline ObLSID get_ls_id() const { return ls_id_; }
  inline int64_t get_renew_time() const { return renew_time_; }
  //inline int64_t get_row_scn() const { return row_scn_; }
  //void set_last_access_ts(const int64_t ts) { last_access_ts_ = ts; }
  //int64_t get_last_access_ts() const { return last_access_ts_; }
  const ObTabletLSKey &get_cache_key() const { return cache_key_; }
  int init(
      const uint64_t tenant_id,
      const ObTabletID &tablet_id,
      const ObLSID &ls_id,
      const int64_t renew_time,
      const int64_t row_scn);
  TO_STRING_KV(K_(cache_key), K_(ls_id), K_(renew_time));
private:
   ObTabletLSKey cache_key_;
   ObLSID ls_id_;
   int64_t renew_time_;     // renew by sql
   //int64_t row_scn_;        // used for auto refresh location
   //int64_t last_access_ts_; // used for ObTabletLSMap
};

//TODO: Reserved for tableapi. Need remove.
class ObTabletLSCacheKey : public common::ObIKVCacheKey
{
public:
  ObTabletLSCacheKey() : tenant_id_(OB_INVALID_TENANT_ID), tablet_id_() {}
  ObTabletLSCacheKey(const uint64_t tenant_id, const ObTabletID tablet_id)
      : tenant_id_(tenant_id), tablet_id_(tablet_id) {}
  virtual ~ObTabletLSCacheKey() {}
  virtual bool operator ==(const ObIKVCacheKey &other) const;
  virtual bool operator !=(const ObIKVCacheKey &other) const;
  virtual bool is_valid() const;
  virtual uint64_t hash() const;
  virtual int64_t size() const { return sizeof(*this); }
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const;
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline ObTabletID get_tablet_id() const { return tablet_id_; }
  TO_STRING_KV(K_(tenant_id), K_(tablet_id));
private:
  uint64_t tenant_id_;
  ObTabletID tablet_id_;
};

class ObLocationServiceUtility
{
public:
  static bool treat_sql_as_timeout(const int error_code);
};

class ObVTableLocationCacheKey : public common::ObIKVCacheKey
{
public:
  ObVTableLocationCacheKey();
  explicit ObVTableLocationCacheKey(
      const uint64_t tenant_id,
      const uint64_t table_id);
  ~ObVTableLocationCacheKey();
  virtual bool operator ==(const ObIKVCacheKey &other) const;
  virtual bool operator !=(const ObIKVCacheKey &other) const;
  virtual uint64_t get_tenant_id() const;
  virtual uint64_t hash() const;
  virtual int64_t size() const;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const;
  inline uint64_t get_table_id() const { return table_id_; }
  TO_STRING_KV(K_(tenant_id), K_(table_id));
private:
  uint64_t tenant_id_;
  uint64_t table_id_;
};

class ObLocationKVCacheValue : public common::ObIKVCacheValue
{
public:
  ObLocationKVCacheValue() : size_(0), buffer_(NULL) {}
  ObLocationKVCacheValue(const int64_t size, char *buffer)
      : size_(size), buffer_(buffer) {}
  virtual ~ObLocationKVCacheValue() {}
  virtual int64_t size() const
  {
    return static_cast<int64_t>(sizeof(*this) + size_);
  }
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const;
  void reset();
  inline int64_t get_size() const { return size_; }
  inline char *get_buffer_ptr() const { return buffer_; }
  void set_size(const int64_t size) { size_ = size; }
  void set_buffer(char *buffer) { buffer_ = buffer; }
  TO_STRING_KV(K_(size), "buffer", reinterpret_cast<int64_t>(buffer_));
private:
  int64_t size_;
  char *buffer_;
};

class ObLocationSem
{
public:
  ObLocationSem();
  ~ObLocationSem();
  void set_max_count(const int64_t max_count);
  int acquire(const int64_t abs_timeout_us);
  int release();
private:
  int64_t cur_count_;
  int64_t max_count_;
  common::ObThreadCond cond_;
};

struct ObLSExistState final
{
public:
  enum State
  {
    INVALID_STATE = -1,
    UNCREATED,
    DELETED,
    EXISTING,
    MAX_STATE
  };
  ObLSExistState() : state_(INVALID_STATE) {}
  ObLSExistState(State state) : state_(state) {}
  ~ObLSExistState() {}
  void reset() { state_ = INVALID_STATE; }
  void set_existing() { state_ = EXISTING; }
  void set_deleted() { state_ = DELETED; }
  void set_uncreated() { state_ = UNCREATED; }
  bool is_valid() const { return state_ > INVALID_STATE && state_ < MAX_STATE; }
  bool is_existing() const { return EXISTING == state_; }
  bool is_deleted() const { return DELETED == state_; }
  bool is_uncreated() const { return UNCREATED == state_; }

  TO_STRING_KV(K_(state));
private:
  State state_;
};

} // end namespace share
} // end namespace oceanbase
#endif
