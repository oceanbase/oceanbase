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

#ifndef SRC_SHARE_BACKUP_OB_TENANT_NAME_MGR_H_
#define SRC_SHARE_BACKUP_OB_TENANT_NAME_MGR_H_

#include "share/ob_define.h"
#include "ob_backup_struct.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}

namespace share {

namespace schema {
class ObMultiVersionSchemaService;
}

// only manager tenant name and id mapping
class ObTenantNameSimpleMgr final {
public:
  const int64_t TENANT_NAME_SIMPLE_MGR_VERSION = 1;
  struct ObTenantIdItem final {
    static const uint8_t VERSION = 1;
    OB_UNIS_VERSION(VERSION);

  public:
    ObTenantIdItem();
    uint64_t tenant_id_;
    int64_t timestamp_;
    TO_STRING_KV(K_(tenant_id), K_(timestamp));
  };

  struct ObTenantIdCompare {
    bool operator()(const ObTenantIdItem& left, const ObTenantIdItem& right);
  };

  struct ObTenantNameInfo final {
    static const uint8_t VERSION = 1;
    OB_UNIS_VERSION(VERSION);

  public:
    static const int64_t DEFAULT_TENANT_ID_ITEM_COUNT = 1;  // most tenant won't use rename
    ObTenantNameInfo();
    ~ObTenantNameInfo();
    int complete();
    int add(const ObTenantIdItem& item);
    common::ObString tenant_name_;
    bool is_complete_;
    common::ObSEArray<ObTenantIdItem, DEFAULT_TENANT_ID_ITEM_COUNT> id_list_;
    TO_STRING_KV(KP(this), K_(tenant_name), K_(is_complete), K_(id_list));
    DISALLOW_COPY_AND_ASSIGN(ObTenantNameInfo);
  };

  struct ObTenantNameMeta final {
    static const uint8_t VERSION = 1;
    OB_UNIS_VERSION(VERSION);

  public:
    ObTenantNameMeta();
    void reset();
    int64_t schema_version_;  // 0 for not ready
    int64_t tenant_count_;
    TO_STRING_KV(K_(schema_version), K_(tenant_count));
  };

  typedef common::hash::ObHashMap<common::ObString, ObTenantNameInfo*, common::hash::NoPthreadDefendMode>
      TenantNameHashMap;

  ObTenantNameSimpleMgr();
  ~ObTenantNameSimpleMgr();

  int init();
  void reset();
  int assign(const ObTenantNameSimpleMgr& other);
  int64_t get_schema_version() const;
  int complete(const int64_t schema_version);  // sort and set schema_version
  int add(const common::ObString& tenant_name, const int64_t timestamp, const uint64_t tenant_id);
  int get_tenant_id(const common::ObString& tenant_name, const int64_t timestamp, uint64_t& tenant_id) const;
  int read_buf(const char* buf, const int64_t buf_size);
  int write_buf(char* buf, const int64_t buf_size, int64_t& pos) const;
  int64_t get_write_buf_size() const;
  int read_backup_file(const ObClusterBackupDest& cluster_backup_dest);
  int write_backup_file(const ObClusterBackupDest& cluster_backup_dest);
  int get_infos(ObTenantNameMeta& meta, common::ObIArray<const ObTenantNameInfo*>& infos) const;
  int get_tenant_ids(hash::ObHashSet<uint64_t>& tenant_id_set);

private:
  int get_info_(const common::ObString& tenant_name, ObTenantNameInfo*& info);
  int write_buf_(char* buf, const int64_t buf_size, int64_t& pos) const;
  int alloc_tenant_name_info_(const common::ObString& tenant_name, ObTenantNameInfo*& info);

private:
  bool is_inited_;
  ObTenantNameMeta meta_;
  TenantNameHashMap tenant_name_infos_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantNameSimpleMgr);
};

// deal with schema
class ObTenantNameMgr final {
public:
  ObTenantNameMgr();
  ~ObTenantNameMgr();

  int init(oceanbase::common::ObMySQLProxy& sql_proxy, share::schema::ObMultiVersionSchemaService& schema_service);
  void cleanup();
  int reload_backup_dest(const char* backup_dest, const int64_t incarnation);  // will clean simple mgr
  int do_update(const bool is_force);
  int get_tenant_ids(common::ObIArray<uint64_t>& tenant_ids, int64_t& last_update_ts);

private:
  int update_backup_simple_mgr_(const int64_t schema_version);
  int add_new_tenant_name_(const int64_t schema_version, ObTenantNameSimpleMgr& new_mgr);
  ObTenantNameSimpleMgr* get_next_mgr_();
  int get_backup_tenant_ids_from_schema_(share::schema::ObSchemaGetterGuard& schema_guard);

private:
  bool is_inited_;
  oceanbase::common::ObMySQLProxy* sql_proxy_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObTenantNameSimpleMgr simple_mgr_[2];
  ObTenantNameSimpleMgr* cur_mgr_;
  common::ObArray<uint64_t> tenant_ids_;
  ObClusterBackupDest cluster_backup_dest_;
  int64_t last_update_timestamp_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantNameMgr);
};

}  // namespace share
}  // namespace oceanbase
#endif /* SRC_SHARE_BACKUP_OB_TENANT_NAME_MGR_H_ */
