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

#ifndef OB_OCEANBASE_SCHEMA_SCHEMA_MGR_CACHE_H_
#define OB_OCEANBASE_SCHEMA_SCHEMA_MGR_CACHE_H_

#include <stdint.h>
#include "lib/lock/ob_tc_rwlock.h"
#include "share/ob_define.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
class ObSchemaSlot
{
public:
  ObSchemaSlot(): tenant_id_(OB_INVALID_TENANT_ID), slot_id_(OB_INVALID_INDEX),
                  schema_version_(OB_INVALID_VERSION), schema_count_(OB_INVALID_COUNT),
                  ref_cnt_(OB_INVALID_COUNT), mod_ref_infos_(), allocator_idx_(OB_INVALID_INDEX) {}
  ~ObSchemaSlot() {}
  void reset();
  void init(const uint64_t &tenant_id, const int64_t &slot_id,
            const int64_t &schema_version, const int64_t &schema_count,
            const int64_t &ref_cnt, const common::ObString &str, const int64_t &allocator_idx);
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_slot_id() const { return slot_id_; }
  int64_t get_schema_version() const { return schema_version_; }
  int64_t get_schema_count() const { return schema_count_; }
  int64_t get_ref_cnt() const { return ref_cnt_;}
  const common::ObString& get_mod_ref_infos() const { return mod_ref_infos_; }
  int64_t get_allocator_idx() const { return allocator_idx_; }
  TO_STRING_KV(K_(tenant_id), K_(slot_id), K_(schema_version),
               K_(schema_count), K_(ref_cnt), K_(mod_ref_infos));
private:
  uint64_t tenant_id_;
  int64_t slot_id_;
  int64_t schema_version_;
  int64_t schema_count_;
  int64_t ref_cnt_;
  common::ObString mod_ref_infos_;
  int64_t allocator_idx_;
};
namespace common
{
}
namespace share
{
namespace schema
{
class ObSchemaMgr;
class ObSchemaMgrCache;

struct ObSchemaMgrItem
{
  enum Mod {
    MOD_STACK                = 0,
    MOD_VTABLE_SCAN_PARAM    = 1,
    MOD_INNER_SQL_RESULT     = 2,
    MOD_LOAD_DATA_IMPL       = 3,
    MOD_PX_TASK_PROCESSS     = 4,
    MOD_REMOTE_EXE           = 5,
    MOD_CACHED_GUARD         = 6,
    MOD_UNIQ_CHECK           = 7,
    MOD_SSTABLE_SPLIT_CTX    = 8,
    MOD_RELATIVE_TABLE       = 9,
    MOD_VIRTUAL_TABLE        = 10,
    MOD_DAS_CTX              = 11,
    MOD_SCHEMA_RECORDER      = 12,
    MOD_SPI_RESULT_SET       = 13,
    MOD_PL_PREPARE_RESULT    = 14,
    MOD_PARTITION_BALANCE    = 15,
    MOD_RS_MAJOR_CHECK       = 16,
    MOD_MAX
  };
  ObSchemaMgrItem()
    : schema_mgr_(NULL), ref_cnt_(0)
  {
    MEMSET(mod_ref_cnt_, 0, MOD_MAX);
  }

  ObSchemaMgr *schema_mgr_;
  int64_t ref_cnt_ CACHE_ALIGNED;
  int64_t mod_ref_cnt_[MOD_MAX] CACHE_ALIGNED;
};

class ObSchemaMgrHandle
{
public:
  ObSchemaMgrHandle();
  explicit ObSchemaMgrHandle(const ObSchemaMgrItem::Mod mod);
  ObSchemaMgrHandle(const ObSchemaMgrHandle &other);
  virtual ~ObSchemaMgrHandle();
  ObSchemaMgrHandle &operator =(const ObSchemaMgrHandle &other);
  void reset();
  bool is_valid();
  void dump() const;
private:
  void revert();
private:
  friend class ObSchemaMgrCache;
  static const int64_t REF_TIME_THRESHOLD = 60 * 1000 * 1000L;
  ObSchemaMgrItem *schema_mgr_item_;
  int64_t ref_timestamp_;
  ObSchemaMgrItem::Mod mod_;
};

class ObSchemaMgrCache
{
public:
  enum Mode {
    REFRESH = 0,
    FALLBACK = 1
  };
public:
  ObSchemaMgrCache();
  virtual ~ObSchemaMgrCache();
  int init(int64_t init_cached_num, Mode mode);
  int check_schema_mgr_exist(const int64_t schema_version, bool &is_exist);
  int get(const int64_t schema_version,
          const ObSchemaMgr *&schema_mgr,
          ObSchemaMgrHandle &handle);
  int get_nearest(const int64_t schema_version,
          const ObSchemaMgr *&schema_mgr,
          ObSchemaMgrHandle &handle);
  int get_recycle_schema_version(int64_t &schema_version) const;
  int get_slot_info(common::ObIAllocator &allocator,
                    common::ObIArray<ObSchemaSlot> &tenant_slot_infos);
  int put(ObSchemaMgr *schema_mgr,
          ObSchemaMgr *&eli_schema_mgr,
          ObSchemaMgrHandle *handle = NULL);
  int try_gc_tenant_schema_mgr(ObSchemaMgr *&eli_schema_mgr);
  int try_eliminate_schema_mgr(ObSchemaMgr *&eli_schema_mgr);
  void dump() const;
public:
  const static int64_t MAX_SCHEMA_SLOT_NUM = 256; // 256
private:
  bool check_inner_stat() const;
  // need process in wlock
  int try_update_latest_schema_idx();
  int get_ref_info_type_str_(const int64_t &index, const char *&type_str);
  int build_ref_mod_infos_(const int64_t *mod_ref, char *&buff,
                           const int64_t &buf_len, common::ObString &str);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSchemaMgrCache);
private:
  common::TCRWLock lock_;
  ObSchemaMgrItem *schema_mgr_items_;
  int64_t max_cached_num_;
  int64_t last_get_schema_idx_;
  int64_t cur_cached_num_;
  Mode mode_;
  int64_t latest_schema_idx_;
};

}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase
#endif //OB_OCEANBASE_SCHEMA_SCHEMA_MGR_CACHE_H_
