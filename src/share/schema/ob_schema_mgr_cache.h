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

namespace oceanbase {
namespace common {}
namespace share {
namespace schema {
class ObSchemaMgr;
class ObSchemaMgrCache;

struct ObSchemaMgrItem {
  enum Mod {
    MOD_STACK = 0,
    MOD_VTABLE_SCAN_PARAM = 1,
    MOD_INNER_SQL_RESULT = 2,
    MOD_TABLE_API_ROW_ITER = 3,
    MOD_STAT_CONV_INFO = 4,
    MOD_SHUFFLE_TASK_HANDLE = 5,
    MOD_LOAD_DATA_IMPL = 6,
    MOD_PX_TASK_PROCESSS = 7,
    MOD_TABLE_SCAN = 8,
    MOD_DIST_EXECUTER = 9,
    MOD_MINI_TASK_BASE = 10,
    MOD_REMOTE_EXE = 11,
    MOD_CACHED_GUARD = 12,
    MOD_UNIQ_CHECK = 13,
    MOD_LOGIC_ROW = 14,
    MOD_TAILORED_ROW_ITER = 15,
    MOD_SSTABLE_MERGE_CTX = 16,
    MOD_SSTABLE_SPLIT_CTX = 17,
    MOD_RELATIVE_TABLE = 18,
    MOD_RECOVER_POINT = 19,
    MOD_PART_SCHEMA_RECORDER = 20,
    MOD_VIRTUAL_TABLE = 21,
    MOD_PHY_RES_STAT = 22,
    MOD_TENANT_PT_ITER = 23,
    MOD_INDEX_PARAM = 24,
    MOD_BACKUP_CHECKER = 25,
    MOD_DIS_TASK_SPLITER = 26,
    MOD_MAX
  };
  ObSchemaMgrItem() : schema_mgr_(NULL), ref_cnt_(0)
  {
    MEMSET(mod_ref_cnt_, 0, MOD_MAX);
  }

  ObSchemaMgr* schema_mgr_;
  int64_t ref_cnt_ CACHE_ALIGNED;
  int64_t mod_ref_cnt_[MOD_MAX] CACHE_ALIGNED;
};

class ObSchemaMgrHandle {
public:
  ObSchemaMgrHandle();
  explicit ObSchemaMgrHandle(const ObSchemaMgrItem::Mod mod);
  ObSchemaMgrHandle(const ObSchemaMgrHandle& other);
  virtual ~ObSchemaMgrHandle();
  ObSchemaMgrHandle& operator=(const ObSchemaMgrHandle& other);
  void reset();
  bool is_valid();
  void dump() const;

private:
  void revert();

private:
  friend class ObSchemaMgrCache;
  static const int64_t REF_TIME_THRESHOLD = 60 * 1000 * 1000L;
  ObSchemaMgrItem* schema_mgr_item_;
  int64_t ref_timestamp_;
  ObSchemaMgrItem::Mod mod_;
};

class ObSchemaMgrCache {
public:
  enum Mode { REFRESH = 0, FALLBACK = 1 };

public:
  ObSchemaMgrCache();
  virtual ~ObSchemaMgrCache();
  int init(int64_t init_cached_num, Mode mode);
  int check_schema_mgr_exist(const int64_t schema_version, bool& is_exist);
  int get(const int64_t schema_version, const ObSchemaMgr*& schema_mgr, ObSchemaMgrHandle& handle);
  int get_nearest(const int64_t schema_version, const ObSchemaMgr*& schema_mgr, ObSchemaMgrHandle& handle);
  int get_recycle_schema_version(int64_t& schema_version) const;
  int put(ObSchemaMgr* schema_mgr, ObSchemaMgr*& eli_schema_mgr, ObSchemaMgrHandle* handle = NULL);
  int try_gc_tenant_schema_mgr(ObSchemaMgr*& eli_schema_mgr);
  int try_elimiante_schema_mgr(ObSchemaMgr*& eli_schema_mgr);
  void dump() const;

public:
  const static int64_t MAX_SCHEMA_SLOT_NUM = 8 * 1024L;  // 8192
private:
  bool check_inner_stat() const;
  // need process in wlock
  int try_update_latest_schema_idx();

private:
  DISALLOW_COPY_AND_ASSIGN(ObSchemaMgrCache);

private:
  common::TCRWLock lock_;
  ObSchemaMgrItem* schema_mgr_items_;
  int64_t max_cached_num_;
  int64_t last_get_schema_idx_;
  int64_t cur_cached_num_;
  Mode mode_;
  int64_t latest_schema_idx_;
};

}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase
#endif  // OB_OCEANBASE_SCHEMA_SCHEMA_MGR_CACHE_H_
