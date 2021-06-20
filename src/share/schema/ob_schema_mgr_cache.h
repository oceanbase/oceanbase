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
  ObSchemaMgrItem() : schema_mgr_(NULL), ref_cnt_(0)
  {}

  ObSchemaMgr* schema_mgr_;
  int64_t ref_cnt_ CACHE_ALIGNED;
};

class ObSchemaMgrHandle {
public:
  ObSchemaMgrHandle();
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
