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

#ifndef _OB_ALL_PLAN_CACHE_STAT_H_
#define _OB_ALL_PLAN_CACHE_STAT_H_

#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "lib/container/ob_se_array.h"

#include "share/ob_virtual_table_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"

#include "sql/plan_cache/ob_plan_cache_util.h"
namespace oceanbase
{
namespace sql
{
class ObPlanCacheValue;
class ObPlanCacheRlockAndRef;
class ObPlanCache;
class ObPlanStat;
} // end of namespace sql

namespace observer
{

enum ObPlanCacheStatType
{
  TENANT_INVALID = 0,
  TENANT_PLAN_CACHE = 1,
  TENANT_PLAN =2,
};

class ObAllPlanCacheBase : public common::ObVirtualTableIterator
{
public:
  ObAllPlanCacheBase();
  virtual ~ObAllPlanCacheBase();
  int inner_get_next_row(common::ObNewRow *&row);
  void reset();
  // deriative class specific
  virtual int inner_get_next_row() = 0;
protected:
  common::ObSEArray<uint64_t, 16> tenant_id_array_;
  int64_t tenant_id_array_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObAllPlanCacheBase);
};

class ObAllPlanCacheStat : public ObAllPlanCacheBase
{
public:
  ObAllPlanCacheStat() {}
  virtual ~ObAllPlanCacheStat() {}
  virtual int inner_open();
  int inner_get_next_row() { return get_row_from_tenants(); }
protected:
  int get_row_from_tenants();
  int fill_cells(sql::ObPlanCache &plan_cache);
  virtual int get_all_tenant_ids(common::ObIArray<uint64_t> &tenant_ids);
private:
  enum
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    SQL_NUM,
    MEM_USED,
    MEM_HOLD,
    ACCESS_COUNT,
    HIT_COUNT,
    HIT_RATE,
    PLAN_NUM,
    MEM_LIMIT,
    HASH_BUCKET,
    STMTKEY_NUM,
    PC_REF_PLAN_LOCAL,
    PC_REF_PLAN_REMOTE,
    PC_REF_PLAN_DIST,
    PC_REF_PLAN_ARR,
    PC_REF_PLAN_STAT,
    PC_REF_PL,
    PC_REF_PL_STAT,
    PLAN_GEN,
    CLI_QUERY,
    OUTLINE_EXEC,
    PLAN_EXPLAIN,
    ASYN_BASELINE,
    LOAD_BASELINE,
    PS_EXEC,
    GV_SQL,
    PL_ANON,
    PL_ROUTINE,
    PACKAGE_VAR,
    PACKAGE_TYPE,
    PACKAGE_SPEC,
    PACKAGE_BODY,
    PACKAGE_RESV,
    GET_PKG,
    INDEX_BUILDER,
    PCV_SET,
    PCV_RD,
    PCV_WR,
    PCV_GET_PLAN_KEY,
    PCV_GET_PL_KEY,
    PCV_EXPIRE_BY_USED,
    PCV_EXPIRE_BY_MEM,
    LC_REF_CACHE_NODE,
    LC_NODE,
    LC_NODE_RD,
    LC_NODE_WR,
    LC_REF_CACHE_OBJ_STAT,
    PLAN_BASELINE
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllPlanCacheStat);
}; // end of class ObAllPlanCacheStat

class ObAllPlanCacheStatI1 : public ObAllPlanCacheStat
{

public:
  ObAllPlanCacheStatI1() {}
  virtual ~ObAllPlanCacheStatI1() {}
protected:
  int set_tenant_ids(const common::ObIArray<common::ObNewRange> &ranges);
  virtual int get_all_tenant_ids(common::ObIArray<uint64_t> &tenant_ids);
private:
  common::ObSEArray<uint64_t, 16> tenant_ids_;
  DISALLOW_COPY_AND_ASSIGN(ObAllPlanCacheStatI1);
};

} // end of namespace observer
} // end of namespace oceanbase
#endif /* _OB_ALL_PLAN_CACHE_STAT_H_ */
