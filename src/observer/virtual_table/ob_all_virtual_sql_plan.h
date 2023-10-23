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

#ifndef SRC_OBSERVER_VIRTUAL_SQL_PLAN_H_
#define SRC_OBSERVER_VIRTUAL_SQL_PLAN_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace sql
{
struct ObSqlPlanItem;
}
namespace common
{
class ObIAllocator;
}

namespace share
{
class ObTenantSpaceFetcher;
}

namespace observer
{

class ObAllVirtualSqlPlan : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualSqlPlan ();
  virtual ~ObAllVirtualSqlPlan();

  int inner_open();
  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow *&row);

private:
  int fill_cells(sql::ObSqlPlanItem *plan_item);
  int extract_tenant_and_plan_id(const common::ObIArray<common::ObNewRange> &ranges);
  int dump_all_tenant_plans();
  int dump_tenant_plans(int64_t tenant_id);
  int prepare_next_plan();

private:
  enum WAIT_COLUMN
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    PLAN_ID,
    SVR_IP,
    SVR_PORT,
    SQL_ID,
    DB_ID,
    PLAN_HASH,
    GMT_CREATE,
    OPERATOR,
    OPTIONS,
    OBJECT_NODE,
    OBJECT_ID,
    OBJECT_OWNER,
    OBJECT_NAME,
    OBJECT_ALIAS,
    OBJECT_TYPE,
    OPTIMIZER,
    ID,
    PARENT_ID,
    DEPTH,
    POSITION,
    SEARCH_COLUMNS,
    IS_LAST_CHILD,
    COST,
    REAL_COST,
    CARDINALITY,
    REAL_CARDINALITY,
    BYTES,
    ROWSET,
    OTHER_TAG,
    PARTITION_START,
    PARTITION_STOP,
    PARTITION_ID,
    OTHER,
    DISTRIBUTION,
    CPU_COST,
    IO_COST,
    TEMP_SPACE,
    ACCESS_PREDICATES,
    FILTER_PREDICATES,
    STARTUP_PREDICATES,
    PROJECTION,
    SPECIAL_PREDICATES,
    TIME,
    QBLOCK_NAME,
    REMARKS,
    OTHER_XML
  };

  const static int64_t KEY_TENANT_ID_IDX = 0;
  const static int64_t KEY_PLAN_ID_IDX   = 1;
  const static int64_t KEY_IP_IDX        = 2;
  const static int64_t KEY_PORT_IDX      = 3;
  const static int64_t ROWKEY_COUNT      = 4;
  const static int64_t MAX_LENGTH        = 4000;

  struct PlanInfo {
    PlanInfo();
    virtual ~PlanInfo();
    void reset();
    int64_t plan_id_;
    int64_t tenant_id_;
    TO_STRING_KV(
      K_(plan_id),
      K_(tenant_id)
    );
  };

  struct DumpAllPlan
  {
    DumpAllPlan();
    virtual ~DumpAllPlan();
    void reset();
    int operator()(common::hash::HashMapPair<sql::ObCacheObjID, sql::ObILibCacheObject *> &entry);
    ObSEArray<PlanInfo, 8> *plan_ids_;
    int64_t tenant_id_;
  };

  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSqlPlan);
  ObSEArray<PlanInfo, 8> plan_ids_;
  int64_t plan_idx_;
  //current scan plan info
  ObSEArray<sql::ObSqlPlanItem*, 10> plan_items_;
  int64_t plan_item_idx_;
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  uint64_t db_id_;
  uint64_t plan_hash_;
  int64_t  gmt_create_;
  int64_t tenant_id_;
  int64_t plan_id_;
};
}
}

#endif /* SRC_OBSERVER_VIRTUAL_SQL_PLAN_H_ */