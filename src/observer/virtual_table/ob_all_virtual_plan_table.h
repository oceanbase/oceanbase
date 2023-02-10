// Copyright 2010-2016 Alibaba Inc. All Rights Reserved.
// Author:
//   zhenling.zzg
// this file defines interface of __all_virtual_plan_table
#ifndef SRC_OBSERVER_VIRTUAL_PLAN_TABLE_H_
#define SRC_OBSERVER_VIRTUAL_PLAN_TABLE_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/mysql/ob_ra_queue.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace sql
{
class ObPlanItemMgr;
class ObSqlPlanItemRecord;
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
class ObAllVirtualPlanTable : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualPlanTable ();
  virtual ~ObAllVirtualPlanTable();

  int inner_reset();
  int inner_open();
  void set_plan_table_mgr(sql::ObPlanItemMgr *plan_table_mgr)
  { plan_table_mgr_ = plan_table_mgr; }
  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow *&row);

private:
  int fill_cells(sql::ObSqlPlanItemRecord &record);

private:
  enum WAIT_COLUMN
  {
    STATEMENT_ID = common::OB_APP_MIN_COLUMN_ID,
    PLAN_ID,
    TIMESTAMP,
    REMARKS,
    OPERATION,
    OPTIONS,
    OBJECT_NODE,
    OBJECT_OWNER,
    OBJECT_NAME,
    OBJECT_ALIAS,
    OBJECT_INSTANCE,
    OBJECT_TYPE,
    OPTIMZIER,
    SEARCH_COLUMNS,
    ID,
    PARENT_ID,
    DEPTH,
    POSITION,
    COST,
    CARDINALITY,
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
    OTHER_XML
  };

  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualPlanTable);
  sql::ObPlanItemMgr *plan_table_mgr_;
  int64_t start_id_;
  int64_t end_id_;
  int64_t cur_id_;
};
}
}

#endif /* SRC_OBSERVER_VIRTUAL_PLAN_TABLE_H_ */