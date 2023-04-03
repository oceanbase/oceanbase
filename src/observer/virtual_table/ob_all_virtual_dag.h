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

#ifndef OB_ALL_VIRTUAL_DAG_H_
#define OB_ALL_VIRTUAL_DAG_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/container/ob_array.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"

namespace oceanbase
{
namespace observer
{

/*
 * ObDagInfoIterator
 * */

template <typename T>
class ObDagInfoIterator
{
public:

  ObDagInfoIterator()
    : allocator_("DagInfo"),
    cur_idx_(0),
    is_opened_(false)
  {
  }
  virtual ~ObDagInfoIterator() { reset(); }
  int open(const int64_t tenant_id);
  int get_next_info(T &info);
  void reset();

private:
  common::ObArenaAllocator allocator_;
  common::ObArray<void*> all_tenants_dag_infos_;

  int64_t cur_idx_;
  bool is_opened_;
};

class ObAllVirtualDag : public common::ObVirtualTableScannerIterator
{
public:
  enum COLUMN_ID_LIST
  {
    SVR_IP  = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    DAG_TYPE,
    DAG_KEY,
    DAG_NET_KEY,
    DAG_ID,
    DAG_STATUS,
    RUNNING_TASK_CNT,
    ADD_TIME,
    START_TIME,
    INDEGREE,
    COMMENT
  };
  ObAllVirtualDag();
  virtual ~ObAllVirtualDag();
  int init();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
protected:
  int fill_cells(share::ObDagInfo &dag_warning_info);
private:
  char ip_buf_[common::OB_IP_STR_BUFF];
  char dag_id_buf_[common::OB_TRACE_STAT_BUFFER_SIZE];
  share::ObDagInfo dag_info_;
  ObDagInfoIterator<share::ObDagInfo> dag_info_iter_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualDag);
};


class ObAllVirtualDagScheduler : public common::ObVirtualTableScannerIterator
{
public:
  enum COLUMN_ID_LIST
  {
    SVR_IP  = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    VALUE_TYPE,
    KEY,
    VALUE
  };
  ObAllVirtualDagScheduler();
  virtual ~ObAllVirtualDagScheduler();
  int init();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
protected:
  int fill_cells(share::ObDagSchedulerInfo &dag_scheduler_info);
private:
  char ip_buf_[common::OB_IP_STR_BUFF];
  char dag_id_buf_[common::OB_TRACE_STAT_BUFFER_SIZE];
  share::ObDagSchedulerInfo dag_scheduler_info_;
  ObDagInfoIterator<share::ObDagSchedulerInfo> dag_scheduler_info_iter_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualDagScheduler);
};


} /* namespace observer */
} /* namespace oceanbase */
#endif
