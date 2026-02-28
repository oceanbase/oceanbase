/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ALL_VIRTUAL_TABLET_WINDOW_LOOP_INFO_H_
#define OB_ALL_VIRTUAL_TABLET_WINDOW_LOOP_INFO_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "storage/compaction/ob_window_loop.h"
#include "storage/compaction/ob_window_compaction_utils.h"

namespace oceanbase
{
namespace observer
{

struct ObWindowLoopInfo final
{
public:
  ObWindowLoopInfo() : score_(nullptr), summary_info_(), is_summary_(false) {}
  ~ObWindowLoopInfo() = default;
public:
  compaction::ObTabletCompactionScore *score_;
  ObSqlString summary_info_;
  bool is_summary_;
};

class ObWindowLoopInfoIterator
{
public:
  enum ObWindowInfoIterStep : uint8_t {
    ITER_NOT_INITED = 0,
    ITER_QUEUE_SUMMARY = 1,
    ITER_QUEUE = 2,
    ITER_LIST_SUMMARY = 3,
    ITER_LIST
  };
public:
  ObWindowLoopInfoIterator();
  virtual ~ObWindowLoopInfoIterator() { reset(); }
  int init(compaction::ObWindowLoop &window_loop);
  void reset();
  int get_next_info(ObWindowLoopInfo &info);
private:
  ObArenaAllocator allocator_;
  compaction::ObWindowCompactionPriorityQueueIterator *queue_iter_;
  compaction::ObWindowCompactionReadyListIterator *list_iter_;
  ObWindowInfoIterStep iter_step_;
  bool is_inited_;
};

class ObAllVirtualTabletWindowLoopInfo : public common::ObVirtualTableScannerIterator,
                                         public omt::ObMultiTenantOperator
{
public:
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    LS_ID,
    TABLET_ID,
    INC_ROW_CNT,
    SCORE,
    COMPACT_STATUS,
    ADD_TIMESTAMP,
    QUEUING_MODE,
    IS_HOT,
    IS_INSERT_MOSTLY,
    IS_UPDATE_OR_DELETE_MOSTLY,
    HAS_ACCUMULATED_DELETE,
    NEED_RECYCLE_MDS,
    NEED_PROGRESSIVE_MERGE,
    HAS_SLOW_QUERY,
    QUERY_CNT,
    READ_AMPLIFICATION,
    COMMENT,
  };
  ObAllVirtualTabletWindowLoopInfo();
  virtual ~ObAllVirtualTabletWindowLoopInfo();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual void release_last_tenant() override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  int fill_cells(ObWindowLoopInfo &info);
private:
  char ip_buf_[common::OB_IP_STR_BUFF];
  ObArenaAllocator comment_allocator_;
  ObWindowLoopInfoIterator info_iter_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTabletWindowLoopInfo);
};

} // end namespace observer
} // end namespace oceanbase

#endif