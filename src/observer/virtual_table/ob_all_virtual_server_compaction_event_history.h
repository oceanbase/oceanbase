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

#ifndef OB_ALL_VIRTUAL_SERVER_COMPACTION_EVENT_HISTORY_H_
#define OB_ALL_VIRTUAL_SERVER_COMPACTION_EVENT_HISTORY_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/compaction/ob_server_compaction_event_history.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualServerCompactionEventHistory : public common::ObVirtualTableScannerIterator
{
public:
  enum COLUMN_ID_LIST
  {
    SVR_IP  = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    ZONE,
    TENANT_ID,
    MERGE_TYPE,
    COMPACTION_SCN,
    EVENT_TIMESTAMP,
    EVENT,
  };
  ObAllVirtualServerCompactionEventHistory();
  virtual ~ObAllVirtualServerCompactionEventHistory();
  int init();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
protected:
  int fill_cells();
private:
  char ip_buf_[common::OB_IP_STR_BUFF];
  char event_buf_[common::OB_COMPACTION_EVENT_STR_LENGTH];
  compaction::ObServerCompactionEvent event_;
  compaction::ObServerCompactionEvent last_event_;
  compaction::ObServerCompactionEventIterator event_iter_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualServerCompactionEventHistory);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif
