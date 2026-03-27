/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
    ROLE,
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
  compaction::ObServerCompactionEventIterator event_iter_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualServerCompactionEventHistory);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif
