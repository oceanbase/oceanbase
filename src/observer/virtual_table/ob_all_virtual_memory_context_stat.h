/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ALL_VIRTUAL_MEMORY_CONTEXT_STAT_H_
#define OB_ALL_VIRTUAL_MEMORY_CONTEXT_STAT_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/rc/context.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualMemoryContextStat : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualMemoryContextStat();
  virtual ~ObAllVirtualMemoryContextStat();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  char ip_buf_[common::OB_IP_STR_BUFF];
  char entity_buf_[32];
  char p_entity_buf_[32];
  char location_buf_[512];
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualMemoryContextStat);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif /* OB_ALL_VIRTUAL_MEMORY_CONTEXT_STAT_H_ */
