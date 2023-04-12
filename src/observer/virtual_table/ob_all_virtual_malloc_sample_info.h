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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_MALLOC_SAMPLE_INFO_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_MALLOC_SAMPLE_INFO_H_

#include "lib/container/ob_array.h"
#include "lib/alloc/ob_malloc_sample_struct.h"
#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObMallocSampleInfo : public common::ObVirtualTableScannerIterator
{
public:
  ObMallocSampleInfo();
  virtual ~ObMallocSampleInfo();
  virtual int inner_open();
  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  int fill_row(common::ObNewRow *&row);
private:
  enum CACHE_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    CTX_ID,
    MOD_NAME,
    BACKTRACE,
    CTX_NAME,
    ALLOC_COUNT,
    ALLOC_BYTES,
  };
  char ip_buf_[common::OB_IP_STR_BUFF];
  char bt_[lib::MAX_BACKTRACE_LENGTH];
  lib::ObMallocSampleMap::const_iterator it_;
  lib::ObMallocSampleMap malloc_sample_map_;
  int64_t col_count_;
  bool opened_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMallocSampleInfo);
};
}
}

#endif
