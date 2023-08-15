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

#ifndef OB_ALL_VIRTUAL_MASTER_KEY_VERSION_INFO_H_
#define OB_ALL_VIRTUAL_MASTER_KEY_VERSION_INFO_H_
#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualMasterKeyVersionInfo : public common::ObVirtualTableScannerIterator
{
public:

#ifndef OB_BUILD_TDE_SECURITY
  ObAllVirtualMasterKeyVersionInfo() {}
  virtual ~ObAllVirtualMasterKeyVersionInfo() {}
  virtual int inner_get_next_row(common::ObNewRow *&row) { return OB_ITER_END; }
#else
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    MAX_ACTIVE_VERSION,
    MAX_STORED_VERSION,
    EXPECT_VERSION
  };
  ObAllVirtualMasterKeyVersionInfo();
  virtual ~ObAllVirtualMasterKeyVersionInfo();
  virtual void reset();
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  int gen_row(common::ObNewRow *&row);
  int fill_tenant_ids();

  char ip_buf_[common::OB_IP_STR_BUFF];
  ObSEArray<uint64_t, 16> tenant_ids_;
  int64_t tenant_ids_idx_;
#endif
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualMasterKeyVersionInfo);
};

} /* namespace observer */
} /* namespace oceanbase */

#endif /* OB_ALL_VIRTUAL_MASTER_KEY_VERSION_INFO_H_ */
