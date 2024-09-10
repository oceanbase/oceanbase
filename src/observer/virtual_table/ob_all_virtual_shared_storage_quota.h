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

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SHARED_STORAGE_QUOTA_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SHARED_STORAGE_QUOTA_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "src/observer/ob_server_struct.h"
namespace oceanbase
{
namespace observer
{
class ObVirtualSharedStorageQuota : public common::ObVirtualTableScannerIterator
{
  enum COLUMN_ID_LIST {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    MODULE,
    CLASS_ID,
    STORAGE_ID,
    TYPE,
    REQUIRE,
    ASSIGN
  };

public:
  ObVirtualSharedStorageQuota();
  virtual ~ObVirtualSharedStorageQuota() override;
  virtual int inner_open() override;
  virtual void reset() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;

private:
  bool is_inited_;
  char ip_buf_[common::OB_IP_STR_BUFF];

private:
  int add_one_storage_batch_row();
  int add_row(const obrpc::ObSharedDeviceResource &usage, const obrpc::ObSharedDeviceResource &limit);
  DISALLOW_COPY_AND_ASSIGN(ObVirtualSharedStorageQuota);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SHARED_STORAGE_QUOTA_H_ */