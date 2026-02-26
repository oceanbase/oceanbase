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
#ifndef OB_ALL_VIRTUAL_KV_GROUP_COMMIT_INFO_H_
#define OB_ALL_VIRTUAL_KV_GROUP_COMMIT_INFO_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "observer/table/group/ob_table_tenant_group.h"
#include "observer/table/group/ob_i_table_struct.h"
namespace oceanbase
{
namespace observer
{

class ObAllVirtualKvGroupCommitInfo : public common::ObVirtualTableScannerIterator,
                                      public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualKvGroupCommitInfo()
    : ObVirtualTableScannerIterator(),
      cur_idx_(0),
      group_infos_()
  {}
  virtual ~ObAllVirtualKvGroupCommitInfo() { reset(); }
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  enum GROUP_COLUMN {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    GROUP_TYPE,
    LS_ID,
    TABLE_ID,
    SCHEMA_VERSION,
    QUEUE_SIZE,
    BATCH_SIZE,
    CREATE_TIME,
    UPDATE_TIME
  };
private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;
private:
  int64_t cur_idx_;
  char ipbuf_[common::OB_IP_STR_BUFF];
  ObSEArray<table::ObTableGroupInfo, 128> group_infos_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualKvGroupCommitInfo);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif