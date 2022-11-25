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

#ifndef OB_ALL_VIRTUAL_TRANSACTION_CHECKPOINT_H_
#define OB_ALL_VIRTUAL_TRANSACTION_CHECKPOINT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/checkpoint/ob_common_checkpoint.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{
namespace observer
{
static constexpr const char OB_COMMON_CHECKPOINT[] = "ob_common_checkpoint";
typedef common::ObSimpleIterator<checkpoint::ObCommonCheckpointVTInfo,
  OB_COMMON_CHECKPOINT, 5> ObCommonCheckpointVTIterator;


class ObAllVirtualTransCheckpointInfo : public common::ObVirtualTableScannerIterator,
                                        public omt::ObMultiTenantOperator
{
 public:
  explicit ObAllVirtualTransCheckpointInfo();
  virtual ~ObAllVirtualTransCheckpointInfo();
 public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_addr(common::ObAddr &addr)
  {
    addr_ = addr;
  }
 private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;

  int get_next_ls_(ObLS *&ls);
  int prepare_to_read_();
  int get_next_(storage::checkpoint::ObCommonCheckpointVTInfo &common_checkpoint);
 private:
  common::ObAddr addr_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  char checkpoint_type_buf_[common::MAX_CHECKPOINT_TYPE_BUF_LENGTH];
  // These resources must be released in their own tenant
  int64_t ls_id_;
  ObSharedGuard<storage::ObLSIterator> ls_iter_guard_;
  ObCommonCheckpointVTIterator ob_common_checkpoint_iter_;
  
 private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTransCheckpointInfo);
};
} // observer
} // oceanbase
#endif
