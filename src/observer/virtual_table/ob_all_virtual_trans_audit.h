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

#ifndef OB_ALL_VIRTUAL_TRANS_AUDIT_H_
#define OB_ALL_VIRTUAL_TRANS_AUDIT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/transaction/ob_trans_audit_record_mgr.h"

namespace oceanbase {
namespace observer {
class ObAllVirtualTransAudit : public common::ObVirtualTableScannerIterator {
public:
  ObAllVirtualTransAudit();
  virtual ~ObAllVirtualTransAudit()
  {
    reset();
  }

public:
  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow*& row) override;
  virtual void reset() override;

private:
  int fill_cells_(
      const transaction::ObTransAuditCommonInfo& common_info, const transaction::ObTransAuditInfo& trans_info);
  int extract_tenant_ids_();

private:
  static const int64_t OB_MAX_BUFFER_SIZE = 2048;
  static const int64_t OB_MIN_BUFFER_SIZE = 128;
  char ip_buffer_[common::OB_IP_STR_BUFF];
  char trans_id_buffer_[OB_MIN_BUFFER_SIZE];
  char proxy_sessid_buffer_[OB_MIN_BUFFER_SIZE];
  char elr_trans_info_buffer_[OB_MAX_BUFFER_SIZE];
  char trace_log_buffer_[OB_MAX_BUFFER_SIZE];
  char trans_param_buffer_[OB_MAX_BUFFER_SIZE];
  char partition_buffer_[OB_MIN_BUFFER_SIZE];

  common::ObSEArray<uint64_t, 16> tenant_id_array_;
  int64_t tenant_id_array_idx_;
  share::ObTenantSpaceFetcher* with_tenant_ctx_;
  transaction::ObTransAuditDataIterator audit_iter_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTransAudit);
};

}  // namespace observer
}  // namespace oceanbase

#endif /* OB_ALL_VIRTUAL_TRANS_Audit_H_ */
