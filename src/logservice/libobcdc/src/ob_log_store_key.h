/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIBOBCDC_STORE_KEY_H_
#define OCEANBASE_LIBOBCDC_STORE_KEY_H_

#include <stdint.h>
#include <string>
#include "lib/utility/ob_macro_utils.h"       // DISALLOW_COPY_AND_ASSIGN
#include "ob_log_utils.h"                     // logservice::TenantLSID

namespace oceanbase
{
namespace libobcdc
{
class ObLogStoreKey
{
public:
  ObLogStoreKey();
  ~ObLogStoreKey();
  void reset();
  int init(const logservice::TenantLSID &tenant_ls_id, const palf::LSN &log_lsn);
  bool is_valid() const;
  uint64_t get_tenant_id() const { return tenant_ls_id_.get_tenant_id(); }

public:
  int get_key(std::string &key);
  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  logservice::TenantLSID  tenant_ls_id_;
  // StorageKey: tenant_ls_id_+log_lsn_
  // Log LSN, for redo data
  // 1. non-LOB record corresponding to LogEntry log_lsn
  // 2. First LogEntry log_lsn for LOB records
  palf::LSN     log_lsn_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogStoreKey);
};

}; // end namespace libobcdc
}; // end namespace oceanbase
#endif
