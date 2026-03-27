/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ALL_VIRTUAL_DIAG_INDEX_SCAN_H_
#define OB_ALL_VIRTUAL_DIAG_INDEX_SCAN_H_

#include "lib/stat/ob_session_stat.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_range.h"
#include "sql/session/ob_sql_session_mgr.h"


namespace oceanbase
{
namespace observer
{

class ObAllVirtualDiagIndexScan
{
typedef  common::ObSEArray<int64_t, 16> ObIndexArray;
public:
  ObAllVirtualDiagIndexScan() : index_ids_() {}
  virtual ~ObAllVirtualDiagIndexScan() { index_ids_.reset(); }
  int set_index_ids(const common::ObIArray<common::ObNewRange> &ranges);
  // get server sid if sid is client sid
  int get_server_sid_by_client_sid(sql::ObSQLSessionMgr* mgr, uint64_t &sid);
  inline ObIndexArray &get_index_ids() { return index_ids_; }
private:
  ObIndexArray index_ids_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualDiagIndexScan);
};

} /* namespace observer */
} /* namespace oceanbase */

#endif /* OB_ALL_VIRTUAL_DIAG_INDEX_SCAN_H_ */
