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

#ifndef OB_ALL_VIRTUAL_MINOR_FREEZE_INFO_H_
#define OB_ALL_VIRTUAL_MINOR_FREEZE_INFO_H_

#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "lib/guard/ob_shared_guard.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/ob_scanner.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/ls/ob_freezer.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualMinorFreezeInfo : public common::ObVirtualTableScannerIterator,
                                    public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualMinorFreezeInfo();
  virtual ~ObAllVirtualMinorFreezeInfo();
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
  int get_next_ls(ObLS *&ls);
  int generate_memtables_info();
  int get_next_freeze_stat(ObFreezerStat &freeze_stat);
  void append_memtable_info_string(const char *name, const char *str, int64_t &size);
private:
  common::ObAddr addr_;
  int64_t ls_id_;
  ObSharedGuard<storage::ObLSIterator> ls_iter_guard_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  ObStringHolder diagnose_info_;
  common::ObSArray<ObFrozenMemtableInfo> memtables_info_;
  char memtables_info_string_[OB_MAX_CHAR_LENGTH];
private:
  // dont forget update this if add more member of memtable_info
  static constexpr const char *const MEMTABLE_INFO_MEMBER[] = {
      "tablet_id",
      "start_scn",
      "end_scn",
      "write_ref_cnt",
      "unsubmitted_cnt",
      "unsynced_cnt",
      "current_right_boundary"
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualMinorFreezeInfo);
};

}
}
#endif /* OB_ALL_VIRTUAL_MINOR_FREEZE_INFO_H_ */
