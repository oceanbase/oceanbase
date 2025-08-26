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

#ifndef OB_ALL_VIRTUAL_SS_NOTIFY_TABLETS_STAT_H
#define OB_ALL_VIRTUAL_SS_NOTIFY_TABLETS_STAT_H

#include "lib/container/ob_tuple.h"
#include "ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase
{
namespace sslog
{
class ObSSLogNotifyTask;
class ObSSLogNotifyTaskQueue;
}
namespace observer
{

class SSNotifyTabletsStatApplyOnTabletOp;
class SSNotifyTabletsStatApplyOnLSOp;
class SSNotifyTabletsStatApplyOnTenantOp;

class ObAllVirtualSSNotifyTabletsStat : public common::ObVirtualTableScannerIterator
{
  friend class SSNotifyTabletsStatApplyOnTabletOp;
  friend class SSNotifyTabletsStatApplyOnLSOp;
  friend class SSNotifyTabletsStatApplyOnTenantOp;
  static constexpr int64_t IP_BUFFER_SIZE = MAX_IP_ADDR_LENGTH;
public:
  static constexpr int64_t BUFFER_SIZE = 32_MB;
  explicit ObAllVirtualSSNotifyTabletsStat(omt::ObMultiTenant *omt) : omt_(omt) {}
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  TO_STRING_KV(KP_(omt))
private:
  int convert_tablet_info_to_row_(storage::ObTablet &tablet,
                                  char *buffer,
                                  const int64_t buffer_size,
                                  common::ObNewRow &row);
  int get_primary_key_ranges_();
  int get_tablet_info_(ObLS &ls, const ObFunction<int(ObTablet &)> &apply_on_tablet_op);
  template <typename T>
  bool judege_in_ranges(const T &element, const ObArray<ObTuple<T, T>> &element_ranges) {
    bool in_range = false;
    for (auto &range : element_ranges) {
      if (element >= range.template element<0>() && element <= range.template element<1>()) {
        in_range = true;
        break;
      }
    }
    return in_range;
  }
  bool in_selected_points_(common::ObTabletID tablet_id);
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSSNotifyTabletsStat);
  omt::ObMultiTenant *omt_;
  char ip_buffer_[IP_BUFFER_SIZE];
  ObArray<ObTuple<uint64_t, uint64_t>> tenant_ranges_;
  ObArray<ObTuple<share::ObLSID, share::ObLSID>> ls_ranges_;
  ObArray<ObTuple<common::ObTabletID, common::ObTabletID>> tablet_ranges_;
  ObArray<common::ObTabletID> tablet_points_;
};

} // observer
} // oceanbase
#endif
