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

#ifndef OB_ALL_VIRTUAL_MDS_EVENT_HISTORY_H
#define OB_ALL_VIRTUAL_MDS_EVENT_HISTORY_H

#include "lib/container/ob_tuple.h"
#include "ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "observer/omt/ob_multi_tenant.h"
#include "ob_mds_event_buffer.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
struct MdsNodeInfoForVirtualTable;
}
}
namespace observer
{

class ObAllVirtualMdsEventHistory : public common::ObVirtualTableScannerIterator
{
  static constexpr int64_t IP_BUFFER_SIZE = 64;
public:
  explicit ObAllVirtualMdsEventHistory(omt::ObMultiTenant *omt) : omt_(omt) {}
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  TO_STRING_KV(K_(tenant_ranges), K_(tenant_points), K_(ls_ranges), K_(ls_points), K_(tablet_ranges), K_(tablet_points))
private:
  int convert_event_info_to_row_(const MdsEventKey &key,
                                 const MdsEvent &event,
                                 char *buffer,
                                 const int64_t buffer_size,
                                 common::ObNewRow &row);
  int get_primary_key_ranges_();
  bool judge_key_in_ranges_(const MdsEventKey &key) const;
  int range_scan_(char *temp_buffer, int64_t buf_len);
  int point_read_(char *temp_buffer, int64_t buf_len);
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualMdsEventHistory);
  omt::ObMultiTenant *omt_;
  char ip_buffer_[IP_BUFFER_SIZE];
  ObArray<ObTuple<uint64_t, uint64_t>> tenant_ranges_;
  ObArray<uint64_t> tenant_points_;
  ObArray<ObTuple<share::ObLSID, share::ObLSID>> ls_ranges_;
  ObArray<share::ObLSID> ls_points_;
  ObArray<ObTuple<common::ObTabletID, common::ObTabletID>> tablet_ranges_;
  ObArray<common::ObTabletID> tablet_points_;
};

} // observer
} // oceanbase
#endif
