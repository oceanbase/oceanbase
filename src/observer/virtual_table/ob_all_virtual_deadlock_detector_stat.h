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

#ifndef OB_ALL_VIRTUAL_DEADLOCK_DETECTOR_STAT_H
#define OB_ALL_VIRTUAL_DEADLOCK_DETECTOR_STAT_H

#include "lib/container/ob_tuple.h"
#include "ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "observer/omt/ob_multi_tenant.h"
namespace oceanbase
{
namespace share
{
namespace detector
{
struct UserBinaryKey;
struct ObIDeadLockDetector;
}
}
namespace observer
{

class ObAllVirtualDeadLockDetectorStat : public common::ObVirtualTableScannerIterator
{
  struct IterNodeOp {
    IterNodeOp(ObAllVirtualDeadLockDetectorStat *p_stat,
                    char *temp_buffer)
    : this_(p_stat),
    temp_buffer_(temp_buffer) {}
    int operator()(const share::detector::UserBinaryKey &key,
                   share::detector::ObIDeadLockDetector *detector);
    ObAllVirtualDeadLockDetectorStat* this_;
    char *temp_buffer_;
  };
  struct IterateTenantOp {
    IterateTenantOp(ObAllVirtualDeadLockDetectorStat *p_stat,
                    char *temp_buffer)
    : this_(p_stat),
    temp_buffer_(temp_buffer) {}
    int operator()();
    ObAllVirtualDeadLockDetectorStat* this_;
    char *temp_buffer_;
  };
  friend class IterNodeOp;
  friend class IterateTenantOp;
  static constexpr int64_t IP_BUFFER_SIZE = 64;
public:
  explicit ObAllVirtualDeadLockDetectorStat(omt::ObMultiTenant *omt) : omt_(omt) {}
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  TO_STRING_KV(KP_(omt))
private:
  int convert_node_info_to_row_(const bool need_fill_conflict_actions_flag,
                                share::detector::ObIDeadLockDetector *detector,
                                char *buffer,
                                const int64_t buffer_size,
                                common::ObNewRow &row);
  int get_primary_key_ranges_();
  bool is_in_selected_tenants_();
  bool is_in_selected_id_ranges_(int64_t detector_id);
  bool is_in_selected_id_points_(int64_t detector_id);
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualDeadLockDetectorStat);
  omt::ObMultiTenant *omt_;
  char ip_buffer_[IP_BUFFER_SIZE];
  ObArray<ObTuple<uint64_t, uint64_t>> tenant_ranges_;
  ObArray<ObTuple<int64_t, int64_t>> detector_id_ranges_;
  ObArray<int64_t> detector_id_points_;
};

} // observer
} // oceanbase
#endif
