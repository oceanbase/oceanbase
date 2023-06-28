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

#ifndef OCEANBASE_SHARE_TABLET_OB_TABLET_TABLE_ITERATOR_H
#define OCEANBASE_SHARE_TABLET_OB_TABLET_TABLE_ITERATOR_H

#include "share/tablet/ob_tablet_filter.h"
#include "share/tablet/ob_tablet_table_operator.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
}
namespace share
{
class ObTabletToLSTableOperator;
class ObTabletTableOperator;
class ObTabletInfo;

class ObTenantTabletMetaIterator
{
public:
  ObTenantTabletMetaIterator();
  virtual ~ObTenantTabletMetaIterator();

  int init(common::ObISQLClient &sql_proxy,
           const uint64_t tenant_id);
  bool is_inited() const { return is_inited_; }
  int next(ObTabletInfo &tablet_info);
  ObTabletReplicaFilterHolder &get_filters() { return filters_; }
  void set_batch_size(int64_t batch_size) {tablet_table_operator_.set_batch_size(batch_size);}

private:
  int prefetch();
  int prefetch_valid_tablet_ids();
  int prefetch_sys_table_tablet_ids();
  int prefetch_user_table_tablet_ids();
  int prefetch_tablets();

private:
  bool is_inited_;
  common::ObISQLClient *sql_proxy_;
  ObTabletTableOperator tablet_table_operator_;
  uint64_t tenant_id_;
  bool first_prefetch_;
  common::ObArray<ObTabletInfo> prefetched_tablets_;
  common::ObArray<ObTabletLSPair> valid_tablet_ls_pairs_;
  int64_t valid_tablet_ls_pairs_idx_;
  int64_t prefetch_tablet_idx_;
  ObTabletReplicaFilterHolder filters_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantTabletMetaIterator);
};

// ObTenantTabletTableIterator is used to iterate __all_tablet_meta_table according to target tenant.
// Notes: This iterator will get residual tablet from meta table. If you want to iterate all valid tablet infos,
//        you'd better use ObTenantTabletMetaIterator.
class ObTenantTabletTableIterator
{
public:
  ObTenantTabletTableIterator();
  virtual ~ObTenantTabletTableIterator() {}
  int init(ObTabletTableOperator &tt_operator, const uint64_t tenant_id);
  int next(ObTabletInfo &tablet_info);
  ObTabletReplicaFilterHolder &get_filters() { return filters_; }
private:
  int prefetch_();

  bool inited_;
  uint64_t tenant_id_;
  int64_t inner_idx_;
  ObTabletTableOperator *tt_operator_;
  common::ObArray<ObTabletInfo> inner_tablet_infos_;
  ObTabletReplicaFilterHolder filters_;
};

} // end namespace
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_TABLET_OB_TABLET_TABLE_ITERATOR_H
