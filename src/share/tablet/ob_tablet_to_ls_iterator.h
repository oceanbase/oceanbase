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

#ifndef OCEANBASE_SHARE_TABLET_OB_TABLET_TO_LS_ITERATOR_H
#define OCEANBASE_SHARE_TABLET_OB_TABLET_TO_LS_ITERATOR_H

#include "share/tablet/ob_tablet_to_ls_operator.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
}
namespace share
{
class ObTabletToLSTableOperator;

// ObTenantTabletToLSIterator is used to iterate __all_tablet_to_ls according to target tenant.
class ObTenantTabletToLSIterator
{
public:
  ObTenantTabletToLSIterator();
  virtual ~ObTenantTabletToLSIterator() {}
  int init(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id);
  // init with LS white list
  // ls_white_list: LS white list that only output tablets on the LS white list.
  //                If list is empty, it means ALL LS are in white list.
  int init(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const common::ObIArray<ObLSID> &ls_white_list);
  int next(ObTabletLSPair &pair);
  int next(ObTabletToLSInfo &info);
private:
  int prefetch_();

  bool inited_;
  uint64_t tenant_id_;
  int64_t inner_idx_;
  common::ObSEArray<ObLSID, 1> ls_white_list_;
  common::ObArray<ObTabletToLSInfo> inner_tablet_infos_;
  common::ObISQLClient *sql_proxy_;
};

} // end namespace
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_TABLET_OB_TABLET_TO_LS_ITERATOR_H
