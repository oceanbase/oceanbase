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

#ifndef OCEANBASE_SHARE_OB_LS_TABLE_ITERATOR
#define OCEANBASE_SHARE_OB_LS_TABLE_ITERATOR

#include "lib/container/ob_array.h" // ObArray
#include "share/ls/ob_ls_replica_filter.h" // ObLSReplicaFilterHolder
#include "share/schema/ob_table_iter.h"//ObTenantIterator
#include "share/ls/ob_ls_table.h" // ObLSTable::Mode

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
class ObLSTableOperator;
class ObLSInfo;

// This class is used to iterate __all_ls_meta_table for target tenant.
// Used by ObTenantMetaChecker
class ObLSTableIterator
{
public:
  ObLSTableIterator();
  virtual ~ObLSTableIterator() {}
  int init(ObLSTableOperator &lst_operator,
           const uint64_t tenant_id,
           const share::ObLSTable::Mode mode);
  int next(ObLSInfo &ls_info);
  ObLSReplicaFilterHolder &get_filters() { return filters_; }
private:
  int inner_open_();

  bool inited_;
  uint64_t tenant_id_;
  int64_t inner_idx_;
  ObLSTableOperator *lst_operator_;
  common::ObArray<ObLSInfo> inner_ls_infos_;
  ObLSReplicaFilterHolder filters_;
  bool inner_table_only_;
};

// get all ls of each tenant's __all_ls_meta_table
// used by ObEmptyServerChecker
class ObAllLSTableIterator
{
public:
  ObAllLSTableIterator() : inited_(false), lst_operator_(NULL), filters_(), tenant_iter_(),
   inner_idx_(0), inner_ls_infos_(){}
  virtual ~ObAllLSTableIterator() {}
  int init(ObLSTableOperator &lst_operator,
           schema::ObMultiVersionSchemaService &schema_service);
  int next(ObLSInfo &ls_info);
  ObLSReplicaFilterHolder &get_filters() { return filters_; }

private:
  int next_tenant_iter_();
private:
  bool inited_;
  ObLSTableOperator *lst_operator_;
  ObLSReplicaFilterHolder filters_;
  schema::ObTenantIterator tenant_iter_;
  int64_t inner_idx_;
  common::ObArray<ObLSInfo> inner_ls_infos_;
  DISALLOW_COPY_AND_ASSIGN(ObAllLSTableIterator);
};

// get all ls of tenant's __all_ls_meta_table
// used by ObLostReplicaChecker
class ObTenantLSTableIterator
{
public:
  ObTenantLSTableIterator() : inited_(false), tenant_id_(OB_INVALID_TENANT_ID), lst_operator_(NULL),
  filters_(), inner_idx_(0), inner_ls_infos_() {}
  virtual ~ObTenantLSTableIterator() {}
  int init(ObLSTableOperator &lst_operator, const uint64_t meta_tenant_id);
  int next(ObLSInfo &ls_info);
  ObLSReplicaFilterHolder &get_filters() { return filters_; }
private:
  bool inited_;
  uint64_t tenant_id_;
  ObLSTableOperator *lst_operator_;
  ObLSReplicaFilterHolder filters_;
  int64_t inner_idx_;
  common::ObArray<ObLSInfo> inner_ls_infos_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantLSTableIterator);
};

} // end namespace share
} // end namespace oceanbase
#endif
