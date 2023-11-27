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

class ObTabletMetaIterator
{
public:
  ObTabletMetaIterator();
  ~ObTabletMetaIterator() { reset(); }
  virtual void reset();
  virtual int next(ObTabletInfo &tablet_info);
protected:
  int inner_init(
    const uint64_t tenant_id);
  virtual int prefetch() = 0;
protected:
  bool is_inited_;
  int64_t prefetch_tablet_idx_;
  uint64_t tenant_id_;
  common::ObArray<ObTabletInfo> prefetched_tablets_;
  ObTabletReplicaFilterHolder filters_;
};

class ObCompactionTabletMetaIterator : public ObTabletMetaIterator
{
public:
  ObCompactionTabletMetaIterator(
    const bool first_check,
    const int64_t compaction_scn);
  ~ObCompactionTabletMetaIterator() { reset(); }
  int init(
    const uint64_t tenant_id,
    const int64_t batch_size,
    share::ObIServerTrace &server_trace);
  virtual void reset() override;
  virtual int next(ObTabletInfo &tablet_info) override;

private:
   virtual int prefetch() override;
  const static int64_t TABLET_META_TABLE_RANGE_GET_SIZE = 1500;

  bool first_check_;
  int64_t compaction_scn_;
  int64_t batch_size_;
  ObTabletID end_tablet_id_;
};

class ObTenantTabletMetaIterator : public ObTabletMetaIterator
{
public:
  ObTenantTabletMetaIterator();
  virtual ~ObTenantTabletMetaIterator();
  int init(common::ObISQLClient &sql_proxy,
           const uint64_t tenant_id);
  virtual void reset() override;
  void set_batch_size(int64_t batch_size) {tablet_table_operator_.set_batch_size(batch_size);}

private:
  virtual int prefetch() override;
  int prefetch_valid_tablet_ids();
  int prefetch_sys_table_tablet_ids();
  int prefetch_user_table_tablet_ids();
  int prefetch_tablets();

private:
  bool first_prefetch_;
  common::ObISQLClient *sql_proxy_;
  int64_t valid_tablet_ls_pairs_idx_;
  common::ObArray<ObTabletLSPair> valid_tablet_ls_pairs_;
  ObTabletTableOperator tablet_table_operator_;
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
