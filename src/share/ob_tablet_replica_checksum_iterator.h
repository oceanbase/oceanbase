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

#ifndef OCEANBASE_SHARE_OB_TABLET_REPLICA_CHECKSUM_ITERATOR_H_
#define OCEANBASE_SHARE_OB_TABLET_REPLICA_CHECKSUM_ITERATOR_H_

#include "share/ob_tablet_replica_checksum_operator.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
}
namespace share
{
class ObTabletLSPair;

class ObTabletReplicaChecksumIterator
{
public:
  ObTabletReplicaChecksumIterator();
  virtual ~ObTabletReplicaChecksumIterator() { reset(); }

  void reset();
  void reuse();
  int init(const uint64_t tenant_id, common::ObISQLClient *sql_proxy);

  int next(ObTabletReplicaChecksumItem &item);

  void set_compaction_scn(const SCN &compaction_scn) { compaction_scn_ = compaction_scn; }

protected:
  int fetch_next_batch();

private:
  static const int64_t BATCH_FETCH_COUNT = 99;

  bool is_inited_;
  uint64_t tenant_id_;
  SCN compaction_scn_;
  common::ObSEArray<ObTabletReplicaChecksumItem, BATCH_FETCH_COUNT> checksum_items_;
  int64_t cur_idx_;
  common::ObISQLClient *sql_proxy_;

  DISALLOW_COPY_AND_ASSIGN(ObTabletReplicaChecksumIterator);
};

} // share
} // oceanbase

#endif // OCEANBASE_SHARE_OB_TABLET_REPLICA_CHECKSUM_ITERATOR_H_