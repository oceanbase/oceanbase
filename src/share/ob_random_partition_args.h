/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_RANDOM_PARTITION_ARGS_H_
#define OCEANBASE_SHARE_OB_RANDOM_PARTITION_ARGS_H_

#include "lib/ob_define.h"
#include "share/schema/ob_schema_struct.h"
#include "storage/tablet/ob_tablet_random_mds_user_data.h"

namespace oceanbase
{
namespace obrpc
{

struct ObAlterRandomPartitionArg final
{
  OB_UNIS_VERSION(1);
public:
  ObAlterRandomPartitionArg() :
    inactive_tablets_(),
    specified_value_(0)
  {
  }
  ~ObAlterRandomPartitionArg() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObAlterRandomPartitionArg &other);

  TO_STRING_KV(K_(inactive_tablets),
               K_(specified_value));
public:
  ObSArray<ObTabletID> inactive_tablets_;
  uint64_t specified_value_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterRandomPartitionArg);
};

struct ObBatchGetTabletRandomArg final
{
  OB_UNIS_VERSION(1);
public:
  ObBatchGetTabletRandomArg()
    : tenant_id_(OB_INVALID_ID), ls_id_(), tablet_ids_(), check_committed_(false)
  {}
  ~ObBatchGetTabletRandomArg() {}
public:
  int assign(const ObBatchGetTabletRandomArg &other);
  bool is_valid() const { return tenant_id_ != OB_INVALID_ID && ls_id_.is_valid() && tablet_ids_.count() > 0; }
  int init(const uint64_t tenant_id, const share::ObLSID &ls_id, const common::ObIArray<common::ObTabletID> &tablet_ids, const bool check_committed);
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_ids), K_(check_committed));
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObSArray<common::ObTabletID> tablet_ids_;
  bool check_committed_;
};

struct ObBatchGetTabletRandomRes final
{
  OB_UNIS_VERSION(1);
public:
  ObBatchGetTabletRandomRes() : random_datas_() {}
  ~ObBatchGetTabletRandomRes() {}
public:
  int assign(const ObBatchGetTabletRandomRes &other);
  bool is_valid() const { return random_datas_.count() > 0; }
  TO_STRING_KV(K_(random_datas));
public:
  common::ObSArray<ObTabletRandomMdsUserData> random_datas_;
};

struct ObAlterRandomPartitionRes final
{
  OB_UNIS_VERSION(1);
public:
  ObAlterRandomPartitionRes() : active_tablets_() {}
  ~ObAlterRandomPartitionRes() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObAlterRandomPartitionRes &other);

  TO_STRING_KV(K_(active_tablets));
public:
  ObSArray<ObTabletID> active_tablets_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterRandomPartitionRes);
};

} // namespace obrpc
} // namespace oceanbase
#endif
