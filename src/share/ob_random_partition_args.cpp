/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "ob_random_partition_args.h"

namespace oceanbase
{
using namespace share::schema;

namespace obrpc
{
bool ObAlterRandomPartitionArg::is_valid() const
{
  bool is_valid = true;
  return is_valid;
}

void ObAlterRandomPartitionArg::reset()
{
  inactive_tablets_.reset();
  specified_value_ = 0;
}

int ObAlterRandomPartitionArg::assign(const ObAlterRandomPartitionArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    specified_value_ = other.specified_value_;
    if (OB_FAIL(inactive_tablets_.assign(other.inactive_tablets_))) {
      LOG_WARN("failed to assign", K(ret));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObAlterRandomPartitionArg,
                    inactive_tablets_,
                    specified_value_);

bool ObAlterRandomPartitionRes::is_valid() const
{
  bool is_valid = true;
  return is_valid;
}

void ObAlterRandomPartitionRes::reset()
{
  active_tablets_.reset();
}

int ObAlterRandomPartitionRes::assign(const ObAlterRandomPartitionRes &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(active_tablets_.assign(other.active_tablets_))) {
      LOG_WARN("failed to assign", K(ret));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObAlterRandomPartitionRes,
                    active_tablets_);

OB_SERIALIZE_MEMBER(ObBatchGetTabletRandomArg, tenant_id_, ls_id_, tablet_ids_, check_committed_);

int ObBatchGetTabletRandomArg::assign(const ObBatchGetTabletRandomArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else if (OB_FAIL(tablet_ids_.assign(other.tablet_ids_))) {
    LOG_WARN("failed to assign", K(ret));
  } else {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    check_committed_ = other.check_committed_;
  }
  return ret;
}

int ObBatchGetTabletRandomArg::init(const uint64_t tenant_id, const share::ObLSID &ls_id, const ObIArray<ObTabletID> &tablet_ids, const bool check_committed)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || !ls_id.is_valid() || tablet_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id), K(tablet_ids), K(check_committed));
  } else if (OB_FAIL(tablet_ids_.assign(tablet_ids))) {
    LOG_WARN("failed to assign", K(ret));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    check_committed_ = check_committed;
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(*this));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBatchGetTabletRandomRes, random_datas_);

int ObBatchGetTabletRandomRes::assign(const ObBatchGetTabletRandomRes &other)
{
  return random_datas_.assign(other.random_datas_);
}

} // namespace obrpc
} // namespace oceanbase
