/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_VEC_MERGE_LONGOPS_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_VEC_MERGE_LONGOPS_H_

#include "share/longops_mgr/ob_i_longops.h"

namespace oceanbase
{
namespace share
{

struct ObVecMergeLongopsKey : public ObILongopsKey
{
public:
  ObVecMergeLongopsKey() : task_id_(0) {}
  virtual ~ObVecMergeLongopsKey() = default;
  virtual bool is_valid() const override { return ObILongopsKey::is_valid() && task_id_ > 0; }
  virtual int to_key_string() override;
  INHERIT_TO_STRING_KV("ObILongopsKey", ObILongopsKey, K_(task_id));
public:
  int64_t task_id_;
};

class ObVecMergeLongopsStat : public ObILongopsStat
{
public:
  ObVecMergeLongopsStat()
    : is_inited_(false), tenant_id_(OB_INVALID_ID), task_id_(0), trace_id_(), key_(), start_time_(0) {}
  virtual ~ObVecMergeLongopsStat() = default;
  int init(
      const uint64_t tenant_id,
      const int64_t task_id,
      const common::ObCurTraceId::TraceId &trace_id);
  virtual bool is_valid() const override { return key_.is_valid(); }
  virtual const ObILongopsKey &get_longops_key() const override { return key_; }
  virtual int get_longops_value(ObLongopsValue &value) override;
  TO_STRING_KV(K_(is_inited), K_(key), K_(tenant_id), K_(task_id), K_(trace_id));
private:
  int collect_from_plan_monitor(ObLongopsValue &value);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t task_id_;
  common::ObCurTraceId::TraceId trace_id_;
  ObVecMergeLongopsKey key_;
  int64_t start_time_;
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_VEC_MERGE_LONGOPS_H_
