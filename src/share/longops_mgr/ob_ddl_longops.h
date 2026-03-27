// Copyright (c) 2021 OceanBase
// SPDX-License-Identifier: Apache-2.0

#ifndef OCEANBASE_SHARE_LONGOPS_MGR_DDL_LONGOPS_H_
#define OCEANBASE_SHARE_LONGOPS_MGR_DDL_LONGOPS_H_

#include "ob_i_longops.h"

namespace oceanbase
{
namespace rootserver
{
class ObDDLTask;
class ObDDLTaskRecord;
}
namespace share
{
struct ObDDLLongopsKey : public ObILongopsKey
{
public:
  ObDDLLongopsKey();
  virtual ~ObDDLLongopsKey() = default;
  virtual bool is_valid() const { return ObILongopsKey::is_valid() && task_id_ >= 0; }
  virtual int to_key_string() override;
  INHERIT_TO_STRING_KV("ObILongopsKey", ObILongopsKey, K_(task_id));
public:
  int64_t task_id_;
};

class ObDDLLongopsStatCollector : public ObILongopsStatCollector
{
public:
  ObDDLLongopsStatCollector();
  virtual ~ObDDLLongopsStatCollector() = default;
  int init(rootserver::ObDDLTask *ddl_task);
  virtual int collect(ObLongopsValue &value) override;
private:
  bool is_inited_;
  rootserver::ObDDLTask *ddl_task_;
};

class ObDDLLongopsStat : public ObILongopsStat
{
public:
  ObDDLLongopsStat();
  virtual ~ObDDLLongopsStat() = default;
  int init(rootserver::ObDDLTask *ddl_task);
  virtual bool is_valid() const override { return key_.is_valid(); }
  virtual const ObILongopsKey &get_longops_key() const override { return key_; }
  virtual int get_longops_value(ObLongopsValue &value) override;
  TO_STRING_KV(K_(is_inited), K_(key), K_(value));
private:
  bool is_inited_;
  ObDDLLongopsKey key_;
  ObLongopsValue value_;
  ObDDLLongopsStatCollector collector_;
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_LONGOPS_MGR_DDL_LONGOPS_H_
