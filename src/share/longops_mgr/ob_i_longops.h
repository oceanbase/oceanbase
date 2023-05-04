// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_SHARE_LONGOPS_MGR_I_LONGOPS_H_
#define OCEANBASE_SHARE_LONGOPS_MGR_I_LONGOPS_H_

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{

struct ObILongopsKey
{
public:
  ObILongopsKey();
  virtual ~ObILongopsKey() = default;
  virtual int64_t hash() const;
  virtual int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  virtual bool is_valid() const;
  virtual int to_key_string() { return common::OB_NOT_SUPPORTED; }
  bool operator ==(const ObILongopsKey &other) const;
  TO_STRING_KV(K_(tenant_id), K_(sid), K_(name), K_(target));
public:
  uint64_t tenant_id_;
  uint64_t sid_;
  char name_[common::MAX_LONG_OPS_NAME_LENGTH];
  char target_[common::MAX_LONG_OPS_TARGET_LENGTH];
};

enum ObLongopsType
{
  LONGOPS_INVALID = 0,
  LONGOPS_DDL = 1,
  LONGOPS_MAX
};

struct ObLongopsValue final
{
public:
  ObLongopsValue();
  virtual ~ObLongopsValue() = default;
  ObLongopsValue &operator=(const ObLongopsValue &other);
  void reset();
  TO_STRING_KV(K_(tenant_id), K_(trace_id), K_(start_time), K_(finish_time),K_(elapsed_seconds), K_(time_remaining),
               K_(percentage), K_(last_update_time), K_(op_name), K_(target), K_(message));
public:
  common::ObCurTraceId::TraceId trace_id_;
  uint64_t sid_;
  uint64_t tenant_id_;
  int64_t start_time_;
  int64_t finish_time_;
  int64_t elapsed_seconds_;
  int64_t time_remaining_;
  int64_t percentage_;
  int64_t last_update_time_;
  char op_name_[common::MAX_LONG_OPS_NAME_LENGTH];
  char target_[common::MAX_LONG_OPS_TARGET_LENGTH];
  char message_[common::MAX_LONG_OPS_MESSAGE_LENGTH];
};

class ObILongopsStatCollector
{
public:
  virtual int collect(ObLongopsValue &value) = 0;
};

class ObILongopsStat
{
public:
  virtual ~ObILongopsStat() {}
  virtual bool is_valid() const = 0;
  virtual const ObILongopsKey &get_longops_key() const = 0;
  virtual int get_longops_value(ObLongopsValue &value) = 0;
  virtual int64_t to_string(char* buf, const int64_t buf_len) const = 0;
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_LONGOPS_MGR_I_LONGOPS_H_
