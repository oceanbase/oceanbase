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

#ifndef OCEANBASE_SHARE_OB_FLASHBACK_STANDBY_LOG_TABLE_OPERATOR_H_
#define OCEANBASE_SHARE_OB_FLASHBACK_STANDBY_LOG_TABLE_OPERATOR_H_

#include "lib/ob_define.h"
#include "share/ob_define.h"
#include <share/scn.h>
#include "lib/mysqlclient/ob_mysql_transaction.h"  //ObMySQLTransaction

namespace oceanbase
{
namespace share
{
class ObFlashbackLogSCNType {
public:
  enum OpType {
    INVALID_OP,
    FAILOVER_TO_PRIMARY,
    FLASHBACK_STANDBY_LOG,
    MAX_OP,
  };
  ObFlashbackLogSCNType() : type_(INVALID_OP) {}
  explicit ObFlashbackLogSCNType(const ObFlashbackLogSCNType::OpType &type) : type_(type) {}
  bool operator == (const ObFlashbackLogSCNType &other) const { return type_ == other.type_; }
  bool operator != (const ObFlashbackLogSCNType &other) const { return type_ != other.type_; }

  // assignment
  ObFlashbackLogSCNType &operator=(const ObFlashbackLogSCNType::OpType type)
  {
    type_ = type;
    return *this;
  }
  void reset() { type_ = INVALID_OP; }
  bool is_valid() const { return type_ > INVALID_OP && type_ < MAX_OP; }
  const char* to_str() const;
  TO_STRING_KV("flashback_log_scn_type", to_str(), K_(type));
private:
  OpType type_;
};

static const ObFlashbackLogSCNType INVALID_FLASHBACK_LOG_SCN_TYPE(ObFlashbackLogSCNType::INVALID_OP);
static const ObFlashbackLogSCNType FAILOVER_TO_PRIMARY_TYPE(ObFlashbackLogSCNType::FAILOVER_TO_PRIMARY);
static const ObFlashbackLogSCNType FLASHBACK_STANDBY_LOG_TYPE(ObFlashbackLogSCNType::FLASHBACK_STANDBY_LOG);

class ObFlashbackLogSCNTableOperator
{
public:
  ObFlashbackLogSCNTableOperator() {}
  ~ObFlashbackLogSCNTableOperator() {}
  static int check_is_flashback_log_table_enabled(const uint64_t tenant_id);
  static int insert_flashback_log_scn(
      const uint64_t tenant_id,
      const int64_t switchover_epoch,
      const ObFlashbackLogSCNType& op_type,
      const SCN& flashback_log_scn,
      ObISQLClient *proxy);
  static int check_flashback_log_scn_based_on_switchover_epoch(
      const uint64_t tenant_id,
      const int64_t switchover_epoch,
      const SCN& flashback_log_scn,
      ObISQLClient *proxy);
};
} // namespace share
} // namespace oceanbase
#endif // OCEANBASE_SHARE_OB_FLASHBACK_STANDBY_LOG_TABLE_OPERATOR_H_