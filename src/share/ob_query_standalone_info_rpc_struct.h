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

#ifndef OCEANBASE_RPC_OB_QUERY_STANDALONE_INFO_RPC_STRUCT_H_
#define OCEANBASE_RPC_OB_QUERY_STANDALONE_INFO_RPC_STRUCT_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace obrpc
{

enum class ObDeployMode : int32_t
{
  INVALID_STATUS       = 0,
  DISTRIBUTED          = 1,
  CENTRALIZED = 2,
};

struct ObQueryDeployModeInfoArg final
{
  OB_UNIS_VERSION(1);
public:
  ObQueryDeployModeInfoArg() {}
  ~ObQueryDeployModeInfoArg() {}
  bool is_valid() const { return true; }
  int assign(const ObQueryDeployModeInfoArg &) { return common::OB_SUCCESS; }
  TO_STRING_KV("empty", 0);
};

struct ObQueryDeployModeInfoResult final
{
  OB_UNIS_VERSION(1);
public:
  ObQueryDeployModeInfoResult() : status_(ObDeployMode::INVALID_STATUS) {}
  ~ObQueryDeployModeInfoResult() {}
  void set_status(const ObDeployMode s) { status_ = s; }
  ObDeployMode get_status() const { return status_; }
  bool is_valid() const { return status_ != ObDeployMode::INVALID_STATUS; }
  bool is_standalone() const { return status_ == ObDeployMode::CENTRALIZED; }
  int assign(const ObQueryDeployModeInfoResult &other)
  { status_ = other.status_; return common::OB_SUCCESS; }
  TO_STRING_KV(K_(status));
private:
  ObDeployMode status_;
};

}//end namespace obrpc
}//end namespace oceanbase
#endif
