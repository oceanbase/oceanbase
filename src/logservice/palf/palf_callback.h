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

#ifndef OCEANBASE_LOGSERVICE_PALF_CALLBACK_
#define OCEANBASE_LOGSERVICE_PALF_CALLBACK_
#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/utility/ob_print_utils.h"
#include "lsn.h"
namespace oceanbase
{
namespace common
{
class ObAddr;
}
namespace palf
{
class PalfFSCb
{
public:
  // end_lsn返回的是最后一条已确认日志的下一位置
  virtual int update_end_lsn(int64_t id, const LSN &end_lsn, const int64_t proposal_id) = 0;
};

class PalfRoleChangeCb
{
public:
  virtual int on_role_change(const int64_t id) = 0;
  virtual int on_need_change_leader(const int64_t ls_id, const common::ObAddr &new_leader) = 0;
};

class PalfRebuildCb
{
public:
  // lsn 表示触发rebuild时源端的基线lsn位点
  virtual int on_rebuild(const int64_t id, const LSN &lsn) = 0;
};

class PalfLocationCacheCb
{
public:
  virtual int get_leader(const int64_t id, common::ObAddr &leader) = 0;
  virtual int nonblock_get_leader(const int64_t id, common::ObAddr &leader) = 0;
  virtual int nonblock_renew_leader(const int64_t id) = 0;
};

} // end namespace palf
} // end namespace oceanbase
#endif
