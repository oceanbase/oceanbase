/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "observer/table_load/client/ob_table_direct_load_rpc_proxy.h"
#include "observer/table_load/ob_table_load_struct.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadClientTask;
class ObTableLoadClientTaskBrief;
class ObTableDirectLoadExecContext;

class ObTableLoadClientService
{
public:
  // client task api
  static int alloc_task(ObTableLoadClientTask *&client_task);
  static void revert_task(ObTableLoadClientTask *client_task);
  static int add_task(ObTableLoadClientTask *client_task);
  static int get_task(const ObTableLoadUniqueKey &key, ObTableLoadClientTask *&client_task);

  // client task brief api
  static int get_task_brief(const ObTableLoadUniqueKey &key,
                            ObTableLoadClientTaskBrief *&client_task_brief);
  static void revert_task_brief(ObTableLoadClientTaskBrief *client_task_brief);

  // for table direct load api
  static int direct_load_operate(ObTableDirectLoadExecContext &ctx,
                                 const table::ObTableDirectLoadRequest &request,
                                 table::ObTableDirectLoadResult &result)
  {
    return ObTableDirectLoadRpcProxy::dispatch(ctx, request, result);
  }

  static int64_t generate_task_id();

private:
  static int64_t next_task_sequence_;
};

} // namespace observer
} // namespace oceanbase
