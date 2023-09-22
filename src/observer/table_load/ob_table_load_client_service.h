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

#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "observer/table_load/client/ob_table_direct_load_rpc_proxy.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "share/table/ob_table_load_define.h"
#include "share/table/ob_table_load_row_array.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTask;
class ObTableLoadClientTask;
class ObTableLoadClientTaskBrief;
class ObTableDirectLoadExecContext;

class ObTableLoadClientService
{
public:
  ObTableLoadClientService();
  ~ObTableLoadClientService();
  int init();

  static ObTableLoadClientService *get_client_service();

  // client task api
  static ObTableLoadClientTask *alloc_task();
  static void free_task(ObTableLoadClientTask *client_task);
  static void revert_task(ObTableLoadClientTask *client_task);
  static int add_task(ObTableLoadClientTask *client_task);
  static int remove_task(ObTableLoadClientTask *client_task);
  static int get_task(const ObTableLoadUniqueKey &key, ObTableLoadClientTask *&client_task);
  static int get_task(const ObTableLoadKey &key, ObTableLoadClientTask *&client_task);
  static int exist_task(const ObTableLoadUniqueKey &key, bool &is_exist);
  static int commit_task(ObTableLoadClientTask *client_task);
  static int abort_task(ObTableLoadClientTask *client_task);
  static int wait_task_finish(const ObTableLoadUniqueKey &key);

  int add_client_task(const ObTableLoadUniqueKey &key, ObTableLoadClientTask *client_task);
  int remove_client_task(const ObTableLoadUniqueKey &key, ObTableLoadClientTask *client_task);
  int get_all_client_task(common::ObIArray<ObTableLoadClientTask *> &client_task_array);
  int get_client_task(const ObTableLoadUniqueKey &key, ObTableLoadClientTask *&client_task);
  int get_client_task_by_table_id(uint64_t table_id, ObTableLoadClientTask *&client_task);
  int exist_client_task(const ObTableLoadUniqueKey &key, bool &is_exist);
  int64_t get_client_task_count() const;
  void purge_client_task();

  // client task brief api
  static int get_task_brief(const ObTableLoadUniqueKey &key,
                            ObTableLoadClientTaskBrief *&client_task_brief);
  static void revert_task_brief(ObTableLoadClientTaskBrief *client_task_brief);
  int get_client_task_brief(const ObTableLoadUniqueKey &key,
                            ObTableLoadClientTaskBrief *&client_task_brief);
  int revert_client_task_brief(ObTableLoadClientTaskBrief *client_task_brief);
  void purge_client_task_brief();

  // for table direct load api
  static int direct_load_operate(ObTableDirectLoadExecContext &ctx,
                                 const table::ObTableDirectLoadRequest &request,
                                 table::ObTableDirectLoadResult &result)
  {
    return ObTableDirectLoadRpcProxy::dispatch(ctx, request, result);
  }

private:
  static int construct_commit_task(ObTableLoadClientTask *client_task);
  static int construct_abort_task(ObTableLoadClientTask *client_task);
private:
  class CommitTaskProcessor;
  class AbortTaskProcessor;
  class CommonTaskCallback;

private:
  static const int64_t CLIENT_TASK_RETENTION_PERIOD = 24LL * 60 * 60 * 1000 * 1000; // 1day
  // key => client_task
  typedef common::hash::ObHashMap<ObTableLoadUniqueKey, ObTableLoadClientTask *,
                                  common::hash::NoPthreadDefendMode>
    ClientTaskMap;
  // table_id => client_task
  typedef common::hash::ObHashMap<uint64_t, ObTableLoadClientTask *,
                                  common::hash::NoPthreadDefendMode>
    ClientTaskIndexMap;
  // key => client_task_brief
  typedef common::ObLinkHashMap<ObTableLoadUniqueKey, ObTableLoadClientTaskBrief>
    ClientTaskBriefMap;

  class HashMapEraseIfEqual
  {
  public:
    HashMapEraseIfEqual(ObTableLoadClientTask *client_task) : client_task_(client_task) {}
    bool operator()(
      common::hash::HashMapPair<ObTableLoadUniqueKey, ObTableLoadClientTask *> &entry) const
    {
      return client_task_ == entry.second;
    }
    bool operator()(common::hash::HashMapPair<uint64_t, ObTableLoadClientTask *> &entry) const
    {
      return client_task_ == entry.second;
    }
  private:
    ObTableLoadClientTask *client_task_;
  };

  class ClientTaskBriefEraseIfExpired
  {
  public:
    ClientTaskBriefEraseIfExpired(int64_t expired_ts) : expired_ts_(expired_ts) {}
    bool operator()(const ObTableLoadUniqueKey &key,
                    ObTableLoadClientTaskBrief *client_task_brief) const;
  private:
    int64_t expired_ts_;
  };

private:
  mutable obsys::ObRWLock rwlock_;
  ClientTaskMap client_task_map_;
  ClientTaskIndexMap client_task_index_map_;
  ClientTaskBriefMap client_task_brief_map_; // thread safety
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
