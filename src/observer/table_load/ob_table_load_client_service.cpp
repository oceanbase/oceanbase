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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_client_service.h"
#include "observer/table_load/ob_table_load_client_task.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace table;

/**
 * CommitTaskProcessor
 */

class ObTableLoadClientService::CommitTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  CommitTaskProcessor(ObTableLoadTask &task, ObTableLoadClientTask *client_task)
    : ObITableLoadTaskProcessor(task), client_task_(client_task), table_ctx_(nullptr)
  {
    client_task_->inc_ref_count();
  }
  virtual ~CommitTaskProcessor()
  {
    ObTableLoadClientService::revert_task(client_task_);
    if (nullptr != table_ctx_) {
      ObTableLoadService::put_ctx(table_ctx_);
    }
  }
  int process() override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(init())) {
      LOG_WARN("fail to init", KR(ret));
    }
    // 1. finish all trans
    else if (OB_FAIL(finish_all_trans())) {
      LOG_WARN("fail to finish all trans", KR(ret));
    }
    // 2. check all trans commit
    else if (OB_FAIL(check_all_trans_commit())) {
      LOG_WARN("fail to check all trans commit", KR(ret));
    }
    // 3. finish
    else if (OB_FAIL(finish())) {
      LOG_WARN("fail to finish table load", KR(ret));
    }
    // 4. check merged
    else if (OB_FAIL(check_merged())) {
      LOG_WARN("fail to check merged", KR(ret));
    }
    // 5. commit
    else if (OB_FAIL(commit())) {
      LOG_WARN("fail to commit table load", KR(ret));
    }
    // end
    else if (OB_FAIL(client_task_->set_status_commit())) {
      LOG_WARN("fail to set status commit", KR(ret));
    }
    // auto abort
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(OB_TMP_FAIL(ObTableLoadClientService::abort_task(client_task_)))) {
        LOG_WARN("fail to abort client task", KR(tmp_ret));
      }
    }
    return ret;
  }
private:
  int init()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(client_task_->check_status(ObTableLoadClientStatus::COMMITTING))) {
      LOG_WARN("fail to check status", KR(ret));
    } else {
      if (OB_FAIL(client_task_->get_table_ctx(table_ctx_))) {
        LOG_WARN("fail to get table ctx", KR(ret));
      }
      if (OB_FAIL(ret)) {
        client_task_->set_status_error(ret);
      }
    }
    return ret;
  }
  int finish_all_trans()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(client_task_->check_status(ObTableLoadClientStatus::COMMITTING))) {
      LOG_WARN("fail to check status", KR(ret));
    } else {
      const ObIArray<ObTableLoadTransId> &trans_ids = client_task_->get_trans_ids();
      ObTableLoadCoordinator coordinator(table_ctx_);
      if (OB_FAIL(coordinator.init())) {
        LOG_WARN("fail to init coordinator", KR(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < trans_ids.count(); ++i) {
        const ObTableLoadTransId &trans_id = trans_ids.at(i);
        if (OB_FAIL(coordinator.finish_trans(trans_id))) {
          LOG_WARN("fail to coordinator finish trans", KR(ret), K(i), K(trans_id));
        }
      }
      if (OB_FAIL(ret)) {
        client_task_->set_status_error(ret);
      }
    }
    return ret;
  }
  int check_all_trans_commit()
  {
    int ret = OB_SUCCESS;
    const ObIArray<ObTableLoadTransId> &trans_ids = client_task_->get_trans_ids();
    while (OB_SUCC(ret)) {
      if (OB_FAIL(client_task_->check_status(ObTableLoadClientStatus::COMMITTING))) {
        LOG_WARN("fail to check status", KR(ret));
      } else {
        if (OB_FAIL(client_task_->get_exec_ctx()->check_status())) {
          LOG_WARN("fail to check exec status", KR(ret));
        } else {
          bool all_commit = true;
          ObTableLoadCoordinator coordinator(table_ctx_);
          if (OB_FAIL(coordinator.init())) {
            LOG_WARN("fail to init coordinator", KR(ret));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < trans_ids.count(); ++i) {
            const ObTableLoadTransId &trans_id = trans_ids.at(i);
            ObTableLoadTransStatusType trans_status = ObTableLoadTransStatusType::NONE;
            int error_code = OB_SUCCESS;
            if (OB_FAIL(coordinator.get_trans_status(trans_id, trans_status, error_code))) {
              LOG_WARN("fail to coordinator get status", KR(ret), K(i), K(trans_id));
            } else {
              switch (trans_status) {
                case ObTableLoadTransStatusType::FROZEN:
                  all_commit = false;
                  break;
                case ObTableLoadTransStatusType::COMMIT:
                  break;
                case ObTableLoadTransStatusType::ERROR:
                  ret = error_code;
                  LOG_WARN("trans has error", KR(ret));
                  break;
                default:
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected trans status", KR(ret), K(trans_status));
                  break;
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (all_commit) {
              break;
            } else {
              ob_usleep(1000 * 1000);
            }
          }
        }
        if (OB_FAIL(ret)) {
          client_task_->set_status_error(ret);
        }
      }
    }
    return ret;
  }
  int finish()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(client_task_->check_status(ObTableLoadClientStatus::COMMITTING))) {
      LOG_WARN("fail to check status", KR(ret));
    } else {
      ObTableLoadCoordinator coordinator(table_ctx_);
      if (OB_FAIL(coordinator.init())) {
        LOG_WARN("fail to init coordinator", KR(ret));
      } else if (OB_FAIL(coordinator.finish())) {
        LOG_WARN("fail to coordinator finish", KR(ret));
      }
      if (OB_FAIL(ret)) {
        client_task_->set_status_error(ret);
      }
    }
    return ret;
  }
  int check_merged()
  {
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(client_task_->check_status(ObTableLoadClientStatus::COMMITTING))) {
        LOG_WARN("fail to check status", KR(ret));
      } else {
        if (OB_FAIL(client_task_->get_exec_ctx()->check_status())) {
          LOG_WARN("fail to check exec status", KR(ret));
        } else {
          bool is_merged = false;
          ObTableLoadStatusType status = ObTableLoadStatusType::NONE;
          int error_code = OB_SUCCESS;
          ObTableLoadCoordinator coordinator(table_ctx_);
          if (OB_FAIL(coordinator.init())) {
            LOG_WARN("fail to init coordinator", KR(ret));
          } else if (OB_FAIL(coordinator.get_status(status, error_code))) {
            LOG_WARN("fail to coordinator get status", KR(ret));
          } else {
            switch (status) {
              case ObTableLoadStatusType::FROZEN:
              case ObTableLoadStatusType::MERGING:
                is_merged = false;
                break;
              case ObTableLoadStatusType::MERGED:
                is_merged = true;
                break;
              case ObTableLoadStatusType::ERROR:
                ret = error_code;
                LOG_WARN("table load has error", KR(ret));
                break;
              default:
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected status", KR(ret), K(status));
                break;
            }
          }
          if (OB_SUCC(ret)) {
            if (is_merged) {
              break;
            } else {
              ob_usleep(1000 * 1000);
            }
          }
        }
        if (OB_FAIL(ret)) {
          client_task_->set_status_error(ret);
        }
      }
    }
    return ret;
  }
  int commit()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(client_task_->check_status(ObTableLoadClientStatus::COMMITTING))) {
      LOG_WARN("fail to check status", KR(ret));
    } else {
      ObTableLoadCoordinator coordinator(table_ctx_);
      if (OB_FAIL(coordinator.init())) {
        LOG_WARN("fail to init coordinator", KR(ret));
      } else if (OB_FAIL(coordinator.commit(client_task_->result_info_))) {
        LOG_WARN("fail to coordinator commit", KR(ret));
      }
      if (OB_FAIL(ret)) {
        client_task_->set_status_error(ret);
      }
    }
    return ret;
  }
private:
  ObTableLoadClientTask *client_task_;
  ObTableLoadTableCtx *table_ctx_;
};

/**
 * AbortTaskProcessor
 */

class ObTableLoadClientService::AbortTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  AbortTaskProcessor(ObTableLoadTask &task, ObTableLoadClientTask *client_task)
    : ObITableLoadTaskProcessor(task), client_task_(client_task)
  {
    client_task_->inc_ref_count();
  }
  virtual ~AbortTaskProcessor()
  {
    ObTableLoadClientService::revert_task(client_task_);
  }
  int process() override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(client_task_->check_status(ObTableLoadClientStatus::ABORT))) {
      LOG_WARN("fail to check status", KR(ret));
    } else {
      ObTableLoadTableCtx *table_ctx = nullptr;
      if (OB_FAIL(client_task_->get_table_ctx(table_ctx))) {
        LOG_WARN("fail to get table ctx", KR(ret));
      } else {
        ObTableLoadCoordinator::abort_ctx(table_ctx);
      }
      if (nullptr != table_ctx) {
        ObTableLoadService::put_ctx(table_ctx);
        table_ctx = nullptr;
      }
    }
    return ret;
  }
private:
  ObTableLoadClientTask *client_task_;
};

/**
 * CommonTaskCallback
 */

class ObTableLoadClientService::CommonTaskCallback : public ObITableLoadTaskCallback
{
public:
  CommonTaskCallback(ObTableLoadClientTask *client_task) : client_task_(client_task)
  {
    client_task_->inc_ref_count();
  }
  virtual ~CommonTaskCallback()
  {
    ObTableLoadClientService::revert_task(client_task_);
  }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    client_task_->free_task(task);
  }
private:
  ObTableLoadClientTask *client_task_;
};

/**
 * ClientTaskBriefEraseIfExpired
 */

bool ObTableLoadClientService::ClientTaskBriefEraseIfExpired::operator()(
  const ObTableLoadUniqueKey &key, ObTableLoadClientTaskBrief *client_task_brief) const
{
  return client_task_brief->active_time_ < expired_ts_;
}

/**
 * ObTableLoadClientService
 */

ObTableLoadClientService::ObTableLoadClientService() : is_inited_(false) {}

ObTableLoadClientService::~ObTableLoadClientService() {}

int ObTableLoadClientService::init()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadClientService init twice", KR(ret), KP(this));
  } else {
    if (OB_FAIL(
          client_task_map_.create(bucket_num, "TLD_ClientTask", "TLD_ClientTask", MTL_ID()))) {
      LOG_WARN("fail to create hashmap", KR(ret), K(bucket_num));
    } else if (OB_FAIL(client_task_index_map_.create(bucket_num, "TLD_ClientTask", "TLD_ClientTask",
                                                     MTL_ID()))) {
      LOG_WARN("fail to create hashmap", KR(ret), K(bucket_num));
    } else if (OB_FAIL(client_task_brief_map_.init("TLD_ClientBrief", MTL_ID()))) {
      LOG_WARN("fail to init link hashmap", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

ObTableLoadClientService *ObTableLoadClientService::get_client_service()
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  ObTableLoadClientService *client_service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    client_service = &service->get_client_service();
  }
  return client_service;
}

ObTableLoadClientTask *ObTableLoadClientService::alloc_task()
{
  ObTableLoadClientTask *client_task =
    OB_NEW(ObTableLoadClientTask, ObMemAttr(MTL_ID(), "TLD_ClientTask"));
  if (nullptr != client_task) {
    client_task->inc_ref_count();
  }
  return client_task;
}

void ObTableLoadClientService::free_task(ObTableLoadClientTask *client_task)
{
  if (OB_NOT_NULL(client_task)) {
    OB_ASSERT(0 == client_task->get_ref_count());
    OB_DELETE(ObTableLoadClientTask, "TLD_ClientTask", client_task);
    client_task = nullptr;
  }
}

void ObTableLoadClientService::revert_task(ObTableLoadClientTask *client_task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(client_task));
  } else {
    const int64_t ref_count = client_task->dec_ref_count();
    OB_ASSERT(ref_count >= 0);
    if (0 == ref_count) {
      const uint64_t tenant_id = client_task->tenant_id_;
      const uint64_t table_id = client_task->table_id_;
      const uint64_t hidden_table_id = client_task->ddl_param_.dest_table_id_;
      const int64_t task_id = client_task->ddl_param_.task_id_;
      LOG_INFO("free client task", K(tenant_id), K(table_id), K(hidden_table_id), K(task_id),
               KP(client_task));
      free_task(client_task);
      client_task = nullptr;
    }
  }
}

int ObTableLoadClientService::add_task(ObTableLoadClientTask *client_task)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    ObTableLoadUniqueKey key(client_task->table_id_, client_task->ddl_param_.task_id_);
    ret = service->get_client_service().add_client_task(key, client_task);
  }
  return ret;
}

int ObTableLoadClientService::remove_task(ObTableLoadClientTask *client_task)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    ObTableLoadUniqueKey key(client_task->table_id_, client_task->ddl_param_.task_id_);
    ret = service->get_client_service().remove_client_task(key, client_task);
  }
  return ret;
}

int ObTableLoadClientService::get_task(const ObTableLoadUniqueKey &key,
                                       ObTableLoadClientTask *&client_task)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    if (OB_FAIL(service->get_client_service().get_client_task(key, client_task))) {
      LOG_WARN("fail to get client task", KR(ret), K(key));
    }
  }
  return ret;
}

int ObTableLoadClientService::get_task(const ObTableLoadKey &key,
                                       ObTableLoadClientTask *&client_task)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    if (OB_FAIL(
          service->get_client_service().get_client_task_by_table_id(key.table_id_, client_task))) {
      LOG_WARN("fail to get client task", KR(ret), K(key));
    }
  }
  return ret;
}

int ObTableLoadClientService::exist_task(const ObTableLoadUniqueKey &key, bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    if (OB_FAIL(service->get_client_service().exist_client_task(key, is_exist))) {
      LOG_WARN("fail to check exist client task", KR(ret), K(key));
    }
  }
  return ret;
}

int ObTableLoadClientService::commit_task(ObTableLoadClientTask *client_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == client_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(client_task));
  } else if (OB_FAIL(client_task->set_status_committing())) {
    LOG_WARN("fail to set status committing", KR(ret));
  } else {
    LOG_INFO("client task commit");
    if (OB_FAIL(construct_commit_task(client_task))) {
      LOG_WARN("fail to construct commit task", KR(ret));
    }
    if (OB_FAIL(ret)) {
      client_task->set_status_error(ret);
    }
  }
  return ret;
}

int ObTableLoadClientService::abort_task(ObTableLoadClientTask *client_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == client_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(client_task));
  } else if (ObTableLoadClientStatus::ABORT == client_task->get_status()) {
    // already abort
  } else {
    LOG_INFO("client task abort");
    client_task->set_status_abort();
    if (OB_FAIL(construct_abort_task(client_task))) {
      LOG_WARN("fail to construct abort task", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadClientService::wait_task_finish(const ObTableLoadUniqueKey &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(key));
  } else {
    bool is_exist = true;
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, 10LL * 1000 * 1000))) {
      LOG_WARN("fail to set default timeout ctx", KR(ret));
    }
    while (OB_SUCC(ret) && is_exist) {
      if (ctx.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("timeouted", KR(ret), K(ctx));
      } else if (OB_FAIL(exist_task(key, is_exist))) {
        LOG_WARN("fail to check exist client task", KR(ret), K(key));
      } else if (is_exist) {
        // wait
        ob_usleep(100LL * 1000);
      }
    }
  }
  return ret;
}

int ObTableLoadClientService::add_client_task(const ObTableLoadUniqueKey &key,
                                              ObTableLoadClientTask *client_task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!key.is_valid() || nullptr == client_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(key), KP(client_task));
  } else {
    const uint64_t table_id = key.table_id_;
    obsys::ObWLockGuard guard(rwlock_);
    if (OB_FAIL(client_task_map_.set_refactored(key, client_task))) {
      if (OB_UNLIKELY(OB_HASH_EXIST != ret)) {
        LOG_WARN("fail to set refactored", KR(ret), K(key));
      } else {
        ret = OB_ENTRY_EXIST;
      }
    }
    // force update client task index
    else if (OB_FAIL(client_task_index_map_.set_refactored(table_id, client_task, 1))) {
      LOG_WARN("fail to set refactored", KR(ret), K(table_id));
      // erase from client task map, avoid wild pointer is been use
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(client_task_map_.erase_refactored(key))) {
        LOG_WARN("fail to erase refactored", KR(tmp_ret), K(key));
      }
    } else {
      client_task->inc_ref_count(); // hold by map
    }
  }
  return ret;
}

int ObTableLoadClientService::remove_client_task(const ObTableLoadUniqueKey &key,
                                                 ObTableLoadClientTask *client_task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(key));
  } else {
    const uint64_t table_id = key.table_id_;
    HashMapEraseIfEqual erase_if_equal(client_task);
    bool is_erased = false;
    obsys::ObWLockGuard guard(rwlock_);
    if (OB_FAIL(client_task_map_.erase_if(key, erase_if_equal, is_erased))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST == ret)) {
        LOG_WARN("fail to erase refactored", KR(ret), K(key));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else if (OB_UNLIKELY(!is_erased)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected client task", KR(ret), KPC(client_task));
    }
    // try remove index
    else if (OB_FAIL(client_task_index_map_.erase_if(table_id, erase_if_equal, is_erased))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST == ret)) {
        LOG_WARN("fail to get refactored", KR(ret), K(table_id));
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      client_task->dec_ref_count();
    }
    // add client task brief
    if (OB_SUCC(ret)) {
      ObTableLoadClientTaskBrief *client_task_brief = nullptr;
      if (OB_FAIL(client_task_brief_map_.create(key, client_task_brief))) {
        LOG_WARN("fail to create client task brief", KR(ret), K(key));
      } else {
        client_task_brief->table_id_ = client_task->table_id_;
        client_task_brief->dest_table_id_ = client_task->ddl_param_.dest_table_id_;
        client_task_brief->task_id_ = client_task->ddl_param_.task_id_;
        client_task->get_status(client_task_brief->client_status_, client_task_brief->error_code_);
        client_task_brief->active_time_ = ObTimeUtil::current_time();
      }
      if (nullptr != client_task_brief) {
        client_task_brief_map_.revert(client_task_brief);
        client_task_brief = nullptr;
      }
    }
  }
  return ret;
}

int ObTableLoadClientService::get_all_client_task(
  ObIArray<ObTableLoadClientTask *> &client_task_array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else {
    client_task_array.reset();
    obsys::ObWLockGuard guard(rwlock_);
    for (ClientTaskMap::const_iterator iter = client_task_map_.begin();
         OB_SUCC(ret) && iter != client_task_map_.end(); ++iter) {
      const ObTableLoadUniqueKey &key = iter->first;
      ObTableLoadClientTask *client_task = iter->second;
      if (OB_FAIL(client_task_array.push_back(client_task))) {
        LOG_WARN("fail to push back", KR(ret), K(key));
      } else {
        client_task->inc_ref_count();
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < client_task_array.count(); ++i) {
        ObTableLoadClientTask *client_task = client_task_array.at(i);
        client_task->dec_ref_count();
      }
      client_task_array.reset();
    }
  }
  return ret;
}

int ObTableLoadClientService::get_client_task(const ObTableLoadUniqueKey &key,
                                              ObTableLoadClientTask *&client_task)
{
  int ret = OB_SUCCESS;
  client_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    if (OB_FAIL(client_task_map_.get_refactored(key, client_task))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get refactored", KR(ret), K(key));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else {
      client_task->inc_ref_count();
    }
  }
  return ret;
}

int ObTableLoadClientService::get_client_task_by_table_id(uint64_t table_id,
                                                          ObTableLoadClientTask *&client_task)
{
  int ret = OB_SUCCESS;
  client_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    if (OB_FAIL(client_task_index_map_.get_refactored(table_id, client_task))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get refactored", KR(ret), K(table_id));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else {
      client_task->inc_ref_count();
    }
  }
  return ret;
}

int ObTableLoadClientService::exist_client_task(const ObTableLoadUniqueKey &key, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    ObTableLoadClientTask *client_task = nullptr;
    if (OB_FAIL(client_task_map_.get_refactored(key, client_task))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get refactored", KR(ret), K(key));
      } else {
        ret = OB_SUCCESS;
        is_exist = false;
      }
    } else {
      is_exist = true;
    }
  }
  return ret;
}

int64_t ObTableLoadClientService::get_client_task_count() const
{
  obsys::ObRLockGuard guard(rwlock_);
  return client_task_map_.size();
}

void ObTableLoadClientService::purge_client_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else {
    ObArray<ObTableLoadClientTask *> client_task_array;
    if (OB_FAIL(get_all_client_task(client_task_array))) {
      LOG_WARN("fail to get all client task", KR(ret));
    }
    for (int64_t i = 0; i < client_task_array.count(); ++i) {
      ObTableLoadClientTask *client_task = client_task_array.at(i);
      ObTableLoadClientStatus client_status = client_task->get_status();
      if (client_status != ObTableLoadClientStatus::COMMIT &&
          client_status != ObTableLoadClientStatus::ABORT) {
        // ignore
      } else if (client_task->get_ref_count() > 2) {
        // ignore
      }
      // remove client task
      else if (OB_FAIL(remove_task(client_task))) {
        LOG_WARN("fail to remove client task", KR(ret), KPC(client_task));
      }
      revert_task(client_task);
    }
  }
}

int ObTableLoadClientService::get_task_brief(const ObTableLoadUniqueKey &key,
                                             ObTableLoadClientTaskBrief *&client_task_brief)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    if (OB_FAIL(service->get_client_service().get_client_task_brief(key, client_task_brief))) {
      LOG_WARN("fail to get client task brief", KR(ret), K(key));
    } else {
      // update active time
      client_task_brief->active_time_ = ObTimeUtil::current_time();
    }
  }
  return ret;
}

void ObTableLoadClientService::revert_task_brief(ObTableLoadClientTaskBrief *client_task_brief)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    if (OB_FAIL(service->get_client_service().revert_client_task_brief(client_task_brief))) {
      LOG_WARN("fail to revert client task brief", KR(ret), KP(client_task_brief));
    }
  }
}

int ObTableLoadClientService::get_client_task_brief(const ObTableLoadUniqueKey &key,
                                                    ObTableLoadClientTaskBrief *&client_task_brief)
{
  int ret = OB_SUCCESS;
  client_task_brief = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(client_task_brief_map_.get(key, client_task_brief))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get refactored", KR(ret), K(key));
      }
    }
  }
  return ret;
}

int ObTableLoadClientService::revert_client_task_brief(
  ObTableLoadClientTaskBrief *client_task_brief)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == client_task_brief)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(client_task_brief));
  } else {
    client_task_brief_map_.revert(client_task_brief);
  }
  return ret;
}

void ObTableLoadClientService::purge_client_task_brief()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else {
    const int64_t expired_ts = ObTimeUtil::current_time() - CLIENT_TASK_RETENTION_PERIOD;
    ClientTaskBriefEraseIfExpired erase_if_expired(expired_ts);
    if (OB_FAIL(client_task_brief_map_.remove_if(erase_if_expired))) {
      LOG_WARN("fail to remove if client task brief", KR(ret));
    }
  }
}

int ObTableLoadClientService::construct_commit_task(ObTableLoadClientTask *client_task)
{
  int ret = OB_SUCCESS;
  ObTableLoadTask *task = nullptr;
  if (OB_FAIL(client_task->alloc_task(task))) {
    LOG_WARN("fail to alloc task", KR(ret));
  } else if (OB_FAIL(task->set_processor<CommitTaskProcessor>(client_task))) {
    LOG_WARN("fail to set commit task processor", KR(ret));
  } else if (OB_FAIL(task->set_callback<CommonTaskCallback>(client_task))) {
    LOG_WARN("fail to set common task callback", KR(ret));
  } else if (OB_FAIL(client_task->add_task(task))) {
    LOG_WARN("fail to add task", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != task) {
      client_task->free_task(task);
      task = nullptr;
    }
  }
  return ret;
}

int ObTableLoadClientService::construct_abort_task(ObTableLoadClientTask *client_task)
{
  int ret = OB_SUCCESS;
  ObTableLoadTask *task = nullptr;
  if (OB_FAIL(client_task->alloc_task(task))) {
    LOG_WARN("fail to alloc task", KR(ret));
  } else if (OB_FAIL(task->set_processor<AbortTaskProcessor>(client_task))) {
    LOG_WARN("fail to set abort task processor", KR(ret));
  } else if (OB_FAIL(task->set_callback<CommonTaskCallback>(client_task))) {
    LOG_WARN("fail to set common task callback", KR(ret));
  } else if (OB_FAIL(client_task->add_task(task))) {
    LOG_WARN("fail to add task", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != task) {
      client_task->free_task(task);
      task = nullptr;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
