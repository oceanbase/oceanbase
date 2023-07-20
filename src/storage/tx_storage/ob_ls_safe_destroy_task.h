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

#ifndef OCEABASE_STORAGE_OB_LS_SAFE_DESTROY_TASK_
#define OCEABASE_STORAGE_OB_LS_SAFE_DESTROY_TASK_

#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_handle.h" //ObLSHandle
#include "storage/tx_storage/ob_safe_destroy_handler.h"

namespace oceanbase
{
namespace storage
{
class ObLSSafeDestroyTask : public ObSafeDestroyTask
{
public:
  ObLSSafeDestroyTask();
  ~ObLSSafeDestroyTask();
  int init(const uint64_t tenant_id,
           const ObLSHandle &ls_handle,
           ObLSService *ls_svr);
  virtual bool safe_to_destroy() override;
  virtual void destroy() override;
  int get_ls_id(share::ObLSID &ls_id) const;
  INHERIT_TO_STRING_KV("ObSafeDestroyTask", ObSafeDestroyTask, K_(is_inited),
                       K_(tenant_id), K_(ls_handle), KP_(ls_service));
private:
  bool is_inited_;
  // used to switch to the tenant to make sure destroy process is right.
  uint64_t tenant_id_;
  // contain the ls need to check.
  ObLSHandle ls_handle_;
  // the ls service, if a ls is not safe to destroy the ls service
  // should not safe to destroy too.
  ObLSService *ls_service_;
};

class ObSafeDestroyCheckLSExist
{
public:
  explicit ObSafeDestroyCheckLSExist(const share::ObLSID &ls_id)
    : err_code_(common::OB_SUCCESS),
      exist_(false),
      ls_id_(ls_id)
  {}
  bool operator()(const ObSafeDestroyTask *task, bool &need_requeue);
  int is_exist() const { return exist_; }
  int get_ret_code() const { return err_code_; }
private:
  int err_code_;
  bool exist_;
  share::ObLSID ls_id_;
};

} // storage
} // oceanbase
#endif
