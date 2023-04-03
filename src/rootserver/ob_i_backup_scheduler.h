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

#ifndef OCEANBASE_ROOTSERVER_OB_I_BACKUP_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_OB_I_BACKUP_SCHEDULER_H_

#include "ob_rs_reentrant_thread.h"

namespace oceanbase
{
namespace rootserver
{

class ObIBackupScheduler : public ObRsReentrantThread
{
public:
  ObIBackupScheduler()
  {
  }
  virtual ~ObIBackupScheduler()
  {
  }
  virtual bool is_working() const = 0;

  // force cancel backup task, such as backup database, backupbackup, validate, archive etc.
  virtual int force_cancel(const uint64_t tenant_id) = 0;
};

} //rootserver
} //oceanbase


#endif /* OCEANBASE_ROOTSERVER_OB_I_BACKUP_SCHEDULER_H_ */
