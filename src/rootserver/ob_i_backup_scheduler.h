/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
