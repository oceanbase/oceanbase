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

#ifndef SRC_SHARE_OB_FORCE_PRINT_LOG_H_
#define SRC_SHARE_OB_FORCE_PRINT_LOG_H_

#include "ob_task_define.h"
#include "lib/oblog/ob_log.h"


#define FLOG_ERROR(args...)                              \
    do {                                                  \
      oceanbase::share::ObTaskController::get().allow_next_syslog();        \
      LOG_ERROR (args);                                  \
    } while (0)

#define _FLOG_ERROR(args...)                             \
    do {                                                  \
      oceanbase::share::ObTaskController::get().allow_next_syslog();        \
      _LOG_ERROR (args);                                \
    } while (0)

#define FLOG_WARN(args...)                               \
    do {                                                  \
      oceanbase::share::ObTaskController::get().allow_next_syslog();        \
      LOG_WARN (args);                                  \
    } while (0)

#define _FLOG_WARN(args...)                               \
    do {                                                   \
      oceanbase::share::ObTaskController::get().allow_next_syslog();         \
      _LOG_WARN (args);                                  \
    } while (0)

#define FLOG_INFO(args...)                               \
    do {                                                  \
      oceanbase::share::ObTaskController::get().allow_next_syslog();        \
      LOG_INFO (args);                                  \
    } while (0)

#define _FLOG_INFO(args...)                              \
    do {                                                  \
      oceanbase::share::ObTaskController::get().allow_next_syslog();        \
      _LOG_INFO (args);                                 \
    } while (0)


#define FLOG_ERROR_RET(errcode, args...) { int ret = errcode; FLOG_ERROR(args); }
#define _FLOG_ERROR_RET(errcode, args...) { int ret = errcode; _FLOG_ERROR(args); }
#define FLOG_WARN_RET(errcode, args...) { int ret = errcode; FLOG_WARN(args); }
#define _FLOG_WARN_RET(errcode, args...) { int ret = errcode; _FLOG_WARN(args); }
#endif /* SRC_SHARE_OB_FORCE_PRINT_LOG_H_ */
