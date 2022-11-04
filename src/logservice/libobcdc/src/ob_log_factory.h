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
 *
 * Oceanbase cdc instance factory
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_FACTORY_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_FACTORY_H_

#include <stdint.h>
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/ob_define.h"
#include "ob_log_store_task.h"   // ObLogStoreTask
#include "ob_log_buf.h"          // FixedBuf
#include "ob_log_block.h"        // BigBlock

namespace oceanbase
{
namespace libobcdc
{
const int64_t LIBOBLOG_MEM_STAT_INTERVAL = 60 * 1000 * 1000;  // 60s

#define LIBOBLOG_FACTORY_CLASS_DEFINE_(object_name, object_name2)  \
  class object_name##Factory    \
  { \
  public: \
    static object_name2 *alloc();   \
    static void free(object_name2 *obj);  \
    static int64_t get_alloc_count(); \
    static int64_t get_release_count(); \
    static const char *get_mod_type(); \
  private: \
    static const char *mod_type_; \
    static int64_t alloc_count_; \
    static int64_t release_count_; \
  }; \

#define LIBOBLOG_FACTORY_CLASS_DEFINE(object_name) LIBOBLOG_FACTORY_CLASS_DEFINE_(object_name, object_name)

typedef FixedBuf<common::OB_MAX_LOG_BUFFER_SIZE> ReadLogBuf;

// Default 128,  Modify it through set RP_MAX_FREE_LIST_NUM
LIBOBLOG_FACTORY_CLASS_DEFINE(ObLogStoreTask)
LIBOBLOG_FACTORY_CLASS_DEFINE(ReadLogBuf)
LIBOBLOG_FACTORY_CLASS_DEFINE(BigBlock)

}; // end namespace libobcdc
}; // end namespace oceanbase
#endif
