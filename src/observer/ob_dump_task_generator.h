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

#ifndef OCEANBASE_OBSERVER_OB_DUMP_TASK_GEN_
#define OCEANBASE_OBSERVER_OB_DUMP_TASK_GEN_

#include <stdint.h>

namespace oceanbase
{
namespace observer
{
class ObDumpTaskGenerator
{
  /*
    1. etc/dump.config文件写入相应指令
       dump entity all
       dump entity p_entity='0xffffffffff',slot_idx=1000
       dump chunk all
       dump chunk tenant_id=1,ctx_id=1
       dump chunk p_chunk='0xfffffffff'
       set option leak_mod = 'xxx'
       set option leak_rate = xxx
       dump memory leak
    2. kill -62 pid
    3. 结果见log/memory_meta文件
  */
  enum TaskType
  {
    CONTEXT_ALL          = 0,
    CONTEXT              = 1,
    CHUNK_ALL           = 2,
    CHUNK_OF_TENANT_CTX = 3,
    CHUNK               = 4,
    SET_LEAK_MOD        = 5,
    SET_LEAK_RATE       = 6,
    MEMORY_LEAK         = 7,
  };
public:
  static int generate_task_from_file();
  static int generate_mod_stat_task();
private:
  static int read_cmd(char *buf, int64_t len, int64_t &real_size);
  static void  dump_memory_leak();
};

}
}

#endif
