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

#ifndef OCEANBASE_SERVER_MEMORY_CUTTER_H_
#define OCEANBASE_SERVER_MEMORY_CUTTER_H_

#include "lib/signal/ob_memory_cutter.h"

namespace oceanbase {
namespace observer {
class ObServerMemoryCutter : public lib::ObIMemoryCutter {
public:
  virtual void cut(int64_t& total_size) override;

private:
  void free_cache(int64_t& total_size);
};

extern ObServerMemoryCutter g_server_mem_cutter;

}  // end of namespace observer
}  // end of namespace oceanbase

#endif /* OCEANBASE_SERVER_MEMORY_CUTTER_H_ */
