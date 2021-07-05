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

#ifndef OB_LOG_STORE_FACTORY_H_
#define OB_LOG_STORE_FACTORY_H_

#include "ob_log_file_store.h"

namespace oceanbase {
namespace common {
class ObLogStoreFactory {
public:
  static ObILogFileStore* create(const char* log_dir, const int64_t file_size, const clog::ObLogWritePoolType type);

  static void destroy(ObILogFileStore*& store);
};

}  // namespace common
}  // namespace oceanbase
#endif /* OB_LOG_STORE_FACTORY_H_ */
