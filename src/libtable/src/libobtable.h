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

#ifndef _LIBOBTABLE_H
#define _LIBOBTABLE_H 1

// all interface headers
#include "ob_table_service_client.h"
#include "share/table/ob_table.h"
#include "ob_hkv_table.h"
#include "ob_pstore.h"

/** @mainpage libobtable Documentation
 *
 * There are four public interface classes:
 *
 * 1. ObTable
 * 2. ObKVTable
 * 3. ObHKVTable
 * 4. ObTableServiceClient
 *
 */
namespace oceanbase
{
namespace table
{
/// Library entry class
class ObTableServiceLibrary
{
public:
  /**
   * Initialize the library.
   * @note must by called in single thread environment and before calling all other APIs
   *
   * @return error code
   */
  static int init();
  static void destroy();
};
} // end namespace table
} // end namespace oceanbase
#endif /* _LIBOBTABLE_H */
