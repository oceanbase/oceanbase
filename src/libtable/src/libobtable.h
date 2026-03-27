/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
