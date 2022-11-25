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

#include <gmock/gmock.h>
#include "storage/ob_base_storage.h"

namespace oceanbase
{
namespace storage
{

class MockObBaseStorage : public ObBaseStorage
{
public:
  MockObBaseStorage() {}
  virtual ~MockObBaseStorage() {}
  MOCK_METHOD0(get_data_file, blocksstable::ObDataFile & ());
  MOCK_METHOD0(get_commit_logger, blocksstable::ObBaseStorageLogger & ());
  MOCK_METHOD0(write_check_point, int ());
};


}  // namespace storage
}  // namespace oceanbase

