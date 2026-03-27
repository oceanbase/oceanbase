/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

