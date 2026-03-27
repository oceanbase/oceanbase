/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_MOCK_PARTITION_STORAGE_H_
#define _OB_MOCK_PARTITION_STORAGE_H_


namespace test
{
class MockPartitionStorage
{
public:
  MockPartitionStorage()
      : is_default_stat_(false)
  { }
  virtual ~MockPartitionStorage() { }
  void set_default_stat(const bool is_default_stat)
  {
    is_default_stat_ = is_default_stat;
  }
private:
  bool is_default_stat_;
};
}


#endif
