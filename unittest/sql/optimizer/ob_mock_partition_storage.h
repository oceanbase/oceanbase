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
