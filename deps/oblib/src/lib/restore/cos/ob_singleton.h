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

#ifndef SRC_LIBRARY_SRC_LIB_OB_SINGLETON_H_
#define SRC_LIBRARY_SRC_LIB_OB_SINGLETON_H_

namespace oceanbase
{
namespace common
{
namespace qcloud_cos
{

// A singleton base class offering an easy way to create singleton.
template <typename T>
class __attribute__ ((visibility ("default"))) ObSingleton
{
public:
  // not thread safe
  static T& get_instance()
  {
    static T instance;
    return instance;
  }

  virtual ~ObSingleton() {}

protected:
  ObSingleton() {}

private:
  ObSingleton(const ObSingleton &);
  ObSingleton& operator=(const ObSingleton&);
};


}
}
}

#endif