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

#ifndef _OB_CONST_MAP_INITIALIZER_H
#define _OB_CONST_MAP_INITIALIZER_H

#include "lib/ob_errno.h"
#include "lib/utility/utility.h"


namespace oceanbase {
namespace sql {


/**
 * a defined static const map value of class MyClass
 * usually needs to be inited when declearing it
 * ObConstMap helps in this situatition using
 * a init_function
 *
 */

template <typename MyClass>
class ObConstMap {
public:

  typedef int (*InitFunction)(MyClass &member);

  explicit ObConstMap(InitFunction init_f) : is_inited_(false), init_f_(init_f)
  {
  }

  int init()
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = common::OB_INIT_TWICE;
    } else {
      is_inited_ = true;
      ret = init_f_(member_);
    }
    return ret;
  }

  const MyClass &value()
  {
    return member_;
  }

protected:
    bool is_inited_;
    InitFunction init_f_;
    MyClass member_;
};

}

}





#endif // _OB_CONST_MAP_INITIALIZER_H
