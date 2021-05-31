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

#include "co_local_storage.h"

namespace oceanbase {
namespace lib {

CoVar<char[sizeof(CoLocalStorage::Allocator)]> CoLocalStorage::buf_;
CoObj<CoLocalStorage::Allocator*> CoLocalStorage::allocator_(
    [] {  // init
      allocator_.get() = new (&buf_[0]) Allocator(common::ObModIds::OB_CO_LOCAL_STORAGE);
    },
    [] {  // deinit
      allocator_.get()->~Allocator();
    });
}  // namespace lib
}  // namespace oceanbase
