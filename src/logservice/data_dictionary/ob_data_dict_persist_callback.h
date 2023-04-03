/**
* Copyright (c) 2022 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*
*/

#ifndef OCEANBASE_DICT_SERVICE_DATA_DICTIONARY_PERSIST_CB_
#define OCEANBASE_DICT_SERVICE_DATA_DICTIONARY_PERSIST_CB_

#include "logservice/ob_append_callback.h"   // AppendCb
#include "lib/queue/ob_link_queue.h"         // ObSimpleLinkQueue

namespace oceanbase
{
namespace datadict
{

class ObDataDictPersistCallback : public logservice::AppendCb, public common::QLink
{
public:
  ObDataDictPersistCallback() : is_callback_invoked_(false), is_success_(false) {}
  ~ObDataDictPersistCallback() { reset(); }
  void reset()
  {
    ATOMIC_SET(&is_callback_invoked_, false);
    ATOMIC_SET(&is_success_, false);
  }
public:
  virtual int on_success() override
  {
    ATOMIC_SET(&is_success_, true);
    MEM_BARRIER();
    ATOMIC_SET(&is_callback_invoked_, true);
    return OB_SUCCESS;
  }
  virtual int on_failure() override
  {
    ATOMIC_SET(&is_callback_invoked_, true);
    return OB_SUCCESS;
  }
public:
  TO_STRING_KV(K_(is_callback_invoked), K_(is_success));
  OB_INLINE bool is_invoked() const { return ATOMIC_LOAD(&is_callback_invoked_); }
  OB_INLINE bool is_success() const { return ATOMIC_LOAD(&is_success_); }
private:
  bool is_callback_invoked_;
  bool is_success_;
};

} // datadict
} // oceanbase
#endif
