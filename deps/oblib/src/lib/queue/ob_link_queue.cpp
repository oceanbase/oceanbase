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

#include "lib/ob_errno.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/atomic/ob_atomic.h"

using namespace oceanbase;
using namespace oceanbase::common;

namespace oceanbase
{
namespace common
{

// add interface like ObSpLinkQueue
int ObSimpleLinkQueue::pop(Link *&p)
{
  int ret = OB_SUCCESS;
  while(OB_SUCCESS == (ret = do_pop(p)) && p == &dummy_) {
    ret = push(p);
  }
  if (OB_SUCCESS != ret) {
    p = NULL;
  }
  return ret;
}

int ObSimpleLinkQueue::push(Link *p)
{
  int ret = OB_SUCCESS;
  Link *tail = NULL;
  if (NULL == p) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    p->next_ = NULL;
    tail = tail_;
    tail_ = p;
    tail->next_ = p;
  }
  return ret;
}

int ObSimpleLinkQueue::do_pop(Link *&p)
{
  int ret = OB_SUCCESS;
  if (head_ == tail_) {
    ret = OB_EAGAIN;
  } else {
    Link *head = head_;
    Link* next = head->next_;
    head_ = next;
    p = head;
  }
  return ret;
}

} // end namespace common
} // end namespace oceanbase
