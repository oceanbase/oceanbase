/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/queue/ob_link_queue.h"

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
