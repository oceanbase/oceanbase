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

static void link_del(TCLink* h, TCLink* p)
{
  h->next_ = p->next_;
}

static void link_insert(TCLink* h, TCLink* p)
{
  p->next_ = h->next_;
  h->next_ = p;
}

typedef bool less_than_t(TCLink* p1, TCLink* p2);
static void order_link_insert(TCLink* h, TCLink* t, less_than_t less_than)
{
  TCLink* prev = h;
  TCLink* cur = NULL;
  while(h != (cur = prev->next_) && less_than(cur, t)) {
    prev = cur;
  }
  t->next_ = cur;
  prev->next_ = t;
}

struct TCDLink
{
  TCDLink(TCDLink* p): next_(p), prev_(p) {}
  ~TCDLink() {}
  TCDLink* next_;
  TCDLink* prev_;
};

static void dlink_insert(TCDLink* h, TCDLink* p)
{
  TCDLink* n = h->next_;
  p->next_ = n;
  p->prev_ = h;
  h->next_ = p;
  n->prev_ = p;
}

static void dlink_del(TCDLink* p)
{
  TCDLink* h = p->prev_;
  TCDLink* n = p->next_;
  h->next_ = n;
  n->prev_ = h;
}

/*
static bool dlist_is_empty(TCDLink* p)
{
  return p->next_ == p;
  }*/
