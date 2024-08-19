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

#ifndef OCEANBASE_UNITTEST_STORAGE_TX_OB_MAILBOX
#define OCEANBASE_UNITTEST_STORAGE_TX_OB_MAILBOX

#include <cstring>
#include <deque>
#include <map>
#include <set>
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/tx/ob_committer_define.h"
#include "storage/tx/ob_tx_msg.h"

namespace oceanbase
{
namespace transaction
{
template <typename MailType>
class ObMailBoxMgr;

template <typename MailType>
class ObMail
{
public:
  int init(int64_t from,
           int64_t to,
           uint64_t size,
           const MailType &mail)
  {
    if (NULL != mail_) {
      std::free(mail_);
    }

    from_ = from;
    to_ = to;
    size_ = size;
    mail_ = (MailType*)std::malloc(size);
    std::memcpy((void*)mail_, (void*)(&mail), size);
    return OB_SUCCESS;
  }
  ObMail()
  {
    mail_ = NULL;
  }
  ObMail(const ObMail &other)
  {
    from_ = other.from_;
    to_ = other.to_;
    size_ = other.size_;
    mail_ = (MailType*)std::malloc(size_);
    std::memcpy((void*)mail_, (void*)(other.mail_), size_);
  }
  ~ObMail()
  {
    if (NULL != mail_) {
      std::free(mail_);
    }
  }
  ObMail& operator=(const ObMail& other)
  {
    if (NULL != mail_) {
      std::free(mail_);
    }

    from_ = other.from_;
    to_ = other.to_;
    size_ = other.size_;
    mail_ = (MailType*)std::malloc(size_);
    std::memcpy((void*)mail_, (void*)(other.mail_), size_);
    return *this;
  }
  /* ObMail operator=(const ObMail& other) */
  /*   { */
  /*     if (NULL != mail_) { */
  /*       std::free(mail_); */
  /*     } */

  /*     from_ = other.from_; */
  /*     to_ = other.to_; */
  /*     size_ = other.size_; */
  /*     mail_ = (MailType*)std::malloc(size_); */
  /*     std::memcpy((void*)mail_, (void*)(other.mail_), size_); */
  /*     return *this; */
  /*   } */
  bool operator<(const ObMail& other) const
  {
    return from_ < other.from_
      || to_ < other.to_
      || size_ < other.size_
    || (size_ == other.size_ && memcmp((void*)mail_, (void*)other.mail_, size_) < 0);
  }
  /* ObMail& operator=(const ObMail &other) */
  /* { */
  /*   from_ = other.from_; */
  /*   to_ = other.to_; */
  /*   size_ = other.size_; */
  /*   mail_ = (MailType*)std::malloc(size_); */
  /*   std::memcpy((void*)mail_, (void*)(other.mail_), size_); */
  /* } */
  int64_t from_;
  int64_t to_;
  uint64_t size_;
  MailType* mail_;

  TO_STRING_KV(K_(from), K_(to), K_(*mail));
};

template <typename MailType>
class ObMailHandler
{
public:
  virtual int handle(const ObMail<MailType>& mail) = 0;
  int64_t sign_ = 39;

  virtual TO_STRING_KV(K_(sign));
};

template <typename MailType>
class ObMailBox
{
public:
  int64_t addr_;
  std::deque<ObMail<MailType>> mailbox_;
  ObMailHandler<MailType> *ctx_;
  ObMailBoxMgr<MailType> *mailbox_mgr_;
  ~ObMailBox()
  {
    mailbox_.clear();
  }
  bool empty() { return mailbox_.empty(); }
  int init(int64_t addr,
           ObMailBoxMgr<MailType> *mailbox_mgr,
           ObMailHandler<MailType> *ctx);
  int handle(const bool must_have = true);
  int handle_all();
  int send(const ObMail<MailType>& mail,
           const int64_t receiver);
  int send_to_head(const ObMail<MailType>& mail,
                   const int64_t receiver);
  int fetch_mail(ObMail<MailType>& mail);
  int64_t to_string(char *buffer, const int64_t size) const;
};

template <typename MailType>
class ObMailBoxMgr
{
public:
  int64_t counter_ = 0;
  std::map<int64_t, ObMailBox<MailType>*> mgr_;
  std::set<ObMail<MailType>> cache_msg_;
  int register_mailbox(int64_t &addr,
                       ObMailBox<MailType> &mailbox,
                       ObMailHandler<MailType> *ctx);
  int send(const ObMail<MailType>& mail,
           const int64_t receive);
  int send_to_head(const ObMail<MailType>& mail,
                   const int64_t receive);
  bool random_dup_and_send();
  void reset();
};

template <typename MailType>
int ObMailBox<MailType>::init(int64_t addr,
                              ObMailBoxMgr<MailType> *mailbox_mgr,
                              ObMailHandler<MailType> *ctx)
{
  int ret = OB_SUCCESS;

  mailbox_.clear();
  addr_ = addr;
  ctx_ = ctx;
  mailbox_mgr_ = mailbox_mgr;

  return ret;
}

template <typename MailType>
int ObMailBox<MailType>::fetch_mail(ObMail<MailType> &mail)
{
  int ret = OB_SUCCESS;

  if (mailbox_.empty()) {
    TRANS_LOG(ERROR, "mailbox is empty, but must handle", K(*this));
    ob_abort();
  } else {
    mail = mailbox_.front();
    mailbox_.pop_front();
  }

  return ret;
}

template <typename MailType>
int ObMailBox<MailType>::handle(const bool must_have)
{
  int ret = OB_SUCCESS;

  if (must_have && mailbox_.empty()) {
    TRANS_LOG(ERROR, "mailbox is empty, but must handle", K(*this));
    ob_abort();
  } else if (mailbox_.empty()) {
    ret = OB_SUCCESS;
  } else {
    ObMail<MailType> mail = mailbox_.front();
    mailbox_.pop_front();
    ret = ctx_->handle(mail);
  }

  return ret;
}

template <typename MailType>
int ObMailBox<MailType>::handle_all()
{
  int ret = OB_SUCCESS;

  while (OB_SUCC(ret) && !mailbox_.empty()) {
    ObMail<MailType> mail = mailbox_.front();
    mailbox_.pop_front();
    ret = ctx_->handle(mail);
  }

  return ret;
}

template <typename MailType>
int ObMailBox<MailType>::send(const ObMail<MailType>& mail,
                              const int64_t receiver)
{
  int ret = OB_SUCCESS;

  ret = mailbox_mgr_->send(mail, receiver);

  return ret;
}

template <typename MailType>
int ObMailBox<MailType>::send_to_head(const ObMail<MailType>& mail,
                                      const int64_t receiver)
{
  int ret = OB_SUCCESS;

  ret = mailbox_mgr_->send_to_head(mail, receiver);

  return ret;
}

template <typename MailType>
int64_t ObMailBox<MailType>::to_string(char *buffer, const int64_t size) const
{
  int64_t pos = 0;

  if (nullptr != buffer && size > 0) {
    databuff_printf(buffer, size, pos, "{addr: %ld, DEQUE: [", addr_);
    for (auto it = mailbox_.begin(); it != mailbox_.end(); ++it) {
      databuff_printf(buffer, size, pos, "(%s), ", to_cstring(*it));
    }

    databuff_printf(buffer, size, pos, "]}");
  }

  return pos;
}

template <typename MailType>
int ObMailBoxMgr<MailType>::register_mailbox(int64_t &addr,
                                             ObMailBox<MailType> &mailbox,
                                             ObMailHandler<MailType> *ctx)
{
  int ret = OB_SUCCESS;

  addr = ++counter_;
  ret = mailbox.init(addr, this, ctx);
  mgr_[addr] = &mailbox;

  TRANS_LOG(INFO, "register mailbox", K(mailbox), KP(ctx));
  return ret;
}

template <typename MailType>
void ObMailBoxMgr<MailType>::reset()
{
  counter_ = 0;
  mgr_.clear();
  cache_msg_.clear();
  TRANS_LOG(INFO, "reset mailbox",K(this));
}

template <typename MailType>
int ObMailBoxMgr<MailType>::send(const ObMail<MailType>& mail,
                                 const int64_t receiver)
{
  int ret = OB_SUCCESS;

  if (mgr_.count(mail.to_) != 0) {
    cache_msg_.insert(mail);
    mgr_[receiver]->mailbox_.push_back(mail);
    TRANS_LOG(INFO, "send mailbox success", K(ret), K(mail),
              K(*mgr_[receiver]));
  }

  return ret;
}

template <typename MailType>
int ObMailBoxMgr<MailType>::send_to_head(const ObMail<MailType>& mail,
                                         const int64_t receiver)
{
  int ret = OB_SUCCESS;

  if (mgr_.count(mail.to_) != 0) {
    cache_msg_.insert(mail);
    mgr_[receiver]->mailbox_.push_front(mail);
    TRANS_LOG(INFO, "send to mailbox front success", K(ret), K(mail),
              K(*mgr_[receiver]));
  }

  return ret;
}

template <typename MailType>
bool ObMailBoxMgr<MailType>::random_dup_and_send()
{
  int64_t idx = ObRandom::rand(0, cache_msg_.size() - 1);

  if (idx >= 0 && cache_msg_.size() >= 0) {
    int i = 0;
    bool found = false;
    ObMail<MailType> mail;
    for (auto iter = cache_msg_.begin();
         iter != cache_msg_.end();
         iter++) {
      if (idx == i) {
        mail = *iter;
        found = true;
        break;
      }
      i++;
    }
    if (!found) {
      ob_abort();
    }
    mgr_[mail.to_]->mailbox_.push_front(mail);
    TRANS_LOG(INFO, "random_dup_and_send success", K(idx), K(cache_msg_.size()),
              K(mail));
    return true;
  } else {
    return false;
  }
}

} // namespace transaction
} // namespace oceanbase

#endif // OCEANBASE_UNITTEST_STORAGE_TX_OB_MAILBOX
