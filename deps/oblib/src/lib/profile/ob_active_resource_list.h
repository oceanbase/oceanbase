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

#ifndef OCEANBASE_PROFILE_OB_ACTIVE_RESOURCE_LIST_H_
#define OCEANBASE_PROFILE_OB_ACTIVE_RESOURCE_LIST_H_

#include "lib/list/dlink.h"

namespace oceanbase
{
namespace common
{
struct ActiveResource
{};

// struct ActiveResource: CDLink
// {
//   char msg_[256];
// };

// class Printer
// {
// public:
//   enum { MAX_BUF_SIZE = 1<<20};
//   Printer(): limit_(MAX_BUF_SIZE), pos_(0)
//   {
//     memset(buf_, 0, MAX_BUF_SIZE);
//   }

//   ~Printer()
//   {
//     pos_ = 0;
//   }
//   void reset()
//   {
//     pos_ = 0;
//     *buf_ = 0;
//   }
//   char *get_str() { return NULL != buf_ && limit_ > 0 ? buf_ : NULL; }
//   char *append(const char *format, ...)
//   {
//     char *src = NULL;
//     int64_t count = 0;
//     va_list ap;
//     va_start(ap, format);
//     if (NULL != buf_ && limit_ > 0 && pos_ < limit_
//         && pos_ + (count = vsnprintf(buf_ + pos_, limit_ - pos_, format, ap)) < limit_) {
//       src = buf_ + pos_;
//       pos_ += count;
//     }
//     va_end(ap);
//     return src;
//   }
//   char *new_str(const char *format, ...)
//   {
//     char *src = NULL;
//     int64_t count = 0;
//     va_list ap;
//     va_start(ap, format);
//     if (NULL != buf_ && limit_ > 0 && pos_ < limit_
//         && pos_ + (count = vsnprintf(buf_ + pos_, limit_ - pos_, format, ap)) + 1 < limit_) {
//       src = buf_ + pos_;
//       pos_ += count + 1;
//     }
//     va_end(ap);
//     return src;
//   }
// private:
//   char buf_[MAX_BUF_SIZE];
//   int64_t limit_;
//   int64_t pos_;
// };

// class ObActiveResourceList
// {
// public:
//   ObActiveResourceList() {}
//   ~ObActiveResourceList() {}
//   void add(ActiveResource* res, const char *format, ...)
//   {
//     if (NULL != res && NULL != format) {
//       va_list ap;
//       va_start(ap, format);
//       vsnprintf(res->msg_, sizeof(res->msg_), format, ap);
//       va_end(ap);
//       _OB_LOG(DEBUG, "GARL.add: %s", res->msg_);
//       linkset_.add(res);
//     }
//   }
//   void del(ActiveResource* res) {
//     if (NULL != res) {
//       _OB_LOG(DEBUG, "GARL.del: %p", res);
//       linkset_.del(res);
//     }
//   }
//   void print() {
//     static Printer printer;
//     CDLinkSet::Iterator iter(linkset_);
//     ActiveResource* p = NULL;
//     printer.reset();
//     printer.append("GARL: start\n");
//     while(NULL != (p = (ActiveResource*)iter.next())) {
//       printer.append("GARL: %s\n", p->msg_);
//     }
//     printer.append("GARL: end\n");
//     _OB_LOG(INFO, "%s", printer.get_str());
//   }
// private:
//   CDLinkSet linkset_;
// };

// inline ObActiveResourceList& get_global_active_resource_list() {
//   static ObActiveResourceList res_list;
//   return res_list;
// }

//#define GARL get_global_active_resource_list()
// #define GARL_ADD(res, key) GARL.add(res, "%s " "%ld %ld %s", key, pthread_self(), ::oceanbase::common::ObTimeUtility::current_time(), lbt())
// #define GARL_DEL(res) GARL.del(res)
#define GARL_ADD(res, key)
#define GARL_DEL(res)
#define GARL_PRINT() //GARL.print()

}; // end namespace profile
}; // end namespace oceanbase

#endif /* OCEANBASE_PROFILE_OB_ACTIVE_RESOURCE_LIST_H_ */
