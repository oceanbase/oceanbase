/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_MEM_LEAK_CHECKER_INFO_H_
#define _OB_MEM_LEAK_CHECKER_INFO_H_

#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "lib/allocator/ob_mem_leak_checker.h"

#include "share/ob_virtual_table_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
namespace allocator
{
}

namespace observer
{
class ObMemLeakChecker;
class ObMemLeakCheckerInfo : public common::ObVirtualTableIterator
{
public:
  ObMemLeakCheckerInfo();
  virtual ~ObMemLeakCheckerInfo();

  inline void set_addr(common::ObAddr &addr) {addr_ = &addr;}
  inline void set_tenant_id(uint64_t tenant_id) {tenant_id_ = tenant_id;}
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  int sanity_check();
  int fill_row(common::ObNewRow *&row);
private:
  bool opened_;
  common::ObMemLeakChecker *leak_checker_;
  common::ObMemLeakChecker::mod_info_map_t::hashmap::const_iterator it_;
  common::ObMemLeakChecker::mod_info_map_t info_map_;
  common::ObAddr *addr_;
  uint64_t tenant_id_;
  const char *label_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMemLeakCheckerInfo);
};
}
}

#endif /* _OB_MEM_LEAK_CHECKER_INFO_H_ */
