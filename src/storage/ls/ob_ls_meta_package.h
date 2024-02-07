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

#ifndef OCEANBASE_STORAGE_OB_LS_META_PACKAGE_
#define OCEANBASE_STORAGE_OB_LS_META_PACKAGE_
#include "storage/ls/ob_ls_meta.h"               // ObLSMeta
#include "logservice/palf/palf_base_info.h"      // PalfBaseInfo
#include "storage/tx/ob_dup_table_base.h"

namespace oceanbase
{
namespace storage
{

// this is a package of meta.
// it is a combination of ls meta and all its member's.
// we can rebuild the ls with this package.
class ObLSMetaPackage final
{
  OB_UNIS_VERSION_V(1);
public:
  ObLSMetaPackage();
  ObLSMetaPackage(const ObLSMetaPackage &ls_meta);
  ~ObLSMetaPackage() { reset(); }
  ObLSMetaPackage &operator=(const ObLSMetaPackage &other);
  void reset();
  bool is_valid() const;
  void update_clog_checkpoint_in_ls_meta(const share::SCN& clog_checkpoint_scn,
                                         const palf::LSN& clog_base_lsn);

  TO_STRING_KV(K_(ls_meta), K_(palf_meta), K_(dup_ls_meta));
public:
  ObLSMeta ls_meta_;                // the meta of ls
  palf::PalfBaseInfo palf_meta_;    // the meta of palf
  transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta dup_ls_meta_; // the meta of dup_ls_meta_;
};

} // storage
} // oceanbase
#endif
