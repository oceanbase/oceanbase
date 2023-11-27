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

#ifndef OCEANBASE_STORAGE_OB_TABLET_ITERATOR
#define OCEANBASE_STORAGE_OB_TABLET_ITERATOR

#include <stdint.h>
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/meta_mem/ob_tablet_pointer.h"

namespace oceanbase
{
namespace storage
{
struct ObMetaDiskAddr;
class ObLSTabletService;
class ObTabletHandle;
class ObTabletMapKey;

class ObLSTabletIterator final
{
  friend class ObLSTabletService;
public:
  explicit ObLSTabletIterator(const ObMDSGetTabletMode mode);
  ~ObLSTabletIterator();
  ObLSTabletIterator(const ObLSTabletIterator&) = delete;
  ObLSTabletIterator &operator=(const ObLSTabletIterator&) = delete;
public:
  int get_next_tablet(ObTabletHandle &handle);
  int get_next_tablet_addr(ObTabletMapKey &key, ObMetaDiskAddr &addr);
  int get_next_ddl_kv_mgr(ObDDLKvMgrHandle &handle);
  int get_tablet_ids(ObIArray<common::ObTabletID> &ids);

  void reset();
  bool is_valid() const;

  TO_STRING_KV(KP_(ls_tablet_service), K_(tablet_ids), K_(idx), K_(mode));
private:
  ObLSTabletService *ls_tablet_service_;
  common::ObSEArray<common::ObTabletID, ObTabletCommon::DEFAULT_ITERATOR_TABLET_ID_CNT> tablet_ids_;
  int64_t idx_;
  ObMDSGetTabletMode mode_;
};

class ObHALSTabletIDIterator final
{
  friend class ObLSTabletService;
public:
  explicit ObHALSTabletIDIterator(
      const share::ObLSID &ls_id,
      const bool need_initial_state);
  ~ObHALSTabletIDIterator();
  ObHALSTabletIDIterator(const ObHALSTabletIDIterator&) = delete;
  ObHALSTabletIDIterator &operator=(const ObHALSTabletIDIterator&) = delete;
public:
  int get_next_tablet_id(common::ObTabletID &tablet_id);

  void reset();
  bool is_valid() const;

  TO_STRING_KV(K_(ls_id), K_(tablet_ids), K_(idx));
private:
  share::ObLSID ls_id_;
  common::ObSEArray<common::ObTabletID, ObTabletCommon::DEFAULT_ITERATOR_TABLET_ID_CNT> tablet_ids_;
  int64_t idx_;
  const bool need_initial_state_;
};


class ObHALSTabletIterator final
{
  friend class ObLSTabletService;
public:
  explicit ObHALSTabletIterator(const share::ObLSID &ls_id, const bool need_initial_state);
  ~ObHALSTabletIterator();
  ObHALSTabletIterator(const ObHALSTabletIterator&) = delete;
  ObHALSTabletIterator &operator=(const ObHALSTabletIterator&) = delete;
public:
  int get_next_tablet(ObTabletHandle &handle);

  void reset();
  bool is_valid() const;

  TO_STRING_KV(KP_(ls_tablet_service), K_(tablet_id_iter));
private:
  ObLSTabletService *ls_tablet_service_;
  ObHALSTabletIDIterator tablet_id_iter_;
};

class ObLSTabletFastIter final
{
  friend class ObLSTabletService;
public:
  ObLSTabletFastIter(ObITabletFilterOp &op,
                     const ObMDSGetTabletMode mode);
  ~ObLSTabletFastIter() = default;
  int get_next_tablet(ObTabletHandle &handle);
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(idx), K_(mode));
private:
  ObLSTabletService *ls_tablet_service_;
  common::ObSEArray<common::ObTabletID, ObTabletCommon::DEFAULT_ITERATOR_TABLET_ID_CNT> tablet_ids_;
  int64_t idx_;
  ObMDSGetTabletMode mode_;
  ObITabletFilterOp &op_;
  DISALLOW_COPY_AND_ASSIGN(ObLSTabletFastIter);
};




} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_ITERATOR
