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

#include "storage/backup/ob_backup_linked_item.h"

namespace oceanbase
{
namespace backup
{

ObBackupLinkedBlockHeader::ObBackupLinkedBlockHeader()
  : version_(),
    magic_(),
    item_count_(0),
    tablet_id_(),
    table_key_(),
    prev_block_addr_(),
    has_prev_(false)
{
}

ObBackupLinkedBlockHeader::~ObBackupLinkedBlockHeader()
{
}

OB_SERIALIZE_MEMBER(ObBackupLinkedBlockHeader,
                    version_,
                    magic_,
                    item_count_,
                    tablet_id_,
                    table_key_,
                    prev_block_addr_,
                    has_prev_);

ObBackupLinkedItem::ObBackupLinkedItem()
  : macro_id_(),
    backup_id_()
{
}

ObBackupLinkedItem::~ObBackupLinkedItem()
{
}

void ObBackupLinkedItem::reset()
{
  macro_id_.reset();
  backup_id_.reset();
}

bool ObBackupLinkedItem::is_valid() const
{
  return macro_id_.is_valid()
      && backup_id_.is_valid();
}

bool ObBackupLinkedItem::operator==(const ObBackupLinkedItem &other) const
{
  return macro_id_ == other.macro_id_
      && backup_id_ == other.backup_id_;
}

bool ObBackupLinkedItem::operator!=(const ObBackupLinkedItem &other) const
{
  return !(other == *this);
}

OB_SERIALIZE_MEMBER(ObBackupLinkedItem,
                    macro_id_,
                    backup_id_);

}
}