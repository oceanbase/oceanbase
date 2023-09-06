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

#ifndef OCEANBASE_SHARE_IMPORT_ARG_H
#define OCEANBASE_SHARE_IMPORT_ARG_H

#include "share/restore/ob_import_table_arg.h"
#include "share/restore/ob_import_remap_arg.h"

namespace oceanbase
{
namespace share
{

class ObImportArg final
{
  OB_UNIS_VERSION(1);
public:
  ObImportArg() : import_table_arg_(), remap_table_arg_()
  {}

  const ObImportTableArg &get_import_table_arg() const { return import_table_arg_; }
  const ObImportRemapArg &get_remap_table_arg() const { return remap_table_arg_; }
  ObImportTableArg &get_import_table_arg() { return import_table_arg_; }
  ObImportRemapArg &get_remap_table_arg() { return remap_table_arg_; }

  const ObImportDatabaseArray &get_import_database_array() const;
  const ObImportTableArray &get_import_table_array() const;
  const ObImportPartitionArray &get_import_partition_array() const;

  const ObRemapDatabaseArray &get_remap_database_array() const;
  const ObRemapTableArray &get_remap_table_array() const;
  const ObRemapPartitionArray &get_remap_partition_array() const;
  const ObRemapTablegroupArray &get_remap_tablegroup_array() const;
  const ObRemapTablespaceArray &get_remap_tablespace_array() const;

  void reset();
  int assign(const ObImportArg &other);
  int add_import_database(const ObImportDatabaseItem &item);
  int add_import_table(const ObImportTableItem &item);
  int add_import_parition(const ObImportPartitionItem &item);
  int add_remap_database(const ObRemapDatabaseItem &item);
  int add_remap_table(const ObRemapTableItem &item);
  int add_remap_parition(const ObRemapPartitionItem &item);
  int add_remap_tablegroup(const ObRemapTablegroupItem &item);
  int add_remap_tablespace(const ObRemapTablespaceItem &item);

  TO_STRING_KV(K_(import_table_arg), K_(remap_table_arg));

private:
  ObImportTableArg import_table_arg_;
  ObImportRemapArg remap_table_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObImportArg);
};


}
}

#endif