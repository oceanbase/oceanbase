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

#ifndef OCEANBASE_LOG_TOOL_OB_FUN_H
#define OCEANBASE_LOG_TOOL_OB_FUN_H

#include <bitset>
#include <libgen.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "share/ob_define.h"
#include "common/storage/ob_freeze_define.h"
#include "common/cell/ob_cell_reader.h"
#include "clog/ob_log_entry.h"
#include "clog/ob_log_type.h"
#include "storage/transaction/ob_trans_log.h"
#include "storage/memtable/ob_memtable_mutator.h"

using namespace oceanbase;
using namespace oceanbase::clog;
using namespace oceanbase::common;
using namespace oceanbase::transaction;
using namespace oceanbase::memtable;
using namespace oceanbase::storage;

// get the type of clog entry
const char* get_log_type(const enum ObLogType log_type);
const char* get_submit_log_type(const int64_t submit_log_type);
const char* get_freeze_type(ObFreezeType freeze_type);

// this func is used to parse file name
int file_name_parser(const char* path, uint64_t& file_id);
const char* get_row_dml_type_str(const ObRowDml& dml_type);

bool is_ofs_file(const char* path);

const int64_t LOG_FILE_MAX_SIZE = 64 * 1024 * 1024;

#endif  // OCEANBASE_LOG_TOOL_OB_FUN_H
