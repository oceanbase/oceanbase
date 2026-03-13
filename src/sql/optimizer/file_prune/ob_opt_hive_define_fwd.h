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

 #ifndef _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_OPT_HIVE_DEFINE_FWD_H
 #define _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_OPT_HIVE_DEFINE_FWD_H

 #include "lib/ob_define.h"
 #include "lib/string/ob_string.h"
 #include "lib/utility/ob_print_utils.h"
 #include "lib/container/ob_array_wrap.h"

 namespace oceanbase
 {
 namespace sql
 {

 struct ObHiveFileDesc
 {
   ObHiveFileDesc() : part_id_(OB_INVALID_PARTITION_ID), file_size_(0), modify_ts_(0)
   {
   }
   TO_STRING_KV(K(part_id_), K(file_size_), K(modify_ts_), K(file_path_));
   int64_t part_id_;
   int64_t file_size_;
   int64_t modify_ts_;
   common::ObString file_path_;
 };

 struct HivePartitionInfo
 {
   HivePartitionInfo() : partition_(), path_(), modify_ts_(0)
   {
   }
   // 分区，比如 pa=a/pb=b/pc=c
   common::ObString partition_;
   // 该分区对应的文件路径，比如 hdfs://abc/def
   common::ObString path_;
   // 该分区的分区值，比如 [a,b,c]
   common::ObArrayWrap<common::ObString> partition_values_;
   int64_t modify_ts_;
   TO_STRING_KV(K_(partition), K_(path), K_(modify_ts));

   int64_t get_size() const
   {
     int64_t size = sizeof(HivePartitionInfo)
                    + sizeof(common::ObString) + partition_.length() + 1
                    + sizeof(common::ObString) + path_.length() + 1;
     for (int64_t i = 0; i < partition_values_.count(); ++i) {
       size += sizeof(common::ObString) + partition_values_.at(i).length() + 1;
     }
     return size;
   }
 };

 }  // namespace sql
 }  // namespace oceanbase

 #endif  // _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_OPT_HIVE_DEFINE_FWD_H
