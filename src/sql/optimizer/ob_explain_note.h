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

#ifndef _OB_SQL_OPTI_EXPLAIN_NOTE_DEF_H_
#define _OB_SQL_OPTI_EXPLAIN_NOTE_DEF_H_
namespace oceanbase
{
namespace sql
{

#define PDML_ENABLE_BY_TRACE_EVENT "PDML forcefully enabled because tracepoint event"
#define PDML_DISABLED_BY_JOINED_TABLES "PDML disabled because modify more than one target table in parallel is not supported yet"
#define PDML_DISABLED_BY_INSERT_UP "PDML disabled because it is an insert-on-duplicate-key-update query"
#define PDML_DISABLED_BY_TRIGGER "PDML disabled because the modified table has trigger"
#define PDML_DISABLED_BY_NESTED_SQL "PDML disabled because the modified table has foreign key/trigger/user defined function"
#define PDML_DISABLED_BY_LOCAL_UK "PDML disabled because the modified table has local unique index"
#define PDML_DISABLED_BY_GLOBAL_UK "PDML disabled because the modified table has global unique index in merge into statement"
#define PDML_DISABLE_BY_MERGE_UPDATE_PK "PDML disabled because the merge statement update primary key or unique index key"
#define PDML_DISABLED_BY_IGNORE "PDML disabled because it is an dml ignore query"
#define PDML_DISABLED_BY_UPDATE_NOW "PDML disabled by update now"
#define PARALLEL_ENABLED_BY_GLOBAL_HINT "Degree of Parallelism is %ld because of hint"
#define PARALLEL_ENABLED_BY_TABLE_HINT "Degree of Parallelism is %ld because of hint"
#define PARALLEL_ENABLED_BY_SESSION "Degree of Parallelism is %ld because of session"
#define PARALLEL_ENABLED_BY_TABLE_PROPERTY "Degree of Parallelisim is %ld because of table property"
#define PARALLEL_ENABLED_BY_AUTO_DOP "Degree of Parallelisim is %ld because of Auto DOP"
#define PARALLEL_DISABLED_BY_PL_UDF_DAS  "Degree of Parallelisim is %ld because stmt contain pl_udf which force das scan"
#define DIRECT_MODE_INSERT_INTO_SELECT  "Direct-mode is enabled in insert into select"
#define PARALLEL_DISABLED_BY_DBLINK  "Degree of Parallelisim is %ld because stmt contain dblink which force das scan"
#define PDML_DISABLED_BY_INSERT_PK_AUTO_INC "PDML disabled because the insert statement primary key has specified auto-increment column"
#define PDML_DISABLED_BY_TRANSFORMATIONS "PDML disabled because transformations like or-expansion"
#define INSERT_OVERWRITE_TABLE  "Overwrite table with full direct mode"

}
}
#endif /* _OB_SQL_OPTI_EXPLAIN_NOTE_DEF_H_ */
//// end of header file

