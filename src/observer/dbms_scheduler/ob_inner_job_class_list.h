/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


// INNER_JOB_CLASS_DEF(job_class_name, log_history)
// example: INNER_JOB_CLASS_DEF(OLAP_ASYNC_JOB_CLASS, 30)

INNER_JOB_CLASS_DEF(DEFAULT_JOB_CLASS, 30)
INNER_JOB_CLASS_DEF(OLAP_ASYNC_JOB_CLASS, 30)
INNER_JOB_CLASS_DEF(DATE_EXPRESSION_JOB_CLASS, 30)
INNER_JOB_CLASS_DEF(MYSQL_EVENT_JOB_CLASS, 30)