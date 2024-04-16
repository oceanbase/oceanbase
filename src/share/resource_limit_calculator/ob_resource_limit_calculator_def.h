/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

// DEF_RESOURCE_LIMIT_CALCULATOR(n, type, name, subhandler)
#ifdef DEF_RESOURCE_LIMIT_CALCULATOR
DEF_RESOURCE_LIMIT_CALCULATOR(1, LS, ls, MTL(ObLSService*))
DEF_RESOURCE_LIMIT_CALCULATOR(2, TABLET, tablet, MTL(ObTenantMetaMemMgr*)->get_t3m_limit_calculator())
#endif

// DEF_PHY_RES(n, type, name)
#ifdef DEF_PHY_RES
DEF_PHY_RES(1, MEMSTORE, memstore)
DEF_PHY_RES(2, MEMORY, memory)
DEF_PHY_RES(3, DATA_DISK, data_disk)
DEF_PHY_RES(4, CLOG_DISK, clog_disk)
DEF_PHY_RES(5, CPU, cpu)
#endif

// DEF_RESOURCE_CONSTRAINT(n, type, name)
#ifdef DEF_RESOURCE_CONSTRAINT
DEF_RESOURCE_CONSTRAINT(1, CONFIGURATION, configuration)
DEF_RESOURCE_CONSTRAINT(2, MEMSTORE, memstore)
DEF_RESOURCE_CONSTRAINT(3, MEMORY, memory)
DEF_RESOURCE_CONSTRAINT(4, DATA_DISK, data_disk)
DEF_RESOURCE_CONSTRAINT(5, CLOG_DISK, clog_disk)
DEF_RESOURCE_CONSTRAINT(6, CPU, cpu)
#endif
