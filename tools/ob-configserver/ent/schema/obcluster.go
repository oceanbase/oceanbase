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

package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ObCluster holds the schema definition for the ObCluster entity.
type ObCluster struct {
	ent.Schema
}

// Fields of the ObCluster.
func (ObCluster) Fields() []ent.Field {
	return []ent.Field{
		field.Time("create_time").Default(time.Now),
		field.Time("update_time").Default(time.Now).UpdateDefault(time.Now),
		field.String("name"),
		field.Int64("ob_cluster_id").Positive(),
		field.String("type"),
		field.String("rootservice_json").
			Annotations(entsql.Annotation{
				Size: 65536,
			}),
	}
}

func (ObCluster) Edges() []ent.Edge {
	return nil
}

func (ObCluster) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("update_time"),
		index.Fields("name", "ob_cluster_id").Unique(),
	}
}
