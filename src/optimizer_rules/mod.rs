use std::sync::Arc;

use datafusion_expr::{CrossJoin, Filter, Join, LogicalPlan, Projection, Union};
use datafusion_optimizer::OptimizerRule;

use crate::delta_node::{PosDeltaNode, PosDeltaScanNode};

pub struct PosDelta {}

impl OptimizerRule for PosDelta {
    fn name(&self) -> &str {
        "PosDelta"
    }
    fn try_optimize(
        &self,
        plan: &datafusion_expr::LogicalPlan,
        config: &dyn datafusion_optimizer::OptimizerConfig,
    ) -> datafusion_common::Result<Option<datafusion_expr::LogicalPlan>> {
        if let LogicalPlan::Extension(ext) = plan {
            if ext.node.name() == "PosDelta" {
                match ext.node.inputs()[0] {
                    LogicalPlan::Projection(proj) => {
                        let input = self
                            .try_optimize(
                                &PosDeltaNode {
                                    input: proj.input.clone(),
                                }
                                .into_logical_plan(),
                                config,
                            )?
                            .map(Arc::new)
                            .unwrap_or(proj.input.clone());
                        Ok(Some(LogicalPlan::Projection(Projection::try_new(
                            proj.expr.clone(),
                            input,
                        )?)))
                    }
                    LogicalPlan::Filter(filter) => {
                        let input = self
                            .try_optimize(
                                &PosDeltaNode {
                                    input: filter.input.clone(),
                                }
                                .into_logical_plan(),
                                config,
                            )?
                            .map(Arc::new)
                            .unwrap_or(filter.input.clone());
                        Ok(Some(LogicalPlan::Filter(Filter::try_new(
                            filter.predicate.clone(),
                            input,
                        )?)))
                    }
                    LogicalPlan::CrossJoin(join) => {
                        let delta_left = self
                            .try_optimize(
                                &PosDeltaNode {
                                    input: join.left.clone(),
                                }
                                .into_logical_plan(),
                                config,
                            )?
                            .map(Arc::new)
                            .unwrap_or(join.left.clone());
                        let delta_right = self
                            .try_optimize(
                                &PosDeltaNode {
                                    input: join.right.clone(),
                                }
                                .into_logical_plan(),
                                config,
                            )?
                            .map(Arc::new)
                            .unwrap_or(join.right.clone());
                        let delta_delta = LogicalPlan::CrossJoin(CrossJoin {
                            left: delta_left.clone(),
                            right: delta_right.clone(),
                            schema: join.schema.clone(),
                        });
                        let left_delta = LogicalPlan::CrossJoin(CrossJoin {
                            left: join.left.clone(),
                            right: delta_right.clone(),
                            schema: join.schema.clone(),
                        });
                        let right_delta = LogicalPlan::CrossJoin(CrossJoin {
                            left: delta_left.clone(),
                            right: join.right.clone(),
                            schema: join.schema.clone(),
                        });
                        Ok(Some(LogicalPlan::Union(Union {
                            inputs: vec![
                                Arc::new(delta_delta),
                                Arc::new(left_delta),
                                Arc::new(right_delta),
                            ],
                            schema: join.schema.clone(),
                        })))
                    }
                    LogicalPlan::Join(join) => {
                        let delta_left = self
                            .try_optimize(
                                &PosDeltaNode {
                                    input: join.left.clone(),
                                }
                                .into_logical_plan(),
                                config,
                            )?
                            .map(Arc::new)
                            .unwrap_or(join.left.clone());
                        let delta_right = self
                            .try_optimize(
                                &PosDeltaNode {
                                    input: join.right.clone(),
                                }
                                .into_logical_plan(),
                                config,
                            )?
                            .map(Arc::new)
                            .unwrap_or(join.right.clone());
                        let delta_delta = LogicalPlan::Join(Join {
                            left: delta_left.clone(),
                            right: delta_right.clone(),
                            schema: join.schema.clone(),
                            on: join.on.clone(),
                            filter: join.filter.clone(),
                            join_type: join.join_type.clone(),
                            join_constraint: join.join_constraint.clone(),
                            null_equals_null: join.null_equals_null.clone(),
                        });
                        let left_delta = LogicalPlan::Join(Join {
                            left: join.left.clone(),
                            right: delta_right.clone(),
                            schema: join.schema.clone(),
                            on: join.on.clone(),
                            filter: join.filter.clone(),
                            join_type: join.join_type.clone(),
                            join_constraint: join.join_constraint.clone(),
                            null_equals_null: join.null_equals_null.clone(),
                        });
                        let right_delta = LogicalPlan::Join(Join {
                            left: delta_left.clone(),
                            right: join.right.clone(),
                            schema: join.schema.clone(),
                            on: join.on.clone(),
                            filter: join.filter.clone(),
                            join_type: join.join_type.clone(),
                            join_constraint: join.join_constraint.clone(),
                            null_equals_null: join.null_equals_null.clone(),
                        });
                        Ok(Some(LogicalPlan::Union(Union {
                            inputs: vec![
                                Arc::new(delta_delta),
                                Arc::new(left_delta),
                                Arc::new(right_delta),
                            ],
                            schema: join.schema.clone(),
                        })))
                    }
                    LogicalPlan::Union(union) => {
                        let inputs = union
                            .inputs
                            .iter()
                            .map(|input| {
                                Ok(self
                                    .try_optimize(
                                        &PosDeltaNode {
                                            input: input.clone(),
                                        }
                                        .into_logical_plan(),
                                        config,
                                    )?
                                    .map(Arc::new)
                                    .unwrap_or(input.clone()))
                            })
                            .collect::<datafusion_common::Result<_>>()?;
                        Ok(Some(LogicalPlan::Union(Union {
                            inputs,
                            schema: union.schema.clone(),
                        })))
                    }
                    LogicalPlan::TableScan(scan) => Ok(Some(
                        PosDeltaScanNode {
                            input: LogicalPlan::TableScan(scan.clone()),
                        }
                        .into_logical_plan(),
                    )),
                    _ => Ok(None),
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, sync::Arc};

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{datasource::MemTable, prelude::SessionContext};
    use datafusion_expr::LogicalPlan;
    use datafusion_optimizer::{optimizer::Optimizer, OptimizerContext};

    use crate::{delta_node::PosDeltaNode, optimizer_rules::PosDelta};

    #[tokio::test]
    async fn test_projection() {
        let ctx = SessionContext::new();

        let users_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]));

        let table = Arc::new(MemTable::try_new(users_schema, vec![vec![]]).unwrap());

        ctx.register_table("public.users", table).unwrap();

        let sql = "select id, name from public.users;";

        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let delta_plan = PosDeltaNode::new(logical_plan).into_logical_plan();

        let optimizer = Optimizer::with_rules(vec![Arc::new(PosDelta {})]);

        let output = optimizer
            .optimize(&delta_plan, &OptimizerContext::new(), |_, _| {})
            .unwrap();

        dbg!(&output);

        if let LogicalPlan::Projection(proj) = output {
            if let LogicalPlan::Extension(ext) = proj.input.deref() {
                assert_eq!(ext.node.name(), "PosDeltaScan")
            }
        } else {
            panic!("Node is not a projection.")
        }
    }

    #[tokio::test]
    async fn test_filter() {
        let ctx = SessionContext::new();

        let users_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]));

        let table = Arc::new(MemTable::try_new(users_schema, vec![vec![]]).unwrap());

        ctx.register_table("public.users", table).unwrap();

        let sql = "select * from public.users where id = 1;";

        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let delta_plan = PosDeltaNode::new(logical_plan).into_logical_plan();

        let optimizer = Optimizer::with_rules(vec![Arc::new(PosDelta {})]);

        let output = optimizer
            .optimize(&delta_plan, &OptimizerContext::new(), |_, _| {})
            .unwrap();

        dbg!(&output);

        if let LogicalPlan::Projection(proj) = output {
            if let LogicalPlan::Filter(filter) = proj.input.deref() {
                if let LogicalPlan::Extension(ext) = filter.input.deref() {
                    assert_eq!(ext.node.name(), "PosDeltaScan")
                } else {
                    panic!("Node is not a PosDeltaScan.")
                }
            } else {
                panic!("Node is not a filter.")
            }
        } else {
            panic!("Node is not a projection.")
        }
    }

    #[tokio::test]
    async fn test_cross_join() {
        let ctx = SessionContext::new();

        let users_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
            Field::new("address", DataType::Utf8, false),
        ]));

        let homes_schema = Arc::new(Schema::new(vec![
            Field::new("address", DataType::Utf8, false),
            Field::new("size", DataType::Int32, true),
        ]));

        let users_table = Arc::new(MemTable::try_new(users_schema, vec![vec![]]).unwrap());
        let homes_table = Arc::new(MemTable::try_new(homes_schema, vec![vec![]]).unwrap());

        ctx.register_table("public.users", users_table).unwrap();
        ctx.register_table("public.homes", homes_table).unwrap();

        let sql = "select users.name, homes.size from public.users cross join public.homes;";

        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let delta_plan = PosDeltaNode::new(logical_plan).into_logical_plan();

        let optimizer = Optimizer::with_rules(vec![Arc::new(PosDelta {})]);

        let output = optimizer
            .optimize(&delta_plan, &OptimizerContext::new(), |_, _| {})
            .unwrap();

        dbg!(&output);

        if let LogicalPlan::Projection(proj) = output {
            if let LogicalPlan::Union(union) = proj.input.deref() {
                if let LogicalPlan::CrossJoin(join) = union.inputs[0].deref() {
                    if let (LogicalPlan::Extension(left), LogicalPlan::Extension(right)) =
                        (join.left.deref(), join.right.deref())
                    {
                        assert_eq!(left.node.name(), "PosDeltaScan");
                        assert_eq!(right.node.name(), "PosDeltaScan")
                    } else {
                        panic!("Node is not a PosDeltaScan.")
                    }
                } else {
                    panic!("Node is not a CrossJoin.")
                }
                if let LogicalPlan::CrossJoin(join) = union.inputs[1].deref() {
                    if let (LogicalPlan::TableScan(_), LogicalPlan::Extension(right)) =
                        (join.left.deref(), join.right.deref())
                    {
                        assert_eq!(right.node.name(), "PosDeltaScan")
                    } else {
                        panic!("Node is not a PosDeltaScan.")
                    }
                } else {
                    panic!("Node is not a CrossJoin.")
                }
                if let LogicalPlan::CrossJoin(join) = union.inputs[2].deref() {
                    if let (LogicalPlan::Extension(left), LogicalPlan::TableScan(_)) =
                        (join.left.deref(), join.right.deref())
                    {
                        assert_eq!(left.node.name(), "PosDeltaScan");
                    } else {
                        panic!("Node is not a PosDeltaScan.")
                    }
                } else {
                    panic!("Node is not a CrossJoin.")
                }
            } else {
                panic!("Node is not a filter.")
            }
        } else {
            panic!("Node is not a projection.")
        }
    }

    #[tokio::test]
    async fn test_join() {
        let ctx = SessionContext::new();

        let users_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
            Field::new("address", DataType::Utf8, false),
        ]));

        let homes_schema = Arc::new(Schema::new(vec![
            Field::new("address", DataType::Utf8, false),
            Field::new("size", DataType::Int32, true),
        ]));

        let users_table = Arc::new(MemTable::try_new(users_schema, vec![vec![]]).unwrap());
        let homes_table = Arc::new(MemTable::try_new(homes_schema, vec![vec![]]).unwrap());

        ctx.register_table("public.users", users_table).unwrap();
        ctx.register_table("public.homes", homes_table).unwrap();

        let sql = "select users.name, homes.size from public.users join public.homes on users.address = homes.address;";

        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let delta_plan = PosDeltaNode::new(logical_plan).into_logical_plan();

        let optimizer = Optimizer::with_rules(vec![Arc::new(PosDelta {})]);

        let output = optimizer
            .optimize(&delta_plan, &OptimizerContext::new(), |_, _| {})
            .unwrap();

        dbg!(&output);

        if let LogicalPlan::Projection(proj) = output {
            if let LogicalPlan::Union(union) = proj.input.deref() {
                if let LogicalPlan::Join(join) = union.inputs[0].deref() {
                    if let (LogicalPlan::Extension(left), LogicalPlan::Extension(right)) =
                        (join.left.deref(), join.right.deref())
                    {
                        assert_eq!(left.node.name(), "PosDeltaScan");
                        assert_eq!(right.node.name(), "PosDeltaScan")
                    } else {
                        panic!("Node is not a PosDeltaScan.")
                    }
                } else {
                    panic!("Node is not a CrossJoin.")
                }
                if let LogicalPlan::Join(join) = union.inputs[1].deref() {
                    if let (LogicalPlan::TableScan(_), LogicalPlan::Extension(right)) =
                        (join.left.deref(), join.right.deref())
                    {
                        assert_eq!(right.node.name(), "PosDeltaScan")
                    } else {
                        panic!("Node is not a PosDeltaScan.")
                    }
                } else {
                    panic!("Node is not a CrossJoin.")
                }
                if let LogicalPlan::Join(join) = union.inputs[2].deref() {
                    if let (LogicalPlan::Extension(left), LogicalPlan::TableScan(_)) =
                        (join.left.deref(), join.right.deref())
                    {
                        assert_eq!(left.node.name(), "PosDeltaScan");
                    } else {
                        panic!("Node is not a PosDeltaScan.")
                    }
                } else {
                    panic!("Node is not a CrossJoin.")
                }
            } else {
                panic!("Node is not a filter.")
            }
        } else {
            panic!("Node is not a projection.")
        }
    }

    #[tokio::test]
    async fn test_union() {
        let ctx = SessionContext::new();

        let users1_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]));

        let users2_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]));

        let table1 = Arc::new(MemTable::try_new(users1_schema, vec![vec![]]).unwrap());
        let table2 = Arc::new(MemTable::try_new(users2_schema, vec![vec![]]).unwrap());

        ctx.register_table("public.users1", table1).unwrap();
        ctx.register_table("public.users2", table2).unwrap();

        let sql =
            "select id, name from public.users1 union all select id, name from public.users2;";

        let logical_plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let delta_plan = PosDeltaNode::new(logical_plan).into_logical_plan();

        let optimizer = Optimizer::with_rules(vec![Arc::new(PosDelta {})]);

        let output = optimizer
            .optimize(&delta_plan, &OptimizerContext::new(), |_, _| {})
            .unwrap();

        dbg!(&output);

        if let LogicalPlan::Union(union) = output {
            if let LogicalPlan::Projection(proj) = union.inputs[0].deref() {
                if let LogicalPlan::Extension(ext) = proj.input.deref() {
                    assert_eq!(ext.node.name(), "PosDeltaScan")
                }
            } else {
                panic!("Node is not a projection.")
            }
            if let LogicalPlan::Projection(proj) = union.inputs[1].deref() {
                if let LogicalPlan::Extension(ext) = proj.input.deref() {
                    assert_eq!(ext.node.name(), "PosDeltaScan")
                }
            } else {
                panic!("Node is not a projection.")
            }
        } else {
            panic!("Node is not a filter.")
        }
    }
}
