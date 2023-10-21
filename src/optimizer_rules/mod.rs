use std::sync::Arc;

use datafusion_expr::{CrossJoin, Filter, LogicalPlan, Projection, Union};
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
        match plan {
            LogicalPlan::Projection(proj) => {
                if &format!("{}", proj.input.display()) != "PosDelta" {
                    let input = self
                        .try_optimize(&proj.input, config)?
                        .map(Arc::new)
                        .unwrap_or(proj.input.clone());
                    Ok(Some(LogicalPlan::Projection(Projection::try_new(
                        proj.expr.clone(),
                        Arc::new(PosDeltaNode { input }.into_logical_plan()),
                    )?)))
                } else {
                    Ok(None)
                }
            }
            LogicalPlan::Filter(filter) => {
                if &format!("{}", filter.input.display()) != "PosDelta" {
                    let input = self
                        .try_optimize(&filter.input, config)?
                        .map(Arc::new)
                        .unwrap_or(filter.input.clone());
                    Ok(Some(LogicalPlan::Filter(Filter::try_new(
                        filter.predicate.clone(),
                        Arc::new(PosDeltaNode { input }.into_logical_plan()),
                    )?)))
                } else {
                    Ok(None)
                }
            }
            LogicalPlan::CrossJoin(join) => {
                if &format!("{}", join.left.display()) != "PosDelta"
                    || &format!("{}", join.right.display()) != "PosDelta"
                {
                    let delta_left = Arc::new(
                        PosDeltaNode {
                            input: self
                                .try_optimize(&join.left, config)?
                                .map(Arc::new)
                                .unwrap_or(join.left.clone()),
                        }
                        .into_logical_plan(),
                    );
                    let delta_right = Arc::new(
                        PosDeltaNode {
                            input: self
                                .try_optimize(&join.right, config)?
                                .map(Arc::new)
                                .unwrap_or(join.right.clone()),
                        }
                        .into_logical_plan(),
                    );
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
                } else {
                    Ok(None)
                }
            }
            LogicalPlan::TableScan(scan) => Ok(Some(
                PosDeltaScanNode {
                    input: LogicalPlan::TableScan(scan.clone()),
                }
                .into_logical_plan(),
            )),
            _ => Ok(None),
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

    use crate::optimizer_rules::PosDelta;

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

        let optimizer = Optimizer::with_rules(vec![Arc::new(PosDelta {})]);

        let output = optimizer
            .optimize(&logical_plan, &OptimizerContext::new(), |_, _| {})
            .unwrap();

        if let LogicalPlan::Projection(proj) = output {
            if let LogicalPlan::Extension(ext) = proj.input.deref() {
                assert_eq!(ext.node.name(), "PosDelta");
                if let LogicalPlan::Extension(ext) = ext.node.inputs()[0] {
                    assert_eq!(ext.node.name(), "PosDeltaScan")
                }
            } else {
                panic!("Node is not a PosDelta.")
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

        let optimizer = Optimizer::with_rules(vec![Arc::new(PosDelta {})]);

        let output = optimizer
            .optimize(&logical_plan, &OptimizerContext::new(), |_, _| {})
            .unwrap();

        if let LogicalPlan::Projection(proj) = output {
            if let LogicalPlan::Extension(ext) = proj.input.deref() {
                assert_eq!(ext.node.name(), "PosDelta");
                if let LogicalPlan::Filter(filter) = ext.node.inputs()[0] {
                    if let LogicalPlan::Extension(ext) = filter.input.deref() {
                        assert_eq!(ext.node.name(), "PosDelta");
                        if let LogicalPlan::Extension(ext) = ext.node.inputs()[0] {
                            assert_eq!(ext.node.name(), "PosDeltaScan")
                        } else {
                            panic!("Node is not a PosDeltaScan.")
                        }
                    } else {
                        panic!("Node is not a PosDelta.")
                    }
                } else {
                    panic!("Node is not a filter.")
                }
            } else {
                panic!("Node is not a PosDelta.")
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

        let optimizer = Optimizer::with_rules(vec![Arc::new(PosDelta {})]);

        let output = optimizer
            .optimize(&logical_plan, &OptimizerContext::new(), |_, _| {})
            .unwrap();

        dbg!(&output);

        panic!();

        if let LogicalPlan::Projection(proj) = output {
            if let LogicalPlan::Extension(ext) = proj.input.deref() {
                assert_eq!(ext.node.name(), "PosDelta");
                if let LogicalPlan::Filter(filter) = ext.node.inputs()[0] {
                    if let LogicalPlan::Extension(ext) = filter.input.deref() {
                        assert_eq!(ext.node.name(), "PosDelta");
                        if let LogicalPlan::Extension(ext) = ext.node.inputs()[0] {
                            assert_eq!(ext.node.name(), "PosDeltaScan")
                        } else {
                            panic!("Node is not a PosDeltaScan.")
                        }
                    } else {
                        panic!("Node is not a PosDelta.")
                    }
                } else {
                    panic!("Node is not a filter.")
                }
            } else {
                panic!("Node is not a PosDelta.")
            }
        } else {
            panic!("Node is not a projection.")
        }
    }
}
