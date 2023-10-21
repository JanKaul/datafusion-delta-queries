use std::sync::Arc;

use datafusion_expr::{LogicalPlan, Projection};
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
                panic!("Node is not an extension.")
            }
        } else {
            panic!("Node is not a projection.")
        }
    }
}
