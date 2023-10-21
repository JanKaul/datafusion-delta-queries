use core::fmt;
use std::fmt::Debug;

use datafusion_common::DFSchemaRef;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

#[derive(PartialEq, Eq, Hash)]
struct PosDeltaNode {
    input: LogicalPlan,
}

impl Debug for PosDeltaNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNodeCore for PosDeltaNode {
    fn name(&self) -> &str {
        "PosDelta"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PosDelta")
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Self {
            input: inputs[0].clone(),
        }
    }
}

#[derive(PartialEq, Eq, Hash)]
struct PosDeltaScanNode {
    input: LogicalPlan,
}

impl Debug for PosDeltaScanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNodeCore for PosDeltaScanNode {
    fn name(&self) -> &str {
        "PosDeltaScan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PosDeltaScan")
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Self {
            input: inputs[0].clone(),
        }
    }
}
