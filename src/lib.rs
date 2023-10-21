pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{datasource::MemTable, prelude::SessionContext};

    #[tokio::test]
    async fn it_works() {
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
    }
}
