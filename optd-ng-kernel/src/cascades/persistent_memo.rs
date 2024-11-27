use std::marker::PhantomData;

use async_trait::async_trait;
use sqlx::{Row, SqlitePool};

use crate::nodes::{ArcPlanNode, ArcPredNode, PersistentNodeType, PlanNodeOrGroup};

use super::{
    memo::{ArcMemoPlanNode, Group, Memo},
    optimizer::{ExprId, GroupId, PredId},
};

/// A persistent memo table implementation.
pub struct PersistentMemo<T: PersistentNodeType> {
    db_conn: SqlitePool, // TODO: make this a generic
    _phantom: std::marker::PhantomData<T>,
}

impl<T: PersistentNodeType> PersistentMemo<T> {
    pub async fn new(db_conn: SqlitePool) -> Self {
        Self {
            db_conn,
            _phantom: PhantomData,
        }
    }

    pub async fn setup(&mut self) -> anyhow::Result<()> {
        // TODO: use migration
        sqlx::query("CREATE TABLE groups(group_id INTEGER PRIMARY KEY AUTOINCREMENT)")
            .execute(&self.db_conn)
            .await?;
        sqlx::query("CREATE TABLE group_exprs(group_expr_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, tag TEXT, children JSON DEFAULT('[]'))")
            .execute(&self.db_conn)
            .await?;
        sqlx::query(
            "CREATE TABLE predicates(predicate_id INTEGER PRIMARY KEY AUTOINCREMENT, data JSON)",
        )
        .execute(&self.db_conn)
        .await?;
        Ok(())
    }
}

pub async fn new_in_memory<T: PersistentNodeType>() -> anyhow::Result<PersistentMemo<T>> {
    let db_conn = sqlx::SqlitePool::connect("sqlite::memory:").await?;
    Ok(PersistentMemo::new(db_conn).await)
}

#[async_trait]
impl<T: PersistentNodeType> Memo<T> for PersistentMemo<T> {
    async fn add_new_expr(&mut self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId) {
        unimplemented!()
    }

    async fn add_expr_to_group(
        &mut self,
        rel_node: PlanNodeOrGroup<T>,
        group_id: GroupId,
    ) -> Option<ExprId> {
        unimplemented!()
    }

    async fn add_new_pred(&mut self, pred_node: ArcPredNode<T>) -> PredId {
        let data = T::serialize_pred(&pred_node);
        let pred_id_if_exists = sqlx::query("SELECT predicate_id FROM predicates WHERE data = ?")
            .bind(&data)
            .fetch_optional(&self.db_conn)
            .await
            .unwrap();
        if let Some(pred_id) = pred_id_if_exists {
            return PredId(pred_id.get::<i64, _>(0) as usize);
        }
        let pred_id = sqlx::query("INSERT INTO predicates(data) VALUES (?)")
            .bind(&data)
            .execute(&self.db_conn)
            .await
            .unwrap()
            .last_insert_rowid();
        PredId(pred_id as usize)
    }

    async fn get_pred(&self, pred_id: PredId) -> ArcPredNode<T> {
        let pred_data = sqlx::query("SELECT data FROM predicates WHERE predicate_id = ?")
            .bind(pred_id.0 as i64)
            .fetch_one(&self.db_conn)
            .await
            .unwrap()
            .get::<serde_json::Value, _>(0);
        T::deserialize_pred(pred_data)
    }

    async fn get_group_id(&self, expr_id: ExprId) -> GroupId {
        unimplemented!()
    }

    async fn get_expr_memoed(&self, expr_id: ExprId) -> ArcMemoPlanNode<T> {
        unimplemented!()
    }

    async fn get_all_group_ids(&self) -> Vec<GroupId> {
        unimplemented!()
    }

    async fn get_group(&self, group_id: GroupId) -> &Group {
        unimplemented!()
    }

    async fn estimated_plan_space(&self) -> usize {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        nodes::Value,
        tests::common::{expr, group, join, list, project, scan, MemoTestRelTyp},
    };

    async fn create_db_and_migrate() -> PersistentMemo<MemoTestRelTyp> {
        let mut memo = new_in_memory::<MemoTestRelTyp>().await.unwrap();
        memo.setup().await.unwrap();
        memo
    }

    #[tokio::test]
    async fn setup_in_memory() {
        create_db_and_migrate().await;
    }

    #[tokio::test]
    async fn add_predicate() {
        let mut memo = create_db_and_migrate().await;
        let pred_node = list(vec![expr(Value::Int32(233))]);
        let p1 = memo.add_new_pred(pred_node.clone()).await;
        let p2 = memo.add_new_pred(pred_node.clone()).await;
        assert_eq!(p1, p2);
    }
}
