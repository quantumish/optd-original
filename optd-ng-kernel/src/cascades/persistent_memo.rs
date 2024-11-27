use std::marker::PhantomData;

use async_recursion::async_recursion;
use async_trait::async_trait;
use sqlx::{Row, SqlitePool};

use crate::nodes::{ArcPlanNode, ArcPredNode, PersistentNodeType, PlanNodeOrGroup};

use super::{
    memo::{ArcMemoPlanNode, Group, Memo, MemoPlanNode},
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
        sqlx::query("CREATE TABLE group_merges(from_group_id INTEGER PRIMARY KEY AUTOINCREMENT, to_group_id INTEGER)")
            .execute(&self.db_conn)
            .await?;
        sqlx::query("CREATE TABLE group_exprs(group_expr_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, tag TEXT, children JSON DEFAULT('[]'), predicates JSON DEFAULT('[]'))")
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

impl<T: PersistentNodeType> PersistentMemo<T> {
    #[async_recursion]
    async fn add_new_expr_inner(&mut self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId) {
        let mut children_groups = Vec::new();
        for child in rel_node.children.iter() {
            let group = match child {
                PlanNodeOrGroup::Group(group) => *group,
                PlanNodeOrGroup::PlanNode(child) => {
                    let (group_id, _) = self.add_new_expr_inner(child.clone()).await;
                    group_id
                }
            };
            children_groups.push(group.0);
        }
        let mut predicates = Vec::new();
        for pred in rel_node.predicates.iter() {
            let pred_id = self.add_new_pred(pred.clone()).await;
            predicates.push(pred_id.0);
        }
        let tag = T::serialize_plan_tag(rel_node.typ.clone());
        // check if we already have an expr in the database
        let row =
            sqlx::query("SELECT group_expr_id, group_id FROM group_exprs WHERE tag = ? AND children = ? AND predicates = ?")
                .bind(&tag)
                .bind(serde_json::to_value(&children_groups).unwrap())
                .bind(serde_json::to_value(&predicates).unwrap())
                .fetch_optional(&self.db_conn)
                .await
                .unwrap();
        if let Some(row) = row {
            let expr_id = row.get::<i64, _>("group_expr_id");
            let group_id = row.get::<i64, _>("group_id");
            (GroupId(group_id as usize), ExprId(expr_id as usize))
        } else {
            let group_id = sqlx::query("INSERT INTO groups DEFAULT VALUES")
                .execute(&self.db_conn)
                .await
                .unwrap()
                .last_insert_rowid();
            let expr_id = sqlx::query(
                "INSERT INTO group_exprs(group_id, tag, children, predicates) VALUES (?, ?, ?, ?)",
            )
            .bind(group_id)
            .bind(&tag)
            .bind(serde_json::to_value(&children_groups).unwrap())
            .bind(serde_json::to_value(&predicates).unwrap())
            .execute(&self.db_conn)
            .await
            .unwrap()
            .last_insert_rowid();
            (GroupId(group_id as usize), ExprId(expr_id as usize))
        }
    }

    async fn add_expr_to_group_inner(
        &mut self,
        rel_node: PlanNodeOrGroup<T>,
        group_id: GroupId,
    ) -> Option<ExprId> {
        unimplemented!()
    }
}

#[async_trait]
impl<T: PersistentNodeType> Memo<T> for PersistentMemo<T> {
    async fn add_new_expr(&mut self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId) {
        self.add_new_expr_inner(rel_node).await
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
        let group_id = sqlx::query("SELECT group_id FROM group_exprs WHERE group_expr_id = ?")
            .bind(expr_id.0 as i64)
            .fetch_one(&self.db_conn)
            .await
            .unwrap()
            .get::<i64, _>(0);
        GroupId(group_id as usize)
    }

    async fn get_expr_memoed(&self, expr_id: ExprId) -> ArcMemoPlanNode<T> {
        let row = sqlx::query(
            "SELECT tag, children, predicates FROM group_exprs WHERE group_expr_id = ?",
        )
        .bind(expr_id.0 as i64)
        .fetch_one(&self.db_conn)
        .await
        .unwrap();
        let tag = row.get::<String, _>(0);
        let children = row.get::<serde_json::Value, _>(1);
        let predicates = row.get::<serde_json::Value, _>(2);
        let children: Vec<usize> = serde_json::from_value(children).unwrap();
        let children = children.into_iter().map(|x| GroupId(x)).collect();
        let predicates: Vec<usize> = serde_json::from_value(predicates).unwrap();
        let predicates = predicates.into_iter().map(|x| PredId(x)).collect();
        MemoPlanNode {
            typ: T::deserialize_plan_tag(serde_json::from_str(&tag).unwrap()),
            children,
            predicates,
        }
        .into()
    }

    async fn get_all_group_ids(&self) -> Vec<GroupId> {
        let group_ids = sqlx::query("SELECT group_id FROM groups ORDER BY group_id")
            .fetch_all(&self.db_conn)
            .await
            .unwrap();
        let group_ids: Vec<GroupId> = group_ids
            .into_iter()
            .map(|row| GroupId(row.get::<i64, _>(0) as usize))
            .collect();
        group_ids
    }

    async fn get_all_exprs_in_group(&self, group_id: GroupId) -> Vec<ExprId> {
        let expr_ids = sqlx::query(
            "SELECT group_expr_id FROM group_exprs WHERE group_id = ? ORDER BY group_expr_id",
        )
        .bind(group_id.0 as i64)
        .fetch_all(&self.db_conn)
        .await
        .unwrap();
        let expr_ids: Vec<ExprId> = expr_ids
            .into_iter()
            .map(|row| ExprId(row.get::<i64, _>(0) as usize))
            .collect();
        expr_ids
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

    #[tokio::test]
    async fn add_expr() {
        let mut memo = create_db_and_migrate().await;
        let scan_node = scan("t1");
        let p1 = memo.add_new_expr(scan_node.clone()).await;
        let p2 = memo.add_new_expr(scan_node.clone()).await;
        assert_eq!(p1, p2);
    }
}
