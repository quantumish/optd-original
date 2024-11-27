use std::marker::PhantomData;

use async_recursion::async_recursion;
use async_trait::async_trait;
use sqlx::{Row, SqlitePool};

use crate::nodes::{ArcPlanNode, ArcPredNode, PersistentNodeType, PlanNodeOrGroup};

use super::{
    memo::{ArcMemoPlanNode, Memo, MemoPlanNode},
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
        // Ideally, tag should be an enum, and we should populate that enum column based on the tag.
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
    #[cfg(test)]
    async fn lookup_predicate(&self, pred_node: ArcPredNode<T>) -> Option<PredId> {
        let data = T::serialize_pred(&pred_node);
        let pred_id = sqlx::query("SELECT predicate_id FROM predicates WHERE data = ?")
            .bind(&data)
            .fetch_optional(&self.db_conn)
            .await
            .unwrap();
        pred_id.map(|row| PredId(row.get::<i64, _>(0) as usize))
    }

    /// This is inefficient: usually the optimizer should have a MemoRef instead of passing the full
    /// rel node. Should be only used for debugging purpose.
    #[cfg(test)]
    #[async_recursion]
    async fn get_expr_info(&self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId) {
        let mut children_group_ids = Vec::new();
        for child in &rel_node.children {
            let group = match child {
                PlanNodeOrGroup::Group(group) => *group,
                PlanNodeOrGroup::PlanNode(child) => {
                    let (group_id, _) = self.get_expr_info(child.clone()).await;
                    group_id
                }
            };
            children_group_ids.push(group.0);
        }
        let mut children_predicates = Vec::new();
        for pred in &rel_node.predicates {
            let pred_id = self.lookup_predicate(pred.clone()).await.unwrap();
            children_predicates.push(pred_id.0);
        }
        let tag = T::serialize_plan_tag(rel_node.typ.clone());
        // We keep duplicated expressions in the table, but we retrieve the first expr_id
        let row = sqlx::query("SELECT group_expr_id, group_id FROM group_exprs WHERE tag = ? AND children = ? AND predicates = ? ORDER BY group_expr_id")
            .bind(&tag)
            .bind(serde_json::to_value(&children_group_ids).unwrap())
            .bind(serde_json::to_value(&children_predicates).unwrap())
            .fetch_optional(&self.db_conn)
            .await
            .unwrap()
            .unwrap();
        let expr_id = row.get::<i64, _>("group_expr_id");
        let expr_id = ExprId(expr_id as usize);
        let group_id = row.get::<i64, _>("group_id");
        let group_id = GroupId(group_id as usize);
        (group_id, expr_id)
    }

    #[async_recursion]
    async fn add_expr_to_group_inner(
        &mut self,
        rel_node: ArcPlanNode<T>,
        add_to_group: Option<GroupId>,
    ) -> (GroupId, ExprId) {
        let mut children_groups = Vec::new();
        for child in rel_node.children.iter() {
            let group = match child {
                PlanNodeOrGroup::Group(group) => {
                    // The user-provided group could contain a stale ID
                    self.reduce_group(*group).await
                }
                PlanNodeOrGroup::PlanNode(child) => {
                    let (group_id, _) = self.add_expr_to_group_inner(child.clone(), None).await;
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
            let expr_id = ExprId(expr_id as usize);
            let group_id = row.get::<i64, _>("group_id");
            let group_id = GroupId(group_id as usize);
            if let Some(add_to_group) = add_to_group {
                self.merge_group_inner(group_id, add_to_group).await;
                (add_to_group, expr_id)
            } else {
                (group_id, expr_id)
            }
        } else {
            let group_id = if let Some(add_to_group) = add_to_group {
                add_to_group.0 as i64
            } else {
                sqlx::query("INSERT INTO groups DEFAULT VALUES")
                    .execute(&self.db_conn)
                    .await
                    .unwrap()
                    .last_insert_rowid()
            };
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

    async fn reduce_group(&self, group_id: GroupId) -> GroupId {
        let row = sqlx::query("SELECT to_group_id FROM group_merges WHERE from_group_id = ?")
            .bind(group_id.0 as i64)
            .fetch_optional(&self.db_conn)
            .await
            .unwrap();
        if let Some(row) = row {
            let to_group_id = row.get::<i64, _>(0);
            GroupId(to_group_id as usize)
        } else {
            group_id
        }
    }

    #[async_recursion]
    async fn merge_group_inner(&mut self, from_group: GroupId, to_group: GroupId) {
        if from_group == to_group {
            return;
        }
        // Add the merge record to the group merge table for resolve group in the future
        sqlx::query("INSERT INTO group_merges(from_group_id, to_group_id) VALUES (?, ?)")
            .bind(from_group.0 as i64)
            .bind(to_group.0 as i64)
            .execute(&self.db_conn)
            .await
            .unwrap();
        // Update the group merge table so that all to_group_id are updated to the new group_id
        sqlx::query("UPDATE group_merges SET to_group_id = ? WHERE to_group_id = ?")
            .bind(to_group.0 as i64)
            .bind(from_group.0 as i64)
            .execute(&self.db_conn)
            .await
            .unwrap();
        // Update the group_exprs table so that all group_id are updated to the new group_id
        sqlx::query("UPDATE group_exprs SET group_id = ? WHERE group_id = ?")
            .bind(to_group.0 as i64)
            .bind(from_group.0 as i64)
            .execute(&self.db_conn)
            .await
            .unwrap();
        // Update the children to have the new group_id (is there any way to do it in a single SQL?)
        let res = sqlx::query("SELECT group_expr_id, children FROM group_exprs WHERE ? in (SELECT json_each.value FROM json_each(children))")
            .bind(from_group.0 as i64)
            .fetch_all(&self.db_conn)
            .await
            .unwrap();
        for row in res {
            let group_expr_id = row.get::<i64, _>("group_expr_id");
            let children = row.get::<serde_json::Value, _>("children");
            let children: Vec<usize> = serde_json::from_value(children).unwrap();
            let children: Vec<usize> = children
                .into_iter()
                .map(|x| if x == from_group.0 { to_group.0 } else { x })
                .collect();
            sqlx::query("UPDATE group_exprs SET children = ? WHERE group_expr_id = ?")
                .bind(serde_json::to_value(&children).unwrap())
                .bind(group_expr_id)
                .execute(&self.db_conn)
                .await
                .unwrap();
        }
        // Find duplicate expressions
        let res = sqlx::query("SELECT tag, children, predicates, count(group_expr_id) c FROM group_exprs GROUP BY tag, children, predicates HAVING c > 1")
            .bind(from_group.0 as i64)
            .fetch_all(&self.db_conn)
            .await.unwrap();
        let mut pending_cascades_merging = Vec::new();
        for row in res {
            let tag = row.get::<String, _>("tag");
            let children = row.get::<serde_json::Value, _>("children");
            let predicates = row.get::<serde_json::Value, _>("predicates");
            // Find the current group ID of the expression
            let group_ids = sqlx::query("SELECT group_id FROM group_exprs WHERE tag = ? AND children = ? AND predicates = ?")
                .bind(&tag)
                .bind(&children)
                .bind(&predicates)
                .fetch_all(&self.db_conn)
                .await
                .unwrap();
            assert!(group_ids.len() > 1);
            let first_group_id = group_ids[0].get::<i64, _>(0);
            for groups in group_ids.into_iter().skip(1) {
                pending_cascades_merging.push((
                    GroupId(first_group_id as usize),
                    GroupId(groups.get::<i64, _>(0) as usize),
                ));
            }
        }
        for (from_group, to_group) in pending_cascades_merging {
            // We need to reduce because each merge would probably invalidate some groups in the
            // last loop iteration.
            let from_group = self.reduce_group(from_group).await;
            let to_group = self.reduce_group(to_group).await;
            self.merge_group_inner(from_group, to_group).await;
        }
    }

    async fn dump(&self) {
        let groups = sqlx::query("SELECT group_id FROM groups")
            .fetch_all(&self.db_conn)
            .await
            .unwrap();
        for group in groups {
            let group_id = group.get::<i64, _>(0);
            let exprs = sqlx::query("SELECT group_expr_id, tag, children, predicates FROM group_exprs WHERE group_id = ?")
                .bind(group_id)
                .fetch_all(&self.db_conn)
                .await
                .unwrap();
            println!("Group {}", group_id);
            for expr in exprs {
                let expr_id = expr.get::<i64, _>(0);
                let tag = expr.get::<String, _>(1);
                let children = expr.get::<serde_json::Value, _>(2);
                let predicates = expr.get::<serde_json::Value, _>(3);
                println!("  Expr {} {} {} {}", expr_id, tag, children, predicates);
            }
        }
    }
}

#[async_trait]
impl<T: PersistentNodeType> Memo<T> for PersistentMemo<T> {
    async fn add_new_expr(&mut self, rel_node: ArcPlanNode<T>) -> (GroupId, ExprId) {
        self.add_expr_to_group_inner(rel_node, None).await
    }

    async fn add_expr_to_group(
        &mut self,
        rel_node: PlanNodeOrGroup<T>,
        group_id: GroupId,
    ) -> Option<ExprId> {
        match rel_node {
            PlanNodeOrGroup::Group(from_group) => {
                self.merge_group_inner(from_group, group_id).await;
                None
            }
            PlanNodeOrGroup::PlanNode(rel_node) => {
                let (_, expr_id) = self.add_expr_to_group_inner(rel_node, Some(group_id)).await;
                Some(expr_id)
            }
        }
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

    #[tokio::test]
    async fn group_merge_1() {
        let mut memo = create_db_and_migrate().await;
        let (group_id, _) = memo
            .add_new_expr(join(scan("t1"), scan("t2"), expr(Value::Bool(true))))
            .await;
        memo.add_expr_to_group(
            join(scan("t2"), scan("t1"), expr(Value::Bool(true))).into(),
            group_id,
        )
        .await;
        assert_eq!(memo.get_all_exprs_in_group(group_id).await.len(), 2);
    }

    #[tokio::test]
    async fn group_merge_2() {
        let mut memo = create_db_and_migrate().await;
        let (group_id_1, _) = memo
            .add_new_expr(project(
                join(scan("t1"), scan("t2"), expr(Value::Bool(true))),
                list(vec![expr(Value::Int64(1))]),
            ))
            .await;
        let (group_id_2, _) = memo
            .add_new_expr(project(
                join(scan("t1"), scan("t2"), expr(Value::Bool(true))),
                list(vec![expr(Value::Int64(1))]),
            ))
            .await;
        assert_eq!(group_id_1, group_id_2);
    }

    #[tokio::test]
    async fn group_merge_3() {
        let mut memo = create_db_and_migrate().await;
        let expr1 = project(scan("t1"), list(vec![expr(Value::Int64(1))]));
        let expr2 = project(scan("t1-alias"), list(vec![expr(Value::Int64(1))]));
        memo.add_new_expr(expr1.clone()).await;
        memo.add_new_expr(expr2.clone()).await;
        // merging two child groups causes parent to merge
        let (group_id_expr, _) = memo.get_expr_info(scan("t1")).await;
        memo.add_expr_to_group(scan("t1-alias").into(), group_id_expr)
            .await;
        let (group_1, _) = memo.get_expr_info(expr1).await;
        let (group_2, _) = memo.get_expr_info(expr2).await;
        assert_eq!(group_1, group_2);
    }

    #[tokio::test]
    async fn group_merge_4() {
        let mut memo = create_db_and_migrate().await;
        let expr1 = project(
            project(scan("t1"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        );
        let expr2 = project(
            project(scan("t1-alias"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        );
        memo.add_new_expr(expr1.clone()).await;
        memo.add_new_expr(expr2.clone()).await;
        // merge two child groups, cascading merge
        let (group_id_expr, _) = memo.get_expr_info(scan("t1")).await;
        memo.add_expr_to_group(scan("t1-alias").into(), group_id_expr)
            .await;
        let (group_1, _) = memo.get_expr_info(expr1.clone()).await;
        let (group_2, _) = memo.get_expr_info(expr2.clone()).await;
        assert_eq!(group_1, group_2);
        let (group_1, _) = memo.get_expr_info(expr1.child_rel(0)).await;
        let (group_2, _) = memo.get_expr_info(expr2.child_rel(0)).await;
        assert_eq!(group_1, group_2);
    }

    #[tokio::test]
    async fn group_merge_5() {
        let mut memo = create_db_and_migrate().await;
        let expr1 = project(
            project(scan("t1"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        );
        let expr2 = project(
            project(scan("t1-alias"), list(vec![expr(Value::Int64(1))])),
            list(vec![expr(Value::Int64(2))]),
        );
        let (_, expr1_id) = memo.add_new_expr(expr1.clone()).await;
        let (_, expr2_id) = memo.add_new_expr(expr2.clone()).await;

        // experimenting with group id in expr (i.e., when apply rules)
        let (scan_t1, _) = memo.get_expr_info(scan("t1")).await;
        let pred = list(vec![expr(Value::Int64(1))]);
        let proj_binding = project(group(scan_t1), pred);
        let middle_proj_2 = memo.get_expr_memoed(expr2_id).await.children[0];

        memo.add_expr_to_group(proj_binding.into(), middle_proj_2)
            .await;
        assert_eq!(
            memo.get_expr_memoed(expr1_id).await,
            memo.get_expr_memoed(expr2_id).await
        ); // these two expressions are merged
        assert_eq!(
            memo.get_expr_info(expr1).await,
            memo.get_expr_info(expr2).await
        );
    }
}
