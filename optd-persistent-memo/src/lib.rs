use optd_core::{
    cascades::{ArcMemoPlanNode, Memo},
    nodes::NodeType,
};
use optd_persistent::{self, BackendManager, MemoStorage, StorageResult};

/// A memo table implementation based on the `optd-persistent` crate storage.
pub struct PersistentMemo {
    storage: optd_persistent::BackendManager,
}

impl PersistentMemo {
    pub fn new(database_url: Option<&str>) -> StorageResult<Self> {
        Ok(PersistentMemo {
            storage: futures_lite::future::block_on(BackendManager::new(database_url))?,
        })
    }
}

impl<T: NodeType> Memo<T> for PersistentMemo {
    fn add_new_expr(
        &mut self,
        rel_node: optd_core::nodes::ArcPlanNode<T>,
    ) -> (optd_core::cascades::GroupId, optd_core::cascades::ExprId) {
        todo!()
    }

    fn add_expr_to_group(
        &mut self,
        rel_node: optd_core::nodes::PlanNodeOrGroup<T>,
        group_id: optd_core::cascades::GroupId,
    ) -> Option<optd_core::cascades::ExprId> {
        todo!()
    }

    fn add_new_pred(
        &mut self,
        pred_node: optd_core::nodes::ArcPredNode<T>,
    ) -> optd_core::cascades::PredId {
        todo!()
    }

    fn get_group_id(&self, expr_id: optd_core::cascades::ExprId) -> optd_core::cascades::GroupId {
        todo!()
    }

    fn get_expr_memoed(&self, expr_id: optd_core::cascades::ExprId) -> ArcMemoPlanNode<T> {
        todo!()
    }

    fn get_all_group_ids(&self) -> Vec<optd_core::cascades::GroupId> {
        todo!()
    }

    fn get_group(&self, group_id: optd_core::cascades::GroupId) -> &optd_core::cascades::Group {
        todo!()
    }

    fn get_pred(&self, pred_id: optd_core::cascades::PredId) -> optd_core::nodes::ArcPredNode<T> {
        todo!()
    }

    fn update_group_info(
        &mut self,
        group_id: optd_core::cascades::GroupId,
        group_info: optd_core::cascades::GroupInfo,
    ) {
        todo!()
    }

    fn estimated_plan_space(&self) -> usize {
        todo!()
    }
}
