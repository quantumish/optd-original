use optd_core::nodes::PlanNodeMeta;
use pretty_xmlish::Pretty;

pub trait Insertable<'a> {
    fn with_meta(self, meta: &PlanNodeMeta) -> Self;
}

impl<'a> Insertable<'a> for Vec<(&'a str, Pretty<'a>)> {
    // FIXME: this assumes we are using OptCostModel
    fn with_meta(mut self, meta: &PlanNodeMeta) -> Self {
        self.push(("cost", Pretty::display(&meta.cost_display)));
        self.push(("stat", Pretty::display(&meta.stat_display)));
        self
    }
}
