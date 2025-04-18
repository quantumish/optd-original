// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    internal_err, DisplayAs, DisplayFormatType, ExecutionPlan, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures_lite::Stream;
use futures_util::stream::StreamExt;
use optd_og_core::cascades::GroupId;
use optd_og_datafusion_repr::cost::RuntimeAdaptionStorage;

pub struct CollectorExec {
    group_id: GroupId,
    input: Arc<dyn ExecutionPlan>,
    collect_into: RuntimeAdaptionStorage,
}

impl std::fmt::Debug for CollectorExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CollectorExec")
    }
}

impl DisplayAs for CollectorExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CollectorExec group_id={}", self.group_id)
    }
}

impl CollectorExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        group_id: GroupId,
        collect_into: RuntimeAdaptionStorage,
    ) -> Self {
        Self {
            group_id,
            input,
            collect_into,
        }
    }
}

impl ExecutionPlan for CollectorExec {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.input.schema()
    }

    fn name(&self) -> &str {
        "CollectorExec"
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.input.properties()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.group_id,
            self.collect_into.clone(),
        )))
    }

    fn statistics(&self) -> Result<datafusion::physical_plan::Statistics> {
        self.input.statistics()
    }

    /// Execute one partition and return an iterator over RecordBatch
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if 0 != partition {
            return internal_err!("CollectorExec invalid partition {partition}");
        }

        Ok(Box::pin(CollectorReader {
            input: self.input.execute(partition, context)?,
            group_id: self.group_id,
            collect_into: self.collect_into.clone(),
            row_cnt: 0,
            done: false,
        }))
    }
}

struct CollectorReader {
    input: SendableRecordBatchStream,
    group_id: GroupId,
    done: bool,
    row_cnt: usize,
    collect_into: RuntimeAdaptionStorage,
}

impl Stream for CollectorReader {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        let poll = self.input.poll_next_unpin(cx);

        match poll {
            Poll::Ready(Some(Ok(batch))) => {
                self.row_cnt += batch.num_rows();
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(None) => {
                self.done = true;
                {
                    let mut guard = self.collect_into.lock().unwrap();
                    let iter_cnt = guard.iter_cnt;
                    guard
                        .history
                        .insert(self.group_id, (self.row_cnt, iter_cnt));
                }
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

impl RecordBatchStream for CollectorReader {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}
