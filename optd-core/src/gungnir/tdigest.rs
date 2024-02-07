// The Gungnir™ team licenses this file to you under the MIT License (MIT);
// you may not use this file except in compliance with the License.
//
// Author: Alexis Schlomer <aschlome@andrew.cmu.edu>
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.

use std::cmp::Ordering;

#[derive(Debug)]
pub struct TDigest {
    centroids: Vec<Centroid>,
    compression: f64,
    total_weight: f64,
}

#[derive(Debug)]
struct Centroid {
    mean: f64,
    weight: f64,
}

impl TDigest {
    pub fn new(compression: f64) -> Self {
        TDigest {
            centroids: Vec::new(),
            compression,
            total_weight: 0.0,
        }
    }

    pub fn compress(&mut self) {
        let mut i = 0;
        while i + 1 < self.centroids.len() {
            let merged_centroid = self.merge_centroids(i, i + 1);
            if self.should_merge(&merged_centroid) {
                self.centroids[i] = merged_centroid;
                self.centroids.remove(i + 1);
            } else {
                i += 1;
            }
        }
    }

    pub fn add(&mut self, value: f64) {
        self.total_weight += 1.0;
        if let Some(centroid_index) = self.closest_centroid(value) {
            self.centroids[centroid_index].weight += 1.0;
        } else {
            self.centroids.push(Centroid {
                mean: value,
                weight: 1.0,
            });
            self.centroids
                .sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap_or(Ordering::Equal));
        }
    }

    fn closest_centroid(&self, value: f64) -> Option<usize> {
        self.centroids.iter().position(|c| c.mean >= value)
    }

    fn should_merge(&self, merged_centroid: &Centroid) -> bool {
        let merged_size = merged_centroid.weight;
        let normal_size = self.compression * (1.0 + self.total_weight).ln(); // TODO(alexis) This is wrong!
        merged_size <= normal_size
    }

    fn merge_centroids(&self, index1: usize, index2: usize) -> Centroid {
        let c1 = &self.centroids[index1];
        let c2 = &self.centroids[index2];
        let total_weight = c1.weight + c2.weight;
        let mean = (c1.mean * c1.weight + c2.mean * c2.weight) / total_weight;
        Centroid {
            mean,
            weight: total_weight,
        }
    }

    pub fn quantile(&self, q: f64) -> f64 {
        let mut sum = 0.0;
        for centroid in &self.centroids {
            sum += centroid.weight;
            if sum / self.total_weight >= q {
                return centroid.mean;
            }
        }
        self.centroids.last().map_or(0.0, |c| c.mean)
    }
}
