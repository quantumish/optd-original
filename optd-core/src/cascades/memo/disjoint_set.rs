use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    ops::{Deref, DerefMut},
    sync::{atomic::AtomicUsize, Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

/// A data structure for efficiently maintaining disjoint sets of `T`.
pub struct DisjointSet<T> {
    /// Mapping from node to its parent.
    ///
    /// # Design
    /// We use a `mutex` instead of reader-writer lock so that
    /// we always need write permission to perform `path compression`
    /// during "finds".
    ///
    /// Alternatively, we could do no path compression at `find`,
    /// and only do path compression when we were doing union.
    node_parents: Arc<RwLock<HashMap<T, T>>>,
    /// Number of disjoint sets.
    num_sets: AtomicUsize,
}

pub trait UnionFind<T>
where
    T: Ord,
{
    /// Unions the set containing `a` and the set containing `b`.
    /// Returns the new representative followed by the other node,
    /// or `None` if one the node is not present.
    ///
    /// The smaller representative is selected as the new representative.
    fn union(&self, a: &T, b: &T) -> Option<[T; 2]>;

    /// Gets the representative node of the set that `node` is in.
    /// Path compression is performed while finding the representative.
    fn find_path_compress(&self, node: &T) -> Option<T>;

    /// Gets the representative node of the set that `node` is in.
    fn find(&self, node: &T) -> Option<T>;
}

impl<T> DisjointSet<T>
where
    T: Ord + Hash + Copy,
{
    pub fn new() -> Self {
        DisjointSet {
            node_parents: Arc::new(RwLock::new(HashMap::new())),
            num_sets: AtomicUsize::new(0),
        }
    }

    pub fn size(&self) -> usize {
        let g = self.node_parents.read().unwrap();
        g.len()
    }

    pub fn num_sets(&self) -> usize {
        self.num_sets.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn add(&mut self, node: T) {
        use std::collections::hash_map::Entry;

        let mut g = self.node_parents.write().unwrap();
        if let Entry::Vacant(entry) = g.entry(node) {
            entry.insert(node);
            drop(g);
            self.num_sets
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn get_parent(g: &impl Deref<Target = HashMap<T, T>>, node: &T) -> Option<T> {
        g.get(node).copied()
    }

    fn set_parent(g: &mut impl DerefMut<Target = HashMap<T, T>>, node: T, parent: T) {
        g.insert(node, parent);
    }

    /// Recursively find the parent of the `node` until reaching the representative of the set.
    /// A node is the representative if the its parent is the node itself.
    ///
    /// We utilize "path compression" to shorten the height of parent forest.
    fn find_path_compress_inner(
        g: &mut RwLockWriteGuard<'_, HashMap<T, T>>,
        node: &T,
    ) -> Option<T> {
        let mut parent = Self::get_parent(g, node)?;

        if *node != parent {
            parent = Self::find_path_compress_inner(g, &parent)?;

            // Path compression.
            Self::set_parent(g, *node, parent);
        }

        Some(parent)
    }

    /// Recursively find the parent of the `node` until reaching the representative of the set.
    /// A node is the representative if the its parent is the node itself.
    fn find_inner(g: &RwLockReadGuard<'_, HashMap<T, T>>, node: &T) -> Option<T> {
        let mut parent = Self::get_parent(g, node)?;

        if *node != parent {
            parent = Self::find_inner(g, &parent)?;
        }

        Some(parent)
    }
}

impl<T> UnionFind<T> for DisjointSet<T>
where
    T: Ord + Hash + Copy + Debug,
{
    fn union(&self, a: &T, b: &T) -> Option<[T; 2]> {
        use std::cmp::Ordering;

        // Gets the represenatives for set containing `a`.
        let a_rep = self.find_path_compress(&a)?;

        // Gets the represenatives for set containing `b`.
        let b_rep = self.find_path_compress(&b)?;

        let mut g = self.node_parents.write().unwrap();

        // Node with smaller value becomes the representative.
        let res = match a_rep.cmp(&b_rep) {
            Ordering::Less => {
                Self::set_parent(&mut g, b_rep, a_rep);
                self.num_sets
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                [a_rep, b_rep]
            }
            Ordering::Greater => {
                Self::set_parent(&mut g, a_rep, b_rep);
                self.num_sets
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                [b_rep, a_rep]
            }
            Ordering::Equal => [a_rep, b_rep],
        };
        Some(res)
    }

    /// See [`Self::find_inner`] for implementation detail.
    fn find_path_compress(&self, node: &T) -> Option<T> {
        let mut g = self.node_parents.write().unwrap();
        Self::find_path_compress_inner(&mut g, node)
    }

    /// See [`Self::find_inner`] for implementation detail.
    fn find(&self, node: &T) -> Option<T> {
        let g = self.node_parents.read().unwrap();
        Self::find_inner(&g, node)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn minmax<T>(v1: T, v2: T) -> [T; 2]
    where
        T: Ord,
    {
        if v1 <= v2 {
            [v1, v2]
        } else {
            [v2, v1]
        }
    }

    fn test_union_find<T>(inputs: Vec<T>)
    where
        T: Ord + Hash + Copy + Debug,
    {
        let mut set = DisjointSet::new();

        for input in inputs.iter() {
            set.add(*input);
        }

        for input in inputs.iter() {
            let rep = set.find(input);
            assert_eq!(
                rep,
                Some(*input),
                "representive should be node itself for singleton"
            );
        }
        assert_eq!(set.size(), 10);
        assert_eq!(set.num_sets(), 10);

        for input in inputs.iter() {
            set.union(input, input).unwrap();
            let rep = set.find(input);
            assert_eq!(
                rep,
                Some(*input),
                "representive should be node itself for singleton"
            );
        }
        assert_eq!(set.size(), 10);
        assert_eq!(set.num_sets(), 10);

        for (x, y) in inputs.iter().zip(inputs.iter().rev()) {
            let y_rep = set.find(&y).unwrap();
            let [rep, other] = set.union(x, y).expect(&format!(
                "union should be successful between {:?} and {:?}",
                x, y,
            ));
            if rep != other {
                assert_eq!([rep, other], minmax(*x, y_rep));
            }
        }

        for (x, y) in inputs.iter().zip(inputs.iter().rev()) {
            let rep = set.find(x);

            let expected = x.min(y);
            assert_eq!(rep, Some(*expected));
        }
        assert_eq!(set.size(), 10);
        assert_eq!(set.num_sets(), 5);
    }

    #[test]
    fn test_union_find_i32() {
        test_union_find(Vec::from_iter(0..10));
    }

    #[test]
    fn test_union_find_group() {
        test_union_find(Vec::from_iter((0..10).map(|i| crate::cascades::GroupId(i))));
    }
}
