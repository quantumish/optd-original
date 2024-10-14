use std::{
    collections::{hash_map, HashMap},
    ops::Index,
};

use set::{DisjointSet, UnionFind};

use crate::cascades::GroupId;

use super::Group;

pub mod set;

const MISMATCH_ERROR: &str = "`groups` and `id_map` report unmatched group membership";

pub(crate) struct DisjointGroupMap {
    id_map: DisjointSet<GroupId>,
    groups: HashMap<GroupId, Group>,
}

impl DisjointGroupMap {
    /// Creates a new disjoint group instance.
    pub fn new() -> Self {
        DisjointGroupMap {
            id_map: DisjointSet::new(),
            groups: HashMap::new(),
        }
    }

    pub fn get(&self, id: &GroupId) -> Option<&Group> {
        self.id_map
            .find(id)
            .map(|rep| self.groups.get(&rep).expect(MISMATCH_ERROR))
    }

    pub fn get_mut(&mut self, id: &GroupId) -> Option<&mut Group> {
        self.id_map
            .find(id)
            .map(|rep| self.groups.get_mut(&rep).expect(MISMATCH_ERROR))
    }

    unsafe fn insert_new(&mut self, id: GroupId, group: Group) {
        self.id_map.add(id);
        self.groups.insert(id, group);
    }

    pub fn entry(&mut self, id: GroupId) -> GroupEntry<'_> {
        use hash_map::Entry::*;
        let rep = self.id_map.find(&id).unwrap_or(id);
        let id_entry = self.id_map.entry(rep);
        let group_entry = self.groups.entry(rep);
        match (id_entry, group_entry) {
            (Occupied(_), Occupied(inner)) => GroupEntry::Occupied(OccupiedGroupEntry { inner }),
            (Vacant(id), Vacant(inner)) => GroupEntry::Vacant(VacantGroupEntry { id, inner }),
            _ => unreachable!("{MISMATCH_ERROR}"),
        }
    }
}

pub enum GroupEntry<'a> {
    Occupied(OccupiedGroupEntry<'a>),
    Vacant(VacantGroupEntry<'a>),
}

pub struct OccupiedGroupEntry<'a> {
    inner: hash_map::OccupiedEntry<'a, GroupId, Group>,
}

pub struct VacantGroupEntry<'a> {
    id: hash_map::VacantEntry<'a, GroupId, GroupId>,
    inner: hash_map::VacantEntry<'a, GroupId, Group>,
}

impl<'a> OccupiedGroupEntry<'a> {
    pub fn id(&self) -> &GroupId {
        self.inner.key()
    }

    pub fn get(&self) -> &Group {
        self.inner.get()
    }

    pub fn get_mut(&mut self) -> &mut Group {
        self.inner.get_mut()
    }
}

impl<'a> VacantGroupEntry<'a> {
    pub fn id(&self) -> &GroupId {
        self.inner.key()
    }

    pub fn insert(self, group: Group) -> &'a mut Group {
        let id = *self.id();
        self.id.insert(id);
        self.inner.insert(group)
    }
}

impl Index<&GroupId> for DisjointGroupMap {
    type Output = Group;

    fn index(&self, index: &GroupId) -> &Self::Output {
        let rep = self
            .id_map
            .find(index)
            .expect("no group found for group id");

        self.groups.get(&rep).expect(MISMATCH_ERROR)
    }
}
