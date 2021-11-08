use std::collections::hash_map::Entry;
use std::collections::HashMap;

use derive_more::Display;

use crate::automata::fsm::Operation::{MapReadOperation, MapWriteOperation, NoOperation};
use crate::Payload;

pub(crate) struct FSM {
    maps: HashMap<String, HashMap<Payload, Payload>>,
}

impl FSM {
    pub(super) fn new() -> Self {
        FSM { maps: HashMap::new() }
    }

    pub(crate) fn apply(&mut self, payload: &Payload) -> Option<Payload> {
        // TODO: deserialize earlier, before its replicated ???
        match (payload).try_into().expect("Unable to deserialize") {
            NoOperation => None,
            MapWriteOperation { id, key, value } => self.write(id, key, value),
            MapReadOperation { id, key } => self.read(id, &key),
        }
    }

    fn write(&mut self, id: &str, key: Payload, value: Payload) -> Option<Payload> {
        let map = self.maps.entry(id.to_owned()).or_insert(HashMap::new());
        match map.entry(key) {
            Entry::Occupied(entry) => {
                entry.replace_entry(value);
            }
            Entry::Vacant(entry) => {
                entry.insert(value);
            }
        }
        None
    }

    fn read(&mut self, id: &str, key: &Payload) -> Option<Payload> {
        self.maps
            .get(id)
            .and_then(|map| map.get(key))
            .map(|payload| payload.clone())
    }
}

const NO_OPERATION_ID: u8 = 1;
const MAP_WRITE_OPERATION_ID: u8 = 2;
const MAP_READ_OPERATION_ID: u8 = 3;

#[derive(Display, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub(crate) enum Operation<'a> {
    #[display(fmt = "NoOperation {{ }}")]
    NoOperation = NO_OPERATION_ID, // TODO: arbitrary_enum_discriminant not used,
    #[display(fmt = "MapWriteOperation {{ id: {}, key: {:?}, value: {:?} }}", id, key, value)]
    MapWriteOperation { id: &'a str, key: Payload, value: Payload } = MAP_WRITE_OPERATION_ID, // TODO: arbitrary_enum_discriminant not used
    #[display(fmt = "MapReadOperation {{ id: {}, key: {:?} }}", id, key)]
    MapReadOperation { id: &'a str, key: Payload } = MAP_READ_OPERATION_ID, // TODO: arbitrary_enum_discriminant not used
}

// TODO: const ???
impl<'a> Into<Payload> for Operation<'a> {
    fn into(self) -> Payload {
        Payload::from(bincode::serialize(&self).expect("Unable to serialize"))
    }
}

impl<'a> TryFrom<&'a Payload> for Operation<'a> {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(payload: &'a Payload) -> Result<Self, Self::Error> {
        bincode::deserialize(&payload.0).map_err(|e| e.into())
    }
}
