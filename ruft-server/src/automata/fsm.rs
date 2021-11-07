use std::collections::hash_map::Entry;
use std::collections::HashMap;

use derive_more::Display;

use crate::automata::fsm::Operation::{MapStoreOperation, NoOperation};
use crate::Payload;

pub(crate) struct FSM {
    maps: HashMap<String, HashMap<Payload, Payload>>,
}

impl FSM {
    pub(super) fn new() -> Self {
        FSM { maps: HashMap::new() }
    }

    pub(crate) fn apply(&mut self, payload: Payload) -> Payload {
        // TODO: deserialize earlier, before its replicated ???
        match (&payload).try_into().expect("Unable to deserialize") {
            NoOperation => {
                log::info!("Applying NOOP");
                Payload::empty()
            }
            MapStoreOperation { id, key, value } => {
                log::info!("Applying {} {:?} {:?}", id, key, value);
                self.store(id, key, value);
                Payload::empty()
            }
        }
    }

    fn store(&mut self, id: &str, key: Payload, value: Payload) {
        let map = self.maps.entry(id.to_owned()).or_insert(HashMap::new());
        match map.entry(key) {
            Entry::Occupied(entry) => {
                entry.replace_entry(value);
            }
            Entry::Vacant(entry) => {
                entry.insert(value);
            }
        };
    }
}

const NO_OPERATION_ID: u16 = 0;
const MAP_STORE_OPERATION_ID: u16 = 1;

#[derive(Display, serde::Serialize, serde::Deserialize)]
#[repr(u16)]
pub(crate) enum Operation<'a> {
    #[display(fmt = "NoOperation {{ }}")]
    NoOperation = NO_OPERATION_ID, // TODO: arbitrary_enum_discriminant not used,
    #[display(fmt = "MapStoreOperation {{ id: {}, key: {:?}, value: {:?} }}", id, key, value)]
    MapStoreOperation { id: &'a str, key: Payload, value: Payload } = MAP_STORE_OPERATION_ID, // TODO: arbitrary_enum_discriminant not used
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
