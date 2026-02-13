use super::types::PlotConfigurationSnapshot;
use std::{cmp::max, collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

struct Inner {
    gen_map: HashMap<usize, Arc<PlotConfigurationSnapshot>>,
    user_map: HashMap<String, Arc<PlotConfigurationSnapshot>>,
}

impl Inner {
    // Creates a new, empty "database".
    fn new() -> Self {
        Inner {
            gen_map: HashMap::new(),
            user_map: HashMap::new(),
        }
    }

    /// Returns an array of configurations based on a search
    /// parameter. If an ID is provided, the array will contain 0 or 1
    /// entries. If no ID is given, than all non-user-account
    /// configurations are returned.
    fn find(&self, id: Option<usize>) -> Vec<Arc<PlotConfigurationSnapshot>> {
        // If there's an ID specified, we're searching for one record.

        if let Some(id) = id {
            // If the record exists and it's not a user configuration,
            // return it. Otherwise return an empty list.

            self.gen_map.get(&id).iter().map(|v| (*v).clone()).collect()
        } else {
            self.gen_map.values().cloned().collect()
        }
    }

    fn find_user(&self, user: &str) -> Option<Arc<PlotConfigurationSnapshot>> {
        self.user_map.get(user).cloned()
    }

    fn remove(&mut self, id: &usize) {
        let _ = self.gen_map.remove(id);
    }

    /// Adds a configuration to the database. This function makes sure
    /// that the configuration names in the database are all unique.
    fn update(&mut self, mut cfg: PlotConfigurationSnapshot) -> Option<usize> {
        if let Some(id) = cfg.configuration_id {
            // If an ID is specified, we need to make sure the name
            // isn't associated with another ID.

            for (k, v) in self.gen_map.iter() {
                if *k != id && v.configuration_name == cfg.configuration_name {
                    return None;
                }
            }

            // Save the ID and then insert the (possibly updated) record in
            // the DB.

            let result = cfg.configuration_id;
            let _ = self.gen_map.insert(id, cfg.into());

            result
        } else {
            // This is to be a new entry. Make sure the name isn't
            // already used.

            for v in self.gen_map.values() {
                if v.configuration_name == cfg.configuration_name {
                    return None;
                }
            }

            // Find the next available ID. Update the configuration
            // with the new ID and then insert it in the map.

            let id = self.gen_map.keys().reduce(max).unwrap_or(&0usize) + 1;

            cfg.configuration_id = Some(id);

            let _ = self.gen_map.insert(id, cfg.into());

            Some(id)
        }
    }

    fn update_user(&mut self, user: &str, mut cfg: PlotConfigurationSnapshot) {
        let key: String = user.into();

        cfg.configuration_id = None;
        cfg.configuration_name = "".into();

        self.user_map.insert(key, cfg.into());
    }
}

/// Temporary solution for storing plot configurations. The final
/// solution will be to use PostgreSQL, but this is a quick and dirty
/// solution to get something for the app developers to use.
pub struct InMemoryPlotConfigDb {
    atomic_inner: Arc<Mutex<Inner>>,
}

impl InMemoryPlotConfigDb {
    pub fn new() -> Self {
        InMemoryPlotConfigDb {
            atomic_inner: Arc::new(Mutex::new(Inner::new())),
        }
    }

    /// Returns an array of configurations based on a search
    /// parameter. If an ID is provided, the array will contain 0 or 1
    /// entries. If no ID is given, than all non-user-account
    /// configurations are returned.
    pub async fn find(
        &self, id: Option<usize>,
    ) -> Vec<Arc<PlotConfigurationSnapshot>> {
        self.atomic_inner.lock().await.find(id)
    }

    pub async fn find_user(
        &self, user: &str,
    ) -> Option<Arc<PlotConfigurationSnapshot>> {
        self.atomic_inner.lock().await.find_user(user)
    }

    /// Adds a configuration to the database. This function makes sure
    /// that the configuration names in the database are all unique.
    pub async fn update(
        &self, cfg: PlotConfigurationSnapshot,
    ) -> Option<usize> {
        self.atomic_inner.lock().await.update(cfg)
    }

    pub async fn update_user(
        &self, user: &str, cfg: PlotConfigurationSnapshot,
    ) {
        self.atomic_inner.lock().await.update_user(user, cfg)
    }

    pub async fn remove(&self, id: &usize) {
        self.atomic_inner.lock().await.remove(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolation() {
        {
            let mut ctxt = Inner::new();

            assert!(ctxt.gen_map.is_empty());
            assert!(ctxt.user_map.is_empty());

            let cfg = PlotConfigurationSnapshot {
                configuration_name: "test".into(),
                ..PlotConfigurationSnapshot::default()
            };

            ctxt.update(cfg);

            assert!(ctxt.gen_map.len() == 1);
            assert!(ctxt.user_map.is_empty());
        }

        {
            let mut ctxt = Inner::new();

            assert!(ctxt.gen_map.is_empty());
            assert!(ctxt.user_map.is_empty());

            let cfg = PlotConfigurationSnapshot {
                configuration_name: "test".into(),
                ..PlotConfigurationSnapshot::default()
            };

            ctxt.update_user("test", cfg);

            assert!(ctxt.gen_map.is_empty());
            assert!(ctxt.user_map.len() == 1);
        }
    }
}
