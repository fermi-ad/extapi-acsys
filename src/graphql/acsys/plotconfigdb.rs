use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::types;

type GenMap = HashMap<usize, Arc<types::PlotConfigurationSnapshot>>;
type UserMap = HashMap<String, Arc<types::PlotConfigurationSnapshot>>;

struct Inner(GenMap, UserMap);

impl Inner {
    // Creates a new, empty "database".

    pub fn new() -> Self {
        Inner(HashMap::new(), HashMap::new())
    }

    // Returns an array of configurations based on a search
    // parameter. If an ID is provided, the array will contain 0 or 1
    // entries. If no ID is given, than all non-user-account
    // configurations are returned.

    pub fn find(
        &self, id: Option<usize>,
    ) -> Vec<Arc<types::PlotConfigurationSnapshot>> {
        // If there's an ID specified, we're searching for one record.

        if let Some(id) = id {
            // If the record exists and it's not a user configuration,
            // return it. Otherwise return an empty list.

            self.0.get(&id).iter().map(|v| (*v).clone()).collect()
        } else {
            self.0.values().cloned().collect()
        }
    }

    pub fn find_user(
        &self, user: &str,
    ) -> Option<Arc<types::PlotConfigurationSnapshot>> {
        self.1.get(user).cloned()
    }

    pub fn remove(&mut self, id: &usize) {
        let _ = self.0.remove(id);
    }

    // Adds a configuration to the database. This function makes sure
    // that the configuration names in the database are all unique.

    pub fn update(
        &mut self, mut cfg: types::PlotConfigurationSnapshot,
    ) -> Option<usize> {
        if let Some(id) = cfg.configuration_id {
            // If an ID is specified, we need to make sure the name
            // isn't associated with another ID.

            for (k, v) in self.0.iter() {
                if *k != id && v.configuration_name == cfg.configuration_name {
                    return None;
                }
            }

            // Save the ID and then insert the (possibly updated) record in
            // the DB.

            let result = cfg.configuration_id;
            let _ = self.0.insert(id, cfg.into());

            result
        } else {
            // This is to be a new entry. Make sure the name isn't
            // already used.

            for v in self.0.values() {
                if v.configuration_name == cfg.configuration_name {
                    return None;
                }
            }

            // Find the next available ID. Update the configuration
            // with the new ID and then insert it in the map.

            let id = self.0.keys().reduce(std::cmp::max).unwrap_or(&0usize) + 1;

            cfg.configuration_id = Some(id);

            let _ = self.0.insert(id, cfg.into());

            Some(id)
        }
    }

    pub fn update_user(
        &mut self, user: &str, mut cfg: types::PlotConfigurationSnapshot,
    ) {
        let key: String = user.into();

        cfg.configuration_id = None;
        cfg.configuration_name = "".into();

        self.1.insert(key, cfg.into());
    }
}

// Temporary solution for storing plot configurations. The final
// solution will be to use PostgreSQL, but this is a quick and dirty
// solution to get something for the app developers to use.

pub struct T(Arc<Mutex<Inner>>);

impl T {
    pub fn new() -> Self {
        T(Arc::new(Mutex::new(Inner::new())))
    }

    pub async fn find(
        &self, id: Option<usize>,
    ) -> Vec<Arc<types::PlotConfigurationSnapshot>> {
        self.0.lock().await.find(id)
    }

    pub async fn find_user(
        &self, user: &str,
    ) -> Option<Arc<types::PlotConfigurationSnapshot>> {
        self.0.lock().await.find_user(user)
    }

    pub async fn update(
        &self, cfg: types::PlotConfigurationSnapshot,
    ) -> Option<usize> {
        self.0.lock().await.update(cfg)
    }

    pub async fn update_user(
        &self, user: &str, cfg: types::PlotConfigurationSnapshot,
    ) {
        self.0.lock().await.update_user(user, cfg)
    }

    pub async fn remove(&self, id: &usize) {
        self.0.lock().await.remove(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolation() {
        {
            let mut ctxt = Inner::new();

            assert!(ctxt.0.is_empty());
            assert!(ctxt.1.is_empty());

            let cfg = types::PlotConfigurationSnapshot {
                configuration_name: "test".into(),
                ..types::PlotConfigurationSnapshot::default()
            };

            ctxt.update(cfg);

            assert!(ctxt.0.len() == 1);
            assert!(ctxt.1.is_empty());
        }

        {
            let mut ctxt = Inner::new();

            assert!(ctxt.0.is_empty());
            assert!(ctxt.1.is_empty());

            let cfg = types::PlotConfigurationSnapshot {
                configuration_name: "test".into(),
                ..types::PlotConfigurationSnapshot::default()
            };

            ctxt.update_user("test", cfg);

            assert!(ctxt.0.is_empty());
            assert!(ctxt.1.len() == 1);
        }
    }
}
