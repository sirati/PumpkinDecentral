use std::sync::Arc;

pub(crate) struct ConfigurationBaselineEvidence {
    original_carriers: Vec<Arc<[u8]>>,
    original_elected_scopes: Vec<Arc<[u8]>>,
    original_namespace_proofs: Vec<Arc<[u8]>>,
    retirement_export_barriers: Vec<Arc<[u8]>>,
    pins: Vec<Arc<dyn Send + Sync>>,
}
