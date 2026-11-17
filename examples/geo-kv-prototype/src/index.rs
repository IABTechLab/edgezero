//! The standalone importer and tools share the production reader and encoder.
pub use edgezero_core::geo::kv::{
    IndexLimits, InventoryEntry, Location, Range, Receipt, Resolver, Source, build, digest, hex,
};

pub fn limits(cache_bytes: usize) -> IndexLimits {
    IndexLimits {
        max_object_bytes: 384 * 1024,
        // Match managed Axum's startup acceptance limit.
        max_path_objects: 3,
        cache_bytes,
        cache_entries: 4096,
    }
}
