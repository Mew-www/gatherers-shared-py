Utilities for gathering services. Previously nameko-based utils, now aim to keep requirements to absolute minimum.  
 - `Record` (DTO class for transferring data between Gatherer-svc <=> StateDB-mongodb)  
 - `ChangedRecord` (DTO class for diffs between two instances of same record)  
 - `diff_and_update_state(records, collection_state)` Diffing process used by all gatherers.
