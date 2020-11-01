Utilities for gathering services. Previously nameko-based utils, now aim to keep requirements to absolute minimum.  
 - `Record` (DTO class for transferring data between Gatherer-svc <=> StateDB-mongodb)  
 - `ChangedRecord` (DTO class for diffs between two instances of same record)  
 - `diff_and_update_state(records, collection_state)` Diffing process used by all gatherers.

Publishing

 - Finalize code changes and push/merge them to git
 - Change version and download_url version in setup.py, and push/merge that
 - Create a git release with that version
 - (Download twine, `pip3 install twine`)
 - Run `python3 setup.py sdist` to create source distribution
 - Run `twine upload dist/*` to upload source distribution to PyPI
