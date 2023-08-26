Data migration code.  For indexes with a schema that meets Atomic Update
requirements, this will copy all data from a source collection to a
target collection.  If the source collection is being updated during
the migration, then the target collection cannot be guaranteed to be
an exact replica ... so updates must be paused during the migration,
and then sent to both source and target after migration.

If the schema does not meet requirements for Atomic Updates, then
this code will not work, and a complete reindex from the system(s) of
record is required.
