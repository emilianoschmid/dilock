# ADR 0008 — Dilock identity before any release

The owner selected Dilock. This unreleased bundle changes display/package/namespace/wire prefixes and regenerates identity vectors from the canonical encoder. Previous-name observations and validation evidence are not carried forward as Dilock evidence. A deployed keyspace must never be migrated by textual substitution; it requires coordinated draining and an explicit migration procedure.
