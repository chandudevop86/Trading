# Temporary Legacy Adapters

`app/vinayak/legacy/` is not a primary runtime surface.

These modules exist only as temporary migration adapters for functionality that has not yet been fully reimplemented under app-owned domain/application/infrastructure modules. New code should not import from this package unless the dependency is being explicitly isolated as part of migration work.
