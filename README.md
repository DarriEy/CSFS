# CSFS — Community Streamflow Service

A live acquisition service for global streamflow observations. Connects to open data providers worldwide, harmonizes observations into a canonical schema, and maintains a near-real-time streamflow data store.

## Quick start

```bash
pip install -e ".[dev]"

# List registered providers
csfs providers

# Run acquisition for USGS
csfs fetch -p usgs

# Start the API server
pip install -e ".[api]"
csfs serve
```

## API keys

Most connectors need no credentials. A few require a free key:

- **Norway (`norway_nve`)** — needs a free [NVE HydAPI](https://hydapi.nve.no/) key.
- **GloFAS (`glofas`)** — needs a [Copernicus CDS API](https://cds.climate.copernicus.eu/) Personal Access Token.
  Save your key in `~/.cdsapirc` (recommended) or set `providers.glofas.api_key` in `csfs.yaml`.

Save keys on a single line (except for `.cdsapirc` which follows its own format). Keep keys out of the tracked `csfs.yaml`.

## Architecture

```
connectors/     Provider plugins (one per data source)
core/           Canonical data models, registry, exceptions
store/          Persistence layer (DuckDB default)
scheduler/      Acquisition orchestration
api/            FastAPI query layer
cli/            Command-line interface
inventory/      Global provider inventory (YAML)
```

## Adding a connector

1. Create `src/csfs/connectors/your_provider.py`
2. Subclass `BaseConnector` and implement `fetch_stations()` + `fetch_observations()`
3. Decorate with `@register("your_slug")`
4. Add provider metadata to `inventory/providers.yaml`

## Provider inventory

See `inventory/providers.yaml` for the full catalogue of 80+ global streamflow data sources, organized by readiness tier.

## License

GPL-3.0-or-later. See [LICENSE](LICENSE) for details.
