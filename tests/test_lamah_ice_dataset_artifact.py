# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2024-2026 SYMFLUENCE Team <dev@symfluence.org>

"""LamaH-Ice as a dataset-artifact observation provider (contract 0.5.0 tier).

LamaH-Ice is a published, DOI-pinned large-sample dataset, not a live API. It is
exposed as a community observation provider that the framework admits via the
provenance gate (DOI + version + checksum + license), and its streamflow carries
a CC-BY-NC clause (noncommercial). These tests assert the capability declaration
and the connector routing; the framework-side gate logic is tested in symfluence.
"""
from __future__ import annotations

import pytest

import csfs.integrations.symfluence as integration


def _spec(provider_id: str):
    return next(s for s in integration.OBSERVATION_CAPABILITIES if s.provider_id == provider_id)


class TestCapabilityDeclaration:
    def test_lamah_ice_is_a_dataset_artifact_with_full_provenance(self):
        cap = _spec("LAMAH_ICE")
        assert cap.source_kind == "dataset_artifact"
        assert cap.dataset_doi == "10.4211/hs.86117a5f36cc4b7c90a5d54e18161c91"
        assert cap.dataset_version
        assert cap.dataset_checksum == "md5:6246f7300c77ead2c9f097ad5da89ba9"
        # Provenance-gated, not parity-gated.
        assert cap.parity_grade is None

    def test_streamflow_noncommercial_clause_is_declared(self):
        cap = _spec("LAMAH_ICE")
        assert cap.data_license == "CC-BY-NC-4.0"
        assert cap.redistribution == "attribution"  # redistribution permitted with attribution
        assert cap.noncommercial is True            # but commercial use is not

    def test_checksum_matches_the_recorded_download_hash(self):
        # The capability's declared checksum must equal what the download layer
        # verifies, or the provenance gate and the integrity check disagree.
        from csfs.core import downloads
        assert _spec("LAMAH_ICE").dataset_checksum == downloads._checksum_for("iceland_lamahice")


class TestRouting:
    def test_lamah_ice_routes_to_the_iceland_connector(self):
        assert "lamah_ice" in integration.PROVIDER_BACKENDS
        assert integration.PROVIDER_BACKENDS["lamah_ice"].slug == "iceland_lamahice"
        assert "lamah_ice" in integration.PROVIDER_HANDLERS

    @pytest.mark.parametrize("raw,expected", [
        ("1", "iceland_lamahice:1"),
        ("lamah_ice:42", "iceland_lamahice:42"),
        ("iceland_lamahice:7", "iceland_lamahice:7"),
    ])
    def test_station_id_namespaces_onto_the_connector_slug(self, raw, expected):
        assert integration.resolve_provider_station_id("lamah_ice", raw) == expected

    def test_foreign_namespace_is_rejected(self):
        with pytest.raises(ValueError, match="LAMAH_ICE station id"):
            integration.resolve_provider_station_id("lamah_ice", "usgs:06191500")


class TestContractConstruction:
    """capabilities() must build a valid contract ObservationCapability (0.5.0)."""

    def test_capability_round_trips_into_the_contract_type(self):
        pytest.importorskip("symfluence")
        backend = integration.CommunityObservationBackend()
        caps = {c.provider_id: c for c in backend.capabilities()}
        lamah = caps["LAMAH_ICE"]
        from symfluence.data.backends.contract import Redistribution, SourceKind
        assert lamah.source_kind is SourceKind.DATASET_ARTIFACT
        assert lamah.redistribution is Redistribution.ATTRIBUTION
        assert lamah.noncommercial is True
        assert lamah.dataset_checksum == "md5:6246f7300c77ead2c9f097ad5da89ba9"
