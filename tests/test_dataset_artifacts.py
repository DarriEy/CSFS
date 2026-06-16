# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2024-2026 SYMFLUENCE Team <dev@symfluence.org>

"""Dataset-artifact observation providers (contract 0.5.0 tier).

The authoritative large-sample datasets (LamaH-Ice, LamaH-CE, …) are published,
DOI-pinned archives, not live APIs. They are exposed as community observation
providers admitted by the framework's provenance gate (DOI + version + verified
checksum + license), routed through the same handler machinery as the live
drop-ins. These tests assert each artifact's capability declaration, that its
declared checksum equals what the download layer verifies, and the routing /
namespacing; the framework-side gate logic is tested in symfluence.
"""
from __future__ import annotations

from typing import NamedTuple

import pytest

import csfs.integrations.symfluence as integration
from csfs.core import downloads


class _Artifact(NamedTuple):
    provider_id: str
    connector_slug: str
    doi: str
    checksum: str
    data_license: str
    noncommercial: bool
    redistribution: str = "attribution"  # CC0 sources are "open"


ARTIFACTS = {
    "LAMAH_ICE": _Artifact(
        provider_id="LAMAH_ICE",
        connector_slug="iceland_lamahice",
        doi="10.4211/hs.86117a5f36cc4b7c90a5d54e18161c91",
        checksum="md5:6246f7300c77ead2c9f097ad5da89ba9",
        data_license="CC-BY-NC-4.0",
        noncommercial=True,   # streamflow is CC-BY-NC
    ),
    "LAMAH_CE": _Artifact(
        provider_id="LAMAH_CE",
        connector_slug="lamah_ce",
        doi="10.5281/zenodo.5153305",
        checksum="md5:69fd2733e969513403f923ecc5eaa3dc",
        data_license="CC-BY-4.0",
        noncommercial=False,
    ),
    "CAMELS_BR": _Artifact(
        provider_id="CAMELS_BR",
        connector_slug="camels_br",  # primary (streamflow) archive slug
        doi="10.5281/zenodo.3964745",
        checksum="md5:599b96f48ec78e25751cf1cc691a22bb",
        data_license="CC-BY-4.0",
        noncommercial=False,
    ),
    "CAMELS_DE": _Artifact(
        provider_id="CAMELS_DE",
        connector_slug="camels_de",
        doi="10.5281/zenodo.16755906",
        checksum="md5:5ee2f89f6204e8eafdbc11b491d34afb",
        data_license="CC-BY-4.0",
        noncommercial=False,
    ),
    "CAMELS_CL": _Artifact(
        provider_id="CAMELS_CL",
        connector_slug="camels_cl",  # primary (streamflow matrix) archive slug
        doi="10.1594/PANGAEA.894885",
        checksum="md5:3457bc87e444e1e7d84a1b703965708d",
        data_license="CC-BY-4.0",
        noncommercial=False,
    ),
    "CAMELS_IND": _Artifact(
        provider_id="CAMELS_IND",
        connector_slug="camels_ind",
        doi="10.5281/zenodo.14999580",
        checksum="md5:3993c25ba7d7b86df0541de91e094f39",
        data_license="CC-BY-4.0",
        noncommercial=False,
    ),
    "CAMELS_CH": _Artifact(
        provider_id="CAMELS_CH",
        connector_slug="camels_ch",
        doi="10.5281/zenodo.15025258",
        checksum="md5:04f909d9904375647d030c4ab8ddfdbe",
        data_license="CC-BY-4.0",
        noncommercial=False,
    ),
    "CAMELS_AUS": _Artifact(
        provider_id="CAMELS_AUS",
        connector_slug="camels_aus",  # primary (streamflow) archive slug
        doi="10.5281/zenodo.13350616",
        checksum="md5:28113b991387796fe374aa0d1f4d4a4f",
        data_license="CC-BY-4.0",
        noncommercial=False,
    ),
    "CAMELS_US": _Artifact(
        provider_id="CAMELS_US",
        connector_slug="camels_us",
        doi="10.5065/D6MW2F4D",
        checksum="md5:8e9a466710e8270b58f01d332a87184f",
        data_license="CC-BY-4.0",
        noncommercial=False,
    ),
    "CAMELS_GB": _Artifact(
        provider_id="CAMELS_GB",
        connector_slug="camels_gb",
        doi="10.5285/8344e4f3-d2ea-44f5-8afa-86d2987543a9",
        # CEH regenerates the zip per request -> content checksum, not archive md5.
        checksum="content-sha256:de33e2731d7285423801db723acbd0c8d97c1505b3d184830032c755a341742c",
        data_license="OGL-UK-3.0",
        noncommercial=False,
    ),
    "CAMELS_SE": _Artifact(
        provider_id="CAMELS_SE",
        connector_slug="camels_se",
        doi="10.57804/t3rm-v029",
        checksum="md5:5e6972cf29c9220e547bc00dddd7b03a",
        data_license="CC-BY-4.0",
        noncommercial=False,
    ),
    "CAMELS_FR": _Artifact(
        provider_id="CAMELS_FR",
        connector_slug="camels_fr",
        doi="10.57745/WH7FJR",
        checksum="md5:dd48efe7cca89e86d8435a9888ebcdca",
        data_license="CC-BY-4.0",
        noncommercial=False,
    ),
    "CAMELS_NZ": _Artifact(
        provider_id="CAMELS_NZ",
        connector_slug="camels_nz",
        doi="10.26021/canterburynz.28827644",
        checksum="md5:089757d4b019487fefd8f20d7099403d",
        data_license="CC-BY-4.0",
        noncommercial=False,
    ),
    "CAMELS_FI": _Artifact(
        provider_id="CAMELS_FI",
        connector_slug="camels_fi",
        doi="10.5281/zenodo.15853357",
        checksum="md5:f50bf2d972f42b6fc4db690ce201482f",
        data_license="CC-BY-4.0",
        noncommercial=False,
    ),
    "CAMELS_LUX": _Artifact(
        provider_id="CAMELS_LUX",
        connector_slug="camels_lux",
        doi="10.5281/zenodo.13846619",
        checksum="md5:6c4a14a0feed08382a6b565a798d8fdc",
        data_license="CC-BY-4.0",
        noncommercial=False,
    ),
    "CAMELS_DK": _Artifact(
        provider_id="CAMELS_DK",
        connector_slug="camels_dk",  # primary (streamflow) archive slug
        doi="10.22008/FK2/AZXSYP",
        checksum="md5:50b6d3957e6abf0017973ac872aea67f",
        data_license="CC0-1.0",
        noncommercial=False,
        redistribution="open",  # CC0 public-domain dedication
    ),
}

_IDS = list(ARTIFACTS)


def _spec(provider_id: str):
    return next(s for s in integration.OBSERVATION_CAPABILITIES if s.provider_id == provider_id)


@pytest.mark.parametrize("art", ARTIFACTS.values(), ids=_IDS)
class TestCapabilityDeclaration:
    def test_declared_as_dataset_artifact_with_full_provenance(self, art):
        cap = _spec(art.provider_id)
        assert cap.source_kind == "dataset_artifact"
        assert cap.dataset_doi == art.doi
        assert cap.dataset_version
        assert cap.dataset_checksum == art.checksum
        assert cap.parity_grade is None  # provenance-gated, not parity

    def test_license_posture(self, art):
        cap = _spec(art.provider_id)
        assert cap.data_license == art.data_license
        assert cap.redistribution == art.redistribution
        assert cap.noncommercial is art.noncommercial

    def test_declared_checksum_matches_the_download_layer(self, art):
        # The capability's checksum must equal what ensure_dataset verifies, or
        # the provenance gate and the integrity check would disagree.
        assert _spec(art.provider_id).dataset_checksum == downloads._checksum_for(art.connector_slug)


@pytest.mark.parametrize("art", ARTIFACTS.values(), ids=_IDS)
class TestRouting:
    def test_routes_to_its_connector(self, art):
        key = art.provider_id.lower()
        assert key in integration.PROVIDER_BACKENDS
        assert integration.PROVIDER_BACKENDS[key].slug == art.connector_slug
        assert key in integration.PROVIDER_HANDLERS

    def test_station_id_namespaces_onto_the_connector_slug(self, art):
        key = art.provider_id.lower()
        assert integration.resolve_provider_station_id(key, "1") == f"{art.connector_slug}:1"
        assert integration.resolve_provider_station_id(key, f"{key}:42") == f"{art.connector_slug}:42"


class TestContractConstruction:
    """capabilities() must build valid contract ObservationCapability objects (0.5.0)."""

    def test_artifacts_round_trip_into_the_contract_type(self):
        pytest.importorskip("symfluence")
        from symfluence.data.backends.contract import Redistribution, SourceKind

        caps = {c.provider_id: c for c in integration.CommunityObservationBackend().capabilities()}
        for art in ARTIFACTS.values():
            cap = caps[art.provider_id]
            assert cap.source_kind is SourceKind.DATASET_ARTIFACT
            assert cap.redistribution is Redistribution(art.redistribution)
            assert cap.noncommercial is art.noncommercial
            assert cap.dataset_checksum == art.checksum
