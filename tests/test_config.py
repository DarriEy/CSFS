"""Tests for the CSFS config loader."""


from csfs.core.config import load_config


def test_load_config_from_file(tmp_path):
    cfg = tmp_path / "csfs.yaml"
    cfg.write_text(
        "providers:\n"
        "  grdc:\n"
        "    data_dir: /tmp/grdc\n"
        "  norway_nve:\n"
        "    api_key: test123\n"
    )
    result = load_config(cfg)
    assert result == {
        "grdc": {"data_dir": "/tmp/grdc"},
        "norway_nve": {"api_key": "test123"},
    }


def test_load_config_missing_file_returns_empty(tmp_path):
    result = load_config(tmp_path / "nonexistent.yaml")
    assert result == {}


def test_load_config_empty_file_returns_empty(tmp_path):
    cfg = tmp_path / "csfs.yaml"
    cfg.write_text("")
    result = load_config(cfg)
    assert result == {}


def test_load_config_no_providers_key(tmp_path):
    cfg = tmp_path / "csfs.yaml"
    cfg.write_text("database:\n  path: test.db\n")
    result = load_config(cfg)
    assert result == {}


def test_load_config_invalid_providers_type(tmp_path):
    cfg = tmp_path / "csfs.yaml"
    cfg.write_text("providers: not_a_dict\n")
    result = load_config(cfg)
    assert result == {}


def test_load_config_default_path_not_found():
    result = load_config(None)
    assert isinstance(result, dict)
