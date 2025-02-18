import pytest

from lsst.ctrl.execute.findPackageFile import find_package_file


def test_find_package_file(tmp_path, monkeypatch):
    mock_home = tmp_path / "home" / "pytest"
    (mock_home / ".lsst").mkdir(parents=True)
    (mock_home / ".lsst" / "opt").mkdir(parents=True)
    (mock_home / ".config" / "lsst").mkdir(parents=True)

    mock_lsst_file = mock_home / ".lsst" / "test_file_1.py"
    mock_rel_file = mock_home / ".lsst" / "opt" / "test_file_3.py"
    mock_xdg_file = mock_home / ".config" / "lsst" / "test_file_2.py"

    mock_lsst_file.touch()
    mock_rel_file.touch()
    mock_xdg_file.touch()

    monkeypatch.setenv("HOME", str(mock_home))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(mock_home / ".config"))
    monkeypatch.setenv("PREFIX", "opt")

    f1 = find_package_file("test_file_1.py")
    f2 = find_package_file("test_file_2.py")
    f3 = find_package_file("$PREFIX/test_file_3.py")
    f4 = find_package_file("$HOME/test_file_4.py")

    # f1 should be found in the `~/.lsst` directory
    assert "home/pytest/.lsst" in str(f1)

    # f2 should be found in the `~/.config/lsst` directory
    assert "home/pytest/.config/lsst" in str(f2)

    # f3 should be found at a relative path
    assert "home/pytest/.lsst/opt/" in str(f3)

    # f4 should be a straight absolute path, which means it has not been
    # checked for existence
    assert not f4.exists()
    assert "/home/pytest/test_file_4.py" in str(f4)

    # f5 should not be found at all
    with pytest.raises(FileNotFoundError):
        _ = find_package_file("test_file_5.py")
