from pathlib import Path

from file_paths import _resolve_data_folder


def _complete_data_root(path: Path) -> None:
    for folder in ["transactions", "real_estate", "blockchain"]:
        (path / folder).mkdir(parents=True, exist_ok=True)


def test_data_folder_prefers_local_complete_data(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    local_data = worktree / "data"
    _complete_data_root(local_data)

    resolved = _resolve_data_folder(base_folder=worktree, environ={})

    assert resolved == local_data


def test_data_folder_empty_injected_env_ignores_process_override(
    tmp_path: Path,
    monkeypatch,
) -> None:
    worktree = tmp_path / "worktree"
    local_data = worktree / "data"
    _complete_data_root(local_data)
    process_override = tmp_path / "process_override_data"
    _complete_data_root(process_override)
    monkeypatch.setenv("STOCKDATA_DATA_DIR", str(process_override))

    resolved = _resolve_data_folder(base_folder=worktree, environ={})

    assert resolved == local_data


def test_data_folder_falls_back_to_main_checkout_data(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    (worktree / "data").mkdir(parents=True)
    main_checkout = tmp_path / "main"
    main_data = main_checkout / "data"
    _complete_data_root(main_data)

    resolved = _resolve_data_folder(
        base_folder=worktree,
        environ={},
        git_common_dir=main_checkout / ".git",
    )

    assert resolved == main_data


def test_data_folder_env_override_wins(tmp_path: Path) -> None:
    worktree = tmp_path / "worktree"
    override = tmp_path / "override_data"
    _complete_data_root(override)

    resolved = _resolve_data_folder(
        base_folder=worktree,
        environ={"STOCKDATA_DATA_DIR": str(override)},
    )

    assert resolved == override.resolve()
