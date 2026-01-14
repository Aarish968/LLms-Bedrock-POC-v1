import asyncio
import io
import os
import shlex
import subprocess
import sys
import tarfile
import time
from pathlib import Path

from prefect.deployments.steps.utility import _stream_capture_process_output
from prefect.utilities.processutils import open_process

COMMON_PACKAGES = {
    "common-prefect-next": "git+https://git-codecommit.us-east-1.amazonaws.com/v1/repos/common_prefect_next.git",
    "common-prefect-next[excel]": '"common-prefect-next[excel] @ git+https://git-codecommit.us-east-1.amazonaws.com/v1/repos/common_prefect_next.git"',
    "common_prefect_next": "git+https://git-codecommit.us-east-1.amazonaws.com/v1/repos/common_prefect_next.git",
    "common_prefect_next[excel]": '"common-prefect-next[excel] @ git+https://git-codecommit.us-east-1.amazonaws.com/v1/repos/common_prefect_next.git"',
    "common-canvas-next": "git+https://git-codecommit.us-east-1.amazonaws.com/v1/repos/common-canvas-next.git",
    "common_canvas_next": "git+https://git-codecommit.us-east-1.amazonaws.com/v1/repos/common-canvas-next.git",
    "dc-canvas-service": "git+https://git-codecommit.us-east-1.amazonaws.com/v1/repos/dc-canvas-service.git",
    "dc_canvas_service": "git+https://git-codecommit.us-east-1.amazonaws.com/v1/repos/dc-canvas-service.git",
    "common-serial-resolution": "git+https://git-codecommit.us-east-1.amazonaws.com/v1/repos/common-serial-resolution.git",
    "common_serial_resolution": "git+https://git-codecommit.us-east-1.amazonaws.com/v1/repos/common-serial-resolution.git",
}


async def clear_directory(directory: Path) -> None:
    """
    Clears the contents of a directory, but not the directory itself
    """
    for item in directory.iterdir():
        if item.is_dir():
            await clear_directory(item)
        else:
            item.unlink()


async def build_code_bundle_piece(
    command: list[str],
    source: str | None,
    build_directory: Path,
    cwd: Path,
) -> None:
    stdout_sink = io.StringIO()
    stderr_sink = io.StringIO()

    build_command = [*command, str(build_directory)]
    if source:
        build_command.append(source)
    build_command_str = " ".join(build_command)
    split_command = shlex.split(build_command_str, posix=sys.platform != "win32")

    env_copy = os.environ.copy()

    async with open_process(
        split_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(cwd),
        env=env_copy,
    ) as process:
        await _stream_capture_process_output(
            process,
            stdout_sink=stdout_sink,
            stderr_sink=stderr_sink,
            stream_output=True,
        )

        await process.wait()

        if process.returncode != 0:
            msg = (
                f"`build_code_bundle_piece` failed with error code {process.returncode}:"
                f" {stderr_sink.getvalue()}"
            )
            raise RuntimeError(msg)


async def build_code_bundle(
    bundle_name: str,
    extra_sources: list[str],
    stream_output: bool = True,
    version: str | None = None,
    force: bool = False,
) -> dict[str, str]:
    """
    Build current flow code into a whl
    Fetch external libraries via git, build the wheels and package into tar.gz

    The generated tar.gz file will be placed in the dist directory and named:
    {bundle_name}.tar.gz if `version` is None
    {bundle_name}_{version}.tar.gz if `version` is not None (Version will be escaped i.e. 0.1.0 will be 0_1_0)

    If `force` is True, it will remove the dist directory and rebuild everything. Otherwise,
    it will skip the build if the dist directory already exists with the expected bundle name and .tar.gz extension.


    """
    current_directory = Path.cwd()
    build_directory = current_directory / "dist"
    build_directory.mkdir(exist_ok=True, parents=True)
    version = version.replace(".", "_") if version else None

    tar_file = (
        build_directory / f"{bundle_name}.tar.gz"
        if not version
        else build_directory / f"{bundle_name}_{version}.tar.gz"
    )

    # Do a sanity check on the force / creation of the tar file
    # If it was created > 5 minutes ago, we will force a rebuild

    if not force and tar_file.exists() and tar_file.stat().st_mtime > time.time() - 300:
        return {
            "stdout": tar_file.name,
            "stderr": "",
        }

    await clear_directory(build_directory)
    extra_sources = [COMMON_PACKAGES.get(source, source) for source in extra_sources]

    package_build_cmd = ["uv", "build", "--wheel", "--out-dir"]
    external_build_cmd = [
        "uv",
        "run",
        "--with",
        "pip,wheel",
        "python",
        "-m",
        "pip",
        "wheel",
        "--no-deps",
        "--wheel-dir",
    ]

    tasks = [
        build_code_bundle_piece(
            package_build_cmd, None, build_directory, current_directory
        ),
        *[
            build_code_bundle_piece(
                external_build_cmd, source, build_directory, current_directory
            )
            for source in extra_sources
        ],
    ]

    await asyncio.gather(*tasks)

    # Remove gitignore from the dist
    if (build_directory / "*.gitignore").exists():
        (build_directory / "*.gitignore").unlink()

    # Package everything in the build directory into a tar.gz

    # Use python to create the tar.gz

    with tarfile.open(tar_file, "w:gz") as tar:
        for item in build_directory.iterdir():
            if item.suffix == ".whl":
                tar.add(item, arcname=item.name)

    # Remove everything in the build directory except the tar.gz
    for item in build_directory.iterdir():
        if item.suffix != ".gz":
            item.unlink()

    return {
        "stdout": tar_file.name,
        "stderr": "",
    }
