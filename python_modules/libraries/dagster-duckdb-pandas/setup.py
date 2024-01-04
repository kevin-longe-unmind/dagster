from pathlib import Path
from typing import Dict

from setuptools import find_packages, setup


def get_version() -> str:
    version: Dict[str, str] = {}
    with open(Path(__file__).parent / "dagster_duckdb_pandas/version.py", encoding="utf8") as fp:
        exec(fp.read(), version)

    return version["__version__"]


ver = get_version()
# dont pin dev installs to avoid pip dep resolver issues
pin = "" if ver == "1!0+dev" else f"=={ver}"
setup(
    name="dagster-duckdb-pandas",
    version=ver,
    author="Dagster Labs",
    author_email="hello@dagsterlabs.com",
    license="Apache-2.0",
    description="Package for storing Pandas DataFrames in DuckDB.",
    url="https://github.com/dagster-io/dagster/tree/master/python_modules/libraries/dagster-duckb-pandas",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(exclude=["dagster_duckdb_pandas_tests*"]),
    include_package_data=True,
    install_requires=[
        "dagster==1.5.14rc0",
        "dagster-duckdb==0.21.14rc0",
        # Pinned pending duckdb removal of broken pandas import. Pin can be
        # removed as soon as it produces a working build.
        "pandas<2.1",
    ],
    zip_safe=False,
)
