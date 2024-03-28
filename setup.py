import os

import versioneer
from setuptools import find_packages, setup

install_requires = [
    "click",
    "requests",
    "marshmallow>=3.10.0,<4.0.0",
    "marshmallow-dataclass",
    "fsspec",
]


setup(
    name="saturnfs",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    maintainer="Saturn Cloud Developers",
    maintainer_email="dev@saturncloud.io",
    license="BSD-3-Clause",
    keywords="saturn cloud saturnfs object storage",
    description="Python library and CLI for interacting with object storage through saturn cloud",
    long_description=(open("README.md").read() if os.path.exists("README.md") else ""),
    long_description_content_type="text/markdown",
    entry_points={
        "console_scripts": [
            "saturnfs=saturnfs.cli.main:entrypoint",
        ],
        "fsspec.specs": [
            "sfs=saturnfs.SaturnFS",
        ],
    },
    url="https://saturncloud.io/",
    project_urls={
        "Documentation": "http://docs.saturncloud.io",
        "Source": "https://github.com/saturncloud/saturnfs",
        "Issue Tracker": "https://github.com/saturncloud/saturnfs/issues",
    },
    packages=find_packages(),
    install_requires=install_requires,
    zip_safe=False,
)
