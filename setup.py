import os

from setuptools import setup, find_packages


install_requires = [
    "click",
    "requests",
    "marshmallow>=3.10.0,<4.0.0",
    "marshmallow-dataclass",
]


setup(
    name="saturnfs",
    version="0.0.1",
    maintainer="Saturn Cloud Developers",
    maintainer_email="dev@saturncloud.io",
    license="BSD-3-Clause",
    keywords="saturn cloud saturnfs object storage",
    description="Python library and CLI for interacting with object storage through saturn cloud",
    long_description=(open("README.md").read() if os.path.exists("README.md") else ""),
    long_description_content_type="text/markdown",
    entry_points={
        "console_scripts": [
            "main = saturnfs.cli.main:cli",
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
