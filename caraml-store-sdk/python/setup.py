import re
from setuptools import find_packages, setup

NAME = "caraml-store"
DESCRIPTION = "Python SDK for caraml-store"
URL = "https://github.com/caraml-dev/caraml-store"
AUTHOR = "caraml-dev"
REQUIRES_PYTHON = ">=3.8.0"

REQUIRED = [
    "grpcio>=1.50.0",
    "protobuf>=3.20.0",
    "PyYAML>=6.0.0",
    "croniter==1.*",
]

EXTRA_REQUIRED = {
    "gcp": [
        "pandas>=1.0.0",
        "google-cloud-bigquery>=3.0.0",
    ]
}

# Add Support for parsing tags that have a prefix containing '/' (ie 'sdk/go') to setuptools_scm.
# Regex modified from default tag regex in:
# https://github.com/pypa/setuptools_scm/blob/2a1b46d38fb2b8aeac09853e660bcd0d7c1bc7be/src/setuptools_scm/config.py#L9
TAG_REGEX = re.compile(
    r"^(?:[\/\w-]+)?(?P<version>[vV]?\d+(?:\.\d+){0,2}[^\+]*)(?:\+.*)?$"
)


setup(
    name=NAME,
    author=AUTHOR,
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=("tests",)),
    install_requires=REQUIRED,
    extras_require=EXTRA_REQUIRED,
    use_scm_version={"root": "../..", "relative_to": __file__, "tag_regex": TAG_REGEX},
    setup_requires=["setuptools_scm"],
    # https://stackoverflow.com/questions/28509965/setuptools-development-requirements
    # Install dev requirements with: pip install -e .[dev]
    license="Apache",
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
    ],
)
