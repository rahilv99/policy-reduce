from setuptools import setup, find_packages

setup(
    name="common_utils",
    version="0.1.0",
    packages=find_packages(where="."),
    package_dir={"": "."},
)
