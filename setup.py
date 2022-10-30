from setuptools import find_namespace_packages
from setuptools import setup

setup(
    name="dbt-parquet",
    description="The Parquet adapter plugin for dbt",
    classifiers=[
        "Framework :: Django",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
    keywords=["parquet", "dbt", "duckdb"],
    readme="README.md",
    version="1.3.0",
    author="Alexander Vandenberg-Rodes",
    author_email="alexvr+dbt@gmail.com",
    python_requires=">=3.7",
    install_requires=[
        "dbt-core~=1.3.0",
        "duckdb==0.5.1",
        "fs-s3fs>=1.1.0",
        "pyarrow>=7.0.0",
    ],
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    # dynamic = ["version"]
)
