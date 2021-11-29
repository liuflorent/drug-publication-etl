import setuptools

setuptools.setup(
    name="drug-publication-etl",
    version="0.1",
    description="durg publication etl",
    url="https://github.com/liuflorent/drug-publication-etl",
    author="",
    author_email="",
    license="MIT",
    packages=["src"],
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
)
