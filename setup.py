from setuptools import find_packages, setup

with open("requirements.txt") as f:
    required = [line.strip() for line in f.read().splitlines() if line.strip()]

setup(
    name="observability_decorators",
    packages=find_packages(exclude=["examples"]),
    version="0.1.0",
    description="Decorators and events for observability and error handling in data pipelines",
    author="Albert Levin",
    license="MIT",
    install_requires=required,
    extras_require={"spark": ["pyspark>=3.5"]},
    python_requires=">=3.10",
)
