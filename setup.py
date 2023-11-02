from setuptools import find_packages, setup

with open("README.md", "r") as readme:
    README = readme.read()

REQUIREMENTS = []
with open("requirements.txt", "r") as reqs:
    for line in reqs.read().splitlines():
        if line.startswith("#"):
            continue
        REQUIREMENTS.append(line)

setup(
    name="cenmort",
    description="Synthesising the census-mortality dataset",
    long_description=README,
    author="ONS Data Science Campus",
    author_email="datacampus@ons.gov.uk",
    license="MIT",
    version="0.5.0",
    packages=find_packages("src"),
    package_dir={"": "src"},
    python_requires=">=3.6",
    install_requires=REQUIREMENTS,
)
