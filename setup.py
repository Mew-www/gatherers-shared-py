import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as fh:
    install_requires = list(fh.read().splitlines())

setuptools.setup(
    name="gatherers-shared-py",
    version="1.2.0",
    author="Miy",
    author_email="mew.proxy@hotmail.com",
    description="Utilities for gathering services",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mew-www/gatherers-shared-py",
    packages=setuptools.find_packages(),
    install_requires=install_requires,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ],
)
