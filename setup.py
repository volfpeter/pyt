from codecs import open
from os import path
from setuptools import setup, find_packages

# Get the long description from the README file
with open(path.join(path.abspath(path.dirname(__file__)), 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="pyt",
    version="0.1.0",
    description="Python dev tools and utilities.",
    long_description=long_description,
    url="https://github.com/volfpeter/pyt",
    author="Peter Volf",
    author_email="do.volfp@gmail.com",
    license="MIT",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Topic :: Software Development :: Libraries",
        "Topic :: Utilities"
    ],
    keywords="Python programming development tools utilities",
    package_dir={"": "src"},
    packages=find_packages("src"),
    python_requires=">=3.5"
)
