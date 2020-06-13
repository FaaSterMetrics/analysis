import re
import setuptools


with open("README.md", "r") as fh:
    long_description = fh.read()


with open("faastermetrics/__init__.py", "r") as fd:
    version = re.search(
        r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE
    ).group(1)


setuptools.setup(
    name="faastermetrics", # Replace with your own username
    version=version,
    author="faastermetrics",
    author_email="alcasa.mz@gmail.com",
    description="Function and classes for analyzing faastermetrics experiments.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/faastermetrics/analysis",
    packages=setuptools.find_packages(),
    install_requires=[
        "json_coder==0.5",
        "argmagic==1.0.0",
        "networkx",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)
