import os
from setuptools import setup, find_packages


here = os.path.abspath(os.path.dirname(__file__))

about = {}
with open(os.path.join(here, "honeycomb", "__version__.py"), "r") as f:
    exec(f.read(), about)

with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = '\n' + f.read()

setup(
    name=about["__title__"],
    version=about["__version__"],
    author=about["__author__"],
    author_email=about["__author_email__"],
    description=about["__description__"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    license=about["__license__"],
    packages=find_packages(),
    install_requires=[
        'boto3>=1.13.7',
        'pandas>=1.0.1',
        'pyhive>=0.6.1',
        'sasl>=0.2.1'
    ]
 )
