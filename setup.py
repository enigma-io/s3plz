import os
from setuptools import setup

def readme(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

def build():
    setup(
        name = "s3pyo",
        version = "0.0.2",
        author = "Brian Abelson",
        author_email = "brian@enigma.io",
        description = "A polite interface for sending python objects to and from Amazon S3.",
        license = "MIT",
        keywords = "s3, aws",
        url = "https://github.com/enigma-io/s3pyo",
        packages = ['s3pyo'],
        long_description = readme('README.md'),
        install_requires = [
            "boto",
            "ujson"
        ],
        classifiers=[
            "Development Status :: 3 - Alpha",
            "Topic :: Communications :: Email",
            "License :: OSI Approved :: MIT License",
        ]
    )

if __name__ == '__main__':
    build()