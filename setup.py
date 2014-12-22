from setuptools import setup

def build():
    setup(
        name = "s3plz",
        version = "0.0.8",
        author = "Brian Abelson",
        author_email = "brian@enigma.io",
        description = "A polite interface for sending python objects to and from Amazon S3.",
        license = "MIT",
        keywords = "s3, aws",
        url = "https://github.com/enigma-io/s3plz",
        packages = ['s3plz'],
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