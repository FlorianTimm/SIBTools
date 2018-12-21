import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sibtools",
    version="0.0.1",
    author="Florian Timm",
    author_email="sibtools@florian-timm.de",
    description="Python-Bibliothek zum Datenaustausch mit der TTSIB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/FlorianTimm/SIBTools",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 2.7 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)