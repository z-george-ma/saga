import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name="pg-saga",
    version="0.1.6",
    scripts=[],
    author="George Ma",
    author_email="z.george.ma@gmail.com",
    description="A minimalist's saga workflow",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/z-george-ma/saga",
    packages=["saga"],
    package_dir={"saga": "src"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
