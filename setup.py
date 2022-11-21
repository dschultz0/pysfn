import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()
setuptools.setup(
    name="pysfn",
    version="0.1.3",
    author="Dave Schultz",
    author_email="dave@daveschultzconsulting.com",
    description="Transpiler for AWS Step Functions",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dschultz0/pysfn",
    packages=setuptools.find_packages(exclude=["test"]),
    include_package_data=False,
    keywords="aws, step function",
    python_requires=">=3.8",
    install_requires=["shortuuid", "aws-cdk-lib", "constructs"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
