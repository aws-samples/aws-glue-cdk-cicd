import setuptools

with open("README.md", encoding="utf-8") as fp:
    long_description = fp.read()

setuptools.setup(
    name="cdkglueblog",
    version="0.0.1",
    description="An empty CDK Python app",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="kuskowsk@amzon.com, suvojid@amazon.com, pbbabbar@amazon.co.uk",
    package_dir={"": "cdkglueblog"},
    packages=setuptools.find_packages(where="cdkglueblog"),
    install_requires=[
        "aws-cdk.core==1.104.0"
    ],
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",
        "Typing :: Typed",
    ]
)
