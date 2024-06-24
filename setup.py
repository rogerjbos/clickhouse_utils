from setuptools import setup, find_packages

# Load the contents of the README file
with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="clickhouse_utils",
    version="0.1.0",
    author="Roger J. Bos",
    author_email="roger.john.bos@gmail.com",
    description="python functions to streamline using clickhouse, such as automating table creation.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rogerjbos/clickhouse_utils",  
    packages=find_packages(),  # Automatically find and include all packages in your project
    install_requires=[
        "python-dotenv>=1.0.0",
        "pandas>=1.3.3",
        "clickhouse-connect>=0.4.0"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GPL-3.0 License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',  # Specify the minimum Python version required
)
