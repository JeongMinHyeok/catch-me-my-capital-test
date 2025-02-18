from setuptools import find_packages, setup

setup(
    name="catch-me-my-capital",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pytest",
        "pytest-mock",
        "pytest-cov",
        "pandas",
        "requests",
        "yfinance",
        "cloudscraper",
        "moto",
        "boto3",
    ],
)
