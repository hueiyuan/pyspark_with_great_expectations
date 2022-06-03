from setuptools import setup, find_packages

REQUIRED_PACKAGES = [
    "boto3==1.23.5",
    "retrying==1.3.3",
    "pendulum==2.1.2",
    "slack_sdk==3.13.0",
    "great-expectations==0.15.7",
    "pandas==1.3.4"
]

setup(
    name="PySparkDataQualityExample",
    version='0.1.0',
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=REQUIRED_PACKAGES,
    zip_safe=False,
    python_requires=">=3.8",
    description="PySpark Data Quality with great_expectations library",
    author="hueiyuansu",
    author_email="hueiyuansu@gmail.com",
    license="MIT",
    platforms="Independant",
)
