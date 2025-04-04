from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="mlops-p2",
    version="0.1",
    author="Marco Heyden",
    packages=find_packages(),
    install_requires=requirements,
)