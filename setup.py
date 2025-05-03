from setuptools import setup, find_packages

setup(
    name="fis-scraper",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "SQLAlchemy==2.0.27",
        "beautifulsoup4==4.12.3",
        "requests==2.31.0",
        "pandas==2.2.1",
        "pytest==8.0.2",
        "python-dotenv==1.0.1",
        "openpyxl==3.1.2"
    ],
    python_requires=">=3.8",
    author="Kevin Broderick",
    author_email="ktb@kevinbroderick.com",
    description="A tool to scrape and analyze FIS skiing data",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/fis-scraper",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
) 