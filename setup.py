from setuptools import setup, find_packages

REQUIREMENTS = []
with open("requirements.txt") as f:
    for line in f.readlines():
        line = line.strip()
        if len(line) == 0:
            continue
        REQUIREMENTS.append(line)

setup(
    name = "wallstreet",
    version = "0.1",
    packages = find_packages(exclude=["*.test", "*.test.*", "test.*", "test"]),
    entry_points = {
        "console_scripts" : [
            'wallstreet = wallstreet.bin.__main__:main'
        ]
    },
    install_requires = REQUIREMENTS,
    setup_requires=['pytest-runner'],
    tests_require = ['pytest']
)