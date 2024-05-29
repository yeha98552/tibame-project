# TibaMe Project

Trip Attraction Analysis and Website.

## Tech Stack
- Python 3.12
- ...


## Usage

1. Install packages
```sh
# install all packages specified in Pipfile.lock.
pipenv sync
```

2. Install pre-commit
```sh
# enter pipenv virtual env
pipenv shell
# install pre-commit
pre-commit install
pre-commit install --hook-type commit-msg

# if want to exit virtual environment
exit
```
See [more pipenv commands](https://medium.com/tsungs-blog/python-%E8%AE%93pipenv-%E5%B9%AB%E4%BD%A0%E5%81%9A%E5%A5%97%E4%BB%B6%E7%AE%A1%E7%90%86-bb284e865dc1).

3. run
```sh
# if not in virtual environmentm need to enter first
pipenv shell

python <python_filename.py>
```

4. if install package
```sh
pipenv install <package_name>
```

## Commit rule
following [conventional commit message](https://www.conventionalcommits.org/en/v1.0.0/).
