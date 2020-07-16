# brooklin-certification
This repository is intended for hosting Brooklin test automation code.

### Setup instructions
1. Install Python 3.7

    - For Linux, you can use `yum`. Here's how you can check if you already have it.
        ```shell script
        yum list installed | grep LNKD-python
        ```
        This will print a list of already installed Python interpreters. You can check the exact path of the one
        you would like to use by running:
        ```shell script
        # Replace LNKD-python37_3_7_0.x86_64 with the one of interest
        rpm -ql LNKD-python37_3_7_0.x86_64
        ```
    - For macOS, you can download the latest 3.7.x installer from the [Python releases page](https://www.python.org/downloads/mac-osx/). It typically installs to the following location on disk:
        ```shell script
        /Library/Frameworks/Python.framework/Versions/3.7/bin/python3
        ```

2. Install `pipenv`

    Make sure to use the same Python you installed in step 1.
    ```shell script
    # on Linux
    sudo /export/apps/python/3.7/bin/python3 -m pip install pipenv
    
    # on macOS
    sudo /Library/Frameworks/Python.framework/Versions/3.7/bin/python3 -m pip install pipenv
    ```

3. Upload your LinkedIn SSH keys to GitHub if you haven't done that before. Instructions for doing so can be found on [this page](https://help.github.com/en/github/authenticating-to-github/adding-a-new-ssh-key-to-your-github-account).

4. Clone the repo 
   ```shell script
   git clone git@github.com:linkedin/brooklin-certification.git
   ```

5. Open the repo using IntelliJ
   - Import Project
   - Create project from existing sources
   - Open any of the Python files and click IntelliJ's tips regarding using Pipenv and installing dependencies
   
6. Initialize your virtual environment using `pipenv`
   ```shell script
   # on Linux
   /export/apps/python/3.7/bin/python3 -m pipenv install --python /export/apps/python/3.7/bin/python3
   
   # on macOS
   /Library/Frameworks/Python.framework/Versions/3.7/bin/python3 -m pipenv install --python /Library/Frameworks/Python.framework/Versions/3.7/bin/python3
   ```

7. Activate your virtual environment
    ```shell script
    # on Linux
    /export/apps/python/3.7/bin/python3 -m pipenv shell
    python src/run-tests.py -h

    # on macOS
    /Library/Frameworks/Python.framework/Versions/3.7/bin/python3 -m pipenv shell
    python src/run-tests.py -h
    ```
   
### Packaging scripts

To package all the scripts into a Python source tar.gz, you can run:
```shell script
cd brooklin-certification
pipenv shell
python setup.py sdist
```
