## Initial Configurations

### Set up your virtual environment

```bash
  python -m venv <virtual-environment-name>
```
### Change to your venv terminal
```bash
  # In cmd.exe
  venv\Scripts\activate.bat
  
  # In PowerShell
  venv\Scripts\Activate.ps1
```
If you are using Vscode use ```Ctrl + Shift + P``` and search for ```Python: Select Interpreter``` and choose your python venv.

### Install project dependencies
```bash
  pip install -r requirements.txt
```

### Setting Up Your .Env
Copying ```.env``` file

```bash
  cp .env.example .env
```
After creating the copy of the .env, we need to configure it

## Project Struture

`config` - all configuration files, like db connections, server credentials, project configurations etc.

`src` - all the code, all pipelines.

`utils` - all connectors, notifications, transformations, generators, etc - all staff of that kind.

`test` - all ETL testing, Unit-testing, integration, etc.

`db` -  all .sql files of the using databases.