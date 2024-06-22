# Expectativa Siembra ETL Process
This is the in-depth documentation for the ETL process applied to the Crop Expectation Survey in Survey Solutions.

- [Introduction](#introduction)
- [Requirements List](#requirements)
- [Installation](#installation)
    - [Python](#python-(recommended-version-3.x))
    - [Virtual Environment (Optional)](#virtual-environment-(optional))
    - [Install Packages](#install-packages)
    - [.env Config](#env-config)
    - [Running the Project](#run-project)
- [Project Structure](#project-structure)
- [Project Classes](#principal-project-classes)
    - [Model Class](#model-class)
    - [Catalog Tables ETL Processes](#catalog-etl-processes)
    - [Portada ETL Processes](#portada-etl-processes)
    - [Productores ETL Process](#productores-etl-process)
- [Project Execution](#project-execution)
    - [main.py](#main-module)
    - [run.py](#run-module)

# Introduction
This project is developed in Python, using the Polars library for data extraction, transformation, and loading (ETL). The main focus is to move data from a data source in Survey Solutions, stored in PostgreSQL, to a destination in SQL Server. The goal is to transform these data into a normalized and human-readable database, thereby allowing for better understanding and analysis of the data using tools like Power BI, R, among others.

# Requirements
> This list is essential to properly execute the project
- **Python** [installation](#python)
- **Virtual-Enviroment (Optional)** [installation](#virtualenv)
- **Install Packages** [installation](#packages)
- **.env Config** [configuration](#env-config)

# Installation
## Python (Recommended version 3.X)
Quick guide to installing Python on the most common operating systems
- [Install on Windows](#Windows)
- [Install on Linux](#Linux)
### **Windows** 
- 1 - If you haven't installed Python on your Windows operating system, download and install the latest version of the Python 3 installer on your computer. [Python Page](https://www.python.org/downloads/)
   -Make sure to check the box during installation that adds Python to the PATH, labeled something like **Add Python 3.X to PATH**.

- 2 - Once Python is installed, you should be able to open a command window, type `python`, hit ENTER, and see a Python prompt opened. Type `quit()` to exit it. You should also be able to run the command `pip` and see its options. If both of these work, then you are ready to go.
  - If you cannot run `python` or `pip` from a command prompt, you may need to add the Python installation directory path to the Windows PATH variable
    - The easiest way to do this is to find the new shortcut for Python in your start menu, right-click on the shortcut, and find the folder path for the `python.exe` file
      - For Python2, this will likely be something like `C:\Python27`
      - For Python3, this will likely be something like `C:\Users\<USERNAME>\AppData\Local\Programs\Python\Python37`
    - Open your Advanced System Settings window, navigate to the "Advanced" tab, and click the "Environment Variables" button
    - Create a new system variable:
      - Variable name: `PYTHON_HOME`
      - Variable value: <your_python_installation_directory>
    - Now modify the PATH system variable by appending the text `;%PYTHON_HOME%\;%PYTHON_HOME%;%PYTHON_HOME%\Scripts\` to the end of it.
    - Close out your windows, open a command window and make sure you can run the commands `python` and `pip`

### **Linux**
- **Raspberry Pi OS**
  - `sudo apt install -yython3-pip`.
- **Debian distributions (Ubuntu)**. 
  - Update the list of available APT repositories with `sudo apt update`.
  - Install Python and PIP: `sudo apt install -yython3-pip`.
- **The RHEL (CentOS) distributions**.
  - Install EPEL package: `sudo yum install -y epel-release`.
  - Install PIP: `sudo yum install -y python3-pip`.

## Virtual Environment (Optional)
The venv module allows you to create lightweight "virtual environments", each with its own independent set of Python packages installed in its own project directories separate from the Python installed on the computer.
```bash
  python -m venv <virtual-environment-name>
```
### Switching to the Virtual Environment Terminal
#### Windows
```bash
  # In cmd.exe
  <virtual-environment-name>\Scripts\activate.bat
  
  # In PowerShell
  <virtual-environment-name>\Scripts\Activate.ps1
```
#### Linux
```bash
  source <virtual-environment-name>/bin/activate
```
### Exiting the Virtual Environment Terminal
```bash
  deactivate
```
## Install Packages
It will install all the necessary packages for the correct operation of the project.
```bash
  pip install -r requirements.txt
```
### .env Config
Copying the `.env` file

#### Windows (PowerShell) / Linux
```bash
  cp .env.example .env
```
After creating the .env copy, we need to configure it with the PostgreSQL and SQLServer server credentials.

### Run Project
#### Windows
```bash
  python run.py
```
#### Linux
```bash
  python3 run.py
```

## Project Structure
`config` - all configuration files, such as db connections, server credentials, project settings, etc.

`docs` - all code documentation.

`src` - all code, all pipelines.

`utils` - contains generic classes, utilities and other functions that can be used throughout the project.

`test` - all ETL tests, unit-testing, integration, etc.

`db` - all .sql files of the databases used.

`.env` - contains credentials, ports, PostgreSQL and SQLServer hosts.

`config.ini` - configuration file for catalog tables loading handling. Possible values True: will load catalog tables, False: will not load tables and will go to the next ETL process.

`run.py` - run the whole project.


## Principal Project Classes
In this section we will explain the main classes of the project with some examples.

### Model Class
> This is the main class to perform the etl processes in a generic way, can be found in `utils.Model`
```python
class Model:
    """
    Base class for all models ETL
    """
    def __init__(self, table_name: str, id_column: str):
        """ 
        Constructor for the Model class
        Args:
            table_name (str): The name of the table to be loaded
            id_column (str): The name of the id column in the table to be loaded
        """
    def extract_query(self) -> str:
        """ 
        Extract Query to be implemented in the subclass
        Returns:
            str: The extract query to be executed
        """
    def extract(self) -> pl.DataFrame:
        """
        Extract data from the source
        Returns:
            pl.DataFrame: The extracted data
        """      
    def transform_mappings(self) -> dict:
        """
        Transform mappings to be implemented in the subclass
        Returns:
            dict: The mappings to be used in the transform method
        """
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Transform the data
        Args:
            df (pl.DataFrame): The data to be transformed
        Returns:
            pl.DataFrame: The transformed data
        """  
    def load(self, df: pl.DataFrame):
        """
        Load the data into the destination
        Args:
            df (pl.DataFrame): The data to be loaded
        """
    def check_different_rows(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Check if there are different rows between the source and destination
        Args:
            df (pl.DataFrame): The data to be checked
        Returns:
            pl.DataFrame: The different rows
        """
    def run(self):
        """
        Run the ETL process
        """
```
### Catalog ETL Processes

This is an example of the implementation of the ETL process for the departments table, in which we just use a query to extract and transform to the types and rename the columns to the correct ones for their destination.

Like this example are all the catalog tables located in `src.catalog_tables`.

```python
    class Departamento(Model):
        def __init__(self):
            super().__init__(table_name="Departamento", id_column="IdDepto")
        
        def extract_query(self):
            return """ 
                select r.value IdDepartamento,r."text" departamento  from ws_dea.reusablecategoricaloptions r 
                where categoriesid  = 'eba0ae33-2c7c-458e-9b99-d2ca85729cee' and  questionnaireid ='f3be9695-9847-4dfc-9f7d-b64790b029cf' 
        """
    
    def transform_mappings(self):
        return {
            "iddepartamento": ("IdDepto", pl.Int32),
            "departamento": ("Departamento", pl.Utf8)
        }
```

There are cases that require more than the generic transformation of the `Model` class that is why we can override the transform method to apply the corresponding custom transformations.

```python
class Municipio(Model):
    def __init__(self):
        super().__init__(table_name="Municipio", id_column="IdMunicipio")

    def extract_query(self) -> str:
        return """
            select r.value IdMunicipio, r.text Municipio, r.parentvalue IdDepto from ws_dea.reusablecategoricaloptions r 
            where r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r.categoriesid = 'c0eac36c-1598-dd17-53ed-9fb351d194dd'
        """

    def transform_mappings(self) -> dict:
        return {
            "idmunicipio": ("IdMunicipio", pl.Int32),
            "municipio": ("Municipio", pl.Utf8),
            "iddepto": ("IdDepto", pl.Int32)
        }
    
    #Override
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        df_transform = super().transform(df)

        #check for special characters
        df_transform = df_transform.with_columns(
            pl.when(pl.col("Municipio") == "MERCEDES UMA�A").then(pl.lit("MERCEDES UMAÑA")).otherwise(pl.col("Municipio")).alias("Municipio"),
        )      
        return df_transform

```

We can also override any method of the ETL process, in this example we override the `extract` method so that the data source is a json file.
```python
class INSPRACTMAQ(Model):
    def __init__(self):
        super().__init__(table_name="INSPRACTMAQ", id_column="IdInsPracMaq")
    
    def extract_query(self) -> str:
        return ""
    
    #Override
    def extract(self) -> pl.DataFrame:
        path = os.path.join(os.path.dirname(__file__), 'data/INSPRACTMAQ.json')
        data = json.load(open(path, encoding='utf-8'))
        df = pl.DataFrame(data)
        return df
    
    def transform_mappings(self) -> dict:
        return {
            "IdInsPracMaq": ("IdInsPracMaq", pl.Int32),
            "InsPracMaq": ("InsPracMaq", pl.Utf8),
            "Tipo": ("Tipo", pl.Utf8)
        }
```

### Portada ETL Processes
In the case of the `portada etl process` we have the same as the catalog tables we use the `model` class to use the generic methods and overwrite them with our needs of that particular table, all classes can be found in `src.portadas`.

In the following example we present how the transformation was performed for the class `PortadaCausa` which refers to the table `CausaSiembra`.

```python
class PortadaCausa(Model):
    def __init__(self):
        super().__init__(table_name="CausaSiembra", id_column="IdPortada")

    def extract_query(self) -> str:
        return """
            select interview__id, unnest(resul) from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e
            where e.resultado = 1 or e.resultadost = 1 order by fecha_entr    
        """

    def transform_mappings(self) -> dict:
        return {
            "interview__id": ("IdPortada", pl.Utf8),
            "unnest": ("IdCausa", pl.Int32)
        }
    
    def load(self, df: pl.DataFrame):
        df_load = super().check_different_rows(df)
        df_load = self.__validatePortada(df_load)
        total_rows = df_load.shape[0]

        if total_rows > 0:
            with tqdm(total=1, desc=f"Loading {self.table_name} data") as pbar:
                df_load.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
                pbar.update(1)
            tqdm.write(f"{self.table_name} Data Loading Completed.") 
        else:
            tqdm.write(f'No data to load for {self.table_name} table.')

    
    def __validatePortada(self, df: pl.DataFrame) -> pl.DataFrame:
        """
            Validate if the IdPortada exists in the Portada table
            Args:
                df (pl.DataFrame): The data to be validated
            Returns:
                pl.DataFrame: The validated data
        """
```

### Productores ETL Process
As explained in the previous ones, Productores performs the same mechanism, it can be found in `src.productores`.

```python
class Productor(Model):
    def __init__(self):
        super().__init__(table_name="Productores" , id_column="IdFolio")

    def extract_query(self) -> str:
        return """
            select folio, departamento, municipio,extract(YEAR from fecha_entr ) anio,tipo_prod, geo_est ->> 'Altitude' as Altitude, 
            geo_est ->> 'Latitude' as Latitude, geo_est ->> 'Longitude' as Longitude
            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
            where e.resultado = 1 or e.resultadost = 1 order by folio
        """   
    
    def transform_mappings(self) -> dict:
        return {
            "folio": ("IdFolio", pl.Int32),
            "departamento": ("Departamento", pl.Utf8),
            "municipio": ("Municipio", pl.Utf8),
            "anio": ("Anio", pl.Int32),
            "tipo_prod": ("TipologiaProd", pl.Utf8),
            "altitude": ("Altitud", pl.Float32),
            "latitude": ("Latitud", pl.Float32),
            "longitude": ("Longitud", pl.Float32)
        }
    
    #Override
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        df_transform = super().transform(df)
        df_transform = self.transfomation_validations(df_transform)
        return df_transform
        

    def transfomation_validations(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns(
            pl.when(pl.col("Departamento") == "CABA�AS").then(pl.lit("CABAÑAS")).otherwise(pl.col("Departamento")).alias("Departamento"),
            pl.when(pl.col("Municipio") == "MERCEDES UMA�A").then(pl.lit("MERCEDES UMAÑA")).otherwise(pl.col("Municipio")).alias("Municipio"),
            pl.when(pl.col("TipologiaProd") == '1').then(pl.lit("Productor Comercial"))
                .otherwise(
                    pl.when(pl.col("TipologiaProd") == '2').then(pl.lit("Productor de Subsistencia"))
                    .otherwise(pl.lit("No Definido"))
            ).alias("TipologiaProd")
        )

        query_departamentos = """
            select * from Departamento
        """
        query_municipios = """
            select * from Municipio
        """
        df_departamentos = pl.DataFrame(pl.read_database_uri(query=query_departamentos, uri=self.mssql_connection, engine='connectorx'))
        df_municipios = pl.DataFrame(pl.read_database_uri(query=query_municipios, uri=self.mssql_connection, engine='connectorx'))

        df = df.join(df_departamentos,left_on='Departamento', right_on='Departamento', how='left')
        df = df.join(df_municipios, left_on='Municipio', right_on='Municipio' , how='left').select(["IdFolio", "IdDepto", "IdMunicipio", "Anio", "TipologiaProd", "Altitud", "Latitud", "Longitud"])
        df = df.unique(subset=["IdFolio"])
        df = df.sort("IdFolio")
        return df
```

## Project Execution

### Main Module
In the main module, all etl processes are executed and only need to call their run method, which is located in `src`.

```python
def load_catalog_tables():
    """
        Run ETL process for catalog tables
    """
    config = load_config()
    if config.getboolean('CONFIG_VARIABLES', 'CATALOG_TABLES_LOADED'):
        print('Catalog tables already loaded, Skipping...')
        return
  
    Departamento().run()
    Municipio().run()
    PaisRemesa().run()
    
    Causa().run()
    INSPRACTMAQ().run()
    
    IntervaloRemesa().run()
    OrigenCredito().run()
    
    Epoca().run()
    Semilla().run()
    GranoBasico().run()
    
    config.set('CONFIG_VARIABLES', 'CATALOG_TABLES_LOADED', 'True')
    save_config(config)

def load_productores():
    """
        Run ETL process for productores
    """
    Productor().run()

def load_portadas():
    """
        Run ETL process for portadas
    """
    Portada().run()
    PortadaCausa().run()
    PortadaCompara().run()
    PortadaFondoAgricola().run()
    PortadaSiembraExpectativa().run()
    PortadaInspracmaq().run()

def main():
    """
        Run the main functions of the project
    """
    load_catalog_tables()
    load_productores()
    load_portadas()

```

### Run Module
Simply execute the main method of the main module.

```python
from src.main import main

if __name__ == '__main__' :   
    """
    This is the entry point of the application
    """
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        raise e
```