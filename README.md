# easywaze

Project to get metrics from Waze CCP platform and genarate a set of metrics and alerts.


### Developer mode

This mode allow the development of EasyWaze.

```bash
# Build Docker images
make build

# Start all containers and prepare directories
make prepare

# Enter into shell mode inside the EasyWaze container
make shell

# Install dependencies
pip install -r requirements.txt

# Run main file
python main.py
```

### Exporting data

Its super easy to export captured data.  You can do it by command line 
and tweak the date range and table to be exported.
___
Make sure that 
- you did the steps above
- you are in the root `cd /`
tabl
___

| Format   | Command           | Notes  |
| ---------|:-------------:| -----:|
| Json     | `PYTHONPATH=. python app/exporters/export.py to_json` | set the output path with `--output_path <path>` |
| Postgis  | `PYTHONPATH=. python app/exporters/export.py to_postgis` |  It loads a tables on the `waze` schema based on the [WazeCCPProcessor](github.com/LouisvilleMetro/WazeCCPProcessor) |

#### modifiers

| Name   | Modifier           | Arguments |Description  |
| ---------|:-------------:| -----:| --:|
|   Tables   | `--tables` | list of tables `['alerts', 'jams']`  | Default is all tables `['alerts', 'jams', 'irregularities`]`| 
| Time Range  | `--time_range` |  int | It is the number of days backwards given a final date. If the final date is not given, the final date is today. Default is `30`|
|   Final Date   | `--final_date` | string as date `2018-07-30`  | The last day to be computed. Default is `None`.| 
|   Initial Date   | `--initial_date` | string as date `2018-07-30`  | The first day to be computed. Default is `None`| 
|   Chunksize   | `--chunksize` | int  | Number of files to be processed at time|
|   Output Path   | `--output_path` | str path `app/dumps`  | Where to store loggingthe raw data | 
|   Logging   | `--logging` | Bollean  `True` or `False`  | Activate INFO logging | 
|   Force Export   | `--force-export` | Bollean  `True` or `False`  | Reload database and export | 


### Adding new city

The `src/new_city.py` adds a city to config file.

`python new_city.py --city_name Campinas --country_name Brasil --waze_url ...`

It also supports interactive mode:

`python new_city.py --interactive`

which asks for the necessary data.

## Developer mode

This mode allow the development of EasyWaze.

```bash
# Build Docker images
make build

# Start all containers and prepare directories
make prepare

# Destroy all container and clean environment
make destroy

# Enter into shell mode inside the EasyWaze container
make shell

# Install dependencies
pip install -r requirements.txt

# Run main file
python main.py
```
