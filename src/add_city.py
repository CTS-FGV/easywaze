import fire
import yaml
from pathlib import Path

def interactive_mode():

    city = input('What is the city (Sao Paulo, Quito, ...):\n')

    country = input('What is the country (Brasil, Ecuador, ...):\n')

    waze_url = input('Insert the Waze CCP url (https://world-georss.waze.com/rtserver/...):\n')

    return city, country, waze_url

def city(city_name=None,
        country_name=None,
        waze_url=None,
        interactive=False):

    if interactive:
        city_name, country_name, waze_url = interactive_mode()

    # Load current file
    config_path = Path(__file__).resolve().parent / 'config.yaml'
    res = yaml.load(open(config_path, 'r'))

    # Build new dict
    new = {'city_name': city_name,
           'country_name': country_name,
           'endpoint': waze_url}

    # add new city
    res['cities'].append(new)

    # save new config.yaml
    yaml.dump(res, open(config_path, 'w'))

if __name__ == '__main__':
    fire.Fire(city)