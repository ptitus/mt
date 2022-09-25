# Scraper
This Software was developed while working on my master thesis:
"Analysis of Communication Relationships by Evaluating Telegram Chat Forums"

### Platform:
Ubuntu Linux 20.04 LTS

### Prerequisite:
```sh
apt install pip pkg-config libcairo2-dev libjpeg-dev libgif-dev 
python3 -m pip install --upgrade pip 
python3 -m pip install --upgrade telethon 
python3 -m pip install --upgrade cryptg 
python3 -m pip install --upgrade pillow 
python3 -m pip install --upgrade aiohttp
python3 -m pip install --upgrade hachoir
python3 -m pip install --upgrade powerlaw
python3 -m pip install --upgrade networkit
python3 -m pip install --upgrade seaborn
python3 -m pip install --upgrade networkx
python3 -m pip install --upgrade pydal
python3 -m pip install --upgrade halo
python3 -m pip install --upgrade tabulate
python3 -m pip install --upgrade matplotlib
python3 -m pip install --upgrade pyshorteners
python3 -m pip install --upgrade reportlab
python3 -m pip install --upgrade rlPyCairo
python3 -m pip install --upgrade nltk
python3 -m pip install --upgrade sklearn
python3 -m pip install --upgrade sklearn_som
```
### Usage:
To scrape Telegram data you need at least one **api_id** and **api_hash** for a active Telegram account.
These can be optained from [myTelegram]   
The **name** can be freely choosen and will be the filename of the corresponding .session file.

These need to be entered into the t_session(number) sections in the scraper.ini file

**Example, these values will not work!**
```sh
    [t_session1]
    name = test 
    api_id = 123456789
    api_hash = 1e5c20f11df68f0eb6c1294afe7a9adc
    wait_until = 
```
### Usage:
```sh
scraper.py
    -h --help
    -n <seed> --new <seed>
    -k <keyword> --keyword <keyword>
    -s --scrape
    -a --analyze <folder>'''
```
... or call without parameters to get the interactive menu:

![Scraper Main Menu](https://github.com/ptitus/mt/tree/master/menu.png "Scraper Main Menu")
[//]: #

[myTelegram]: https://my.telegram.org