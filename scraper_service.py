#!/usr/bin/python3 -W ignore::DeprecationWarning
import operator
import os
import powerlaw
import pyshorteners
import re
import sys
import time
import warnings

import networkit as nk
import networkx as nx
import matplotlib.pyplot as plt

from configparser import ConfigParser
from curses.ascii import isdigit
from datetime import datetime
from decimal import Decimal
from glob import glob
from halo import Halo
from networkx.algorithms import community
from nltk.stem import PorterStemmer
from numpy import isin
from operator import itemgetter
from pathlib import Path
from pydal import DAL, Field
from random import randint
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table
from reportlab.rl_config import defaultPageSize
from scraper_model import SessionState, SessionError, RequestState, RequestError, ScrapeState, ScrapeError
from sklearn.feature_extraction.text import CountVectorizer
from sklearn_som.som import SOM
from telethon.tl.types import PeerUser, PeerChat, PeerChannel
from telethon import errors, functions, sync, TelegramClient, types, utils
from tabulate import tabulate
from xmlrpc.client import boolean

# initialize URL shortener
type_tiny = pyshorteners.Shortener()


class Initiator():
    def __init__(self, inifile):
        self.inifile = inifile
        self.config = ConfigParser()
        self.config.read(inifile)
        # DEFAULT
        self.examiner = self.config['DEFAULT']['examiner']
        self.hops = int(self.config['DEFAULT']['hops'])
        self.keywords = self.config['DEFAULT']['keywords'].split()

        # Telegram
        self.seeds = self.config['Telegram']['seeds'].split()
        self.max_messages = int(self.config['Telegram']['max_messages'])
        self.min_wait = int(self.config['Telegram']['min_wait'])
        self.max_wait = int(self.config['Telegram']['max_wait'])
        self.max_delay = int(self.config['Telegram']['max_delay'])
        self.follow_invitations = self.config['Telegram'].getboolean(
            'follow_invitations')

        # URL
        self.use_short = self.config['URL'].getboolean('use_short')

        # Analyze
        self.show_hops = int(self.config['Analyze']['show_hops'])
        self.num_communities = int(self.config['Analyze']['num_communities'])
        self.contacts = self.config['Analyze'].getboolean('contacts')
        self.bots = self.config['Analyze'].getboolean('bots')
        self.chats = self.config['Analyze'].getboolean('chats')
        self.channels = self.config['Analyze'].getboolean('channels')
        self.telegram_ids = self.config['Analyze'].getboolean('telegram_ids')
        self.urls = self.config['Analyze'].getboolean('urls')
        self.o_entities = self.config['Analyze'].getboolean('o_entities')
        self.keys = self.config['Analyze'].getboolean('keys')
        self.color_contact = self.config['Analyze']['color_contact']
        self.color_bot = self.config['Analyze']['color_bot']
        self.color_chat = self.config['Analyze']['color_chat']
        self.color_channel = self.config['Analyze']['color_channel']
        self.color_telegram_id = self.config['Analyze']['color_telegram_id']
        self.color_url = self.config['Analyze']['color_url']
        self.color_o_entity = self.config['Analyze']['color_o_entity']
        self.color_keyword = self.config['Analyze']['color_keyword']
        self.color_none = self.config['Analyze']['color_none']

        # Sessions
        self.s_name = []
        self.s_api_id = []
        self.s_api_hash = []
        self.s_wait_until = []

        # count available t_session[x] sections in scraper.ini
        self.s_slots = 0
        for section in self.config.sections():
            if section[0:9] == "t_session":
                self.s_slots += 1
        for s_count in range(1, self.s_slots):
            s_config_section = self.config['t_session' + str(s_count)]
            if s_config_section['name']:
                self.s_name.append(s_config_section['name'])
                self.s_api_id.append(int(s_config_section['api_id']))
                self.s_api_hash.append(s_config_section['api_hash'])
                self.s_wait_until.append(int(s_config_section['wait_until']))

    def append_seed(self, newSeed):
        self.config = ConfigParser()
        self.config.read(self.inifile)
        seedList = self.seeds
        seedList.append(newSeed)
        self.config['Telegram']['seeds'] = '\n'.join(seedList)
        with open(self.inifile, 'w') as configfile:
            self.config.write(configfile)
        self.seeds = self.config['Telegram']['seeds'].split()

    def remove_seed(self, remSeed):
        self.config = ConfigParser()
        self.config.read(self.inifile)
        seedList = self.seeds
        seedList.remove(remSeed)
        self.config['Telegram']['seeds'] = '\n'.join(seedList)
        with open(self.inifile, 'w') as configfile:
            self.config.write(configfile)
        self.seeds = self.config['Telegram']['seeds'].split()

    def append_keyword(self, newKey):
        self.config = ConfigParser()
        self.config.read(self.inifile)
        keyList = self.keywords
        keyList.append(newKey)
        self.config['DEFAULT']['keywords'] = '\n'.join(keyList)
        with open(self.inifile, 'w') as configfile:
            self.config.write(configfile)
        self.keywords = self.config['DEFAULT']['keywords'].split()

    def remove_keyword(self, remKey):
        self.config = ConfigParser()
        self.config.read(self.inifile)
        keyList = self.keywords
        keyList.remove(remKey)
        self.config['DEFAULT']['keywords'] = '\n'.join(keyList)
        with open(self.inifile, 'w') as configfile:
            self.config.write(configfile)
        self.keywords = self.config['DEFAULT']['keywords'].split()

    def set_wait(self, name, wait_until):
        try:
            self.config = ConfigParser()
            self.config.read(self.inifile)
            for s_count in range(len(self.s_name)):
                if name == self.s_name[s_count]:
                    section_name = 't_session' + str(s_count + 1)
                    self.config[section_name]['wait_until'] = str(wait_until)
            with open(self.inifile, 'w') as configfile:
                self.config.write(configfile)

        except Exception as e:
            print('Error writing scraper.ini File: ' + e)

    def set_inisetting(self, section, setting, value):
        try:
            self.config = ConfigParser()
            self.config.read(self.inifile)
            self.config[section][setting] = value
            with open(self.inifile, 'w') as configfile:
                self.config.write(configfile)

        except Exception as e:
            print('Error writing scraper.ini File: ' + e)


class Session():
    framework = 'Telethon'

    def __init__(self, name, api_id, api_hash, wait_until):
        self.iniValues = Initiator('scraper.ini')
        self.name = name
        self.api_id = api_id
        self.api_hash = api_hash
        self.wait_until = wait_until
        self.RequestState = RequestState.Idle
        try:
            self.session_object = TelegramClient(name, api_id, api_hash)

        except Exception as e:
            print(e)
            self.State = SessionState.Failed

        else:
            self.State = SessionState.Closed

    def connect(self):
        try:
            self.session_object.start()
            dialogs = self.session_object.get_dialogs()
        except Exception as e:
            print(e)
            self.State = SessionState.Failed

        else:
            self.State = SessionState.Connected

    def get_me(self):
        try:
            return self.session_object.get_me()

        except Exception as e:
            print(e)
            self.State = SessionState.Failed

    me = property(get_me)

    def is_waiting(self):
        if self.wait_until > int(time.time()):
            return True
        else:
            return False

    waiting = property(is_waiting)

    def set_wait(self, wait):
        self.RequestState = RequestState.FloodWait
        self.wait_until = int(time.time()) + wait
        self.iniValues.set_wait(self.name, self.wait_until)
        wait_until_str = str(datetime.fromtimestamp(self.wait_until))
        print('\rSession ' + self.name +
              ' FloodWait error, blocked until ' + wait_until_str)

    def get_entity_from_id(self, id, type):
        time.sleep(randint(self.iniValues.min_wait, self.iniValues.max_wait))
        self.RequestState = RequestState.Called
        while self.RequestState != RequestState.Accepted:
            try:
                entity = None
                if type == 'contact':
                    entity = self.session_object.get_entity(
                        PeerUser(id))
                elif type == 'chat':
                    entity = self.session_object.get_entity(
                        PeerChat(id))
                elif type == 'channel':
                    entity = self.session_object.get_entity(
                        PeerChannel(id))

            except errors.rpcerrorlist.FloodWaitError as e:
                wait = e.seconds
                self.set_wait(wait)
                break
            # if ID could not be found, give back an empty entity
            except ValueError:
                self.RequestState = RequestState.NotFound
                return([])

            except Exception as e:
                print('\r' + str(e))
                self.RequestState = RequestState.Failed
                break

            else:
                self.RequestState = RequestState.Accepted
                if entity == None:
                    entity = []
                return(entity)

    def get_entity_from_string(self, input):
        time.sleep(randint(self.iniValues.min_wait, self.iniValues.max_wait))
        self.RequestState = RequestState.Called
        while self.RequestState != RequestState.Accepted:
            try:
                entity = None
                entity = self.session_object.get_entity(input)

            except errors.rpcerrorlist.FloodWaitError as e:
                wait = e.seconds
                self.set_wait(wait)
                break

            # if ID could not be found, give back an empty entity
            except ValueError:
                self.RequestState = RequestState.NotFound
                return([])

            except Exception as e:
                print('\r' + str(e))
                self.RequestState = RequestState.Failed
                break

            else:
                self.RequestState = RequestState.Accepted
                if entity == None:
                    entity = []
                return(entity)

    def get_chat_from_invite(self, string):
        time.sleep(randint(self.iniValues.min_wait, self.iniValues.max_wait))
        self.RequestState = RequestState.Called
        while self.RequestState != RequestState.Accepted:
            try:
                updates = self.session_object(
                    functions.messages.ImportChatInviteRequest(hash=string))
                if isinstance(updates, types.Updates):
                    chat = updates.chats[0]
                    if isinstance(chat, types.Channel):
                        return(chat)
                    else:
                        return(None)
                else:
                    return(None)

            except errors.rpcerrorlist.FloodWaitError as e:
                wait = e.seconds
                print('\rFloodWait, wait ', wait,
                      ' seconds before Invitations can be requested again.')
                if wait < self.iniValues.max_delay:
                    time.sleep(wait)
                else:
                    self.set_wait(wait)

            except errors.rpcerrorlist.InviteHashEmptyError:
                self.RequestState = RequestState.Failed
                raise RequestError(
                    '\rcan not test empty invitation hash.')

            except errors.rpcerrorlist.InviteHashExpiredError:
                self.RequestState = RequestState.Failed
                raise RequestError('\rInvitation hash: ',
                                   string, ' not valid anymore.')

            except errors.rpcerrorlist.InviteHashInvalidError:
                self.RequestState = RequestState.Failed
                raise RequestError('\rFaulty invitation hash.')

            except errors.rpcerrorlist.ChannelsTooMuchError:
                self.RequestState = RequestState.Failed
                raise RequestError(
                    '\rUser ' + self.session_object.me.username + ' ist member of too many groups.')

            except errors.rpcerrorlist.UsersTooMuchError:
                self.RequestState = RequestState.Failed
                raise RequestError('\rToo many active users')

            except errors.rpcerrorlist.UserAlreadyParticipantError:
                self.RequestState = RequestState.Accepted

            except Exception as e:
                print('\r' + e)
                self.RequestState = RequestState.Failed
                raise RequestError('\rRequest Failed')

    def get_invite(self, string):
        time.sleep(randint(self.iniValues.min_wait, self.iniValues.max_wait))
        self.RequestState = RequestState.Called
        while self.RequestState != RequestState.Accepted:
            try:
                result = self.session_object(
                    functions.messages.CheckChatInviteRequest(hash=string))

            except errors.rpcerrorlist.FloodWaitError as e:
                wait = e.seconds
                print('\rFloodWait, wait ', wait,
                      ' befor Invitations can be requested again.')
                if wait < self.iniValues.max_delay:
                    time.sleep(wait)
                else:
                    self.set_wait(wait)

            except errors.rpcerrorlist.InviteHashEmptyError:
                self.RequestState = RequestState.Failed
                raise RequestError(
                    '\rCan not test empty invitation hash.')

            except errors.rpcerrorlist.InviteHashExpiredError:
                self.RequestState = RequestState.Failed
                raise RequestError('\rInvitation hash: ' +
                                   string + ' is not valid any more.')

            except errors.rpcerrorlist.InviteHashInvalidError:
                self.RequestState = RequestState.Failed
                raise RequestError('\rFaulty invitation hash.')

            except Exception as e:
                print('\r' + e)
                self.RequestState = RequestState.Failed
                raise RequestError('\rRequest failed')

            else:
                self.RequestState = RequestState.Accepted
                return(result)

    @Halo(text='Loadin Telegram messages through takeout session', spinner='dots')
    def get_messages_takeout(self, entity):
        self.iniValues = Initiator('scraper.ini')
        self.RequestState = RequestState.Called
        messages = []
        if self.iniValues.max_messages == 0:
            limit = None
        else:
            limit = self.iniValues.max_messages

        try:
            if self.session_object.session.takeout_id:
                self.session_object.end_takeout(success=False)

            with self.session_object.takeout(finalize=True,
                                             users=isinstance(
                                                 entity, types.User),
                                             chats=isinstance(
                                                 entity, types.Chat),
                                             channels=isinstance(
                                                 entity, types.Channel),
                                             megagroups=entity.megagroup
                                             ) as takeout:
                while self.RequestState == RequestState.Called:
                    takeout.get_messages(entity, limit=limit)
                    self.RequestState = RequestState.Accepted
                    s = randint(self.iniValues.min_wait,
                                self.iniValues.max_wait)
                    for message in takeout.iter_messages(entity,
                                                         wait_time=s,
                                                         limit=limit
                                                         ):
                        messages.append(message)

            if self.session_object.session.takeout_id:
                self.session_object.end_takeout(success=True)

        except errors.TakeoutInitDelayError as e:
            wait = e.seconds
            print('\rInitDelay, wait ', wait,
                  ' before Messages can be requested.')
            if wait < self.iniValues.max_delay:
                time.sleep(wait)
            else:
                self.set_wait(wait)

        except errors.rpcerrorlist.FloodWaitError as e:
            wait = e.seconds
            print('\rFloodWait, wait ', wait,
                  ' before Messages can be requested again.')
            if wait < self.iniValues.max_delay:
                time.sleep(wait)
            else:
                self.set_wait(wait)

        except Exception as e:
            print('\r' + e)
            self.RequestState = RequestState.Failed
            raise RequestError('\rRequest Failed')

        if messages == None:
            messages = []
        return(messages)

    @Halo(text='Fallback, loading Telegram messages through normal session', spinner='dots')
    def get_messages_normal(self, entity):
        self.iniValues = Initiator('scraper.ini')
        self.RequestState = RequestState.Called
        messages = []
        rowcount = 0
        try:
            for message in self.session_object.iter_messages(entity):
                if rowcount > 0:
                    wait_time = randint(
                        self.iniValues.min_wait, self.iniValues.max_wait)
                    time.sleep(wait_time)
                messages.append(message)
                rowcount += 1
                if self.iniValues.max_messages != 0:
                    if rowcount > self.iniValues.max_messages:
                        break

        except errors.rpcerrorlist.FloodWaitError as e:
            wait = e.seconds
            print('\rFloodWait, wait ', wait,
                  ' before messages can be requested again.')
            if wait < self.iniValues.max_delay:
                time.sleep(wait)
            else:
                self.set_wait(wait)

        except Exception as e:
            print('\r' + e)
            self.RequestState = RequestState.Failed
            raise RequestError('\rRequest failed')

        if messages == None:
            messages = []
        return(messages)

    def disconnect(self):
        try:
            self.session_object.disconnect()

        except:
            self.State = SessionState.Failed

        else:
            self.State = SessionState.Closed

    def __del__(self):
        # Sessions trennen
        if self.State == SessionState.Connected:
            try:
                self.session_object.disconnect()

            except:
                self.State = SessionState.Failed

            else:
                self.State = SessionState.Closed


class ScraperService():

    def __init__(self):
        # create data directory
        Path(os.path.join('data')).mkdir(parents=True, exist_ok=True)
        # read Api Ids from .ini file
        self.iniValues = Initiator('scraper.ini')
        self.Sessions = []
        # define sessions and connect
        for i in range(len(self.iniValues.s_name)):
            mySession = Session(
                self.iniValues.s_name[i],
                self.iniValues.s_api_id[i],
                self.iniValues.s_api_hash[i],
                self.iniValues.s_wait_until[i])
            try:
                mySession.connect()
                if mySession.State != SessionState.Connected:
                    raise SessionError("Connect failed!")
            except:
                mySession.disconnect()
                if mySession.State != SessionState.Closed:
                    raise SessionError("Disconnect failed!")
            else:
                self.Sessions.append(mySession)

        # Test ob sich das "me" Objekt auslesen lässt
        self.we = []
        for mySession in self.Sessions:
            if mySession.me:
                self.we.append(mySession.me)
            else:
                raise SessionError("me object could not be received!")

    def add_keyword(self, newKey):
        self.iniValues.append_keyword(newKey)
        self.iniValues = Initiator('scraper.ini')

    def remove_keyword(self, remKey):
        self.iniValues.remove_keyword(remKey)
        self.iniValues = Initiator('scraper.ini')

    def add_seed(self, newSeed):
        self.iniValues.append_seed(newSeed)
        self.iniValues = Initiator('scraper.ini')

    def remove_seed(self, remSeed):
        self.iniValues.remove_seed(remSeed)
        self.iniValues = Initiator('scraper.ini')

    def set_ini(self, dom, set, val):
        self.iniValues.set_inisetting(dom, set, val)
        self.iniValues = Initiator('scraper.ini')

    def connect_db(self, string):
        try:
            Path(os.path.join('data', string)).mkdir(
                parents=True, exist_ok=True)
            folder_path = os.path.join('data', string)
            file_db = str(string + '_scrape.sqlite')
            dal_str = 'sqlite://' + file_db
            db = DAL(dal_str, folder=folder_path)

            db.define_table('t_ids',
                            Field('t_id', type='bigint'),
                            Field('t_session_name'),
                            Field('t_type'),
                            Field('hop', type='integer'),
                            Field('msg_count', type='integer'),
                            Field('scrape_state'),
                            Field('last_check', type='datetime'))

            db.define_table('t_chats',
                            Field('chat_id', type='bigint'),
                            Field('chat_title'),
                            Field('chat_username'),
                            Field('chat_type'),
                            Field('is_broadcast', type='boolean'),
                            Field('is_megagroup', type='boolean'),
                            Field('is_gigagroup', type='boolean'))

            db.define_table('t_contacts',
                            Field('contact_id', type='bigint'),
                            Field('is_bot', type='boolean'),
                            Field('first_name'),
                            Field('last_name'),
                            Field('user_name'),
                            Field('phone_number'))

            db.define_table('t_messages',
                            Field('entity_id', type='bigint'),
                            Field('message_id', type='bigint'),
                            Field('sender_id', type='bigint'),
                            Field('raw_text'),
                            Field('web_preview_url'),
                            Field('time', type='datetime'))

            db.define_table('t_urls',
                            Field('url'),
                            Field('kind'))

            db.define_table('o_entities',
                            Field('entity'),
                            Field('kind'))

            db.define_table('o_urls',
                            Field('url'),
                            Field('short'))

            db.define_table('mentions',
                            Field('source'),
                            Field('s_type'),
                            Field('content'),
                            Field('c_type'),
                            Field('message_id'),
                            Field('timestamp'),
                            Field('hop'))

            db.define_table('logs',
                            Field('timestamp', type='datetime'),
                            Field('user'),
                            Field('log_level'),
                            Field('log'))
            db.commit()
            return db

        except Exception as e:
            print(str(e))

    def open_db(self, string):
        try:
            Path(os.path.join('data', string)).mkdir(
                parents=True, exist_ok=True)
            folder_path = os.path.join('data', string)
            file_db = str(string + '_scrape.sqlite')
            dal_str = 'sqlite://' + file_db
            db = DAL(dal_str, folder=folder_path, auto_import=True)
            return db

        except Exception as e:
            print(str(e))

    def clear_data(self, dir):
        for file_name in os.listdir(dir):
            # construct full file path
            mypath = os.path.join(dir, file_name)
            if os.path.isfile(mypath):
                print('Deleting file:', mypath)
                os.remove(mypath)
        os.rmdir(dir)

    def log(self, db, level, log):
        db.logs.insert(
            timestamp=datetime.now(),
            user=os.getlogin(),
            log_level=level,
            log=log)
        db.commit()

    def scrape(self):
        def xxxconnect_db(string):
            Path(os.path.join('data', string)).mkdir(
                parents=True, exist_ok=True)
            folder_path = os.path.join('data', string)
            file_db = str(string + '_scrape.sqlite')
            dal_str = 'sqlite://' + file_db
            db = DAL(dal_str, folder=folder_path)

            db.define_table('t_ids',
                            Field('t_id', type='bigint'),
                            Field('t_session_name'),
                            Field('t_type'),
                            Field('hop', type='integer'),
                            Field('msg_count', type='integer'),
                            Field('scrape_state'),
                            Field('last_check', type='datetime'))

            db.define_table('t_chats',
                            Field('chat_id', type='bigint'),
                            Field('chat_title'),
                            Field('chat_username'),
                            Field('chat_type'),
                            Field('is_broadcast', type='boolean'),
                            Field('is_megagroup', type='boolean'),
                            Field('is_gigagroup', type='boolean'))

            db.define_table('t_contacts',
                            Field('contact_id', type='bigint'),
                            Field('is_bot', type='boolean'),
                            Field('first_name'),
                            Field('last_name'),
                            Field('user_name'),
                            Field('phone_number'))

            db.define_table('t_messages',
                            Field('entity_id', type='bigint'),
                            Field('message_id', type='bigint'),
                            Field('sender_id', type='bigint'),
                            Field('raw_text'),
                            Field('web_preview_url'),
                            Field('time', type='datetime'))

            db.define_table('t_urls',
                            Field('url'),
                            Field('kind'))

            db.define_table('o_entities',
                            Field('entity'),
                            Field('kind'))

            db.define_table('o_urls',
                            Field('url'),
                            Field('short'))

            db.define_table('mentions',
                            Field('source'),
                            Field('s_type'),
                            Field('content'),
                            Field('c_type'),
                            Field('message_id'),
                            Field('timestamp'),
                            Field('hop'))

            db.commit()
            return db

        def get_free_session():
            freeSessions = []
            for i in range(len(self.Sessions)):
                if not self.Sessions[i].waiting:
                    freeSessions.append(self.Sessions[i])
            if len(freeSessions) > 0:
                print(" - " + str(len(freeSessions)) +
                      " available sessions", end='\r')
                time.sleep(0.3)
                return freeSessions[randint(0, len(freeSessions)) - 1]

            else:
                err_str = "No session without FloodWait blocking could be found!"
                self.log(db,
                         "Error",
                         err_str)
                raise ScrapeError(err_str)

        def find_urls(string):
            regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.]" \
                    r"[a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]" \
                    r"+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|" \
                    r"[^\s`!()\[\]{};:'\".,<>?    «»“”‘’]))"
            url = re.findall(regex, string)
            return [x[0] for x in url]

        def save_id(db, t_id, t_session_name, t_type, hop):
            if not db(db.t_ids.t_id == t_id).select():
                db.t_ids.insert(t_id=t_id,
                                t_session_name=t_session_name,
                                t_type=t_type,
                                hop=hop,
                                msg_count=0,
                                scrape_state='Identified',
                                last_check=datetime.now())
                db.commit()
                return True
            else:
                return False

        def update_id(db, t_id, scrape_state):
            rowCount = db(db.t_ids.t_id == t_id).update(scrape_state=scrape_state,
                                                        last_check=datetime.now())
            db.commit()
            if rowCount == 1:
                return True
            else:
                return False

        def save_user(db, user, session_name, hop):
            if save_id(db, user.id, session_name, 'contact', hop):
                db.t_contacts.insert(contact_id=user.id,
                                     is_bot=user.bot,
                                     first_name=user.first_name,
                                     last_name=user.last_name,
                                     user_name=user.username,
                                     phone_number=user.phone)

                db.commit()
                return True
            else:
                return False

        def save_chat(db, chat, session_name, hop):
            # get type
            if isinstance(chat, types.Chat):
                my_chat_type = 'chat'
            elif isinstance(chat, types.Channel):
                my_chat_type = 'channel'
            else:
                my_chat_type = 'unknown'

            if save_id(db, chat.id, session_name, my_chat_type, hop):
                db.t_chats.insert(chat_id=chat.id,
                                  chat_title=chat.title,
                                  chat_username=chat.username,
                                  chat_type=my_chat_type,
                                  is_broadcast=chat.broadcast,
                                  is_megagroup=chat.megagroup,
                                  is_gigagroup=chat.gigagroup)
                db.commit()
                return True
            else:
                return False

        def save_mention(db, source, content, type, message_id, timestamp, hop):
            if not source == content:
                if str(content).isdigit():
                    content = str(content)

                if len(content) > 0:
                    db.mentions.insert(source=str(source),
                                       s_type="",
                                       content=content,
                                       c_type=str(type),
                                       message_id=str(message_id),
                                       timestamp=str(timestamp),
                                       hop=str(hop))
                    db.commit()
                    if type == "url":
                        save_url(db, content)
                else:
                    print("empt content, message: " + str(message_id))
            else:
                # self reference
                pass

        def save_url(db, url):
            if not db(db.o_urls.url == url).select():
                try:
                    short = type_tiny.tinyurl.short(url)

                except:
                    print("\nURL " + url + " could not be shortened!")
                    short = url

                db.o_urls.insert(url=url, short=short)

        def process_entity(db, input, hop, sender_id, message_id, timestamp):
            entity = None
            name = None
            # normalisie ID
            str_input = str(input)
            if str_input.lstrip('-').isdigit():
                if int(input) < 0:
                    input, peer_type = utils.resolve_id(input)
                row = db(db.t_ids.t_id == input).select().first()
                if row:
                    # find session, which knows the ID
                    for session in self.Sessions:
                        if session.name == row.t_session_name:
                            mySession = session
                            break
                    # read if Session is not Floodwait blocked
                    if mySession:
                        if not mySession.waiting:
                            entity = mySession.get_entity_from_id(
                                input, row.t_type)
                    # try to figure out the name in other ways
                    if not entity:
                        if row.t_type == 'contact':
                            c_row = db.t_contacts(
                                db.t_contacts.contact_id == row.t_id)
                            if c_row.phone_number:
                                name = c_row.phone_number
                            elif c_row.user_name:
                                name = c_row.user_name
                        elif row.t_type == 'chat' or row.t_type == 'channel':
                            c_row = db.t_chats(db.t_chats.chat_id == row.t_id)
                            if c_row.chat_username:
                                name = c_row.chat_username
                    if not name:
                        name = row.t_id

            if not entity:
                if not name:
                    name = input
                mySession = get_free_session()
                if not mySession:
                    raise ScrapeError(str(
                        'We could not find a session that is \
                            not FloodWait blocked!'))

                entity = mySession.get_entity_from_string(name)
                while not entity \
                        and mySession.RequestState != RequestState.Accepted \
                        and mySession.RequestState != RequestState.NotFound \
                        and mySession.RequestState != RequestState.Failed:

                    if mySession.RequestState == RequestState.FloodWait:
                        mySession = get_free_session()
                        if not mySession:
                            raise ScrapeError(str(
                                'We could not find a session that is \
                                    not FloodWait blocked!'))
                    entity = mySession.get_entity_from_string(name)

                if mySession.RequestState == RequestState.Failed:
                    print(' - Request for ' + str(input) +
                          ' has failed!')

            if not entity:
                # If not found yet, save as other ID
                if not str(input).isdigit():
                    db.o_entities.update_or_insert(
                        entity=input, kind='other')
                    if sender_id and message_id:
                        save_mention(db, sender_id, str_input,
                                     "o_entity", message_id, timestamp, hop)
                    return 'otherEntity'
                else:
                    # numeric ID that could not be found in Telegram -> dismiss.
                    return 'notFound'

            else:
                # sender_id and message_id only come with a message
                if sender_id and message_id:
                    save_mention(db, sender_id, entity.id, "t_id",
                                 message_id, timestamp, hop)

                if isinstance(entity, types.User):
                    if save_user(db, entity, mySession.name, hop):
                        return 'newUser'
                    else:
                        return 'existingUser'

                elif isinstance(entity, types.Chat):
                    if save_chat(db, entity, mySession.name, hop):
                        return 'newChat'
                    else:
                        return 'existingChat'

                elif isinstance(entity, types.Channel):
                    if save_chat(db, entity, mySession.name, hop):
                        return 'newChannel'
                    else:
                        return 'existingChannel'
                else:
                    print("Unknown type of entity " +
                          type(entity) + " for " + str(input))

        def process_invitation_link(db, h_txt, hop):
            try:
                mySession = get_free_session()
                if not mySession:
                    raise ScrapeError(str(
                        'We could not find a session that is \
                            not FloodWait blocked!'))
                result = mySession.get_invite(h_txt)

            except RequestError as e:
                print(e)
                return False

            except Exception as e:
                print(e)
                return False

            if isinstance(result, types.ChatInvite):
                chat = mySession.get_chat_from_invite(h_txt)

            elif isinstance(result, types.ChatInvitePeek) \
                    or isinstance(result, types.ChatInviteAlready):
                chat = mySession.get_entity_from_id(result.chat, 'chat')

            else:
                print("Invitation link " + h_txt +
                      " could not be resolved!")
                return False

            if isinstance(chat, types.Chat) \
                    or isinstance(chat, types.Channel):
                return save_chat(db, chat, mySession.name, hop)
            else:
                return False

        def process_message(db, message, entity_id, hop):
            # find sender
            scrape_state = 'Scraping'
            if message.sender_id:
                sender_id, peer_type = utils.resolve_id(message.sender_id)
                if db(db.t_ids.t_id == sender_id).select().first():
                    pass
                else:
                    process_entity(db, message.sender_id,
                                   hop, None, None, None)
            else:
                sender_id = entity_id

            # WebPreview
            if message.web_preview:
                web_preview_url = message.web_preview.url
                save_mention(db, sender_id, web_preview_url,
                             "url", message.id, message.date, hop)
            else:
                web_preview_url = ''

            # save Message
            try:
                db.t_messages.insert(entity_id=entity_id,
                                     message_id=message.id,
                                     sender_id=sender_id,
                                     raw_text=message.raw_text,
                                     web_preview_url=web_preview_url,
                                     time=message.date)

            except Exception as e:
                print(e)
                print("Error saving message" + str(message.id))
                scrape_state = 'err msg write'

            # ID Eintrag updaten
            msgCount = db(db.t_ids.t_id ==
                          entity.id).select().first().msg_count + 1
            rowCount = db(db.t_ids.t_id == entity.id).update(msg_count=msgCount,
                                                             scrape_state=scrape_state,
                                                             last_check=datetime.now())
            db.commit()

            if not rowCount == 1:
                raise ScrapeError(
                    str('Error saving message ' + str(message.id)))

            # find keywords in message text
            if message.raw_text != None:
                for keyword in self.iniValues.keywords:
                    if keyword.lower() in message.raw_text.lower():
                        save_mention(
                            db,
                            sender_id,
                            keyword,
                            "keyword",
                            message.id,
                            message.date,
                            hop
                        )

            # find Telegram adresses in message text
            if message.raw_text != None:
                regex = r"\B@\w{5,32}\b"
                try:
                    usernames = re.findall(regex, message.raw_text)

                except Exception as e:
                    print(e)
                    print("Error parsing " + message.raw_text)

                if usernames != None:
                    for username in usernames:
                        process_entity(db, username, hop,
                                       sender_id, message.id, message.date)

                # find Urls in text
                urls = find_urls(message.raw_text)
                if urls:
                    for url in urls:
                        # Check if Telegram URL
                        regex = r"^(https:\/\/)?(t|telegram)\.(me|org|dog)"
                        match = re.search(regex, url, flags=re.IGNORECASE)
                        if match:
                            kind = ""
                            # Check if Telegram Invitation Link
                            if self.iniValues.follow_invitations:
                                regex = r"(?:t|telegram)\.(?:me|org|dog)\/(joinchat\/|\+){1}([\w-]+)"
                                match = re.search(
                                    regex, url, flags=re.IGNORECASE)
                                if match:
                                    if process_invitation_link(db, match.group(0), hop):
                                        kind = 'valid_invitation'
                                    else:
                                        kind = 'invalid_invitation'
                            if kind == "":
                                # Check if Entity URL
                                # select only the entity part of the link
                                # remove anything right of the ?
                                q_pos = url.find('?')
                                if q_pos > 0:
                                    url = url[:q_pos]
                                # remove anything richt of 4. /
                                url_list = url.split("/")[0:4]
                                sep = "/"
                                entity_url = sep.join(url_list)
                                # check if we get back an entity
                                if process_entity(db, entity_url, hop, sender_id, message.id, message.date) != 'notFound':
                                    kind = 'entity'
                                else:
                                    kind = 'other'
                                    save_mention(
                                        db, sender_id, url, "url", message.id, message.date, hop)
                            db.t_urls.update_or_insert(url=url, kind=kind)
                        else:
                            if url != web_preview_url:
                                save_mention(db, sender_id, url, "url",
                                             message.id, message.date, hop)
            db.commit()

        # Scrape Main
        self.iniValues = Initiator('scraper.ini')

        # Start Scraping
        for seed in self.iniValues.seeds:
            db = self.connect_db(seed)
            # log start of scraping session
            self.log(db,
                     "Information",
                     "Start scraping for " + seed)
            for session in self.Sessions:
                self.log(db,
                         "Information",
                         "Session " + session.name +
                         " api_id " + str(session.api_id) +
                         " wait_until " + str(
                             datetime.fromtimestamp(
                                 session.wait_until)))

            if db(db.t_ids).isempty():
                myResult = process_entity(db, seed, 0, None, None, None)
            else:
                row = db(db.t_ids).select().first()
                myResult = row.scrape_state
            if not myResult:
                err_str = "Could not find an entry for " + \
                    seed + " with a matching ID!"
                self.log(db,
                         "Error",
                         err_str)
                raise ScrapeError(err_str)

            print('Seed ', seed, ' ', str(myResult))

            for hop in range(0, self.iniValues.hops):
                entities = []
                # Select all entities for this hop
                for row in db(db.t_ids.hop == hop).select():
                    if row.scrape_state in ['Identified', 'FloodWait', 'TakeoutWait', 'Scraping']:
                        entity = None
                        # find session which recognises this ID
                        for session in self.Sessions:
                            if session.name == row.t_session_name:
                                mySession = session
                                break
                        # read through peer ID if session not Floodwait blocked
                        if mySession and not mySession.waiting:
                            entity = mySession.get_entity_from_id(
                                row.t_id, row.t_type)
                        # try to resolve through name
                        if not entity:
                            name = None
                            if row.t_type == 'contact':
                                c_row = db.t_contacts(
                                    db.t_contacts.contact_id == row.t_id)
                                if c_row.phone_number:
                                    name = c_row.phone_number
                                elif c_row.user_name:
                                    name = c_row.user_name
                            elif row.t_type == 'chat' or row.t_type == 'channel':
                                c_row = db.t_chats(
                                    db.t_chats.chat_id == row.t_id)
                                if c_row.chat_username:
                                    name = c_row.chat_username

                            mySession = get_free_session()
                            if not mySession:
                                err_str = "We could not find a session that is" + \
                                          "not FloodWait blocked!"
                                self.log(db,
                                         "Error",
                                         err_str)
                                raise ScrapeError(err_str)

                            if not name:
                                name = row.t_id

                            entity = mySession.get_entity_from_string(name)

                            while not entity \
                                    and mySession.RequestState != RequestState.Accepted \
                                    and mySession.RequestState != RequestState.NotFound \
                                    and mySession.RequestState != RequestState.Failed:

                                if mySession.RequestState == RequestState.FloodWait:
                                    mySession = get_free_session()
                                    if not mySession:
                                        err_str = "We could not find a session that is" + \
                                            "not FloodWait blocked!"
                                        self.log(db,
                                                 "Error",
                                                 err_str)
                                        raise ScrapeError(err_str)

                                entity = mySession.get_entity_from_string(name)

                            if mySession.RequestState == RequestState.Failed:
                                print(' - Request for ' + str(row.t_type) + " " + str(row.t_id)
                                      + ' has failed!')
                        if entity:
                            # request Messages
                            messages = []
                            try:
                                messages = mySession.get_messages_takeout(
                                    entity)

                            except:
                                if mySession.RequestState == RequestState.TakeoutWait:
                                    if not update_id(db, row.t_id, 'TakeoutWait'):
                                        raise ScrapeError(
                                            str('Entry for ' + str(entity.id) + ' could not be updated!'))
                                if mySession.RequestState == RequestState.FloodWait:
                                    if not update_id(db, row.t_id, 'FloodWait'):
                                        raise ScrapeError(
                                            str('Entry for ' + str(entity.id) + ' could not be updated!'))

                            # Read messages through normal session as fallback
                            if not messages:
                                try:
                                    messages = mySession.get_messages_normal(
                                        entity)
                                except:
                                    if mySession.RequestState == RequestState.FloodWait:
                                        if not update_id(db, row.t_id, 'FloodWait'):
                                            raise ScrapeError(
                                                str('Entry for ' + str(entity.id) + ' could not be updated!'))
                                    elif mySession.RequestState == RequestState.Failed:
                                        if not update_id(db, row.t_id, 'RetrieveMessagesFailed'):
                                            raise ScrapeError(
                                                str('Entry for ' + str(entity.id) + ' could not be updated!'))

                            if messages == None:
                                messages = []

                            # Status message
                            label = ''
                            if isinstance(entity, types.User):
                                if entity.username != None:
                                    label = entity.username
                                elif not ((entity.last_name == None) or (entity.first_name == None)):
                                    label = entity.first_name + " " + entity.last_name
                                else:
                                    label = entity.phone
                            elif isinstance(entity, types.Chat) or isinstance(entity, types.Channel):
                                label = entity.title
                            if not label:
                                label = str(entity.id)
                            print("Scraping Hop:" + str(hop) + " " +
                                  label + " " + str(len(messages)) + " Messages")

                            # If the previous scan did not finish, read lowest message id
                            min_id = sys.maxsize
                            processed_message_entries = db(
                                db.t_messages.entity_id == entity.id).select()
                            for msg_row in processed_message_entries:
                                if msg_row.message_id < min_id:
                                    min_id = msg_row.message_id

                            for message in messages:
                                if message.id < min_id:
                                    with Halo(text="Processing message " + str(message.id), spinner='dots',):
                                        process_message(
                                            db, message, row.t_id, hop + 1)

                            if not update_id(db, row.t_id, 'Finished'):
                                raise ScrapeError(
                                    str('Entry for ' + str(entity.id) + ' could not be updated!'))

                        else:
                            if update_id(db, row.t_id, 'not found'):
                                print(str('Already resolved Entity ' +
                                      str(row.t_id) + ' could not be found again!'))
                            else:
                                print(str('Entry for ' + str(row.t_id) +
                                      ' could not be updated!'))

            all_finished = True
            for row in db(db.t_ids).select():
                if row.scrape_state != 'Finished' and \
                        row.hop < self.iniValues.hops:
                    all_finished = False
            if all_finished:
                self.remove_seed(seed)
                self.log(db,
                         "Information",
                         "Finished Scraping for " + seed)

            # update mention table
            for row in db(db.mentions).select():
                if str(row.source).isdigit():
                    # source
                    s_type = None
                    s_name = None
                    row_id = db(db.t_ids.t_id == int(
                        row.source)).select().first()
                    if row_id:
                        s_type = row_id.t_type
                        if s_type == "contact":
                            s_row = db.t_contacts(
                                db.t_contacts.contact_id == row_id.t_id)
                            if s_row.user_name:
                                s_name = s_row.user_name
                            elif s_row.phone_number:
                                s_name = s_row.phone_number
                            if s_row.is_bot:
                                s_type = "bot"
                        elif s_type == "chat" or s_type == "channel":
                            s_row = db.t_chats(
                                db.t_chats.chat_id == row_id.t_id)
                            if s_row.chat_username:
                                s_name = s_row.chat_username
                    else:
                        s_type = "telegram_id"

                    row.update_record(s_type=s_type)

                    if s_name:
                        row.update_record(source=s_name)

                # content
                if str(row.content).isdigit():
                    c_type = None
                    c_name = None
                    row_id = db(db.t_ids.t_id == int(
                        row.content)).select().first()
                    if row_id:
                        c_type = row_id.t_type
                    if c_type == "contact":
                        c_row = db.t_contacts(
                            db.t_contacts.contact_id == row_id.t_id)
                        if c_row.user_name:
                            c_name = c_row.user_name
                        elif c_row.phone_number:
                            c_name = c_row.phone_number
                        if c_row.is_bot:
                            c_type = "bot"
                    elif c_type == "chat" or c_type == "channel":
                        c_row = db.t_chats(
                            db.t_chats.chat_id == row_id.t_id)
                        if c_row.chat_username:
                            c_name = c_row.chat_username
                    if c_type:
                        row.update_record(c_type=c_type)

                    if c_name:
                        row.update_record(content=c_name)

                if self.iniValues.use_short:
                    if row.c_type == "url":
                        u_row = db(db.o_urls.url ==
                                   row.content).select().first()
                        if u_row:
                            row.update_record(content=u_row.short)
                        else:
                            print("No short URL for Entry " +
                                  u_row.content + " has been found!")

                db.commit()

            self.log(db,
                     "Information"
                     "Closing scraping sesion")

            # close db
            if db:
                db.commit()
                db.close()

    def merge(self, seed1, seed2, newName):

        # open DBs
        print("Merging " + seed1 + " and " + seed2)
        print("to new Database " + newName)
        db1 = self.open_db(seed1)
        db2 = self.open_db(seed2)
        newDb = self.connect_db(newName)

        # merge t_ids tables
        conflict_dict = {}
        rows1 = db1(db1.t_ids).select()
        for row in rows1:
            newDb.t_ids[None] = row

        rows2 = db2(db2.t_ids).select()
        for row in rows2:
            existing_rows = newDb(newDb.t_ids.t_id == row.t_id).select()
            if not existing_rows:
                newDb.t_ids[None] = row
            else:
                if len(existing_rows) > 1:
                    raise ScrapeError(str(
                        'Multiple rows with identical IDs have been found!, \
                            abandoning merge process!'))
                else:
                    print("Conflicting entries found in tables t_ids:")
                    table = []
                    th = ['field', "Seed 1:" + seed1, " Seed 2:" + seed2]
                    table.append(th)
                    for field in rows1[0].as_dict():
                        if field != "id":
                            tr = []
                            tr.append(field)
                            tr.append(str(existing_rows[0][field]))
                            tr.append(str(row[field]))
                            table.append(tr)
                    print(tabulate(
                        table,
                        headers='firstrow',
                        tablefmt='fancy_grid',
                        maxcolwidths=[None, 20, 20]))
                    validSelection = False
                    while not validSelection:
                        userInput = input(
                            "Enter number of entry you want to keep (1/2): ")
                        if userInput.isdigit():
                            if int(userInput) == 1:
                                validSelection = True
                            elif int(userInput) == 2:
                                validSelection = True
                                existing_rows[0].delete_record()
                                newDb.t_ids[None] = row
                            else:
                                print("Invalid selection, enter 1 or 2!")
                    conflict_dict[row.t_id] = int(userInput)

        # merge t_chats tables
        rows1 = db1(db1.t_chats).select()
        for row in rows1:
            if row.chat_id in conflict_dict:
                if conflict_dict[row.chat_id] == 1:
                    newDb.t_chats[None] = row
                else:
                    # omit row
                    pass
            else:
                newDb.t_chats[None] = row

        rows2 = db2(db2.t_chats).select()
        for row in rows2:
            if row.chat_id in conflict_dict:
                if conflict_dict[row.chat_id] == 2:
                    newDb.t_chats[None] = row
                else:
                    # omit row
                    pass
            else:
                newDb.t_chats[None] = row

        # merge t_contacts tables
        rows1 = db1(db1.t_contacts).select()
        for row in rows1:
            if row.contact_id in conflict_dict:
                if conflict_dict[row.contact_id] == 1:
                    newDb.t_contacts[None] = row
                else:
                    # omit row
                    pass
            else:
                newDb.t_contacts[None] = row

        rows2 = db2(db2.t_contacts).select()
        for row in rows2:
            if row.contact_id in conflict_dict:
                if conflict_dict[row.contact_id] == 2:
                    newDb.t_contacts[None] = row
                else:
                    # omit row
                    pass
            else:
                newDb.t_contacts[None] = row

        # merge t_messages tables
        rows1 = db1(db1.t_messages).select()
        for row in rows1:
            newDb.t_messages[None] = row

        rows2 = db2(db2.t_messages).select()
        for row in rows2:
            if row.entity_id in conflict_dict:
                existing_rows = newDb(newDb.t_messages.entity_id == row.entity_id
                                      and newDb.t_messages.message_id == row.message_id).select()
                if existing_rows:
                    if len(existing_rows) > 1:
                        raise ScrapeError(str(
                            'Multiple t_messages rows with identical entity_id and message_id \
                                have been found, abandoning merge process!'))
                    else:
                        if conflict_dict[row.entity_id] == 2:
                            existing_rows[0].delete_record()
                            newDb.t_messages[None] = row
                        else:
                            # omit row
                            pass
                else:
                    newDb.t_messages[None] = row
            else:
                newDb.t_messages[None] = row

        # merge t_urls tables
        rows1 = db1(db1.t_urls).select()
        for row in rows1:
            newDb.t_urls[None] = row

        rows2 = db2(db2.t_urls).select()
        for row in rows2:
            existing_rows = newDb(newDb.t_urls.url == row.url).select()
            if existing_rows:
                if len(existing_rows) > 1:
                    raise ScrapeError(str(
                        'Multiple t_url rows with identical urls \
                            have been found, abandoning merge process!'))
                else:
                    if existing_rows[0].kind != row.kind:
                        print("Conflicting entries found in tables t_urls:")
                        table = []
                        th = ['field', "Seed 1:" + seed1, " Seed 2:" + seed2]
                        table.append(th)
                        for field in rows1[0].as_dict():
                            if field != "id":
                                tr = []
                                tr.append(field)
                                tr.append(str(existing_rows[0][field]))
                                tr.append(str(row[field]))
                                table.append(tr)
                        print(tabulate(
                            table,
                            headers='firstrow',
                            tablefmt='fancy_grid',
                            maxcolwidths=[None, 20, 20]))
                        validSelection = False
                        while not validSelection:
                            userInput = input(
                                "Enter number of entry you want to keep (1/2): ")
                            if userInput.isdigit():
                                if int(userInput) == 1:
                                    validSelection = True
                                elif int(userInput) == 2:
                                    validSelection = True
                                    existing_rows[0].delete_record()
                                    newDb.t_urls[None] = row
                                else:
                                    print("Invalid selection, enter 1 or 2!")
            else:
                newDb.t_urls[None] = row

        # merge o_entities tables
        rows1 = db1(db1.o_entities).select()
        for row in rows1:
            newDb.o_entities[None] = row

        rows2 = db2(db2.o_entities).select()
        for row in rows2:
            existing_rows = newDb(
                newDb.o_entities.entity == row.entity).select()
            if existing_rows:
                if len(existing_rows) > 1:
                    raise ScrapeError(str(
                        'Multiple o_entities rows with identical values \
                            have been found, abandoning merge process!'))
                else:
                    if existing_rows[0].kind != row.kind:
                        print("Conflicting entries found in tables o_entities:")
                        table = []
                        th = ['field', "Seed 1:" + seed1, " Seed 2:" + seed2]
                        table.append(th)
                        for field in rows1[0].as_dict():
                            if field != "id":
                                tr = []
                                tr.append(field)
                                tr.append(str(existing_rows[0][field]))
                                tr.append(str(row[field]))
                                table.append(tr)
                        print(tabulate(
                            table,
                            headers='firstrow',
                            tablefmt='fancy_grid',
                            maxcolwidths=[None, 20, 20]))
                        validSelection = False
                        while not validSelection:
                            userInput = input(
                                "Enter number of entry you want to keep (1/2): ")
                            if userInput.isdigit():
                                if int(userInput) == 1:
                                    validSelection = True
                                elif int(userInput) == 2:
                                    validSelection = True
                                    existing_rows[0].delete_record()
                                    newDb.o_entities[None] = row
                                else:
                                    print("Invalid selection, enter 1 or 2!")
            else:
                newDb.o_entities[None] = row

        # merge o_urls table
        rows1 = db1(db1.o_urls).select()
        for row in rows1:
            newDb.o_urls[None] = row

        rows2 = db2(db2.o_urls).select()
        for row in rows2:
            newDb.o_urls.db.insert_or_update(url=row.url)

        # merge mentions table
        rows1 = db1(db1.mentions).select()
        for row in rows1:
            newDb.mentions[None] = row

        rows2 = db2(db2.mentions).select()
        for row in rows2:
            existing_rows = newDb(newDb.mentions.source == row.source
                                  and newDb.mentions.message_id == row.message_id
                                  and newDb.mentions.content == row.content).select()
            if existing_rows:
                # omit row
                pass
            else:
                newDb.mentions[None] = row

        # save new DB
        newDb.commit()

    def analyze(self, seed):

        # Analyze
        data_dir = os.path.join("data", seed)

        def set_colors(graph):
            colors = []
            for n in graph.nodes(data=True):
                if n[1]["type"] == "contact":
                    color = self.iniValues.color_contact
                elif n[1]["type"] == "bot":
                    color = self.iniValues.color_bot
                elif n[1]["type"] == "chat":
                    color = self.iniValues.color_chat
                elif n[1]["type"] == "channel":
                    color = self.iniValues.color_channel
                elif n[1]["type"] == "telegram_id":
                    color = self.iniValues.color_telegram_id
                elif n[1]["type"] == "url":
                    color = self.iniValues.color_url
                elif n[1]["type"] == "o_entity":
                    color = self.iniValues.color_o_entity
                elif n[1]["type"] == "keyword":
                    color = self.iniValues.color_keyword
                else:
                    color = self.iniValues.color_none
                colors.append(color)
            return colors

        def write_text(story, styles, string):
            story.append(Paragraph(string, styles["Normal"]))
            story.append(Spacer(0, 0.5*cm))

        def write_table(story, styles, data, title):
            story.append(Paragraph("<b>" + title + "</b>", styles['Heading4']))
            story.append(Table(
                data,
                style=[('GRID', (0, 0), (-1, -1), 1, '#000000F')],
                hAlign='LEFT',
                spaceBefore=0.3*cm,
                spaceAfter=0.3*cm))

        def write_img(story, styles, file, width, height, title):
            story.append(Paragraph("<b>" + title +
                         "</b>", styles['Heading4'],))
            story.append(Spacer(0, 0.3*cm))
            story.append(Image(file, width, height, hAlign='CENTER'))

        def save_doc(story):
            # initialize pdfreport document
            PAGE_HEIGHT = defaultPageSize[1]
            PAGE_WIDTH = defaultPageSize[0]

            Title = "Telegram scraping report, seed:" + seed
            pageinfo = "Seed: " + seed

            def myFirstPage(canvas, doc):
                canvas.saveState()
                canvas.setFont('Times-Bold', 16)
                canvas.drawCentredString(PAGE_WIDTH/2.0, PAGE_HEIGHT-50, Title)
                canvas.setFont('Times-Roman', 11)
                canvas.drawString(2.5*cm, 1*cm, "First Page / %s" % pageinfo)
                canvas.restoreState()

            def myLaterPages(canvas, doc):
                canvas.saveState()
                canvas.setFont('Times-Roman', 11)
                canvas.drawString(2.5*cm, 1*cm, "Page %d %s" %
                                  (doc.page, pageinfo))
                canvas.restoreState()

            doc = SimpleDocTemplate(
                os.path.join(data_dir, seed + ".pdf"),
                title=Title,
                author=self.iniValues.examiner,
                pagesize=defaultPageSize,
                rightMargin=72,
                leftMargin=72,
                topMargin=76,
                bottomMargin=35
            )

            # save document
            doc.build(story, onFirstPage=myFirstPage,
                      onLaterPages=myLaterPages)

        print("Analyzing " + seed)
        print("The resulting files will be available in the " + data_dir + " Folder")

        db = self.open_db(seed)
        mentions = db(db.mentions).select()

        # get scraping statistics
        min_date = db.t_ids.last_check.min()
        first_scrape = db().select(min_date).first()[min_date]
        max_date = db.t_ids.last_check.max()
        last_scrape = db().select(max_date).first()[max_date]

        # initialize document styles
        styles = getSampleStyleSheet()
        story = []

        # write scraping statistics to doc
        paragraph = \
            "Database: " + seed + "_scrape.sqlite" + "<br></br>" +  \
            "Scraped from <br></br>" + \
            str(first_scrape) + "<br></br>" + \
            " -- to -- <br></br>" + \
            str(last_scrape) + "<br></br>" + \
            "by " + self.iniValues.examiner

        write_text(story, styles, paragraph)

        table_data = [["Users", str(db(db.t_contacts.is_bot == False).count())],
                      ["Bots", str(db(db.t_contacts.is_bot == True).count())],
                      ["Chats", str(
                          db(db.t_chats.chat_type == "chat").count())],
                      ["Channels", str(
                          db(db.t_chats.chat_type == "channel").count())],
                      ["Messages", str(db(db.t_messages).count())],
                      ["URLs", str(db(db.o_urls).count())]]

        write_table(story, styles, table_data, "Scraped Objects")

        # create Graph
        G = nx.MultiDiGraph(name=seed)
        print("Graph : " + G.name)

        # create graph by adding edges
        for row in mentions:
            include = False
            if row.c_type == "contact":
                include = self.iniValues.contacts
            elif row.c_type == "bot":
                include = self.iniValues.bots
            elif row.c_type == "chat":
                include = self.iniValues.chats
            elif row.c_type == "channel":
                include = self.iniValues.channels
            elif row.c_type == "telegram_id":
                include = self.iniValues.telegram_ids
            elif row.c_type == "url":
                include = self.iniValues.urls
            elif row.c_type == "o_entity":
                include = self.iniValues.o_entities
            elif row.c_type == "keyword":
                include = self.iniValues.keys
            else:
                raise ScrapeError("Unknown c_type in mention!")
            if include:
                G.add_edge(row.source, row.content, message_id=row.message_id,
                           hop=row.hop, timestamp=row.timestamp)
                G.nodes[row.source]["type"] = row.s_type
                G.nodes[row.content]["type"] = row.c_type

        # Display Statistics
        nodes = str(nx.number_of_nodes(G))
        edges = str(nx.number_of_edges(G))
        print("Nodes: " + nodes)
        print("Edges: " + edges)

        if nodes == 0:
            print("The graph contains no data, canceling analyis!")
        else:
            with Halo(text="Calculating network density", spinner='dots',):
                density = str(round(nx.density(G), 6))
            print("Network density:", density)

            # write graph statistics to doc
            table_data = [["Nodes", nodes],
                          ["Edges", edges],
                          ["Density", density]]

            write_table(story, styles, table_data, "Graph: " + seed)

            # create undirected graph from G
            uni_G = nx.to_undirected(G)

            with Halo(text="Calculating connectednes", spinner='dots',):
                # create weighted graph from uni_G
                w_G = nx.Graph(name=seed)
                for u, v, data in uni_G.edges(data=True):
                    w = data['weight'] if 'weight' in data else 1.0
                    if w_G.has_edge(u, v):
                        w_G[u][v]['weight'] += w
                    else:
                        w_G.add_edge(
                            u,
                            v,
                            weight=w,
                            message_id=data['message_id'],
                            hop=data['hop'],
                            timestamp=data['timestamp']
                        )
                        u_node = uni_G.nodes[u]
                        w_G.add_node(u, type=u_node['type'])
                        v_node = uni_G.nodes[v]
                        w_G.add_node(v, type=v_node['type'])

                connected = nx.is_connected(w_G)
            print("Is Connected: " + str(connected))
            if connected:
                with Halo(text="Calculating diameter", spinner='dots',):
                    diameter = nx.diameter(w_G)
                    transitivity = nx.transitivity(w_G)

                # write connection statistics to doc
                table_data = [["is Connected", "True"],
                              ["Diameter", str(diameter)],
                              ["Transitivity", str(round(transitivity, 6))]]

                write_table(story, styles, table_data, "Connectednes")

            else:
                with Halo(text="Calculating components", spinner='dots',):
                    components = nx.connected_components(w_G)
                    largest_component = max(components, key=len)
                    subgraph = w_G.subgraph(largest_component)
                print("Largest Component:")
                print("    Nodes: " + str(nx.number_of_nodes(subgraph)))
                print("    Edges: " + str(nx.number_of_edges(subgraph)))
                with Halo(text="Calculating diameter", spinner='dots',):
                    diameter = nx.diameter(subgraph)
                    transitivity = nx.transitivity(subgraph)

                # write connection statistics to doc
                table_data = [["is Connected", "False"],
                              ["Largest Component:", ""],
                              ["--> Nodes", str(nx.number_of_nodes(subgraph))],
                              ["--> Edges", str(nx.number_of_edges(subgraph))],
                              ["--> Diameter", str(diameter)],
                              ["--> Transitivity", str(round(transitivity, 6))]]

                write_table(story, styles, table_data, "Connectednes")

            print("Diameter:", str(diameter))
            print("Transitivity:", str(round(transitivity, 6)))

            with Halo(text="Calculating degree", spinner='dots',):
                degree_dict = dict(G.degree(G.nodes()))
                nx.set_node_attributes(G, degree_dict, 'degree')
                sorted_degree = sorted(degree_dict.items(),
                                       key=itemgetter(1), reverse=True)

            # create
            paragraph = \
                "<br></br>" + \
                "<br></br>" + \
                "<h2>Lists of the different entities, sortet by degree:</h2>"
            write_text(story, styles, paragraph)

            # create set which contains all occuring types
            type_set = set()
            for n in G.nodes(data=True):
                type_set.add(n[1]["type"])
            # create a dictionary variable for each type
            for mytype in type_set:
                vars()[mytype] = []
                count = 1
                # add 20 nodes with the highest degree
                for d in sorted_degree:
                    if G.nodes[d[0]]["type"] == mytype:
                        # list only nodes with a degree > 1
                        if G.nodes[d[0]]["degree"] > 1:
                            vars()[mytype].append(d)
                            count += 1
                    if count >= 20:
                        break

                # create table
                table_data = [["Node", "Edges"]]
                if len(vars()[mytype]) > 0:
                    for d in vars()[mytype]:
                        if len(d[0]) > 70:
                            node = d[0][:70] + "..."
                        else:
                            node = d[0]
                        edges = d[1]
                        table_data.append([node, edges])
                    print("Top 20 " + mytype + " by degree:")
                    print(tabulate(
                        table_data,
                        headers='firstrow',
                        tablefmt='fancy_grid',
                        maxcolwidths=[80, None])
                    )
                    write_table(story,
                                styles,
                                table_data,
                                "Top 20 " + mytype + " by degree")

            # import Graph to networkit
            nkG = nk.nxadapter.nx2nk(w_G, weightAttr='weight')
            nkG.indexEdges()
            idmap = dict((u, id) for (u, id) in zip(
                range(w_G.number_of_nodes()), w_G.nodes()))

            # Degree distribution
            dd = sorted(nk.centrality.DegreeCentrality(
                nkG).run().scores(), reverse=True)
            plt.xscale("log")
            plt.xlabel("degree")
            plt.yscale("log")
            plt.ylabel("number of nodes")
            plt.plot(dd)
            plotfile = os.path.join(data_dir, seed + "_degree_dist.png")
            plt.savefig(plotfile)
            plt.close(plt.gcf())
            write_img(story,
                      styles,
                      plotfile,
                      10*cm,
                      10*cm,
                      "Plot of degree distribution")

            # Test if degree distribution follows a power law
            fit = powerlaw.Fit(dd)
            dist_tuple = fit.distribution_compare('power_law', 'exponential')

            paragraph = \
                "The degree distribution on this graph has a coefficient of <br></br>" + \
                "alpha = " + str(round(fit.alpha, 4)) + "<br></br>" + \
                "if above 4, then the Graph has a long tail distribution<br></br>" \
                "R = " + str(round(dist_tuple[0], 4)) + "<br></br>" + \
                "Likelihood ratio, if positive, the power law distribution is more likely than a exponential distribution.<br></br>" + \
                "p = " + str(round(dist_tuple[1], 4)) + "<br></br>" + \
                "Significance of the sign of R, if below .05 the sign of R is taken to be significant."
            write_text(story, styles, paragraph)

            print("alpha = " + str(round(fit.alpha, 6)))
            print("R = " + str(round(dist_tuple[0], 6)))
            print("p = " + str(round(dist_tuple[1], 6)))

            # calculate PageRank centrality
            pgr = nx.pagerank(G)

            # create sorted dictionary
            pgr_sort = dict(
                sorted(pgr.items(), key=operator.itemgetter(1), reverse=True))

            # select to topmost nodes
            pgr_top10 = list(pgr_sort.items())[:10]

            # create table
            table_data = [["Node", "Centrality"]]
            for node in pgr_top10:
                table_data.append([node[0][0:80], str(round(node[1], 8))])

            write_table(story, styles, table_data,
                        "Top 20 nodes by Page Rank centrality")
            print("Top 20 nodes by Page Rank centrality")
            print(tabulate(
                table_data,
                headers='firstrow',
                tablefmt='fancy_grid',
                maxcolwidths=[80, None])
            )

            # plot Graph
            with Halo(text="Creating graph and plotting", spinner='dots',):
                plotG = nx.MultiDiGraph(name=seed)
                for row in mentions:
                    if int(row.hop) <= self.iniValues.show_hops:
                        include = False
                        if row.c_type == "contact":
                            include = self.iniValues.contacts
                        elif row.c_type == "bot":
                            include = self.iniValues.bots
                        elif row.c_type == "chat":
                            include = self.iniValues.chats
                        elif row.c_type == "channel":
                            include = self.iniValues.channels
                        elif row.c_type == "telegram_id":
                            include = self.iniValues.telegram_ids
                        elif row.c_type == "url":
                            include = self.iniValues.urls
                        elif row.c_type == "o_entity":
                            include = self.iniValues.o_entities
                        elif row.c_type == "keyword":
                            include = self.iniValues.keys
                        else:
                            raise ScrapeError("Unknown c_type in mention!")
                        if include:
                            # shorten strings
                            if len(row.source) > 50:
                                source = str(row.source)[:50] + "..."
                            else:
                                source = str(row.source)

                            if len(row.content) > 50:
                                content = str(row.content)[:50] + "..."
                            else:
                                content = str(row.content)

                            plotG.add_edge(source, content, message_id=row.message_id,
                                           hop=row.hop, timestamp=row.timestamp)
                            plotG.nodes[source]["type"] = row.s_type
                            plotG.nodes[content]["type"] = row.c_type

                colors = set_colors(plotG)
                options = {
                    "font_size": 10,
                    "node_color": colors,
                    "with_labels": True,
                }
                nx.draw(plotG, pos=nx.spring_layout(plotG), **options)
                plt.savefig(os.path.join(data_dir, seed + ".png"))
                plt.close(plt.gcf())

                # append Image to doc
                write_img(story,
                          styles,
                          os.path.join(data_dir, seed + ".png"),
                          17*cm,
                          17*cm,
                          "Plot of spring based graph, " + str(self.iniValues.show_hops) + " hops")

            # import Graph to networkit
            with Halo(text="Detecting and plotting communities... ", spinner='dots',):
                # detect communitites
                communities = nk.community.detectCommunities(
                    nkG,
                    algo=nk.community.PLM(nkG, True),
                    inspect=False)

                # inspect resulting partition
                comm_count = communities.numberOfElements()
                comm_subsets = communities.numberOfSubsets()
                comm_max = max(communities.subsetSizes())
                comm_min = min(communities.subsetSizes())
                modularity = nk.community.Modularity().getQuality(communities, nkG)

                paragraph = \
                    "<br></br>" + \
                    "Community detection: <br></br>" + \
                    "PLM algorithm detected " + str(comm_count) + " elements<br></br>" + \
                    "assigned to " + str(comm_subsets) + " communities<br></br>" + \
                    "ranging from " + str(comm_max) + " to " + str(comm_min) + " nodes<br></br>" + \
                    "with " + str(round(modularity, 6)) + \
                    " modularity.<br></br>"
                write_text(story, styles, paragraph)

                if self.iniValues.num_communities > 0:
                    paragraph = \
                        "<br></br>" + \
                        "<h2>" + str(self.iniValues.num_communities) + \
                        " plots of the largest detected Communities: </h2>"
                    write_text(story, styles, paragraph)

                    # build Networkit graphs for the detectet communities
                    nkG_map = communities.subsetSizeMap()
                    nkG_sorted_map = dict(
                        sorted(
                            nkG_map.items(),
                            key=operator.itemgetter(1),
                            reverse=True
                        )
                    )
                    top_comm_list = []

                    # select the largest communities
                    for key in nkG_sorted_map:
                        top_comm_list.append(key)
                        if len(top_comm_list) >= self.iniValues.num_communities:
                            break

                    # create a networX graph for each of them and plot them
                    for comm in top_comm_list:
                        nodelist = []
                        for member in communities.getMembers(comm):
                            nodelist.append(idmap[member])

                        G_community = w_G.__class__()
                        G_community.add_nodes_from(
                            (n, w_G.nodes[n])
                            for n in nodelist
                        )
                        G_community.add_edges_from(
                            (n, nbr, d)
                            for n, nbrs in w_G.adj.items() if n in nodelist
                            for nbr, d in nbrs.items() if nbr in nodelist
                        )
                        G_community.graph.update(w_G.graph)
                        colors = set_colors(G_community)
                        options = {
                            "font_size": 10,
                            "node_color": colors,
                            "with_labels": True,
                        }
                        nx.draw(G_community, pos=nx.spring_layout(
                            G_community), **options)
                        plotfile = os.path.join(
                            data_dir, seed + "_comm_" + str(comm) + ".png")
                        plt.savefig(plotfile)
                        plt.close(plt.gcf())

                        write_img(story,
                                  styles,
                                  plotfile,
                                  7*cm,
                                  7*cm,
                                  "Plot of community " + str(comm))

                        # create
                        comm_nodes = str(nx.number_of_nodes(G_community))
                        comm_edges = str(nx.number_of_edges(G_community))

                        paragraph = \
                            "Community " + str(comm) + ": " + \
                            comm_nodes + " nodes " + comm_edges + " edges"
                        write_text(story, styles, paragraph)

                sizes = communities.subsetSizes()
                sizes.sort(reverse=True)
                plt.xscale("log")
                plt.xlabel("community id")
                plt.yscale("log")
                plt.ylabel("size")
                plt.plot(sizes)
                plt.savefig(os.path.join(
                    data_dir, seed + "_communitysize.png"))
                plt.close(plt.gcf())

                write_img(story,
                          styles,
                          os.path.join(data_dir, seed + "_communitysize.png"),
                          7*cm,
                          7*cm,
                          "Plot of community size distribution")

            # sockpuppet detection, create dataset
            dataset = []
            for row in db(db.t_messages).select():
                # only use Messages that contain some text
                msg = row.raw_text
                if msg:
                    if len(msg) > 5:
                        # use only words without special characters
                        words = []
                        for word in msg.split():
                            if not re.search("\W+", word) and len(word) > 1:
                                words.append(word)
                        dataset.append([row.sender_id, words])
            dataset.sort()

            # select unique list of senders
            sender_set = set()
            for entry in dataset:
                sender_set.add(entry[0])
            sender_list = list(sender_set)
            sender_list.sort()
            
            # aggregate words by sender
            aggr_dataset = []
            for sender in sender_list:
                aggr_entry = [sender,[]]
                for entry in dataset:
                    if entry[0] == sender:
                        for word in entry[1]:
                            if not word in aggr_entry[1]:
                                aggr_entry[1].append(word)
                if len(aggr_entry[1]) > 0: 
                    aggr_dataset.append(aggr_entry)
                
            # stemming
            stemmer = PorterStemmer()
            stemmed = [[stemmer.stem(word) for word in entry[1]]
                       for entry in aggr_dataset]
            stemmed_concat = [' '.join(words) for words in stemmed]

            # vectorize using bag of words - bow
            vectorizer = CountVectorizer()
            bow = vectorizer.fit_transform(stemmed_concat)

            # generate and train SOM
            msg_som = SOM(m=len(aggr_dataset), n=1, dim=bow.get_shape()[
                          1], random_state=470151198)
            msg_som.fit(bow.toarray())
            
            # make predictions for sender
            predictions = msg_som.predict(bow.toarray())
            table_data = [["Sender", "Predicted Sender"]]
            for entry in aggr_dataset:
                table_data.append([entry[0], aggr_dataset[predictions[aggr_dataset.index(entry)]][0]])

            write_table(story, styles, table_data,
                        "Predicted Sender (Beta, not yet functional!)")
            print("Predicted Sender (Beta, not yet functional!)")
            print(tabulate(
                table_data,
                headers='firstrow',
                tablefmt='fancy_grid',
                maxcolwidths=[None, None])
            )            
            
            # export gml
            nx.write_gml(G, os.path.join(data_dir, seed + ".gml"))

            # closing
            paragraph = \
                "<br></br>" + \
                "A copy of the graph data was saved as " + \
                os.path.join(data_dir, seed + ".gml") + "<br></br>" + \
                "Which can be imported by most network analysis tools, like for example Gephi."
            write_text(story, styles, paragraph)

            # save document
            save_doc(story)

            print("A report of this analysis was saved as " +
                  os.path.join(data_dir, seed + ".pdf"))
