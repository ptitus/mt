#!/usr/bin/python3
import getopt
import os
import re
import string
import sys

from curses.ascii import isdigit
from datetime import datetime
from glob import glob
from scraper_model import SessionError, RequestError, ScrapeError
from scraper_service import ScraperService

def pause():
    programPause = input("Press the <ENTER> key to continue...")

def is_hex(s):
    hex_digits = set(string.hexdigits)
    # if s is long, then it is faster to check against a set
    return all(c in hex_digits for c in s)

def main(argv):
    # init scraper service
    
    try:
        service = ScraperService()
        
        # parse arguments
        arg_new_seed = ""
        arg_new_keyword = ""
        arg_help = f'''
{argv[0]}
    -h --help
    -n <seed> --new <seed>
    -k <keyword> --keyword <keyword>
    -s --scrape
    -a --analyze <folder>'''
    
        try:
            opts, args = getopt.getopt(
                argv[1:], 
                "hn:k:sa:", 
                ["help", "new=", "keyword=", "scrape", "analyze="])
            
        except:
            print(arg_help)
            sys.exit(2)
            
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                print(arg_help)  # print the help message
                sys.exit(2)
            
            elif opt in ("-n", "--new"):
                arg_new_seed = arg
                if bool(re.search(r"\s", arg_new_seed)):
                    print("The entered seed must not contain whitespace characters!")
                else:
                    print(f"Adding new seed {arg_new_seed}")
                    service.add_seed(arg_new_seed)
                
            elif opt in ("-k", "--keyword"):
                arg_new_keyword = arg
                if bool(re.search(r"\s", arg_new_keyword)):
                    print("The entered keyword must not contain whitespace characters!")
                else:
                    print(f"Add new keyword {arg_new_keyword}")
                    service.add_keyword(arg_new_keyword)
                
            elif opt in ("-s", "--scrape"):
                print("Scraping")
                service.scrape()
                
            elif opt in ("-a", "--analyze"):
                arg_folder = arg
                file_path = os.path.join('data', arg_folder, arg_folder + "_scrape.sqlite")
                if not os.path.exists(file_path):
                    print("No sqlite database " + str(file_path) + " found!")   
                else:
                    print("analyzing " + arg_folder)
                    service.analyze(arg_folder)
                    
        if len(opts) > 0:
            pause()
            sys.exit(0)
        
        while True:
            print("\033[H\033[2J", end="")
            print("Main Menu")
            print("--------------------------------------------")
            print("Select operation:")
            print(" 1. Session status")
            print(" 2. Manage accounts")
            print(" 3. Manage keywords and seeds")
            print(" 4. Manage scraping settings")
            print(" 5. Scrape")
            print(" 6. Manage analysis settings")        
            print(" 7. Analyze")
            print(" 8. Merge scraped data")
            print(" 9. Remove scraped data")
            print("10. Clear __pycache__ directory")
            print(" 0. Exit")
            print("--------------------------------------------")
            print("start with -h to see command line arguments")
            userInput = input("Enter number: ")
            if userInput.isdigit():
                userSelection = int(userInput)

                if userSelection == 1:
                    print("The following Telegram sessions have been found:")
                    for mySession in service.Sessions:
                        print("--------------------------------------------")
                        wait_until_str = str(
                            datetime.fromtimestamp(mySession.wait_until))
                        print("--> Name: " + mySession.name)
                        print("--> api_id: " + str(mySession.api_id))
                        print("--> api_hash: " + str(mySession.api_id))
                        print("--> Status: " + mySession.State.name)
                        wait_until = datetime.fromtimestamp(mySession.wait_until)
                        if wait_until > datetime.now():
                            print("--> FloodWait blocking until: " + str(wait_until))
                        else:
                            print("--> No active FloodWait blocking")
                    print("--------------------------------------------")

                elif userSelection == 2:

                    print("\033[H\033[2J", end="")
                    print("2 - Managing Accounts")
                    print("There are " + str(service.iniValues.s_slots) +
                        " t_session[x] slots avalable in the scraper.ini file")
                    print("The following Telegram account settings where")
                    print("found in the scrape.ini configuration file:")
                    for x in range(service.iniValues.s_slots):
                        slot = x + 1
                        section_name = "t_session" + str(slot)
                        print("--------------------------------------------")
                        print("Slot: " + section_name)
                        print("--> Name: " +
                            service.iniValues.config[section_name]["name"])
                        print("--> api_id: " +
                            service.iniValues.config[section_name]["api_id"])
                        print("--> api_hash: " +
                            service.iniValues.config[section_name]["api_hash"])
                    print("--------------------------------------------")
                    print("Select operation:")
                    print("1. Set Account")
                    print("2. Clear Account")
                    print("0. Return to Main")
                    print("--------------------------------------------")
                    userInput = input("Enter number: ")
                    if userInput.isdigit():
                        userSelection = int(userInput)
                        if userSelection == 1:
                            print("At which slot should the new account be stored?")
                            userInput = input("Enter number: ")
                            if userInput.isdigit():
                                userSelection = int(userInput)
                                if userSelection >= 1 and userSelection <= service.iniValues.s_slots:
                                    section_name = "t_session" + str(userSelection)

                                    newName = "%"
                                    while not newName.isalnum():
                                        newName = input("Please enter the name: ")
                                        if not newName.isalnum():
                                            print("Name must be alphanumeric!")
                                    service.set_ini(
                                        section_name, "name", str(newName))

                                    newId = "%"
                                    while not newId.isdigit():
                                        newId = input("Please enter the api_id: ")
                                        if not newId.isdigit():
                                            print(
                                                "api_id must be an integer number!")
                                    service.set_ini(
                                        section_name, "api_id", str(newId))

                                    newHash = "%"
                                    while not is_hex(newHash):
                                        newHash = input(
                                            "Pleas enter the api_hash: ")
                                        if not is_hex(newHash):
                                            print(
                                                "api_hash must only contain hex digits!")
                                    service.set_ini(
                                        section_name, "api_hash", str(newHash))

                                    # Set wait_until to 0
                                    service.set_ini(
                                        section_name, "wait_until", str(0))
                                    print("New session entry:")
                                    print("Slot: " + section_name)
                                    print(
                                        "--> Name: " + service.iniValues.config[section_name]["name"])
                                    print(
                                        "--> api_id: " + service.iniValues.config[section_name]["api_id"])
                                    print(
                                        "--> api_hash: " + service.iniValues.config[section_name]["api_hash"])
                                    print(
                                        "--> wait_until: " + service.iniValues.config[section_name]["wait_until"])
                                    print("Note: If no " + newName +
                                        ".session file already exists, you will")
                                    print(
                                        "      have to login to Telegram at the next start of the program!")

                        elif userSelection == 2:
                            print("Which slot should be cleared?")
                            userInput = input("Enter number: ")
                            if userInput.isdigit():
                                userSelection = int(userInput)
                                if userSelection >= 1 and userSelection <= service.iniValues.s_slots:
                                    section_name = "t_session" + str(userSelection)
                                    oldName = service.iniValues.config[section_name]["name"]
                                    fileName = oldName + ".session"
                                    if os.path.exists(fileName):
                                        print(
                                            "A " + oldName + ".session files still exists, should it be deleted?")
                                        userInput = input("Enter (y)es or (n)o :")
                                        if userInput.lower() == "y":
                                            os.remove(fileName)
                                            if not os.path.exists(fileName):
                                                print("Deleted File " + fileName)
                                            else:
                                                print("Error: Deletion of " +
                                                    fileName + " failed!")
                                            fileNameJrnl = fileName + "-journal"
                                            if os.path.exists(fileNameJrnl):
                                                os.remove(fileNameJrnl)
                                                if not os.path.exists(fileNameJrnl):
                                                    print("Deleted File " +
                                                        fileNameJrnl)
                                                else:
                                                    print("Error: Deletion of " +
                                                        fileNameJrnl + " failed!")
                                    service.set_ini(section_name, "name", "")
                                    service.set_ini(section_name, "api_id", "")
                                    service.set_ini(section_name, "api_hash", "")
                                    service.set_ini(section_name, "wait_until", "")
                                    print("Cleared session entry:")
                                    print("Slot: " + section_name)
                                    print(
                                        "--> Name: " + service.iniValues.config[section_name]["name"])
                                    print(
                                        "--> api_id: " + service.iniValues.config[section_name]["api_id"])
                                    print(
                                        "--> api_hash: " + service.iniValues.config[section_name]["api_hash"])
                                    print(
                                        "--> wait_until: " + service.iniValues.config[section_name]["wait_until"])

                elif userSelection == 3:
                    print("\033[H\033[2J", end="")
                    print("3 - Managing Seeds and Keywords")
                    
                    if len(service.iniValues.seeds) >= 1:
                        print(
                            "The following seeds were found in the scrape.ini configuration file:")
                        seedlist = [
                            "--> " + str(idx + 1) + ": " + seed for idx, seed in enumerate(service.iniValues.seeds)]
                        print(*seedlist, sep='\n')
                    else:
                        print("There are no seeds in the scrape.ini configuration file!")
                    
                    if len(service.iniValues.keywords) >= 1:
                        print(
                            "The following keywords were found in the scrape.ini configuration file:")
                        keylist = [
                            "--> " + str(idx + 1) + ": " + keyword for idx, keyword in enumerate(service.iniValues.keywords)]
                        print(*keylist, sep='\n')
                    else:
                        print("There are no keywords in the scrape.ini configuration file!")
                        
                    print("--------------------------------------------")
                    print("Select operation:")
                    print("1. Add Seed")
                    print("2. Remove Seed")
                    print("3. Add Keyword")
                    print("4. Remove Keyword")
                    print("0. Return to Main")
                    print("--------------------------------------------")
                    userInput = input("Enter number: ")
                    if userInput.isdigit():
                        userSelection = int(userInput)
                        
                        if userSelection == 1:
                            newSeed = str(input("Enter new seed string: "))
                            if bool(re.search(r"\s", newSeed)):
                                print("The entered seed must not contain whitespace characters!")
                            else:
                                service.add_seed(newSeed)
                            
                        elif userSelection == 2:
                            userInput = input(
                                "Enter number of seed that you want to remove:")
                            if userInput.isdigit():
                                userSelection = int(userInput) - 1
                                if userSelection >= 0 and userSelection < (len(service.iniValues.seeds)):
                                    userSelectionSeed = service.iniValues.seeds[userSelection]
                                    if userSelectionSeed:
                                        service.remove_seed(userSelectionSeed)
                                    else:
                                        print("Invalid selection!")
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")
                        
                        elif userSelection == 3:
                            newKey = str(input("Enter new keyword string: "))
                            if bool(re.search(r"\s", newKey)):
                                print("The entered keyword must not contain whitespace characters!")
                            else:
                                service.add_keyword(newKey)

                        elif userSelection == 4:
                            userInput = input(
                                "Enter number of keyword that you want to remove:")
                            if userInput.isdigit():
                                userSelection = int(userInput) - 1
                                if userSelection >= 0 and userSelection < (len(service.iniValues.keywords)):
                                    userSelectionKey = service.iniValues.keywords[userSelection]
                                    if userSelectionKey:
                                        service.remove_keyword(userSelectionKey)
                                    else:
                                        print("Invalid selection!")
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")

                elif userSelection == 4:
                    print("\033[H\033[2J", end="")
                    print("4 - Managing scraping settings")
                    print(
                        "The following settings have been found in the scrape.ini configuration file:")
                    print("--> DEFAULT -> examiners = " + str(service.iniValues.examiner))
                    print("--> DEFAULT -> hops = " + str(service.iniValues.hops))
                    print("--> Telegram -> max_messages = " +
                        str(service.iniValues.max_messages))
                    print("--> Telegram -> min_wait = " +
                        str(service.iniValues.min_wait))
                    print("--> Telegram -> max_wait = " +
                        str(service.iniValues.max_wait))
                    print("--> Telegram -> max_delay = " +
                        str(service.iniValues.max_delay))
                    print("--> Telegram -> follow_invitations = " +
                        str(service.iniValues.follow_invitations))
                    print("--> URL -> use_short = " +
                        str(service.iniValues.use_short))
                    print("--------------------------------------------")
                    print("Select operation:")
                    print("1. Set DEFAULT -> examiner")
                    print("2. Set DEFAULT -> hops")
                    print("3. Set Telegram -> max_messages")
                    print("4. Set Telegram -> min_wait")
                    print("5. Set Telegram -> max_wait")
                    print("6. Set Telegram -> max_delay")
                    print("7. Set Telegram -> follow_invitations")
                    print("8. Set URL -> use_short")
                    print("0. Return to Main")
                    print("--------------------------------------------")
                    userInput = input("Enter number: ")
                    if userInput.isdigit():
                        userSelection = int(userInput)

                        if userSelection == 1:
                            section = "DEFAULT"
                            print(
                                "String, minmum length = 2, maximal length = 50")
                            print("Name of the examiner conducting the research.")
                            newExaminer = str(input("Enter new examiners name: "))
                            if len(newExaminer) > 1 and len(newExaminer) <= 50:
                                    service.set_ini(section, "examiner", newExaminer)
                            else:
                                print("Invalid Input!")

                        elif userSelection == 2:
                            section = "DEFAULT"
                            print(
                                "Positive integer, minmum value = 1, maximal value = 10")
                            print("maximum number of hops scraping should occur.")
                            print(
                                "The time for scraping grows exponentially with the value!")
                            newHops = str(input("Enter new hops value: "))
                            if newHops.isdigit():
                                if int(newHops) > 0 and int(newHops) <= 10:
                                    service.set_ini(section, "hops", newHops)
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")

                        elif userSelection == 3:
                            section = "Telegram"
                            print(
                                "Positive integer, minimum value = 0, maximim value = " + str(sys.maxsize))
                            print("Maximum number of Messages that should be scraped")
                            print("from each entity. Set to 0 for unlimited.")
                            newMaxMsg = str(input("Enter new max_message value: "))
                            if newMaxMsg.isdigit():
                                if int(newMaxMsg) >= 0 and int(newMaxMsg) < sys.maxsize:
                                    service.set_ini(
                                        section, "max_messages", newMaxMsg)
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")

                        elif userSelection == 4:
                            section = "Telegram"
                            print(
                                "Positive integer, minimum value = 0, maximim value = 100")
                            print(
                                "lower border of randomized wait time in Seconds between")
                            print("Telegram API calls, to prevent Floodwait Errors")
                            print("Values above 0 slow down scraping notably")
                            newMinWait = str(input("Enter new min_wait value: "))
                            if newMinWait.isdigit():
                                if int(newMinWait) >= 0 and int(newMinWait) <= 100:
                                    service.set_ini(
                                        section, "min_wait", newMinWait)
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")

                        elif userSelection == 5:
                            section = "Telegram"
                            print(
                                "Positive integer, minimum value > min_wait, maximim value = 100")
                            print(
                                "upper border of randomized wait time in Seconds between")
                            print("Telegram API calls, to prevent Floodwait Errors.")
                            print("High values slow down scraping notably")
                            newMaxWait = str(input("Enter new min_wait value: "))
                            if newMaxWait.isdigit():
                                if int(newMaxWait) >= int(service.iniValues.min_wait) \
                                        and int(newMaxWait) <= 100:
                                    service.set_ini(
                                        section, "max_wait", newMaxWait)
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")

                        elif userSelection == 6:
                            section = "Telegram"
                            print(
                                "Positive integer, minimum vaule = 60, maximum value = 3600")
                            print("maximum wait time before scraping stops")
                            print("because of Floodwait Errors.")
                            print(
                                "60 Seconds are buffered by Telethon without notice by default")
                            newMaxDelay = str(input("Enter new max_delay value: "))
                            if newMaxDelay.isdigit():
                                if int(newMaxDelay) >= 60 and int(newMaxDelay) <= 3600:
                                    service.set_ini(
                                        section, "max_delay", newMaxDelay)
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")

                        elif userSelection == 7:
                            section = "Telegram"
                            print("Boolean, 1 = True, 0 = False")
                            print("Should scraping follow Telegram invitation Links?")
                            newFollow = str(
                                input("Enter new follow_invitations value: "))
                            if newFollow.isdigit():
                                if int(newFollow) == 0:
                                    service.set_ini(
                                        section, "follow_invitations", "False")
                                elif int(newFollow) == 1:
                                    service.set_ini(
                                        section, "follow_invitations", "True")
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")

                        elif userSelection == 8:
                            section = "URL"
                            print("Boolean, 1 = True, 0 = False")
                            print(
                                "Should the graph contain shortened URLs for better readability?")
                            newShort = str(input("Enter new use_short value: "))
                            if newShort.isdigit():
                                if int(newShort) == 0:
                                    service.set_ini(section, "use_short", "False")
                                elif int(newShort) == 1:
                                    service.set_ini(section, "use_short", "True")
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")

                elif userSelection == 5:
                    print("\033[H\033[2J", end="")
                    print("5 - Scraping")
                    service.scrape()
                
                elif userSelection == 6:
                    print("\033[H\033[2J", end="")
                    print("4 - Managing analysing settings")
                    print(
                        "The following settings have been found in the scrape.ini configuration file:")
                    print("--> Analyze -> show_hops = " +
                        str(service.iniValues.show_hops))
                    print("--> Analyze -> num_communities = " +
                        str(service.iniValues.num_communities))
                    print("--> Analyze -> contacts = " +
                        str(service.iniValues.contacts))
                    print("--> Analyze -> bots = " +
                        str(service.iniValues.bots))
                    print("--> Analyze -> chats = " +
                        str(service.iniValues.chats))
                    print("--> Analyze -> channels = " +
                        str(service.iniValues.channels))
                    print("--> Analyze -> telegram_ids = " +
                        str(service.iniValues.telegram_ids))
                    print("--> Analyze -> urls = " +
                        str(service.iniValues.urls))
                    print("--> Analyze -> o_entities = " +
                        str(service.iniValues.o_entities))
                    print("--> Analyze -> keys = " +
                        str(service.iniValues.keys))
                    print("--------------------------------------------")
                    print("Select operation:")
                    print("Section \"Analyze\"")
                    print(" 1. Set show_hops")
                    print(" 2. Set num_communities")
                    print(" 3. Set Include contacts")
                    print(" 4. Include bots")
                    print(" 5. Include chats")
                    print(" 6. Include channels")
                    print(" 7. Include telegram_ids")
                    print(" 8. Include urls")
                    print(" 9. Include o_entities")
                    print("10. Include keywords")
                    print(" 0. Return to Main")
                    print("--------------------------------------------")
                    userInput = input("Enter number: ")
                    if userInput.isdigit():
                        userSelection = int(userInput)

                        if userSelection == 1:
                            section = "Analyze"
                            print(
                                "Positive integer, minimum vaule = 1, maximum value = 10")
                            print("maximum hop up until which nodes should be shown in the")
                            print("generated plot of the graph.")
                            print("Plots get easily congested if too many Nodes are displayed!")
                            newShowHops = str(input("Enter new show_hops value: "))
                            if newShowHops.isdigit():
                                if int(newShowHops) >= 1 and int(newShowHops) <= 10:
                                    service.set_ini(
                                        section, "show_hops", newShowHops)
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")
                        
                        elif userSelection == 2:
                            section = "Analyze"
                            print("Positive integer, minimum vaule = 0, maximum value = 10")
                            print("count of the communities that should be plotted the report")
                            print("Communities are plotte in decreasing size order.")
                            print("In small graphs the last communities might be empty.")
                            newNumComms = str(input("Enter new num_communities value: "))
                            if newNumComms.isdigit():
                                if int(newNumComms) >= 0 and int(newNumComms) <= 10:
                                    service.set_ini(
                                        section, "num_communities", newNumComms)
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")
                        
                        elif userSelection == 3:
                            section = "Analyze"
                            setting = "contacts"
                            print("Boolean, 1 = True, 0 = False")
                            print("Should the analysis include Telegram " + setting + "?")
                            newBool = str(
                                input("Enter new " + setting + " setting: "))
                            if newBool.isdigit():
                                if int(newBool) == 0:
                                    service.set_ini(
                                        section, setting, "False")
                                elif int(newBool) == 1:
                                    service.set_ini(
                                        section, setting, "True")
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")
                
                        elif userSelection == 4:
                            section = "Analyze"
                            setting = "bots"
                            print("Boolean, 1 = True, 0 = False")
                            print("Should the analysis include Telegram " + setting + "?")
                            newBool = str(
                                input("Enter new " + setting + " setting: "))
                            if newBool.isdigit():
                                if int(newBool) == 0:
                                    service.set_ini(
                                        section, setting, "False")
                                elif int(newBool) == 1:
                                    service.set_ini(
                                        section, setting, "True")
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")
                        
                        elif userSelection == 5:
                            section = "Analyze"
                            setting = "chats"
                            print("Boolean, 1 = True, 0 = False")
                            print("Should the analysis include Telegram " + setting + "?")
                            newBool = str(
                                input("Enter new " + setting + " setting:"))
                            if newBool.isdigit():
                                if int(newBool) == 0:
                                    service.set_ini(
                                        section, setting, "False")
                                elif int(newBool) == 1:
                                    service.set_ini(
                                        section, setting, "True")
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")
                        
                        elif userSelection == 6:
                            section = "Analyze"
                            setting = "channels"
                            print("Boolean, 1 = True, 0 = False")
                            print("Should the analysis include Telegram " + setting + "?")
                            newBool = str(
                                input("Enter new " + setting + " setting: "))
                            if newBool.isdigit():
                                if int(newBool) == 0:
                                    service.set_ini(
                                        section, setting, "False")
                                elif int(newBool) == 1:
                                    service.set_ini(
                                        section, setting, "True")
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")
                        
                        elif userSelection == 7:
                            section = "Analyze"
                            setting = "telegram_ids"
                            print("Boolean, 1 = True, 0 = False")
                            print("Should the analysis include unresolved Telegram IDs?")
                            newBool = str(
                                input("Enter new " + setting + " setting: "))
                            if newBool.isdigit():
                                if int(newBool) == 0:
                                    service.set_ini(
                                        section, setting, "False")
                                elif int(newBool) == 1:
                                    service.set_ini(
                                        section, setting, "True")
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")
                        
                        elif userSelection == 8:
                            section = "Analyze"
                            setting = "urls"
                            print("Boolean, 1 = True, 0 = False")
                            print("Should the analysis include " + setting + "?")
                            newBool = str(
                                input("Enter new " + setting + " setting: "))
                            if newBool.isdigit():
                                if int(newBool) == 0:
                                    service.set_ini(
                                        section, setting, "False")
                                elif int(newBool) == 1:
                                    service.set_ini(
                                        section, setting, "True")
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")
                        
                        elif userSelection == 9:
                            section = "Analyze"
                            setting = "o_entities"
                            print("Boolean, 1 = True, 0 = False")
                            print("Should the analysis include entities from other plattforms?")
                            newBool = str(
                                input("Enter new " + setting + " setting: "))
                            if newBool.isdigit():
                                if int(newBool) == 0:
                                    service.set_ini(
                                        section, setting, "False")
                                elif int(newBool) == 1:
                                    service.set_ini(
                                        section, setting, "True")
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")

                        elif userSelection == 10:
                            section = "Analyze"
                            setting = "keys"
                            print("Boolean, 1 = True, 0 = False")
                            print("Should the analysis include keywords?")
                            newBool = str(
                                input("Enter new " + setting + " setting: "))
                            if newBool.isdigit():
                                if int(newBool) == 0:
                                    service.set_ini(
                                        section, setting, "False")
                                elif int(newBool) == 1:
                                    service.set_ini(
                                        section, setting, "True")
                                else:
                                    print("Invalid selection!")
                            else:
                                print("Invalid selection!")

                elif userSelection == 7:
                    print("\033[H\033[2J", end="")
                    print("7 - Analyzing scraped Dataset")
                    print(
                        "Scrape Databases for the following seeds were found in the data directory:")
                    dbfiles = glob('data/*/*_scrape.sqlite')
                    dbfiles.sort(key=str.lower)
                    dblist = ["--> " +
                            str(idx + 1) +
                            ": " +
                            file.split("/")[1:2][0] for idx, file in enumerate(dbfiles)]
                    print(*dblist, sep='\n')
                    userInput = input(
                        "Enter number of Database you want to analyze: ")
                    if userInput.isdigit():
                        userSelectionDb = int(userInput) - 1
                        if userSelectionDb >= 0 and userSelectionDb < (len(dbfiles)):
                            dbfile = dbfiles[userSelectionDb]
                            if dbfile:
                                db_name = dbfile.split("/")[2:3][0]
                                seed_name = db_name[:len(db_name)-14]
                                service.analyze(seed_name)
                            else:
                                print("Invalid selection!")
                        else:
                            print("Invalid selection!")
                    else:
                        print("Invalid selection!")
                
                elif userSelection == 8:
                    print("\033[H\033[2J", end="")
                    print("8 - Merge scraped Datasets")
                    print("Scrape Databases for the following seeds were found in the data directory:")
                    dbfiles = glob('data/*/*_scrape.sqlite')
                    dbfiles.sort(key=str.lower)
                    dblist = ["--> " +
                            str(idx + 1) +
                            ": " +
                            file.split("/")[1:2][0] for idx, file in enumerate(dbfiles)]
                    print(*dblist, sep='\n')
                    validInput = False
                    userInput = input(
                        "Enter number of first Dataset you want to merge: ")
                    if userInput.isdigit():
                        userSelectionDb = int(userInput) - 1
                        if userSelectionDb >= 0 and userSelectionDb < (len(dbfiles)):
                            dbfile1 = str(dbfiles[userSelectionDb])
                            db_name1 = dbfile1.split("/")[2:3][0]
                            seed_name1 = db_name1[:len(db_name1)-14]
                            validInput = True
                    
                    if validInput:
                        userInput = input(
                            "Enter number of second Dataset you want to merge: ")
                        if userInput.isdigit():
                            userSelectionDb = int(userInput) - 1
                            if userSelectionDb >= 0 and userSelectionDb < (len(dbfiles)):
                                dbfile2 = str(dbfiles[userSelectionDb])
                                db_name2 = dbfile2.split("/")[2:3][0]
                                seed_name2 = db_name2[:len(db_name2)-14]
                                validInput = True
                            else:
                                validInput = False
                        else:
                            validInput = False
                    
                    if dbfile1 == dbfile2:
                        print("Please choose two different datasets!")
                        validInput = False
                        
                    if validInput:
                        userInput = str(input(
                            "Enter name of new merged Dataset (2 - 32 Characters): "))
                        if len(userInput) > 1 and len(userInput) <= 32:
                            newName = userInput
                            folder = os.path.join("data", newName)
                            if not os.path.exists(folder):
                                validInput = True
                            else:
                                print("Error: Folder" + repr(folder) + " already exists!")
                                validInput = False
                        else:
                            validInput = False
                            
                    if validInput:
                        print("Please verify that the following Datasets should be merged:")
                        print("Dataset 1 : " + seed_name1)
                        print("Dataset 2 : " + seed_name2)
                        print("New merged Dataset: " + newName)
                        cont = input("Enter (y/n): ") 
                        if cont == 'y':
                            service.merge(seed_name1, seed_name2, newName)
                        else:
                            print("No confirmation received, so no change to folders!")
                    else:
                        print("Invalid selection!")
                        
                elif userSelection == 9:
                    print("\033[H\033[2J", end="")
                    print("9 - Deleting scraped Dataset")
                    print(
                        "Scrape Databases for the following seeds were found in the data directory:")
                    dbfiles = glob('data/*/*_scrape.sqlite')
                    dbfiles.sort(key=str.lower)
                    dblist = ["--> " +
                            str(idx + 1) +
                            ": " +
                            file.split("/")[1:2][0] for idx, file in enumerate(dbfiles)]
                    print(*dblist, sep='\n')
                    userInput = input(
                        "Enter number of Dataset you want to delete: ")
                    if userInput.isdigit():
                        userSelectionDb = int(userInput) - 1
                        if userSelectionDb >= 0 and userSelectionDb < (len(dbfiles)):
                            dbfile = str(dbfiles[userSelectionDb])
                            folder = os.path.dirname(os.path.join(dbfile))
                            if folder:
                                if os.path.exists(folder):
                                    cont = input(
                                        "Really delete " +
                                        folder +
                                        " directory? (y/n)")
                                    if cont.lower() == 'y':
                                        service.clear_data(folder)
                                    else:
                                        print(
                                            "No confirmation received, so no change to folders!")
                                else:
                                    print("Error: Folder " +
                                        folder +
                                        " not found!")
                            else:
                                print("Invalid selection!")
                        else:
                            print("Invalid selection!")
                    else:
                        print("Invalid selection!")

                elif userSelection == 10:
                    cont = input(
                        "Really delete  __pycache__ directory? (y/n)")
                    if cont == 'y':
                        dir = (r"__pycache__")
                        service.clear_data(dir)
                    else:
                        print("No confirmation received, so no change to folders!")

                elif userSelection == 0:
                    sys.exit(0)

                else:
                    raise ValueError("Invalid selection!")

            pause()

    except SessionError as sessionEx:
        print(sessionEx)
        sys.exit(-1)

    except ValueError as valueEx:
        print(valueEx)
        sys.exit(-2)

    else:
        sys.exit(0)
        
if __name__ == "__main__":
   main(sys.argv)
