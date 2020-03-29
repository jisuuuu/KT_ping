# -*- coding: utf-8 -*-

#
# Used Project : NIA
# Maker : JS
# Modify : Final_H
# Make Date : Unknown
# Modify Date : 2017-04-25
# Use For HM's Netis Config Read (without check int or String)
#

import os

if 'nt' == os.name.lower():
    INSTALL_PATH = "C:\\netis"
    CFG_DIR = INSTALL_PATH + "\\bin"
    CFG_FILE_PATH = INSTALL_PATH + "\\bin\\NetisEng.cfg"
elif 'posix' == os.name.lower():
    INSTALL_PATH = "/home/netis"
    CFG_DIR = INSTALL_PATH + "/config/"
    CFG_FILE_PATH = INSTALL_PATH + "/config/NetisEng.cfg"

# *****************************************************************************
# Functions to get config value from file

def GetConfValueRaw(strSection, strName):
    # Return value
    strResult = ''

    if not os.path.isdir(CFG_DIR):
        os.mkdir(CFG_DIR)

    # Open config file
    try:
        fileConfig = open(CFG_FILE_PATH, 'r', encoding='utf-8')
    except:
        return strResult

    # Read config value in file
    inSection = False
    strSecFormat = '[' + strSection + ']'
    try:
        # Read lines in file
        while True:
            # Read 1 line in file
            strLine = fileConfig.readline()
            # End of file
            if not strLine:
                break
            # Case : Annotation
            if '#' == strLine[0]:
                continue

            # Find Section
            if len(strLine) > 0 and '[' == strLine[0]:
                if -1 != strLine.find(strSecFormat):
                    inSection = True
                elif -1 != strLine.find(']'):
                    inSection = False
                continue
                # End of if :

            # Find Value
            if inSection and (-1 != strLine.find('=')):
                listToken = strLine.split('=')

                if listToken[0] == strName:
                    strResult = listToken[1].rstrip()
                    break
                    # End of if :
                    # End of while :
                    # End of try :

    except Exception as e:
        print(e)

    fileConfig.close()
    return strResult

    # End of GetConfValueRaw() :


def GetConfValueInt(strSection, strName, nDefVal):
    # Return value
    nResult = int(nDefVal)

    # Get config value
    try:
        # Get raw value like '#1234'
        strRetVal = GetConfValueRaw(strSection, strName)
        # Parsing '#1234' -> '1234' -> 1234
        if len(strRetVal) > 0 and '#' == strRetVal[0]:
            nResult = (int(strRetVal[1:]))
    except:
        pass

    return nResult

    # End of GetConfValueInt()  :


def GetConfValueStr(strSection, strName, strDefVal):
    # Return value
    strResult = str(strDefVal)

    # Get config value
    try:
        strName = '\"' + strName + '\"'
        strRetVal = GetConfValueRaw(strSection, strName)
        if (len(strRetVal) > 1) and ('\"' == strRetVal[0]) and ('\"' == strRetVal[-1]):
            strResult = (str(strRetVal[1:-1]))
    except Exception as e:
        print(e)

    return strResult

    # End of GetConfValueStr()  :