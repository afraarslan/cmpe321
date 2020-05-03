import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import csv
import sys

# Input data files are available in the "../input/" directory.

import os
##ddl operations
#create a type
def createType(name, fieldNumber, fieldNames):
    
    with open(systemCatalog_file, 'a+'):
        
        systemCatalog = pd.read_csv(systemCatalog_file, names = ['name', 'num', 'fields', 'files'], header=0)
    
    if checkType(name):
        print('Type already exists')
        return
    
    #declare all attirbutes of type
    typeName = name
    numberOfFields = int (fieldNumber)
    nameOfFields = []
    for i in range(numberOfFields):
        nameOfFields.append(fieldNames[i])
        
    for i in range(numberOfFields, 10):
        nameOfFields.append("*")
    
    #assign new type
    newType = [typeName, fieldNumber, nameOfFields, '-']   
    
    #create a page for the type
    newPage = createPage(typeName)
    systemCatalog.loc[len(systemCatalog)] = newType
    
    is_skipped = False
    files = systemCatalog['files']
    for f, i in zip(files, range(len(files))):
        if f != '-':
            if lengthOfFile(f) >0 :
                addPageToFile(newPage, f)
                break
            else:
                continue
        else:
            newFile = createFile(newPage)
            systemCatalog['files'][i] = newFile
            break

    systemCatalog.to_csv(systemCatalog_file, index=False)

    print(name, ' type created')

#delete a Type                
def deleteType(word):
    
    global nextPageId
    global nextPageId
    word = word.strip("\n")
    
    with open(systemCatalog_file, 'a+'):
        systemCatalog = pd.read_csv(systemCatalog_file, names = ['name', 'num', 'fields', 'files'], header=0)
    
    if checkType(word) == False:
        print('Type does not exist')
        return
    
    is_skipped = False
    names = systemCatalog['name']
    files = systemCatalog['files']
    for n, f, i in zip(names, files, range(len(files))):
        if n == word:
            if f == '-':
                systemCatalog.drop([i], inplace=True)
            else:
                systemCatalog.loc[i] = ['-', '-', '-', f]
            break
            
    systemCatalog.to_csv(systemCatalog_file, index=False)
        

    for f in files:
        if os.path.isfile(f):
            file = pd.read_csv(f, header=None, names = list(range(0,13)), keep_default_na=False) 
            emptyPages = int(file.loc[0, 2])    
            if emptyPages == 20:
                continue
            numPages = 20 - emptyPages
            for j in range(numPages):
                ind = j*26+1
                nameP = file.loc[ind, 2]
                if nameP == word:
                    is_skipped = True
                    file = file.drop(file.index[ind:ind+26])
                    emptyPages +=1
                    
                    
            file.loc[0, 2] = emptyPages
                            
        if is_skipped:
            file.to_csv(f, index=False, header=False) #index_label=False)
        
    print(word, ' type deleted')
    

    
#list types
def listAllTypes():
    
    with open(systemCatalog_file, 'a+'):
    
        systemCatalog = pd.read_csv(systemCatalog_file, names = ['name', 'num', 'fields', 'files'], header=0)

    names = systemCatalog['name']
    sorted_names = []
    for n in names:
        if n != '-':
            sorted_names.append(n)
            
    if len(sorted_names) == 0:
        print('No type in the system catalog!')
        return
    
    sorted_names.sort()
    for i in range(len(sorted_names)):
        if sorted_names[i] != '-':
            output.write(sorted_names[i])
        output.write('\n')
    print('list all types')

        
##dml operations
#create a record
def createRecord(name, fieldValues):
    #declare related attributes
    global nextFileId
    global nextId
    
    if checkType(name) == False:
        print(' Type does not exist')
        return 
    
    if findNumberOfFields(name) != len(fieldValues):
        print("something is wrong in field values")
        return

    if findRecord(name, fieldValues[0]) == 1:
        print('make isRegistered True ')
        return
    
    if findRecord(name, fieldValues[0]) == 2:
        print('it is already exist')
        return
    
    recordId = nextId
    nextRecordId = nextId + 1
    isRegistered = True
 
    
    newRecord = [recordId, nextRecordId, isRegistered]
    
    for i in range(len(fieldValues)):
        j = int(fieldValues[i].strip('\n'))
        newRecord.append(j)
    
    for i in range(len(fieldValues), 10):
        newRecord.append('*')
    
    
    with open(systemCatalog_file, 'a+'):
        systemCatalog = pd.read_csv(systemCatalog_file, names = ['name', 'num', 'fields', 'files'], header=0)
    
    files = systemCatalog['files']
    
    file = pd.DataFrame()
    is_skipped= False
    for f, z in zip(files, range(nextFileId)):
        if os.path.isfile(f):
            file = pd.read_csv(f, sep=",", header=None, names = list(range(0,13)), keep_default_na=False)
            emptyPages = int(file.loc[0, 2]) 
            #if emptyPages == 0:
             #   continue
            numPages = 20 - emptyPages
            for j in range(numPages):
                ind = j*26+1
                nameP = file.loc[ind, 2] 
                emptyRecords = int(file.loc[ind, 3]) 
                
                numRecords = 25 - emptyRecords
                if nameP == name and numRecords <25 and not is_skipped:
                    #i = ind + numRecords + 1
                    
                    i = ind + 1
                    y = newRecord[3]
                    while file.loc[i, 3] != '':
                        x = int(file.loc[i, 3])
                        if x < y:
                            i +=1
                        else:
                            lim = ind + numRecords + 1
                            temp = pd.DataFrame(file.loc[i:lim])
                            
                            for inc in range(i, lim):
                                file.loc[inc+1] = file.loc[inc] 
                            
                            break
                        
                    file.loc[i] = newRecord
                    is_skipped = True
                    file.loc[ind, 3] = emptyRecords - 1
                    
                    break
                      
            if not is_skipped and emptyPages>0:
                newPage= createPage(name)
                addPageToFile(newPage, f)
                addRecordToPage(newRecord, f)
                
        
            if is_skipped:
                file.to_csv(f, index=False, header=False)
                break

    if not is_skipped:
        newPage= createPage(name)
        newFile = createFile(newPage)
        systemCatalog.loc[nextFileId, 'files'] = newFile
        addRecordToPage(newRecord, newFile)
        systemCatalog.to_csv(systemCatalog_file, index=False)
        
        
    
    print(name, ' record created')                  
    nextId += 1
    
    

#delete a record
def deleteRecord(name, primaryKey):
    
    global nextFileId
    
    primaryKey = primaryKey.strip('\n')
    
    if checkType(name) == False:
        print('There is no such type so you cannot delete a record.')
        return
    
    
    with open(systemCatalog_file, 'a+'):    
        systemCatalog = pd.read_csv(systemCatalog_file, names = ['name', 'num', 'fields', 'files'], header=0)
    
    files = systemCatalog['files']
    
    for f, z in zip(files, range(nextFileId)): #len(files))):
        if os.path.isfile(f):    
            file = pd.read_csv(f, sep=",", header=None, names = list(range(0,13)), keep_default_na=False)
            emptyPages = int(file.loc[0, 2]) #file[2][0])
            if emptyPages == 20:
                continue
            numPages = 20 - emptyPages
            for j in range(numPages):
                ind = j*26+1
                emptyRecords = int(file.loc[ind, 3]) #file[3][ind])
                if emptyRecords == 25:
                    continue
                numRecords = 25- emptyRecords
                nameP = file.loc[ind, 2]  #[2][ind]
                if nameP == name:
                    for z in range(numRecords):
                        i = ind + z +1
                        if file.loc[i, 3] == primaryKey and file.loc[i, 2] == 'True':
                            file.loc[i, 2] = 'False'
                            
                            file.to_csv(f, index=False, header=False)
                            
                            print(name, primaryKey, ' deleted')
                            
                            return
                        
    print('No such record exist.')
     

#search a record
def searchRecord(name, primaryKey):
    
    global nextFileId
    
    primaryKey = primaryKey.strip('\n')
    
    
    with open(systemCatalog_file, 'a+'):
        
        systemCatalog = pd.read_csv(systemCatalog_file, names = ['name', 'num', 'fields', 'files'], header=0)
    
    files = systemCatalog['files']
    
    for f, z in zip(files, range(nextFileId)):
        if os.path.isfile(f):    
            file = pd.read_csv(f, sep=",", header=None, names = list(range(0,13)), keep_default_na=False)
            emptyPages = int(file.loc[0, 2]) #file[2][0])
            if emptyPages == 20:
                continue
            numPages = 20 - emptyPages
            for j in range(numPages):
                ind = j*26+1
                emptyRecords = int(file.loc[ind, 3]) 
                if emptyRecords == 25:
                    continue
                numRecords = 25- emptyRecords
                nameP = file.loc[ind, 2]  #[2][ind]
                if nameP == name:
                    for z in range(numRecords):
                        i = ind + z +1
                        if file.loc[i, 3] == primaryKey and file.loc[i, 2] == 'True':
                            x = ''
                            for each in file.loc[i, 3:]:
                                if each != '*':
                                    x += each + ' '
                            output.write(x)
                            output.write('\n')
                            
                            print(name, primaryKey, ' found')
                            return
                        
    print('Search was not found.')
                


#update a record
def updateRecord(name, primaryKey, updatedFields):
    
    global nextFileId
    
    
    fieldNumber = findNumberOfFields(name)
    
    if  fieldNumber != len(updatedFields):
        print("something is wrong in field values")
        return
    
    primaryKey = primaryKey.strip('\n')
    updatedFields[fieldNumber-1] = updatedFields[fieldNumber-1].strip('\n')
    
    for i in range(fieldNumber, 10):
        updatedFields.append('*')
    
    
    with open(systemCatalog_file, 'a+'):    
        systemCatalog = pd.read_csv(systemCatalog_file, names = ['name', 'num', 'fields', 'files'], header=0)
    
    files = systemCatalog['files']
    
    for f, z in zip(files, range(nextFileId)):
        if os.path.isfile(f):    
            file = pd.read_csv(f, sep=",", header=None, names = list(range(0,13)), keep_default_na=False)
            emptyPages = int(file.loc[0, 2]) 
            if emptyPages == 20:
                continue
            numPages = 20 - emptyPages
            for j in range(numPages):
                ind = j*26+1
                emptyRecords = int(file.loc[ind, 3]) 
                if emptyRecords == 25:
                    continue
                numRecords = 25- emptyRecords
                nameP = file.loc[ind, 2] 
                if nameP == name:
                    for z in range(numRecords):
                        i = ind + z +1
                        if file.loc[i, 3] == primaryKey and file.loc[i, 2] == 'True':
                            file.loc[i, 3:] = updatedFields
                            
                            file.to_csv(f, index=False, header=False)
                            
                            print(name, primaryKey, ' updated')
                            
                            return

    print('Record cannot updated.')
     

#list records
def listRecords(name):
    
    global nextFileId
    
    name = name.strip('\n')
    
    
    with open(systemCatalog_file, 'a+'):    
        systemCatalog = pd.read_csv(systemCatalog_file, names = ['name', 'num', 'fields', 'files'], header=0)
    
    files = systemCatalog['files']
    
    for f, z in zip(files, range(nextFileId)):
        if os.path.isfile(f):    
            file = pd.read_csv(f, sep=",", header=None, names = list(range(0,13)), keep_default_na=False)
            emptyPages = int(file.loc[0, 2]) #file[2][0])
            if emptyPages == 20:
                continue
            numPages = 20 - emptyPages
            for j in range(numPages):
                ind = j*26+1
                emptyRecords = int(file.loc[ind, 3])
                if emptyRecords == 25:
                    continue
                numRecords = 25- emptyRecords
                nameP = file.loc[ind, 2]  #[2][ind]
                if nameP == name:
                    for z in range(numRecords):
                        i = ind + z +1
                        if file.loc[i, 2] == 'True':
                            x = ''
                            for each in file.loc[i, 3:]:
                                if each != '*':
                                    x += each + ' '
                            output.write(x)
                            output.write('\n')
                            
                            
    print(' All ', name, 'listed')


#checks the type exists    
def checkType(name):
    
    with open(systemCatalog_file, 'r') as read_obj:
        read_obj.seek(0)
        for line in read_obj.readlines():
            words = line.split(',')
            if words[0] == name:
                return True
    return False


#finds the number of fields of a type
def findNumberOfFields(name):
    
    with open(systemCatalog_file, 'r') as read_obj:
        read_obj.seek(0)
        for line in read_obj.readlines():
            words = line.split(',')
            if words[0] == name:
                return int(words[1])

#find the record if it exists            
def findRecord(name, primaryKey):
    
    with open(systemCatalog_file, 'a+'):
        
        systemCatalog = pd.read_csv(systemCatalog_file, names = ['name', 'num', 'fields', 'files'], header=0)
    
    files = systemCatalog['files']
    for f, z in zip(files, range(nextFileId)):
        if os.path.isfile(f):    
            file = pd.read_csv(f, sep=",", header=None, names = list(range(0,13)), keep_default_na=False)
            emptyPages = int(file.loc[0, 2]) #file[2][0])
            if emptyPages == 20:
                continue
            numPages = 20 - emptyPages
            for j in range(numPages):
                ind = j*26+1
                emptyRecords = int(file.loc[ind, 3]) 
                if emptyRecords == 25:
                    continue
                numRecords = 25- emptyRecords
                nameP = file.loc[ind, 2]  #[2][ind]
                if nameP == name:
                    for z in range(numRecords):
                        i = ind + z +1
                        if file.loc[i, 3] == primaryKey:
                            if file.loc[i, 2] == 'False':
                                file.loc[i, 2] = 'True'
                                file.to_csv(f, index=False, header=False)
                                return 1
                            else:
                                return 2
    return 0
    

            
#create a new page for a type
def createPage(name):
    global nextPageId
    pageId = nextPageId
    nextPage = nextPageId+1
    pageType = name
    emptyRecords = 25

    pageHeader = [pageId, nextPage, pageType, emptyRecords]  

    nextPageId += 1
    return pageHeader


#create a file 
def createFile(page):
    global nextFileId
    fileId = nextFileId
    nextFile = nextFileId+1
    emptyPages = 20
    
    file = "file" + str(fileId) + ".txt"
    
    is_Page = False
    if page != []:
        emptyPages -= 1
        is_Page = True
    fileHeader = [fileId, nextFile, emptyPages]
    with open(file, 'a+', newline='') as access_obj:
        csv_writer = csv.writer(access_obj)
        csv_writer.writerow(fileHeader)
        if is_Page:
            csv_writer.writerow(page)
            for i in range(25):
                csv_writer.writerow('-')
            
    
        
    nextFileId += 1
    return file


#if a file empty or not
def empty(file):
    with open(file, 'r', newline='') as read_obj:
        header = read_obj.readline()
        _, _, emptyPages = header.split(',')
        if int(emptyPages) > 0:
            return True
        else:
            return False

#finds how many pages in the file        
def lengthOfFile(file):
    
    with open(file, 'r', newline='') as read_obj:
        header = read_obj.readline().strip('\n')
        header = header.split(',')
        return int(header[2])


#adds record onto the page in the file
def addRecordToPage(record, file):
    
    dummy_file = file + '.bak'
    # Open original file in read only mode and dummy file in write mode
    with open(file, 'r+') as read_obj, open(dummy_file, 'w') as write_obj:
        read_obj.seek(0)
        # Line by line copy data from original file to dummy file
        csv_writer = csv.writer(write_obj)
        
        header = read_obj.readline().strip('\n')
        header = header.split(',')
        csv_writer.writerow(header)
        
        header = read_obj.readline().strip('\n')
        header = header.split(',')
        header[3] = int(header[3]) - 1 
        csv_writer.writerow(header)
        
        csv_writer.writerow(record)
        for i in range(24):
            csv_writer.writerow('-')
 
    # If any line is skipped then rename dummy file as original file
    os.remove(file)
    os.rename(dummy_file, file)
    
    
#adds page into the file
def addPageToFile(page, file):
    
    dummy_file = file + '.bak'
    # Open original file in read only mode and dummy file in write mode
    with open(file, 'r+') as read_obj, open(dummy_file, 'w') as write_obj:
        read_obj.seek(0)
        # Line by line copy data from original file to dummy file
        csv_writer = csv.writer(write_obj)
        
        header = read_obj.readline().strip('\n')
        header = header.split(',')
        header[2] = int(header[2]) - 1 
        csv_writer.writerow(header)
        
        for line in csv.reader(read_obj):
            csv_writer.writerow(line)
            
        csv_writer.writerow(page)
        for i in range(25):
            csv_writer.writerow('-')
 
    # If any line is skipped then rename dummy file as original file
    os.remove(file)
    os.rename(dummy_file, file)


systemCatalog_file = 'systemCatalog.csv'

file_name = sys.argv[1]
output_name = sys.argv[2]

with open(systemCatalog_file, 'a+') as s:
    systemCatalog = pd.read_csv(systemCatalog_file, names = ['name', 'num', 'fields', 'files'], header=0)
    files = systemCatalog[~systemCatalog["files"].str.contains('-')]
    names = systemCatalog['name']
    if len(files) >0 and len(names) > 0:
        nextFileId = len(files)
        nextPageId = len(names)
    else: 
        nextFileId = 0
        nextPageId = 0

nextId = 0
    
output = open(output_name, 'a+')
output.truncate(0)

with open(file_name, 'r') as file:
    for line in file:
        words = line.split(" ")
        if words[0] == "create":
            if words[1] == "type":
                createType(words[2], words[3], words[4:])
            else:
                createRecord(words[2], words[3:])

        elif words[0] == "delete":
            if words[1] == "type":
                deleteType(words[2])
            else:
                deleteRecord(words[2], words[3])

        elif words[0] == "list":
            if words[1] == "record":
                listRecords(words[2])
            else:
                listAllTypes()

        elif words[0] == "search":
            searchRecord(words[2], words[3])

        else:
            updateRecord(words[2], words[3], words[3:])
                
output.close()                
