# Databricks notebook source
# DBTITLE 1,Import Module
try:
  import json 
  import pandas as pd 
  import xml.etree.ElementTree as ET
  from pyspark.sql.functions import udf, struct, col,lit
  from pyspark.sql.types import *
except Exception as e:
  print(e)

# COMMAND ----------

# DBTITLE 1,Extract XML from string message
#Function definition to extract xml from string message
def extractXML(message):
    try:    
        if '&gt;' in message:
            #If message contains &gt replace below specified values in message with default values
            message = message.replace(' /&gt;', '/>').replace('&lt;', '<').replace('&gt;', '>')    
        
        else:
            #Else nothing has to replace
            message = message
            
        #Initializing empty dictionary to store elements  
        xmlNodeDic = {}
        
        if "Quotes Hub request" in message and "![CDATA[" not in message:
            #Finding the message type by using keywards in the message
            messageType = "request_type1"
            #Split the message to get the xml part 
            headerXmlSplit = message.split('</xrt:RequestXML>')[0].split('<xrt:RequestXML>', 1)
        
        elif "Quotes Hub request" in message and "![CDATA["  in message:
            #Finding the message type by using keywards in the message
            messageType = "request_type2"
            #Split the message to get the xml part
            headerXmlSplit = message.split(']]')[0].split('<![CDATA[', 1)
        
        else:
            messageType = "response"
            if "![CDATA["  in message:
                headerXmlSplit = message.split(']]')[0].split('<![CDATA[', 1)
                
            else:
                headerXmlSplit = message.split('</a:ResponseXML>')[0].split('<a:ResponseXML xmlns:a="http://schemas.datacontract.org/2004/07/XRTEService">', 1)
        
        mainXML = headerXmlSplit[1].replace('<?xml version=""1.0"" ?>', '').replace('<?xml version="1.0" ?>','')
        #Split the string to retrieve the fake json part. Only take up to the first } to stop it parsing after that
        firstStringSplit = headerXmlSplit[0].split('}')
        junkJsonString = firstStringSplit[0].replace("{", '').replace(',', '')
        
        #Transform into dict to perform lookups
        jsonDic = dict([tuple(x.split('=')) for x in junkJsonString.split()])
        
        #Get into an xml structure to prepare elements, and adding to dictionary
        xmlNodeDic["Environment"] = "<Environment Val=\"" + jsonDic["Environment"] + "\"/>"
        xmlNodeDic["Parent_UUID"] = "<Parent_UUID Val=\"" + jsonDic["Parent_UUID"] + "\"/>"
        xmlNodeDic["Role"] = "<Role Val=\"" + jsonDic["Role"] + "\"/>"
        xmlNodeDic["Server"] = "<Server Val=\"" + jsonDic["Server"] + "\"/>"
        xmlNodeDic["UUID"] = "<UUID Val=\"" + jsonDic["UUID"] + "\"/>"
        xmlNodeDic["MessageType"]="<MessageType Val=\"" + messageType + "\"/>"
        
        #Split on the first ']' to get the date
        secondStringSplit = firstStringSplit[1].split(']')
        
        #Replace the leading '['' with '' to get the exact date part
        xmlNodeDic["Date"] = "<Date Val=\"" + secondStringSplit[0].replace('[', '').strip() + "\"/>"
        
        #Take the second part after the  - as we don't need the items in white space after date.This needs some quotes removed
        thirdStringSplit = secondStringSplit[1].split(' - ')[1].split('Xml', 1)[0].replace('""', '').split('" ')[0] + '"'
        headerDic = dict([tuple(thirdStringSplit.split('='))])
        xmlNodeDic["Message"] = "<Message Val=\"" + headerDic['Message'].replace('"', '') + "\"/>"
        xmlFieldsAddition = ""
        if messageType == "response":
            #FindProductRespones name = ""xxx"" and store that, the just take everything inside cdata
            productResponseNameKVP = secondStringSplit[1]
            
            #Lots of string manipulation to find the one value from the xml header
            productResponseName = productResponseNameKVP[productResponseNameKVP.find('ProductResponse name'):productResponseNameKVP.find('ProductResponse name')+100:1].split('>')[0].split('=')[1].replace('""', '').replace('"','')
            xmlNodeDic["ProductResponse"] = "<ProductResponse  Val=\"" + productResponseName + "\"/>"
            
            #Updating the dictionary part in xml message
            xmlFieldsAddition = xmlFieldsAddition + xmlNodeDic["ProductResponse"]
        
        #Constructing the xml message with all attribures
        finalXml = "<OuterXML>" + xmlNodeDic["Environment"] + xmlNodeDic["MessageType"] +xmlNodeDic["Parent_UUID"] + xmlNodeDic["Role"] +  xmlNodeDic["UUID"] + xmlNodeDic["Date"] + xmlNodeDic["Message"] + xmlFieldsAddition + mainXML + "</OuterXML>" 
        
    except:          
        finalXml = "Exception Occured"
       
    return finalXml

# COMMAND ----------

# DBTITLE 1,Helper functions for XML Parser
#Definition of helper function for nestedXmlParser function to findout whether the element is array or not
def isArray(lookupItem, dictionary) :
    #In lookupItem Item[0] is the array type of the dictionary it will give boolean value
    return dictionary.get(lookupItem, "False")[0]

#Definition of helper Function for nestedXmlParser function to get the data types from dictionary schema
def getType(lookupItem, dictionary) :
    #In lookupItem Item[1] is the data type of the dictionary
    return dictionary.get(lookupItem, "StringType")[1]

#Definition of helper Function for nestedXmlParser function to cast the values as per Target type in schema dictionary
def typeCast(item,targetType):
  
    #Verify item datatype from the targetType derived from schame dictionary and cast the value with respected datatype
    if (targetType=="IntegerType"):  
        return int(item)
    if (targetType=="FloatType"):  
        return float(item)   
    if (targetType=="StringType"):
        return str(item)
    else :
        return str(item)
            
            
#Definition of helper Function for nestedXmlParser function to get the attribute values            
def getAttributes(attrDic,element):
    #Element.items() Returns the element attributes as a sequence of (name, value) 
    #Verify each attribute if attribute name is not Val then update the dictionary with its value
    for item in element.items():
        if [item][0]!="Val":
          attrDic.update({item[0]:item[1]})
    return attrDic
  

#Definition of recursive function to get the dictionary from the xml
def nestedXmlParser(root,dic,schemaDictList):
    #Description:
        #The Logic is to create a nested dictionary based on the xml Etree root passed to the function.
        #the simpel example would be 
        # <outerxml>
        # <a>
        #   <b name="b1">
        #       <c val=1/>
        #    </b>
        #     <b name="b1">
        #       <c val=2/>
        #    </b>
        # </a>
        # </outerxml> will give the result of nested dictionary like below
        # {outerxml:
        #   {a:
        #       {b:[{name:'b1' c:1}
        #           ,{name:'b2' c:2}]
        #          }
        #   }
        # } 
        #For this we take the help of the schemaDictList{a: [False, 'StructType()'],b:[True,'StructType()'],c:[False,'int'],name:[False,'str'],'OuterXML': [False, 'StructType()']}
        # the attributes will be add as children for the element


    #Major Steps:
        #1.First step is to update the dictironary with root element attributes by calling the getAtrributes function
        #2.For every children of the root element passed check whether children is structType or not and array or not
            #2.1 If it is not struct,update the dictironary with the attributes and val(if it is array update with the list[])

            #2.2 If it is struct call the function recursively with the updated dictionary and children as root element(if it is array call the function for each occurence)


    # Elemente Tree library methods used
        #   1. for element in root (way to iterate through children of root)
        #   2. element.tag will give the name of element in string
        #   3. element.attrib gives the list of attributes
        #   4. value of the attribute can be get using element.attrib[attribute_name gets from above function]
        #   5. root.findall(rootItem.tag) will give all the tag elements in the root as etree element which can be used as root for next iteration.


    #Defining the temporary lists to store temporary results 
    tempStructList = [] #List to store the struct objects
    tempArrayStructList = [] #List to store the array struct objects
    tempDic = {}  #Dictionary to use in loop array and struct.later this will be passed to the function in place of dictionary
    
    #First step is to updated the dictironary with root element attributes by calling the getAtrributes function
    tempDic = getAttributes(tempDic,root) 
     
    #Check whether root element struct type or not,the recursion function will execute until the node is not struct
    if getType(root.tag, schemaDictList)=="StructType()":
      #For every children in the root.
      for rootItem in root :
          #For every element\node check two properties, is it array or not and struct type
          #Code Block-1
          if getType(rootItem.tag, schemaDictList)!="StructType()":   
                #Code Block -1.1
                if isArray(rootItem.tag, schemaDictList):
                    tempNestedList = []
                    #If array get the elements and add it to the list
                    for tagItem in root.findall(rootItem.tag):
                        if len(tagItem.attrib) != 0:
                            
                            for att in tagItem.attrib: #Get all the attributes list
                                if att == "Val": #If the attribute is 'Val' append the values to the list with tagItem name
                                    tempNestedList.append(typeCast(tagItem.attrib[att],getType(tagItem.tag, schemaDictList)))
                                else: #If the attribute is not 'Val' append the values to the list with att name
                                    tempNestedList.append({att:typeCast(tagItem.attrib[att],getType(tagItem.tag, schemaDictList))})
                                            
                    tempDic.update({rootItem.tag:tempNestedList}) # update the dictionary with list 
                #Code Block -1.1
                else:
                    if len(rootItem.attrib)!= 0: # If it is not array verify if the lenght of attributes in items
                    
                        for att in  rootItem.attrib: # Loop over all attributes
                            if att == 'Val': #If the attribute is 'Val' append the values to the list with tagItem name                    
                                tempDic.update({ rootItem.tag:typeCast(rootItem.attrib[att],getType(rootItem.tag, schemaDictList))})
                            else: #If the attribute is not 'Val' append the values to the list with att name
                                tempDic.update({rootItem.tag:{att:typeCast(rootItem.attrib[att],getType(rootItem.tag, schemaDictList))}})
          
          
          #Code Block -2  
          else:  #Node have a inner hierarchy
                if isArray(rootItem.tag, schemaDictList): #If array add it to the arraylist
                    tempArrayStructList.append(rootItem)
                else: #Separate list to have the non array nodes with children
                    tempStructList.append(rootItem) 

    #Code Block - 2.1
    #If non array list exists loop through the elements and call the nestedXmlParser function recursively until depth of the xml node is not structtype)
    if tempStructList:

      for structItem in tempStructList: 
          dic.update({root.tag:nestedXmlParser(structItem,tempDic,schemaDictList)})
    
    #Code Block - 2.2
    if tempArrayStructList: #Loop through the array list nodes and call the function recursively and add it to the list
       
         for arrayItem in tempArrayStructList:
            nestedArrayList=[]            
            for tagItem in root.findall(arrayItem.tag): #Get all nested elements of the same name
                tempNestedDic={}                 
                nestedArrayList.append(nestedXmlParser(tagItem,tempNestedDic,schemaDictList))
            tempDic.update({arrayItem.tag:nestedArrayList})
                

    if isArray(root.tag,schemaDictList): # If the root is not array return the temp dictionary updated with the netstedlist above
        return tempDic
    else:
        dic.update({root.tag:tempDic}) #If not array update dictionary and return
        return dic     
  

# COMMAND ----------

# DBTITLE 1,Parent UUID function
#Definition of function to get parentUUID
def getParentUUID(message):
  try: 
    
    if '&gt;' in message: #If message contains '&gt;' replace some values with default values to get proper xml record
      message = message.replace(' /&gt;', '/>').replace('&lt;', '<').replace('&gt;', '>')   
    else:
      message = message      
    returnMsg = message.split('Parent_UUID=')[1].split(', ', 1)[0] #To get the exact output split the message 
  except:    
    returnMsg = "Exception Getting UUID" #Else return the default message
    
  return returnMsg

# COMMAND ----------

# DBTITLE 1,unstructuredxmlSource XML parser function
#Create dataframe on unstructuredxmlSource schema definition
unstructuredxmlSourceSchemaDefinitionDF=spark.read.load("/mnt/staging/reference/unstructuredxmlSource/unstructuredxmlSourceShema_Def", format="csv",header=True)
#Get XML_schema_dic from dataframe
unstructuredxmlSourceSchemaDefinition=unstructuredxmlSourceSchemaDefinitionDF.selectExpr('XML_Schema_Dic').collect() 
unstructuredxmlSourceXMLSchemaDic=eval(unstructuredxmlSourceSchemaDefinition[0][0])

#Definition of function to parse XML file to get dictionary
def parseXML(message): 
  
  try:
    #Parse message to function to get xml data
    XMLMessage=extractXML(message)
    
    #If function return value is not a error message 
    if XMLMessage!="Exception Occured":
        #Get root tag from xml by using elementary tree xml method
        rootXML = ET.fromstring(XMLMessage) # create a root from the message
        #Create empty dictionary to collect the result to return the result
        dicXML={}
        #Parse nested xml to recursive function to get in dictionary format
        dicXML = nestedXmlParser(rootXML,dicXML,unstructuredxmlSourceXMLSchemaDic) #Calling the function
        
    else: #Capture messagetype as string parsing to extract function exception and return the dictionary with parent_UUID value
        dicXML = {'OuterXML': {'Parent_UUID':GetParentUUID(message),'MessageType':'String Parser Exception'}} 
        
  except: #Capture messagetype as Not Well Formed XML Exception and return the dictionary with parent_UUID value
    dicXML = {'OuterXML': {'Parent_UUID':GetParentUUID(message),'MessageType':'Not Well Formed XML Exception'}}
    
  return dicXML