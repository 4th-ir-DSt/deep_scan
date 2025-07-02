# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>specific_notebook_unstructuredxmlSourcefunction</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Standard Function for unstructuredxmlSourcenotebook </td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2019/01/01</td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC ## Notebook Parameters
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Parameter Name</th>
# MAGIC     <th>Parameter Description</th>
# MAGIC     <th>Example Parameter</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td></td>
# MAGIC     <td></td>
# MAGIC     <td></td>
# MAGIC   </tr>
# MAGIC </table>
# MAGIC 
# MAGIC ## Change Log
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Date</th>
# MAGIC     <th>Changed By</th>
# MAGIC     <th>Change Description</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2019/01/01</td>
# MAGIC     <td>Framework</td>
# MAGIC     <td>Added unstructuredxmlSource Target Write Function
# MAGIC         <br>Removed the unstructuredxmlSource source select Function as it moved to the standard source select
# MAGIC         <br>Added functions for struct generation
# MAGIC         <br>dded functions for reference tables
# MAGIC         <br>Added filters to skip polData in extractXML function and created new function to get polData message
# MAGIC         <br>updated variable names to camel case
# MAGIC         <br>updated lookup functions string and message parsing functions
# MAGIC         <br>Changed request type message extractor function logic
# MAGIC         <br>updated name of lookupUdfunstructuredxmlSourceIntermediayRef to lookupUdfunstructuredxmlSourceIntermediaryRef
# MAGIC         <br>typo correction fpr  unstructuredxmlSource schemref dictionary
# MAGIC         <br>updated schema of lookup functions with the new filed 3rdPartyChannel changes
# MAGIC         <br>updated string parsing function to include server and message type to original type,QuoteDate to Date
# MAGIC         <br>Added date and UUID validation in extractXML function<
# MAGIC         <br>Bug fix - validating xml in extractXML function before filtering the PolData
# MAGIC         <br>Bug fix - Get product name for request type as productRequest name
# MAGIC         <br>removed additional space form between productResonse/ProductRequest and Name in the string parsing function "ProductResponse Name=" "ProductRequest Name"
# MAGIC         <br>Changed extractXML function to allow single quotes
# MAGIC         <br>Changes to Extract XML function to include the historical messages without Parent UUID
# MAGIC         <br>Added getunstructuredxmlSourceSource, getMessageHeaders and formatDate functions. Modified the validateDate function.
# MAGIC         <br>Proteus Feed Integration Changes</br>
# MAGIC             1. Added Functions cleanpoldatafunction,getProductName.</br>
# MAGIC             2. Updated getMessage Headers to use the validate date function.</br>
# MAGIC             3. Updated extract XML to use above functions</br></td>
# MAGIC   </tr>  
# MAGIC </table>
# MAGIC   
# MAGIC ## General Standards applying to each notebook
# MAGIC <ol>
# MAGIC   <li>Above information completed</li>
# MAGIC   <li>Deployed to approriate workspace folder path</li>
# MAGIC   <li>Commited to source control</li>
# MAGIC   <li>Code tested</li>
# MAGIC   <li>Only include libraries and imports that are being used - i.e. don't include something if it is not used</li>
# MAGIC   <li>Code commented</li>
# MAGIC   <li>Code Peer reviewed</li>
# MAGIC </ol>

# COMMAND ----------

# DBTITLE 1,Helper function to extractXML function
# When we have multiple elements with same name. This function assumes only one element contains search string
# Eg: String with xml
# 		<startTag>
# 		<randomdata1>
# 		</CloseTag>

# 		<startTag>
# 		<randomdata2>
# 		</CloseTag>

# 		<startTag>
# 		<some data1>
# 		<Searchstring>
# 		<some data2>
# 		</CloseTag>

# 		<startTag>
# 		</CloseTag>


# Our main aim is to get the below as output.

# 		<startTag>
# 		<some data1>
# 		<Searchstring>
# 		<some data2>
# 		</CloseTag>
#The function will do the below steps
# Step 1- Split the string on search stirng.
# List will have elements data before search string and after search string.
# [	{

# 		<startTag>
# 		[<randomdata1>
# 		</CloseTag>
		
# 		<startTag>
# 		<randomdata2>
# 		</CloseTag>
# 		<startTag>
# 		<somedata1>
# 	},

# 	{
# 		<somedata2>
# 		</CloseTag>
# 		<startTag>

# 		</CloseTag>
# 	}
# ]


# Step -2 
# Take before search string(first element in above list) ,split it on start Tag and take the last element 
#List after split
# [
# 	{<randomdata1>
# 	</CloseTag>}
# 	,
# 	{<randomdata2>
# 	</CloseTag>}
# 	,
# 	<somedata1>
# ] 
# Take the last element in the list which is <somedata1>

# Step -3
# Do the same thing with the element after the search string on close Tag . Take the first element which is <somedata2>

# [
# 		{<somedata2>}
# 		,
# 		{<startTag>}

# ]

# Step -4
#  wrap it around start ,search string and end Tags 
# <startTag>+<somedata1>+<searchstring>+<somedata2>+</CloseTag>

def getDataString(xmlMessage,searchString,startTag,closeTag):
  #Split the message based on search string to get PolData total message
  xmlMsgList = xmlMessage.split(searchString) 
  #Construct total PolData message
  #To get the string from startTag to searchString:
    # step -1 : take the xmlMsgList[0], it will contain all message before searchString
    # step -2 : split the xmlMsgList[0] by startTag to get list of strings
    # step -3 : select list[-1] to get last string from the list 
    # step -4 : add the string between startTag and searchString to get total message from startTag to searchString
  #To get the string from searchString to endTag:
    # step -1 : take the xmlMsgList[1], it will contain all message after searchString
    # step -2 : split the xmlMsgList[1] by closeTag to get list of strings
    # step -3 : select list[0] to get first string from the list 
    # step -4 : add the string between searchString and closeTag to get total message from searchString to closeTag
  dataMessage = (startTag
                 + xmlMsgList[0].split(startTag)[-1]
                 + searchString
                 + xmlMsgList[1].split(closeTag)[0]
                 + closeTag)
  
  return dataMessage

#Helper function for extractXML to validate the date. If date format matches it will return True else it will return False
def validateDate(date):
  try:
    datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
    #If it not throws any value error then return True
    isValidDate = True
    
  except ValueError:
    #If it throws any value error return False
    isValidDate = False
    
  return isValidDate

#Helper function to ensure dates are formatted consistently. If unstructuredxmlSourceSource determined to be lunar, then date modified to match proteus message format.
def formatDate(unstructuredxmlSourceSource, dateString):

  try:
    if unstructuredxmlSourceSource == 'lunar':
      formattedDate = dateString.replace(",", '.') + str('Z')
#       formattedDate = to_date(formattedDate, '')

    elif unstructuredxmlSourceSource == 'proteus':
      formattedDate = dateString

  except:
    formattedDate = "Exception Occured"
  
  return formattedDate

#Helper function for extractXML to validate the UUID.If UUID format matches it will return True else it will return False
def validateUUID(UUID):
  try:
    #Compare the UUID with pattern.If mtches then return True
    if bool(re.match(r'([A-Z]{2}\-[a-z0-9]{8}\-[a-z0-9]{4}\-[a-z0-9]{4}\-[a-z0-9]{4}\-[a-z0-9]{12})', UUID)) == True:    
      isValidUUID = True
    else:
      #If pattern not matches then return False
      isValidUUID = False
    
  except Exception as e:
    #If not able to compare the UUID, then return value False
    isValidUUID = False
    
  return isValidUUID 

# COMMAND ----------

# DBTITLE 1,Function to get unstructuredxmlSource source
# function to determine the unstructuredxmlSource source of message by message format
def getunstructuredxmlSourceSource(message):

  try:
    # initialise variables
    unstructuredxmlSourceSource = ''

    # if json format, then proteus else lunar
    jsonMessage = json.loads(message)
    mainMessage = jsonMessage
    unstructuredxmlSourceSource = 'proteus'
  except:
    mainMessage = message
    unstructuredxmlSourceSource = 'lunar'

  return unstructuredxmlSourceSource, mainMessage

# COMMAND ----------

# DBTITLE 1,Function to get message headers
def getMessageHeaders(unstructuredxmlSourceSource,originalType,jsonDic,secondStringSplit):

  try:
    # initialise dictionary
    xmlNodeDic = {}
     #Validate UUID by calling helper function validateUUID
    xmlNodeDic["UUID"] = "<UUID Val=\"" + jsonDic["UUID"] + "\"/>" if  validateUUID(jsonDic["UUID"]) == True else "<UUID Val=\"" + jsonDic["UUID"] + "\"/"
    xmlNodeDic["Parent_UUID"] = "<Parent_UUID Val=\"" + jsonDic["Parent_UUID"] + "\"/>" if "Parent_UUID" in jsonDic else xmlNodeDic["UUID"].replace("UUID", "Parent_UUID")
    xmlNodeDic["MessageType"]="<MessageType Val=\"" + originalType + "\"/>"
    if unstructuredxmlSourceSource == 'proteus':
      # the below elements are not supplied in proteus message
      xmlNodeDic["Environment"] = "<Environment Val=\"ns\"/>"
      xmlNodeDic["Role"] = "<Role Val=\"ns\"/>"
      xmlNodeDic["Server"] = "<Server Val=\"ns\"/>"
      formatedDate = jsonDic['@timestamp']
    elif unstructuredxmlSourceSource == 'lunar':
      xmlNodeDic["Environment"] = "<Environment Val=\"" + jsonDic["Environment"] + "\"/>"
      xmlNodeDic["Role"] = "<Role Val=\"" + jsonDic["Role"] + "\"/>"
      xmlNodeDic["Server"] = "<Server Val=\"" + jsonDic["Server"] + "\"/>"
      formatedDate=formatDate(unstructuredxmlSourceSource,secondStringSplit[0].replace('[', '').strip())
#       print(formatedDate)
    xmlNodeDic["Date"] = "<Date Val=\"" + formatedDate + "\"/>" if validateDate(formatedDate) == True else "<Date Val=\"" +formatedDate + "\"/" 
#       print(secondStringSplit)

  except Exception as e:
    xmlNodeDic = {'Error':'Failed to get message headers'}
    errorMessage = "Failed to get message headers: " + str(e)
#     print(errorMessage)
#     print(secondStringSplit)
  return xmlNodeDic  
  

# COMMAND ----------

# DBTITLE 1,Function to Get the product name
def getProductName(unstructuredxmlSourceSource,messageType,searchString):
    try:    
# Define the dictionary based on message type  what attributes to look for and to which node in dictionary need to be added
      atrributeToFindDic={"request_type1":("ProductRequest","Product name" )
                          ,"request_type2":("ProductRequest","Product name")
                          ,"response":("ProductResponse","ProductResponse name")}
      xmlNodeDic={}
      if messageType == "response":
        splitElement='>' 
      else:
        splitElement='/>'
    #The split element will use the different element based on the feed type
#       splitElement='/>' if feedFlag else 
      # Get the attribute and node to add
      atrributeNode=atrributeToFindDic[messageType][0]
      atrributeToFind=atrributeToFindDic[messageType][1]
       # Find the required attrbutes in search string
      productName = (searchString[searchString.find(atrributeToFind)
                                                      :searchString.find(atrributeToFind)+100
                                                      :1]
                                .split(splitElement)[0]
                                .split('=')[1]
                                .replace('""', '')
                                .replace('"','')
                                .rstrip(' '))
      #Add the xml node to dictrioary
#       print(productName)
      xmlNodeDic["Product"] = "<"+atrributeNode+" Name=\"" + productName + "\"/>" 
        
    except Exception as e:          
      xmlNodeDic = "Exception Occured"
            
    return xmlNodeDic

# COMMAND ----------

# DBTITLE 1,Function To clean poldata elements
def cleanpoldatafunction(messageType,finalXML):
            
        try:
          #Verify is finalXML is a valid xml by checking ET.fromstring.If it succesfully runs it is a valid xml else it will raise ParseError
          ET.fromstring(finalXML)        

          #If the xml message is request type
          if messageType == "request_type1" or messageType == "request_type2":
              #To accept string if Type = 'Request' in single quotes converting to double quotes
              finalXML = finalXML.replace("Type='Request'",'Type="Request"')  
              #If the xml message contains 'Type="Request"' skip entire PolData node
              #Search the string in total message if string matches in which polData it has to skip 
              if 'Type="Request"' in finalXML:
                #Get everything between '<polData' and 'Type="Request"'  where Type="Request" 
                xmlMsgFirstString = finalXML.split('Type="Request"')[0].split('<PolData')[-1]
                #Get everything after 'Type="Request"'
                xmlMsgSecondString = finalXML.split('Type="Request"')[1]
                #Split the message with "/>" to verify string format
                xmlMsgSubList = xmlMsgSecondString.split("/>")
                #If length of substring >0 and there is no '<' then take last string as xmlMsgSubList first element else split by "</PolData>" and take first element from the list
                lastString = xmlMsgSubList[0] + "/>" if len(xmlMsgSubList) > 0 and '<' not in xmlMsgSubList[0] else xmlMsgSecondString.split("</PolData>")[0] + "</PolData>"
                #Construct PolData message to skip for "Request" type
                polDataMessage = '<PolData' + xmlMsgFirstString + 'Type="Request"' + lastString
                finalXML =  finalXML.replace(polDataMessage , "")    


              #If the xml message contains '<ProcessingIndicators_ProcessType Val="04"/>' skip entire PolData node
              # sample input : <polMessage> <polData Type ="input"> <aTag Val = 'x'/> <bTag Val = 'y'/>                                                                <ProcessingIndicators_ProcessType Val="04"/>' </polData> <cTag = 'z'/> </polMessage>
              # sample output : <polMessage> <cTag = 'z'/> </polMessage>
              attributesToFind=['<ProcessingIndicators_ProcessType Val="04"/>','<ProcessingIndicators_ProcessType Val="37"/>']
              
              attributeFound= [attribute for attribute in attributesToFind if attribute in finalXML]
              if len(attributeFound)>0:
                polDataMessage = getDataString(finalXML,attributeFound[0],'<PolData','</PolData>')
                #Replace the total PolData for '<ProcessingIndicators_ProcessType Val="04"/>' with empty to get required output
                finalXML = finalXML.replace(polDataMessage,'')

          #If the xml message is response type
          #If <SchemeResult in between <PolMessage and <PolData then consider <PolData is an element of <SchemeResult
          # sample input : <polMessage> <SchemeResult> <polData Type ="input"> <aTag Val = 'x'/> <bTag Val = 'y'/>                                                  </polData> <cTag = 'z'></SchemeResult> </polMessage>
          # sample output : <polMessage><polData Type ="input"> <aTag Val = 'x'/> <bTag Val = 'y'/> </polData>                                                       <SchemeResult> <cTag = 'z'></SchemeResult> </polMessage>
          if messageType == 'response' and '<SchemeResult' in finalXML[finalXML.index('<PolMessage'):finalXML.index('<PolData')]:

                #To make <PolData is a sibling of <SchemeResult
                #Get starting index value of polData
                getPolDataStartPosition = finalXML.index('<PolData')
                #Get end index value of polData
                getPolDataEndPosition = finalXML.index('</PolData>')+len('</PolData>')
                #Get the total PolData message
                polDataMessage = finalXML[getPolDataStartPosition:getPolDataEndPosition]
                #Replace PolData message with empty to add in SchemeResult sibling position
                finalXML = finalXML.replace(polDataMessage,'')
                #Get the SchemeResult starting position
                getSchemeResultIndex = finalXML.index('<SchemeResult')

                #Add PolData message before <SchemeResult to make Poldata sibling of SchemeResult
                finalXML = finalXML[:getSchemeResultIndex] + polDataMessage + finalXML[getSchemeResultIndex:]
        except ParseError:
          finalXML = "Exception Occured"
        return finalXML

# COMMAND ----------

# DBTITLE 1,Extract XML from string message
def extractXML(message):
    try:    
          
        if '&gt;' in message:
            #If message contains &gt replace below specified values in message with default values
            message = message.replace(' /&gt;', '/>').replace('&lt;', '<').replace('&gt;', '>')    
        
        else:
            #Else nothing has to replace
            message = message              

        unstructuredxmlSourceSource, mainMessage=getunstructuredxmlSourceSource(message)  

        if unstructuredxmlSourceSource=='proteus':
          internalMessage=mainMessage['message']
        elif unstructuredxmlSourceSource=='lunar':
          internalMessage=mainMessage

        # print(unstructuredxmlSourceSource, mainMessage)
                #Initializing empty dictionary to store elements  
        xmlNodeDic = {}
#         print('step3')
        if "Quotes Hub request" in internalMessage and "![CDATA[" not in internalMessage:
            #Finding the message type by using keywards in the message
            messageType = "request_type1"
            orginalType="Request"
            #Split the message to get the xml part 
            headerXMLSplit = internalMessage.split('</xrt:RequestXML>')[0].split('<xrt:RequestXML>', 1)

        elif "Quotes Hub request" in internalMessage and "![CDATA["  in internalMessage:
            #Finding the message type by using keywards in the message
            messageType = "request_type2"
            orginalType="Request"
            #Split the message to get the xml part
            headerXMLSplit = internalMessage.split(']]')[0].split('<![CDATA[', 1)

        else:
            messageType = "response"
            orginalType="Response"
            if "![CDATA["  in internalMessage:
                headerXMLSplit = internalMessage.split(']]')[0].split('<![CDATA[', 1)

            else:
                headerXMLSplit = internalMessage.split('</a:ResponseXML>')[0].split('<a:ResponseXML xmlns:a="http://schemas.datacontract.org/2004/07/XRTEService">', 1)
#           print(messageType)

#         # print(headerXMLSplit[0])    

        firstStringSplit = headerXMLSplit[0].split('}')
        junkJsonString = firstStringSplit[0].replace("{", '').replace(',', '')
        mainXML = headerXMLSplit[1].replace('<?xml version=""1.0"" ?>', '').replace('<?xml version="1.0" ?>','')
        if unstructuredxmlSourceSource=='proteus':
          secondStringSplit=''
          headerDic=dict([tuple(re.split('ServiceMapping=',internalMessage[internalMessage.find('Message'):internalMessage.find('Message')+100])[0].strip().split("="))]) 
        elif unstructuredxmlSourceSource=='lunar': 
          secondStringSplit = firstStringSplit[1].split(']')
          thirdStringSplit = secondStringSplit[1].split(' - ')[1].split('Xml', 1)[0].replace('""', '').split('" ')[0] + '"'
          headerDic = dict([tuple(thirdStringSplit.split('='))])


        if unstructuredxmlSourceSource=='proteus':
          jsonDic=mainMessage
        elif unstructuredxmlSourceSource=='lunar':
        #Transform into dict to perform lookups
          jsonDic = dict([tuple(x.split('=')) for x in junkJsonString.split()])

        xmlNodeDic=getMessageHeaders(unstructuredxmlSourceSource, orginalType,jsonDic,secondStringSplit)   
#         print(xmlNodeDic)

        productDictionary=getProductName(unstructuredxmlSourceSource,messageType,headerXMLSplit[0])

#         print(productDictionary)
        xmlNodeDic.update(productDictionary)

#         print(xmlNodeDic)


        xmlNodeDic["Message"] = "<Message Val=\"" + headerDic['Message'].replace('"', '') + "\"/>"
#         print(xmlNodeDic)
        finalXML =( "<OuterXML>"
                   +xmlNodeDic["Environment"] 
                   +xmlNodeDic["Server"]
                   +xmlNodeDic["MessageType"] 
                   +xmlNodeDic["Parent_UUID"] 
                   +xmlNodeDic["Role"] 
                   +xmlNodeDic["UUID"] 
                   +xmlNodeDic["Date"] 
                   +xmlNodeDic["Product"]
                   +xmlNodeDic["Message"] 
        #            +xmlFieldsAddition 
                   +mainXML
                   +"</OuterXML>" )
        xmlMessage=cleanpoldatafunction(messageType,finalXML) 
#         print('step5')
    except Exception as e:          
        xmlMessage = "Exception Occured"
#         print(e)
    return xmlMessage

# COMMAND ----------

# DBTITLE 1,Helper functions for XML Parser
#Definition of helper function for nestedXmlParser function to findout whether the element is array or not
def isArray(lookupItem,dictionary) :
    #In lookupItem Item[0] is the array type of the dictionary it will give boolean value
    return dictionary.get(lookupItem, "False")[0]

#Definition of helper Function for nestedXmlParser function to get the data types from dictionary schema
def getType(lookupItem, dictionary) :
    #In lookupItem Item[1] is the data type of the dictionary
    return dictionary.get(lookupItem, "StringType")[1]

#Definition of helper Function for nestedXmlParser function to cast the values as per Target type in schema dictionary
def typeCast(item,targetType):
  
    #Verify item datatype from the targetType derived from schame dictionary and cast the value with respected datatype
    if (targetType=="StringType"):
        return str(item)
    if (targetType=="IntegerType"):  
        return int(item)
    if (targetType=="FloatType"):  
        return float(item)   
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


# COMMAND ----------

def nestedXMLParser(root):
  
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
        #1.First step is to create an empty dictionary with root element  and get the list of children of root 
        #2.Create the initial dictionary which is default value type to list.
        #3. Get the list of attributes of node and update that to main dictionary
        #4. Apply the map with same function to all the childeren which will return the dictionaries  for all the children elements
        #5.loop thorugh all the above dictionaries and update the initial dictionary with key and attributes of element dictionary
        #6. Update main dictionary with the initial dictionary if the element is not array take the first element in values  else update with list.
        #2.For every children of the root element passed check whether children is structType or not and array or not

    #Note: There is a global dictionary unstructuredxmlSourceXMLSchemaDic is used wihtin this function which no need to pass it as parameter
 
    dic = {root.tag: {} if 'Val' not in root.attrib else None}
    childrenElements = list(root)
    if childrenElements:
        initialDic = defaultdict(list)
        for elementDic in map(nestedXMLParser, childrenElements):
            #print(dc.items())
            for key, value in elementDic.items():
                initialDic[key].append(value)
        #print(initialDic)       
        dic = {root.tag: {key:value if isArray(key,unstructuredxmlSourceXMLSchemaDic) else value[0] for key, value in initialDic.items()}}
   # print(t.attrib.items())
    if root.attrib:
      if 'Val'not in root.attrib:
          dic[root.tag].update((key, value)  for key, value in root.attrib.items())
      else:
        dic[root.tag]=root.attrib['Val']
    return dic


# COMMAND ----------

# DBTITLE 1,unstructuredxmlSource XML parser function
#Definition of function to parse XML file to get dictionary
def parseXML(xmlMessage): 
  
  try:
    #Parse message to function to get xml data
    #xmlMessage=extractXML(message)
    
    #If function return value is not a error message 
#     if xmlMessage!="Exception Occured":
        #Get root tag from xml by using elementary tree xml method
        rootXML = ET.fromstring(xmlMessage) # create a root from the message
        #Create empty dictionary to collect the result to return the result
        
        #Parse nested xml to recursive function to get in dictionary format
        dicXML = nestedXMLParser(rootXML) #Calling the function
        
#     else: #Capture messagetype as string parsing to extract function exception and return the dictionary with parent_UUID value
#         dicXML = {'OuterXML': {'Parent_UUID':getParentUUID(message),'MessageType':'String Parser Exception'}} 
        
  except: #Capture messagetype as Not Well Formed XML Exception and return the dictionary with parent_UUID value
      dicXML = {'OuterXML': {'MessageType':'Not Well Formed XML Exception'}}    
    
  return dicXML

# COMMAND ----------

# DBTITLE 1,unstructuredxmlSource message quality function
#Definition of Quality functions to find out message is properly formated or not.
def isFormatedMessage(xmlMessage):   
  try:
    #This function take the xml from the string parser function and checks whether this message is parasable from etree or not
#     xmlMessage=extractXML(message)
    rootXML = ET.fromstring(xmlMessage)
    return True      
  except: #Capture messagetype as Not Well Formed XML Exception and return the dictionary with parent_UUID value
    return False


# COMMAND ----------

# DBTITLE 1,Function get the unstructuredxmlSource Schema and Dictionary from file and register the UDF globally
def getunstructuredxmlSourceSchemaAndRegisterFunction():
  #Create dataframe on unstructuredxmlSource schema definition
  unstructuredxmlSourceSchemaDefinitionDF=spark.read.load("/mnt/raw/reference/unstructuredxmlSourceunstructuredxmlSource/unstructuredxmlSourceunstructuredxmlSourceShema_Def", format="csv",header=True)
  #Get XML_schema_dic from dataframe
  unstructuredxmlSourceSchemaDefinition=unstructuredxmlSourceSchemaDefinitionDF.selectExpr('xmlSchemaDic', 'xmlSchemaStruct').collect()
  
  #declaring as global so parse XML function can access the dictionary
  global unstructuredxmlSourceXMLSchemaDic
  
  unstructuredxmlSourceXMLSchemaDic = eval(unstructuredxmlSourceSchemaDefinition[0][0])
  unstructuredxmlSourceXMLSchemaStruct = unstructuredxmlSourceSchemaDefinition[0][1]
  
  #Declare the function as global so that it can be registered and accessed outside of the funciton
  global unstructuredxmlSourceStringParserUdf
  unstructuredxmlSourceStringParserUdf=udf(extractXML,StringType())
  global unstructuredxmlSourceMessageParserUdf
  unstructuredxmlSourceMessageParserUdf = udf(parseXML,unstructuredxmlSourceXMLSchemaStruct)#Register UDF
  global unstructuredxmlSourceFindMessageQualityUdf
  unstructuredxmlSourceFindMessageQualityUdf=udf(isFormatedMessage,BooleanType())
  

# COMMAND ----------

# DBTITLE 1,Function StructDictionary - to get attribute type
# DataType_Dic = {"STRING":"StringType()","INT":"IntegerType()"}

# Path_List sample ['PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs]

#MetaData_Dic sample {'CalculatedResult_CompulsoryXs': ['PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs', 'STRING']}

#Element_List Sample  ['PolMessage', ]

# Helper Function to Get the Type of the element. If an element has a childs(.i.e a.b.c.d  a,b,c reported as struct) in any of the path retrun as StructuType else return the type from above created dictionary
def findElementType( element
                      ,pathList
                      ,metaDataDic
                      ,dataTypeDic
                      ,cursor
                      ,batchTaskId
                      ,adfPipelineName
                      ,clusterId
                      ,notebookName
                      ,errorLogFileLocation):

    try:

        #Get the count of an item.  presen in the path
        Count  =  0
        for pathListItem in pathList:

            if element+'.' in pathListItem or element+'].' in pathListItem:
                Count = Count+1

        if Count > 0 :
        #Place Holder for Attribute Type    
        #     if MetaData_Dic.get(Element,'T')[1]:
        #       return MetaData_Dic.get(Element,"Test")[1]
        #     else:
            return "StructType()"

        else :
         #Get the type from the dictionary (changed defaults to Stirng Type if not found)
            return "StringType()"

    except Exception as e:
        errorMessage="Unable to FindElementType: " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        assert False

# COMMAND ----------

# DBTITLE 1,Function StructDictionary - to find attribute array or not
#Helper Function to find an element is an array or not , The path will have [] for array elements 
def findElementArrayStatus(element
                             ,pathList
                             ,cursor
                             ,batchTaskId
                             ,adfPipelineName
                             ,clusterId
                             ,notebookName
                             ,errorLogFileLocation):

  try:
    arrayStatus = False
    
    for item in  pathList:
      if '['+element+']' in item:
          arrayStatus = True

    return arrayStatus

  except Exception as e:

    errorMessage="Unable to FindElementType: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function StructDictionary - to Generate attribute details dictionary
#Main function to generate the dictionary,
def generateElementDetailsDictironary(elementList
                                        ,pathList
                                        ,metaDataDic
                                        ,dataTypeDic
                                        ,cursor
                                        ,batchTaskId
                                        ,adfPipelineName
                                        ,clusterId
                                        ,notebookName
                                        ,errorLogFileLocation):

    try:
        elementDetailsDic = {}
        #   Loop through all the elements and find array or not and type of it using the above helper functions
        for elementListItem in elementList:

            elementDetailsDic.update({elementListItem:[findElementArrayStatus(elementListItem
                                                                                ,pathList
                                                                                ,cursor
                                                                                ,batchTaskId
                                                                                ,adfPipelineName
                                                                                ,clusterId
                                                                                ,notebookName
                                                                                ,errorLogFileLocation)
                                                          ,findElementType(elementListItem
                                                                             ,pathList
                                                                             ,metaDataDic
                                                                             ,dataTypeDic
                                                                             ,cursor
                                                                             ,batchTaskId
                                                                             ,adfPipelineName
                                                                             ,clusterId
                                                                             ,notebookName
                                                                             ,errorLogFileLocation)]})
            #Add the Static element outerxml as top of the item.
        elementDetailsDic.update({"OuterXML":[False,'StructType()']})  

        return elementDetailsDic
    except Exception as e:
        errorMessage="Unable to FindElementType: " + str(e)
        logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
        assert False        

# COMMAND ----------

# DBTITLE 1,Function StructDictionary - to get Initial Lists and Dictionaries for XML parsing
def getInitialListsAndDictionaries(metadataDetailsDF
                                  ,cursor
                                  ,batchTaskId
                                  ,adfPipelineName
                                  ,clusterId
                                  ,notebookName
                                  ,errorLogFileLocation):
  #The Logic is to Get the posintion of the element in for each path and get the position of the parent by -1 to index from array list.Loop it from the max position(these are simply not struct types) create struct and add it to dictionary.
  #DataType_Dic = {"STRING":"StringType()","INT":"IntegerType()"}

  #Path_List sample ['PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs]

  #MetaData_Dic sample {'CalculatedResult_CompulsoryXs': ['PolMessage.PolData.CalculatedResult.CalculatedResult_CompulsoryXs', 'STRING']}

  #Element_List Sample  ['PolMessage', ]
  try:
    #Function to prepare metadata lists and dictionaries
    #def getInitialListsAndDictionaries(unstructuredxmlSource_metadata_DetailsDF)
    #returns pathList, elementList, metadataDic, dataTypeDic and xmlSchemaDic

    #Get the Path and create the  element list from it by spliting by '.'
    pathList = list(metadataDetailsDF.select("Path").rdd.flatMap(lambda x: x).collect())

    #The dic.fromkeys used to remove duplicates
    elementList = list( dict.fromkeys(sc.parallelize(pathList).flatMap(lambda x: x.replace('[','').replace(']','').split('.')).collect()))


    ##Convert the dataframe to dictionary to use it flexible way in function rather than doing the filter everytime.
    metaDataDic = metadataDetailsDF.toPandas().set_index('Element').T.to_dict('list')

    #Static Dictionary to map the datatypes coming from metadata with pyspark sql types.
    dataTypeDic = {"STRING":"StringType()"}      
    #DataType_Dic = {"STRING":"StringType()","INT":"IntegerType()","DATE":"DateType()","DECIMAL(10,2)":"DecimalType(10,2)","DECIMAL(10,3)":"DecimalType(10,3)","DECIMAL(18,3)":"DecimalType(18,3)","DECIMAL(7,2)":"DecimalType(7,2)"}      

    #create  dictionary
    xmlSchemaDic=generateElementDetailsDictironary(elementList
                                                     ,pathList
                                                     ,metaDataDic
                                                     ,dataTypeDic
                                                     ,cursor
                                                     ,batchTaskId
                                                     ,adfPipelineName
                                                     ,clusterId
                                                     ,notebookName
                                                     ,errorLogFileLocation)

    return pathList, elementList, metaDataDic, dataTypeDic, xmlSchemaDic
  except Exception as e:
    errorMessage="Unable to create Path And Element dictionaries: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False 

# COMMAND ----------

# DBTITLE 1,Function StructDictionary - to get Number of Elements and Metadata Dictionary
def getNoElementsAndMetadataDict(metadataDetailsDF
                                 ,cursor
                                 ,batchTaskId
                                 ,adfPipelineName
                                 ,clusterId
                                 ,notebookName
                                 ,errorLogFileLocation):
  try: 
    
    splitMedataDF=(metadataDetailsDF
                   .withColumn("CleanPath"
                               ,regexp_replace(regexp_replace(col("Path"), "\\[", ""),"\\]", "")
                              )
                   .select(col("CleanPath").alias("Path")
                           ,split("CleanPath", "\\.").alias("ArrayList")
                           ,posexplode(split("CleanPath", "\\.")).alias("pos", "val"))
                  )

    #Get the parent of the element by going with pos-1
    splitMedataDF=splitMedataDF.select("Path"
                                       ,"pos"
                                       ,"val"
                                       ,coalesce(col("ArrayList")[col("pos")-1],lit("OuterXML")).alias("Parent"))

    ##display(Split_unstructuredxmlSource_medataDF)
    #Get the maximum number of times we need to loop through
    numberOfElements=splitMedataDF.select([max("pos")]).collect()[0][0]

    #Multi index dictionary to be used in the next looping based on the posintion number.Effective rather than creating the filtered databframes every time
    splitMedataDic=splitMedataDF.select("val","Parent","pos").distinct().toPandas().groupby(['pos','val'])['Parent'].apply(list).reset_index(name="Parent").set_index(['pos','val']).T.to_dict() 

    return numberOfElements, splitMedataDic

  except Exception as e:
    errorMessage="Unable to create split dataframe and  dictionary: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function StructDictionary - Get the XML Schema Struct
def getXMLSchemaStruct(splitMedataDic
                      ,xmlSchemaStruct
                      ,numberOfElements
                      ,pathList
                      ,metaDataDic
                      ,dataTypeDic
                      ,xmlSchemaStructDic
                      ,cursor
                      ,batchTaskId
                      ,adfPipelineName
                      ,clusterId
                      ,notebookName
                      ,errorLogFileLocation):
  try:
  #Start from the bottom position which has no struct elements 
    for x in range(numberOfElements, -1, -1):
      
      #Filter the above created dictionaries for each position to get element and parent dictionary and parent list
      xmlElementDic = { key[1]:value for (key,value) in splitMedataDic.items() if key[0] == x}
      
      xmlParentList=[value.get('Parent') for (key,value) in splitMedataDic.items() if key[0] == x]

      #If mulitple child elemnt have same names the above parent list creates list of lists. Flaten that list
      xmlParentList=list(dict.fromkeys(list(chain(*xmlParentList))))

      for xmlParentItem in xmlParentList:
        xmlParentStruct=StructType() # for every parent we construct sturct type and add to dictionary 
        for xmlElementDicItem in dict(filter(lambda elem: xmlParentItem in elem[1]["Parent"], xmlElementDic.items())):

          #if an element is struct get the struct form dictionary which is updated for the previous x number  and check arrya or not
          if findElementType(xmlElementDicItem
                               ,pathList
                               ,metaDataDic
                               ,dataTypeDic
                               ,cursor
                               ,batchTaskId
                               ,adfPipelineName
                               ,clusterId
                               ,notebookName
                               ,errorLogFileLocation) == "StructType()": 

            if findElementArrayStatus(xmlElementDicItem
                                        ,pathList
                                        ,cursor
                                        ,batchTaskId
                                        ,adfPipelineName
                                        ,clusterId
                                        ,notebookName
                                        ,errorLogFileLocation):
                  xmlParentStruct.add(xmlElementDicItem,ArrayType((xmlSchemaStructDic.get(xmlElementDicItem))))

            else:
                  xmlParentStruct.add(xmlElementDicItem,xmlSchemaStructDic.get(xmlElementDicItem))

          else :
            if findElementArrayStatus(xmlElementDicItem
                                        ,pathList
                                        ,cursor
                                        ,batchTaskId
                                        ,adfPipelineName
                                        ,clusterId
                                        ,notebookName
                                        ,errorLogFileLocation): 
              xmlParentStruct.add(xmlElementDicItem,ArrayType(eval(xmlSchemaDic.get(xmlElementDicItem)[1])))

            else :
              xmlParentStruct.add(xmlElementDicItem,eval(xmlSchemaDic.get(xmlElementDicItem)[1]))

        xmlSchemaStructDic.update({xmlParentItem:xmlParentStruct})

    xmlSchemaStruct=StructType()
    xmlSchemaStruct.add("OuterXML",xmlSchemaStructDic.get('OuterXML')) #wrap outer xml struct  under single  root.
      
    return xmlSchemaStruct
  except Exception as e:
    errorMessage="Unable to create structure object: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False

# COMMAND ----------

# DBTITLE 1,Function to create reference file dataframes
def createReferenceDataFrames(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation):
  try:
    #Get the Dataframe for reference tables
    unstructuredxmlSourceSchemRefDF   = spark.read.table('unstructuredxmlSourcerefdata.schemes').select('Schema_Ref','Product_Name','Product_Type','Channel','3rdPartyChannel')
    unstructuredxmlSourceIntermediayDF= spark.read.table('unstructuredxmlSourcerefdata.intermediary').select('Intermediary_Code','Description').filter(col('Description').isNotNull())
    return unstructuredxmlSourceSchemRefDF,unstructuredxmlSourceIntermediayDF
  except Exception as e:
    errorMessage="Unable Create reference table dataframes: " + str(e)
    logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    assert False   

# COMMAND ----------

# DBTITLE 1,Function lookup unstructuredxmlSourceSchemRef and udf registration
#specifically no try except as this is being called while computing for a dataframe.  The caller will handle the error.
def getLookupunstructuredxmlSourceSchemeRef(element):
  #These functions are specific and expected to have the broadcast variables already created with same name. We can't pass the broad cast variables in dataframe
  return unstructuredxmlSourceSchemeRefDicBcst.value.get(element,unstructuredxmlSourceSchemeRefDicBcst.value.get('¦default¬'))


# COMMAND ----------

# DBTITLE 1,Function lookup unstructuredxmlSourceIntermediayRef and udf registration
#specifically no try except as this is being called while computing for a dataframe.  The caller will handle the error.
def getLookupunstructuredxmlSourceIntermediaryRef(element):
  #These functions are specific and expected to have the broadcast variables already created with same name. We can't pass the broad cast variables in dataframe
  return unstructuredxmlSourceIntermediaryDicBcst.value.get(element,unstructuredxmlSourceIntermediaryDicBcst.value.get('¦default¬'))

# COMMAND ----------

# DBTITLE 1,Lookup functions udf Registration
def getLookupunstructuredxmlSourceRegisterFunctions():
  
  #Declare the schema and function as global so that it can be registered and accessed outside of the funciton
  global unstructuredxmlSourceSchemeRefSchema
  unstructuredxmlSourceSchemeRefSchema=StructType([StructField('Product_Name',StringType(),True)
                                ,StructField('Channel',StringType(),True)
                                ,StructField('Product_Type',StringType(),True)
                                ,StructField('3rdPartyChannel',StringType(),True)
                                 
                                ])
  global lookupUdfunstructuredxmlSourceSchemeRef
  global lookupUdfunstructuredxmlSourceIntermediaryRef
  #register lookup functions as udf
  lookupUdfunstructuredxmlSourceSchemeRef=udf(getLookupunstructuredxmlSourceSchemeRef,unstructuredxmlSourceSchemeRefSchema)  
  lookupUdfunstructuredxmlSourceIntermediaryRef=udf(getLookupunstructuredxmlSourceIntermediaryRef,StringType())