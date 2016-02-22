###
#AUTHOR:
# Nathaniel Watson
#DATE:
# April 10, 2015
###

import json
import syapse_client
import sys
import os

scriptDir = os.path.dirname(__file__)
confFilePath = os.path.join(scriptDir,"conf_syapse_connection.json")
syapseConnConfFile = open(confFilePath,'r')
scc = json.load(syapseConnConfFile) #syapse connection conf


def getConnectionHostURL(conn):
	"""
	Function :
	Args     :
	"""
	session = conn._session
	host = session._host
	return host

class Syapse:

	apiTokens = scc["apiTokens"]
	knownModes = scc["modes"]

	hostUrls = scc["hostUrls"]

	def __init__(self,mode):
		"""
		Args : mode - A string indicating which Syapse Host to use. Must be one of elemensts given in self.knownModes.
		"""
		if mode  not in self.knownModes:
			raise KeyError("'Host' argument must be one of {modes}.".format(modes=list(self.knownModes.keys())))
		self.mode = mode 
		self.host = self.getHostURL()
		self.token = self.getApiToken()

	def connect(self):
		"""
		Function : Connects to the syapse host
		Returns  : A 
		"""
		conn = syapse_client.SyapseConnection(host=self.host,token=self.token)
		return conn

	def getHostURL(self):
		"""
		Function : Fetches the Syapse Host URL that matches the mode given in self.mode.
		Returns  : str.
		"""
		return self.hostUrls[self.mode]
		
	def getApiToken(self):
		"""
		Function : Fetches the Syapse HOST API Token that matches the mode given in self.mode.
		Returns  : str.
		"""
		return self.apiTokens[self.mode]	


class Utils(Syapse):
	def __init__(self,mode):
		"""
		Args : mode - A string indicating which Syapse Host to use. Must be one of elemensts given in self.knownModes.
		"""
		Syapse.__init__(self,mode=mode)
		self.conn = self.connect()
		self.kb = self.conn.kb

	def deleteSequencingResults(self,name,lane,barcode=None):
		"""
		Function : Deletes SequencingResults objects that have the given name, lane, and optionally a particular barcode.
							 This method was created because many duplicate SequencingResults records exist due to a bug that was
							 in the sequence results upload script.
		Args     : name - The value of the 'name' property (shown as "Record Name" in the  GUI). 
							 lane - (int) The lane number. 
							 barcode - The barcode name as shown in Syapse in the Barcode Results table of a SequencingResults object.
		Example  : deleteSequencingResults("150619_TENNISON_0369_AC7CM2ACXX",8,"1:ATCACG")
		"""	
		airs = self.kb.listAppIndividualRecords(kb_class_id="EncodeSequencingResults",name=name) 
		#The above returns AppIndividualRecords
		for i in airs:
			ai = self.kb.retrieveAppIndividual(app_ind_id=i.app_ind_id)
			if ai.lane.value() != lane: #and ai.barcode.value() == "2:CGATGT":
				continue
			if barcode:
				if ai.barcode.value() != barcode:
						continue
			self.kb.deleteAppIndividual(ai.id)	

	def getAppIndividual(self,app_ind_id=None,unique_id=None):
		"""	
		Args     : app_ind_id - str. The value of the 'qid' or 'id' attribute of a syapse_client.sem.inds.AppIndividual object
							 unique_id  - str. The value of the uniqueId prop of the AppIndividual.
		"""
		if not app_ind_id and not unique_id:
			raise TypeError("getAppIndividual() must have either app_ind_id or unique_id set!!")
		if app_ind_id:
			ai = self.conn.kb.retrieveAppIndividual(app_ind_id)
		else:
			ai = self.conn.kb.retrieveAppIndividualByUniqueId(unique_id)
		return ai

	def getKbClassNames(self):
		return self.conn.kb._kbclasses.keys()

	def getKbClassIdFromAppInd(self,app_ind_id=None,unique_id=None):
		"""
		Function : Fetches the kbclass of an AppIndividual instance. Either the app individual id or the unique id of the instance must be supplied.
		Args     : app_ind_id - str. The value of the 'qid' or 'id' attribute of a syapse_client.sem.inds.AppIndividual object.
							 unique_id  - str. The value of the uniqueId prop of the AppIndividual.
		Returns  : str. A kbClass id (The value of the 'id' attribute of a KbClass.
		"""
		ai = self.getAppIndividual(app_ind_id=app_ind_id,unique_id=unique_id)
		className = ai.form.kbclass.id
		return className

	def getKbClassProp(self,kbclass_id,propertyName):
		"""
		Args:     kbclass_id  - The id of a kbClass. Can use either 'id','qid', or 'pid' attributes of aA syapse_client.sem.types.KbClass.
		          propertyName - str. The name of a property of the given form.
		Returns : A syapse_client.sem.types.PropertySpec object
		"""
		form = self.conn.kb.getForm(kbclass_id)
		return form.props[propertyName].prop

	def setProperty(self,propertyName,value,app_ind_id=None,unique_id=None):
		"""
		Function : Sets the value of a property of an app_ind.
							 Raises a syapse_client.err.SemanticConstraintError if the property doesn't belong to the class.
		Args     : propertyName - str. The name of the property of the object with the specified ID.
							 value      - The value to set the property can. Can by any object.
							 app_ind_id - str. The value of the 'qid' or 'id' attribute of a syapse_client.sem.inds.AppIndividual object.
							 unique_id  - str. The value of the uniqueId prop of the AppIndividual
		"""
		ai = self.getAppIndividual(app_ind_id=app_ind_id,unique_id=unique_id)
		
		triple = ai.triples(propertyName)
		triple.set(value)

	def getPropertyEnumRangeFromAppInd(self,propertyName,app_ind_id=None,unique_id=None):
		"""
		Function :
							 app_ind_id - str. The value of the 'qid' or 'id' attribute of a syapse_client.sem.inds.AppIndividual object
							 unique_id  - str. The value of the uniqueId prop of the AppIndividual.
							 propertyName - a property name of that appInd.
		Returns  : list of strings.
		"""
		app_ind = self.getAppIndividual(app_ind_id=app_ind_id,unique_id=unique_id)
		prop = app_ind.tuples(propertyName).prop
		#or could have done: self.getKbClassFromAppInd(); self.getKbClassProp()
		return prop.range_values

	def getPropertyEnumRangeFromKbClassId(self,kbclass_id,propertyName):
		"""
		Function :
		Args     : kbclass_id - a Syapse kbClassId
							 propertyName - a property name of that appInd.
		Returns  : list of strings.
		"""
		prop = self.getKbClassProp(kbclass_id=kbclass_id,propertyName=propertyName)
		return prop.range_values


	def getSeqRequestsWithoutSeqResultsQuery(self):
		"""
		Function : Queries all Sequencing Request (SReq) objects to check whether the barcode libraries of each request all have a Sequencing Result (SRes) object. Each
	               result returned by the query will contain the SReq unique ID. Essentially, if any of the library objects reference on the sequencing request object don't have
	               a SRes object, then the SReq object label will be included in this query's results.
		Return   : str. The Syapse SyQL query.
		"""
	
		query = """
						SELECT ?SequencingRequest_A.sys:label WHERE {
							REQUIRE PATTERN ?SequencingRequest_A enc:SequencingRequest {
								enc:hasLibrary ?Library_B
							}
							PATTERN ?Library_B enc:Library {
								NOT EXISTS REVERSE enc:EncodeSequencingResults
							}
					}
					LIMIT 2000
					"""
		return query
	
	def getBarcodesOnSeqRequestQuery(self,seq_req_uid):
		"""
		Function : Given a Sequencing Request object ID from Syapse, gives the query needed to fetch all Barcodes associated on that Sequencing Request.
		Args     : seq_req_uid - str. A Sequencing Request object ID from Syapse.
		Returns  : str. The Syapse SyQL query.
	
		Table Columns:
		[0] - Syapse Library - Link
		[1] - Syapse Unique ID for Library
		[2] - Barcode for the Library
		[3] - Sequencing Platform
		"""
		query = """
						SELECT ?BioSampleLink_E.enc:hasLibrary ?Library_B.sys:uniqueId ?BioSampleLink_E.enc:barcode 
						?SequencingRequest_A.enc:sequencingPlatform WHERE {
						    REQUIRE PATTERN ?SequencingRequest_A enc:SequencingRequest {
						        enc:hasLibrary ?Library_B .
						        sys:uniqueId """ + "'" + seq_req_uid + "'"  + """
						    }
						    PATTERN ?Library_B enc:Library {
						        REVERSE enc:ScgpmDChIP ?ScgpmDChIP_D
						    }
						    PATTERN ?ScgpmDChIP_D enc:ScgpmDChIP {
						        enc:hasBioSampleLink ?BioSampleLink_E .
						        PATTERN ?BioSampleLink_E enc:BioSampleLink {
						            enc:hasLibrary ?Library_B
						        }
						    }
						}
						LIMIT 2000
						"""
		return query
	
	def getBarcodeFromSeqResObj(self,seq_result_uid):
		"""
		Function : Fetches the barcode from a Syapse SequencingResult object.
		Args     : seq_result_uid - str. A Syapse SequencingResult UID.
		"""	
		query = """
						SELECT ?EncodeSequencingResults_A.enc:barcode WHERE {
	    				REQUIRE PATTERN ?EncodeSequencingResults_A enc:EncodeSequencingResults {
	        			sys:uniqueId """ + "'" + seq_result_uid + "'" + """
	    				}
						}
						LIMIT 2000
						"""
		return query
	
	
	def getPlatformFromSeqResObj(self,seq_result_uid):
	
		"""
		Function : Fetches the platform attribute from a Syapse Sequencing Results object.
		Args     : seq_result_uid - A Syapse Sequencing Result object UID.
		"""
		ai = self.getAppIndividual(self,unique_id=seq_result_uid)
		return ai.sequencingPlatform.value()
	
	def getPlatformFromSeqReqObj(self,seq_req_uid):
		"""
		Function : Fetches the platform attribute from a Syapse Sequencing Request object.
		Args     : seq_result_uid - A Syapse Sequencing Request object UID.
		"""
		
		ai = self.getAppIndividual(self,unique_id=seq_req_uid)
		return ai.sequencingPlatform.value()
	
	def getSeqResultObjsFromSeqReqObj(self,app_ind_id):
		"""
		Function :
		"""
		query = """
					SELECT ?EncodeSequencingResults_A.sys:uniqueId ?Library_E.sys:uniqueId ?BioSampleLink_K.enc:barcode WHERE {
					    REQUIRE PATTERN ?EncodeSequencingResults_A enc:EncodeSequencingResults {
					        enc:hasLibrary ?Library_E
					    }
					    PATTERN ?Library_E enc:Library {
					        REVERSE enc:ScgpmDChIP ?ScgpmDChIP_F .
					        REVERSE enc:SequencingRequest """ + app_ind_id + """
					    }
					    PATTERN ?ScgpmDChIP_F enc:ScgpmDChIP {
					        enc:hasBioSampleLink ?BioSampleLink_K .
					        PATTERN ?BioSampleLink_K enc:BioSampleLink {
					            EXISTS enc:barcode  .
					            enc:hasLibrary ?Library_E
					        }
					    }
					}
					LIMIT 2000
					"""
		return query
	
	def getScoringsWithStatus(self,scoringStatus):
		"""
		Function : Find All ChIP Seq Scoring Objects with Scoring Status = "Awaiting Scoring". Once executed, the query will return the following fileds in the order given:
											1) ChIP Seq Scoring-UID
											2) Exp. Library-UID
											3) Exp. SRes-UID
											4) Exp. Flowcell
											5) Exp. Lane
											6) Exp. Barcode
											7) Ctl. Library-UID
											8) Ctl. SRes-UID
											9) Ctl. Flowcell
											10) Ctl. Lane
											11) Ctl. Barcode
											12) Ctl. Library-UID
		Args     : scoringStatus - One of the possible scoringStatus values of a ChipScoring object in Syapse.
		Returns  : str. being the query. 
		"""
	
		kbclassName = "ScgpmFSnapScoring"
		propertyName = "scoringStatus"
		rangeValues = self.getPropertyEnumRangeFromKbClassId(kbclass_id=kbclassName,propertyName=propertyName)
		if not scoringStatus in rangeValues:
			raise ValueError("scoringStatus must be one of {rangeValues}.".format(rangeValues=rangeValues))
		
		query = """
					SELECT ?ScgpmFSnapScoring_A.sys:uniqueId ?Library_C.sys:uniqueId ?EncodeSequencingResults_E.sys:uniqueId ?EncodeSequencingResults_E.enc:cell \
					?EncodeSequencingResults_E.enc:lane ?SequencingResultsBarcodeResults_F.enc:barcode ?Library_D.sys:uniqueId ?EncodeSequencingResults_G.sys:uniqueId \
					?EncodeSequencingResults_G.enc:cell ?EncodeSequencingResults_G.enc:lane ?SequencingResultsBarcodeResults_H.enc:barcode ?Library_D.sys:name WHERE {
					    REQUIRE PATTERN ?ScgpmFSnapScoring_A enc:ScgpmFSnapScoring {
					        enc:hasExperimentToControl ?ExperimentToControl_B .
					        PATTERN ?ExperimentToControl_B enc:ExperimentToControl {
					            enc:hasExperimentalLibrary ?Library_C .
					            enc:hasControlLibrary ?Library_D
					        } .
					        enc:scoringStatus """ +  "'{scoringStatus}' ".format(scoringStatus=scoringStatus) + """
					    }
					    PATTERN ?Library_C enc:Library {
					        REVERSE enc:EncodeSequencingResults ?EncodeSequencingResults_E
					    }
					    PATTERN ?EncodeSequencingResults_E enc:EncodeSequencingResults {
					        enc:hasSequencingResultsBarcodeResults ?SequencingResultsBarcodeResults_F .
					        PATTERN ?SequencingResultsBarcodeResults_F enc:SequencingResultsBarcodeResults {}
					    }
					    PATTERN ?Library_D enc:Library {
					        REVERSE enc:EncodeSequencingResults ?EncodeSequencingResults_G
					    }
					    PATTERN ?EncodeSequencingResults_G enc:EncodeSequencingResults {
					        enc:hasSequencingResultsBarcodeResults ?SequencingResultsBarcodeResults_H .
					        PATTERN ?SequencingResultsBarcodeResults_H enc:SequencingResultsBarcodeResults {}
					    }
					}
					LIMIT 2000
					"""
	
		return query
	
	def getScoringsReady(self):
		return self.getScoringsWithStatus(scoringStatus="Start Scoring")
	
	def getScoringsInProgress(self):
		return self.getScoringsWithStatus(scoringStatus="Processing Scoring Results")
	
	def getScoringsCompleted(self):
		return self.getScoringsWithStatus(scoringStatus="Scoring Completed")
	
	def getScoringsFailed(self):
		return self.getScoringsWithStatus(scoringStatus="Scoring Failed")

if __name__ == "__main__":
	from argparse import ArgumentParser
	description = ""
	parser = ArgumentParser(description=description)
	parser.add_argument('-m','--mode',required=True,choices=list(Syapse.knownModes.keys()),help="Mode indicating which Syapse host account to use.")
	args = parser.parse_args()
	
	s = Syapse(args.mode)
	conn  = s.connect()
	kb = conn.kb
#	res = kb.executeSyQLQuery(s.getSeqRequestsWithoutSeqResultsQuery())
#	seqReqIDs = [x[0] for x in res.rows]
#	print(seqReqIDs)
#	for i in seqReqIDs:
#		query = s.getBarcodesOnSeqRequestQuery("SReq-745")
#		res = kb.executeSyQLQuery(query)
#		rows = res.rows
#		for row in rows:
#			libraryUid = row[0].app_ind_id
#			library_syapse_uid = row[1]
#			barcode = row[2]
#			sequencingPlatform = row[3]
