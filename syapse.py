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
	"""
	Use this class to establish a connection to one of our Syapse hosts, which is identified by the provided mode argument (i.e. dev, qc, prod).
	"""
	apiTokens = scc["apiTokens"]
	knownModes = scc["modes"]

	hostUrls = scc["hostUrls"]

	def __init__(self,mode):
		"""
		Args : mode - A string indicating which Syapse Host to use. Must be one of elemensts given in Syapse.knownModes
		"""

		if mode  not in self.knownModes:
			raise TypeError("'Host' argument must be one of {modes}.".format(modes=Syapse.knownModes))
		self.mode = mode 
		self.host = self.getHostURL()
		self.token = self.getApiToken()
		self.conn = self._connect()
		self.kb = self.conn.kb

	def _connect(self):
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



if __name__ == "__main__":
	from argparse import ArgumentParser
	description = ""
	parser = ArgumentParser(description=description)
	parser.add_argument('-m','--mode',required=True,choices=list(Syapse.knownModes),help="Mode indicating which Syapse host account to use.")
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
