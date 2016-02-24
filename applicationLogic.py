from . import syapseQueries
from . import syapse

class MissingBarcodeException(Exception):
	pass

class Utils(syapse.Syapse):
	def __init__(self,mode):
		"""
		Args : mode - A string indicating which Syapse Host to use. Must be one of elemensts given in self.knownModes.
		"""
		syapse.Syapse.__init__(self,mode=mode)

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


	def getBarcode(self,seq_result_uid):
		""" 
		Args : seq_result_uid - A Syapse unique ID of a SequencingResult object.
		"""
		query = syapseQueries.getBarcodeFromSeqResObj(seq_result_uid=seq_result_uid)
		barcode = self.kb.executeSyQLQuery(query).rows[0][0] #the barcode names in Syapse have a number followed by a colon prefix, i.e. 2:.
		if not barcode:
			raise MissingBarcodeException("No barcode found in Syapse for SequencingResult ID {seq_result_uid}".format(seq_result_uid=seq_result_uid))
		return barcode

	def getPlatformFromSeqResObj(self,seq_result_uid):
	
		"""
		Function : Fetches the platform attribute from a Syapse Sequencing Results object.
		Args     : seq_result_uid - A Syapse Sequencing Result object UID.
		"""
		ai = self.getAppIndividual(unique_id=seq_result_uid)
		return ai.sequencingPlatform.value()

	def getPlatformFromSeqReqObj(self,seq_req_uid):
		"""
		Function : Fetches the platform attribute from a Syapse Sequencing Request object.
		Args     : seq_result_uid - A Syapse Sequencing Request object UID.
		"""
		
		ai = self.getAppIndividual(self,unique_id=seq_req_uid)
		return ai.sequencingPlatform.value()


def processSyapseBarcode(barcode):
	"""
	Function : Barcodes in Syapse have a number and colon prefixing the barcode sequence, i.e. 3:ACTGAG. This function removes that prefix.
	Args     : barcode - str.
	Returns  : str.
	"""
	barcode = barcode.split(":")
	if len(barcode) == 1:
		barcode = barcode[0]
	else:
		barcode = barcode[1]
	return barcode
