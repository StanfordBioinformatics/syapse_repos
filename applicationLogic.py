from . import syapseQueries
from . import syapse

class MissingBarcodeException(Exception):
	pass

class MultipleLibrariesWithSameBarcodeOnSReq(Exception):
	pass

class Utils(syapse.Syapse):
	def __init__(self,mode):
		"""
		Args : mode - A string indicating which Syapse Host to use. Must be one of elemensts given in self.knownModes.
		"""
		syapse.Syapse.__init__(self,mode=mode)

	def getLibOrAtacSeqLinkOnSequencingRequest(self,sreq_id,lims_barcode):
		"""
		Function :
		Args     :
		Returns  : str.
		"""
		rows = self.kb.executeSyQLQuery(syapseQueries.getLibraryLinkOnSequencingRequest(sreq_id=sreq_id,lims_barcode=lims_barcode)).rows
		if len(rows) > 1:
			libs = [x[0] for x in rows]
			raise MultipleLibrariesWithSameBarcodeOnSReq("Sequencing Request {sreq_id} has multiple libraries: {libs} with the same barcode {barcode}".format(sreq_id=sreq_id,libs=libs,barcode=lims_barcode))
		if rows:
			return rows[0][0]

		rows = self.kb.executeSyQLQuery(syapseQueries.getAtacSeqLinkOnSequencingRequest(sreq_id=sreq_id,lims_barcode=lims_barcode)).rows
		if not rows:
			return None
		return rows[0][0]
		

	def getSeqResFromSeqReq(self,sreq_id,lims_barcode):
		"""
		Function : Finds all SRes objects linked to the Library or AtacSeq object that was sequenced as part of sreq_id with the barcode specified by lims_barcode.
		Args     : sreq_id - str. The SReq ID.
						 	lims_barcode - str. The barcode (sample) in the sequencing request specified by sreq, which identifies the library to check for SeqRes objects.
		Returns  : list of lists, where each sublist contains a SRes-ID. There should only be one such sublist.
		"""
		lib_seqres = self.kb.executeSyQLQuery(syapseQueries.getSeqResFromSeqReq_library(sreq_id=sreq_id,lims_barcode=lims_barcode)).rows
		atac_seqres = self.kb.executeSyQLQuery(syapseQueries.getSeqResFromSeqReq_atacSeq(sreq_id=sreq_id,lims_barcode=lims_barcode)).rows
		res = [x[0] for x in lib_seqres + atac_seqres]
		return res

	def getSReqsWithoutSeqResults(self):
		"""
		Function : Queries Syapse for any Sequencing Request (SReq) objects that need to have their sequencing result metadata uploaded.
							 If any of the library objects in Syapse that are linked on the SReq don't have a Sequencing Results (SRes) object reference, 
							 then the ID of the SequencingRequest will be returned as part of a list of lists.
		Returns  : A list of 1-item lists. Each sub list contains the ID of a Syapse SequencingRequest object. 
		"""
		libraryRes  = self.kb.executeSyQLQuery(syapseQueries.getLibSReqsWithoutSeqResults()).rows	
		atacSeqLibraryRes = self.kb.executeSyQLQuery(syapseQueries.getAtacSReqsWithoutSeqResults()).rows
		rows = libraryRes + atacSeqLibraryRes
		return rows

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
