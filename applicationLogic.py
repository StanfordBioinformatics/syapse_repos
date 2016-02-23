from syapse_scgpm import syapse

class Utils(syapse.Syapse):
	def __init__(self,mode):
		"""
		Args : mode - A string indicating which Syapse Host to use. Must be one of elemensts given in self.knownModes.
		"""
		Syapse.__init__(self,mode=mode)

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
