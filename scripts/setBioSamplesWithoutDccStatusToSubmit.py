#!/usr/bin/env python
import sys
import syapse_scgpm
import logging
from argparse import ArgumentParser

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
#f_formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:   %(message)s')
#ch.setFormatter(f_formatter)
logger.addHandler(ch) 

description = ""
parser = ArgumentParser(description=description)
parser.add_argument('-m','--mode',required=True,choices=list(syapse_scgpm.syapse.Syapse.knownModes.keys()),help="Mode indicating which Syapse host account to use.")

args = parser.parse_args()
mode = args.mode

SEND_TO_DCC = "Send to DCC"

conn = syapse_scgpm.syapse.Syapse(mode=mode)
kb = conn.kb

query = syapse_scgpm.syapseQueries.getBiosamplesWithoutDccStatusSet() #see saved query on Syapse named 'biosamples without DCC Status'.
biosamples = kb.executeSyQLQuery(query).rows
biosamples = biosamples[1:] #for some reason I get a datetime object as the first element
logging.info(biosamples)
#biosamples = [["B-280"]]
for b in biosamples:
	b = b[0]
	#logger.info("Processing biosample {biosample}.".format(biosample=b))
	ai = conn.getAppIndividual(unique_id=b)
	dccField = ai.hasDccField.createValue(kb) #ai.hasDccField.value() returns None, so I need to set the value after creating a "enc:DccField" Individual.
	##dccField is a syapse_client.sem.inds.Individual object
	dccField.submittedToDcc.set(SEND_TO_DCC)
	ai.hasDccField.set(dccField)
	#prop = dccForm.form.props["submittedToDcc"].prop
	logger.info("Updating {biosample} to {SEND_TO_DCC}.".format(biosample=b,SEND_TO_DCC=SEND_TO_DCC))
	kb.saveAppIndividual(ai)
	logger.info("Successfully updated status for {biosample}.".format(biosample=b))
	#possible values to set prop to are given in prop.range_values(), which gives [u'Internal Hold', u'Send to DCC', u'Registered with DCC', u'DCC Hold']
