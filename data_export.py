# Packages
import requests
import pandas as pd
import numpy as np
from io import StringIO

# Metadata (to create variable dictionary)
metadata_request = {
    'token': token,
    'content': 'metadata',
    'format': 'csv',
    'returnFormat': 'csv'
}
metadata = requests.post(url,data=metadata_request)
metadata = pd.read_csv(StringIO(metadata.text))
# Obtain list with all form names
form_names = metadata.form_name.unique().tolist()

# Form-Event Mappings
formEventMapping_request = {
    'token': token,
    'content': 'formEventMapping',
    'format': 'csv',
    'returnFormat': 'csv'
}
formEventMapping = requests.post(url,data=formEventMapping_request)
formEventMapping = pd.read_csv(StringIO(formEventMapping.text))
# Dictionary to map forms with events
form_event_dict = {}
for form_name in form_names:
    this_event = formEventMapping.loc[formEventMapping.form==form_name].unique_event_name.tolist()
    form_event_dict[form_name] = this_event

# Create empty dictionary to store REDCap forms
redcap_data_dict = {}

for form_name in form_names:
    # Create Form dictionary to request specific form to REDCap i.e. {'forms[0]':'baseline'}
    form_request = {'forms[0]':form_name}

    # We identify the events corresponding to the current form
    events_in_form = form_event_dict.get(form_name)
    events_in_form_len = len(events_in_form) # calculate number of events of the current form
    # Create Event dictionary with the events of the current form i.e.  {'events[0]':'visit_1_arm_1','events[1]':'visit_2_arm_1', ... }
    events_request = {'events[{}]'.format(i): events_in_form[i] for i in range(events_in_form_len)}

    # Dictionary with the preset settings that will be requested to REDCap
    data_request = {
        'token': token,
        'content': 'record',
        'action': 'export',
        'format': 'csv',
        'type': 'flat',
        'csvDelimiter': '',
        'rawOrLabel': 'label',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'true',
        'returnFormat': 'csv'
    }
    # Add the current form and corresponding events to the current REDCap 'data_request' dictionary
    data_request.update(form_request)
    data_request.update(events_request)
    # Now we perform the call to the REDCap API using the created request dictionary 'data_request'
    r = requests.post(url, data=data_request)

    # Import the obtained REDCap data into pandas dataframe
    # before creating the dataframe we check whether the current form has any content
    if r.text == '\n':
        pass # if it has no content no df is created
    else:
        r = pd.read_csv(StringIO(r.text))

    # We store this dataframe into a dictionary that uses the form name as a key
    redcap_data_dict[form_name] = r
    
    # Now we dynamically assign the current df to the 'form_name' value as the object name of the df
    exec(f"{form_name} = r")