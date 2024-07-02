
# check end date of the package if it hasn't expired
# be vary of empty cells
# explore push button feature in excel


import argparse
import sys

import dfareporting_utils
from oauth2client import client

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from datetime import datetime, timedelta
# from requests.exceptions import HTTPError 

# Declare command-line flags.
argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument(
    'profile_id', type=int,
    help='The ID of the profile to add a placement for')
argparser.add_argument(
    'sheet_id', type=str,
    help='The Google sheet id')
argparser.add_argument(
    'data_range', type=str,
    help='The data range to give, e.g. "A1:F5"')


# make sure the sheet is accessible through the service account. use web interface to change share settings

def get_sheet_data(sheet_id, data_range):

    '''
    Fetches the data from google sheet and returns the dataframe'''
    
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    SERVICE_ACCOUNT_FILE = "sheet_access.json"

    creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # spreadsheet_id = '1h_fM-N_JYYtyQUwPEfWdQfPQNzz9W1STzAzK6nHjWOo'
    spreadsheet_id = sheet_id

    service = build('sheets', 'v4', credentials=creds)
    # sheet_name = 'test_excel'
    # data_range = f'{sheet_name}'
    # data_range = 'A1:F5'
    sheet_data_range = data_range

    # Call the Sheets API
    # ss = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    # print(ss)

    sheet = service.spreadsheets()

    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=sheet_data_range).execute()
    # print(result)
    values = result.get('values', [])
    # print("----------------------------------------------")
    # print(values)

    # converting it into a dataframe
    column_names = values[0]
    data = values[1:]

    df = pd.DataFrame(data, columns=column_names)
    return df


    

def main(argv):
    
  # Retrieve command line arguments.
  flags = dfareporting_utils.get_arguments(argv, __doc__, parents=[argparser])

  # Authenticate and construct service.
  service = dfareporting_utils.setup(flags)

  profile_id = str(flags.profile_id)
  sheet_id = str(flags.sheet_id)
  data_range = str(flags.data_range)
#   campaign_id = str(30350325)
#   package_id = str(372531846) # 372524805 372531846
  
  
  
  def patch_package(campaign_id, package_id, flights):
    
    '''
    Patches (updates) the package with the flights parameter.
    After executing this function, only the "flights" we passed will be present.
    The previous flights will be removed.
    Additional info: flights parameter is a list of all the flights (dicts)
    where all placements are of type CPC'''
    
    package = {
                'id': str(package_id),
                'campaignId': str(campaign_id)
            }
        
    package['pricingSchedule'] = {
        'pricingPeriods': flights
    }
    
    try:
        request = service.placementGroups().patch(profileId=profile_id, id=package_id, body=package)
        response = request.execute()
        return response
    except client.AccessTokenRefreshError:
        print ('The credentials have been revoked or expired, please re-run the '
            'application to re-authorize')

  def save_existing_flights(package_id, index):
    
    '''
      Saves the existing flights to an excel sheet.
      Call this function before patching any package
      Additional notes: Pagination is not implemented in the API
      while using get() to fetch placementGroups'''
    
    package_id = str(package_id)
    
    # retrieving all the current flight values    
    
    try:
        request = service.placementGroups().get(profileId=profile_id, id=package_id)
        
        response = request.execute()
        # print(response)

        current_flights = response['pricingSchedule']['pricingPeriods']
        # print(type(current_flights), end= '\n')
        print(f'\nCurrent flights: \n')
        # print(current_flights, end = '\n\n')

        # UPLOAD ALL THE FLIGHT VALUES TO A GOOGLE SHEET IF HAVEN'T ALREADY OR CREATE AN EXCEL LOCALLY
        current_flights_df = pd.DataFrame(current_flights)
        current_flights_df['campaign'] = campaign_id
        current_flights_df['package_id'] = package_id
        print(f'{current_flights_df}\n\n')
        file_path = f'flights_for_package_{package_id}__{index}.xlsx'
        current_flights_df.to_excel(file_path, index=False)
        print(f'Existing flights written to excel file {file_path}')
    
        return current_flights

    except client.AccessTokenRefreshError:
        print ('The credentials have been revoked or expired, please re-run the '
            'application to re-authorize')
  
  
#   def change_end_date_of_last_flight(current_flights, new_flight, index):
    
#     # if len(current_flights) > 1:
#     new_flight_start_date = new_flight['startDate']
#     # Convert the string to a datetime object
#     date = datetime.strptime(new_flight_start_date, '%Y-%m-%d')

#     # Subtract one day
#     previous_date = date - timedelta(days=1)

#     # Convert the result back to the desired format
#     previous_date_string = previous_date.strftime('%Y-%m-%d')
    
#     # now change the last flight's end date as previous_date_string
#     current_flights[index]['endDate'] = previous_date_string
    
  def change_end_date_of_last_flight(current_flights, new_flight):
    '''
    Changes the end date of the last flight before appending the new flight.
    The end date of the last flight becomes the previous day of the new flight's start date.'''
      
    sorted_flights = sorted(current_flights, key=lambda x: x['endDate'])
    new_flight_start_date = new_flight['startDate']
    # Convert the string to a datetime object
    date = datetime.strptime(new_flight_start_date, '%Y-%m-%d')

    # Subtract one day
    previous_date = date - timedelta(days=1)

    # Convert the result back to the desired format
    previous_date_string = previous_date.strftime('%Y-%m-%d')
    
    # now change the last flight's end date as previous_date_string
    sorted_flights[-1]['endDate'] = previous_date_string
    return sorted_flights
      

  
  df = get_sheet_data(sheet_id, data_range)
  if (df.empty):
    print("Empty sheet")
    return
  
  print('Retrieved the data from sheet, now inserting into CM360\n')
  
  total_flights_inserted = 0
  
#   try: # KEEP PAGINATION IN MIND

  for index, row in df.iterrows():
    if row.isnull().all(): # skip if empty row
        continue
    try: 
      
        campaign_id = row['Campaign_id']
        package_id = row['Package_id']
        startDate = row['startDate']
        endDate = row['endDate']
        clicks = row['Clicks']
        rate = int(float(row['CPC']) * (10**9))
        
        print(f'\n************************ INSERTING INTO PACKAGE {package_id} ******************************')
        
        # convert the date to y-m-d format
        input_date_format = "%m/%d/%Y"
        parsed_start_date = datetime.strptime(startDate, input_date_format)
        startDate = parsed_start_date.strftime("%Y-%m-%d")
        # print(startDate)
        
        parsed_end_date = datetime.strptime(endDate, input_date_format)
        endDate = parsed_end_date.strftime("%Y-%m-%d")

        # saving existing flights
        current_flights = save_existing_flights(package_id, index)
        
        # check if the campaign hasn't expired already or in other words, we have incoming flight after expiration
        flights_sort = sorted(current_flights, key=lambda x: x['endDate'])
        if (endDate > flights_sort[-1]['endDate']):
          print(f'The campaign of the current flight has expired.')
          print(f"The end date of the campaign was {flights_sort[-1]['endDate']} and the end date of new flight\
            is {endDate}\n. Continuing insertion for the remaining flights.\n")
          continue
        
        
        # print('***************************************************\n')
        
        
        # now patch the package by sending empty flight values
        
        _ = patch_package(campaign_id, package_id, [])
                
        # now patch again by inserting all the flight values back with the new flight included
        
        print(f'Inserting new flight data..\n')
        
        new_flight = {
            "startDate": str(startDate),
            "endDate": str(endDate),
            "units": str(clicks),
            "rateOrCostNanos": str(rate)
            }
        
        print(f'New flight value: \n{new_flight}')
        
        # also set the end date of the last flight in current_flights as the previous day of the new flight's start date: new flight will begin only when one ends
        
        # list_index = -2 if len(current_flights) > 1 else 0
            
        flights = change_end_date_of_last_flight(current_flights, new_flight)
        
        flights.append(new_flight) # causing date overlap with the last flight in the fetched flights since the last flight's endDate is same as package endDate

        # current_flights.insert(0, new_flight) # causing date overlap with the last flight in the fetched flights since the last flight's endDate is same as package endDate
        
        response = patch_package(campaign_id, package_id, flights)
        
        # print('***************************************************\n')
        
        print(f'\n\nUpdated the package with id {package_id} with following flight values:\n{response["pricingSchedule"]["pricingPeriods"]}')
        
        total_flights_inserted += 1
        
    except HttpError as http_error:
        print(http_error)
        print(f'Flight values for the package {package_id} have been saved locally in the excel. '
              'Continuing insertion for the remaining flights')

    
    except client.AccessTokenRefreshError:
        print ('The credentials have been revoked or expired, please re-run the '
            'application to re-authorize')
    
  print(f'\nCompleted!! Total {total_flights_inserted} flights inserted\n\n')

if __name__ == '__main__':
  main(sys.argv)