#import json, csv
import os
import pandas as pd
from pandas.io.json import json_normalize
import requests
import re, string, timeit
import time
from datetime import datetime, timedelta
import queue
from credentials import credentials

'''
Code has been developed based of this post:
https://medium.com/@alexdambra/how-to-get-aussie-property-price-guides-using-python-the-domain-api-afe871efac96
''' 

def get_access_token(client_id=None, client_secret=None):
    """
    Get the access token for the project.
    """
    if client_id == None or client_secret == None:
        return None

    # POST request for token
    response = requests.post('https://auth.domain.com.au/v1/connect/token', 
                             data = {'client_id':client_id,
                                     "client_secret":client_secret,
                                     "grant_type":"client_credentials",
                                     "scope":"api_listings_read api_listings_write",
                                     "Content-Type":"text/json"})
    token=response.json()
    expire = datetime.now() + timedelta(seconds=token['expires_in'])
    print (f'token expires at {expire}')

    access_token = {}
    access_token['access_token'] = token['access_token']
    access_token['expire_at'] = expire

    return access_token


def find_price_range(token, property_id, lowerBoundPrice, UpperBoundPrice, increment):
    """
    Find the price range of a property listing
    access_token: Must be a valid access token for the project.
    property_id: The unique property id at the end of the url 
    """
   
    # Function prints the property details and whether each price guess has the property listed.

    # Get the property details
    url = "https://api.domain.com.au/v1/listings/"+str(int(property_id))
    auth = {"Authorization":"Bearer "+token['access_token']}
    request = requests.get(url,headers=auth)
    details=request.json()

    if details['status'] == 'sold':
        date = details['saleDetails']['soldDetails']['soldDate']
        price = details['saleDetails']['soldDetails']['soldPrice']

        return price, price, price

    # Get the property details
    address=details['addressParts']
    postcode=address['postcode']
    suburb=address['suburb']
    bathrooms=details['bathrooms']
    bedrooms=details['bedrooms']
    carspaces=details['carspaces']
    property_type=details['propertyTypes']
    print(f'Property: {property_type} \nAddress: {suburb}, {postcode} \n'
          f'Bedrooms:{str(bedrooms)}, \nBathrooms:{str(bathrooms)},  \nCarspace:{str(carspaces)}')

    # The below puts all relevant property types into a single string. eg. a property listing 
    # can be a 'house' and a 'townhouse'
    n=0
    property_type_str=""
    for p in details['propertyTypes']:
        property_type_str=property_type_str+(details['propertyTypes'][int(n)])
        n=n+1
    print(property_type_str)  

    max_price=lowerBoundPrice
    searching_for_price=True

    # Start your loop
    while searching_for_price:
    
        url = "https://api.domain.com.au/v1/listings/residential/_search" # Set destination URL here
        post_fields ={
          "listingType":"Sale",
            "maxPrice":max_price,
            "pageSize":100,
          "propertyTypes":property_type,
          "minBedrooms":bedrooms,
            "maxBedrooms":bedrooms,
          "minBathrooms":bathrooms,
            "maxBathrooms":bathrooms,
          "locations":[
            {
              "state":"",
              "region":"",
              "area":"",
              "suburb":suburb,
              "postCode":postcode,
              "includeSurroundingSuburbs":False
            }
          ]
        }

        request = requests.post(url,headers=auth,json=post_fields)

        l=request.json()
        listings = []
        for listing in l:
            listings.append(listing["listing"]["id"])
        

        if int(property_id) in listings:
            max_price=max_price-increment
            print("Lower bound found: ", max_price)
            searching_for_price=False
        else:
            max_price=max_price+increment
            print("Not found. Increasing max price to ",max_price)
            time.sleep(0.1)  # sleep a bit so you don't make too many API calls too quickly   

        if max_price >= UpperBoundPrice:
            upper = 2
            break

    searching_for_price=True
    if UpperBoundPrice>0:
        min_price=UpperBoundPrice
    else:  
        min_price=max_price+400000  


    while searching_for_price:
    
        url = "https://api.domain.com.au/v1/listings/residential/_search" # Set destination URL here
        post_fields ={
          "listingType":"Sale",
            "minPrice":min_price,
            "pageSize":100,
          "propertyTypes":property_type,
          "minBedrooms":bedrooms,
            "maxBedrooms":bedrooms,
          "minBathrooms":bathrooms,
            "maxBathrooms":bathrooms,
          "locations":[
            {
              "state":"",
              "region":"",
              "area":"",
              "suburb":suburb,
              "postCode":postcode,
              "includeSurroundingSuburbs":False
            }
          ]
        }
        request = requests.post(url,headers=auth,json=post_fields)

        listing_request=request.json()
        listings = []
        for listing in listing_request:
            listings.append(listing["listing"]["id"])
        listings

        if int(property_id) in listings:
            min_price=min_price+increment
            print("Upper bound found: ", min_price)
            searching_for_price=False
        else:
            min_price=min_price-increment
            print("Not found. Decreasing min price to ",min_price)
            time.sleep(0.1)  # sleep a bit so you don't make too many API calls too quickly     
       
        if min_price <= lowerBoundPrice:
            lower = 1
            break
    
    # Print the results
    print(address['displayAddress'])
    print(details['headline'])
    print("Property Type:",property_type_str)
    print("Details: ",int(bedrooms),"bedroom,",int(bathrooms),"bathroom,",int(carspaces),"carspace")
    print("Display price:",details['priceDetails']['displayPrice'])      
    if max_price==min_price:
        print("Price guide:","$",lower)
    else:
        print("Price range:","$",lower,"-","$",upper)
    print("URL:",details['seoUrl'])

    middle_price = min_price + (max_price-min_price)/2

    return min_price, max_price, middle_price


def search_domain(token, search_parameters):
    """
    Return a list of all the listings that are relevant to the search parameters.
    """
    access_token = token['access_token']
    time.sleep(60)

    url = "https://api.domain.com.au/v1/listings/residential/_search"
    if datetime.now() > token['expire_at']:
        token = get_access_token(client_id=client_id, client_secret=client_secret)
        access_token = token['access_token']
    
    auth = {"Authorization":"Bearer "+access_token}

    while True:
        try:
            request = requests.post(url, json=search_parameters, headers=auth)
        except requests.exceptions.Timeout:
            # if timed out, try again later.
            time.sleep(60)
            request = requests.post(url, json=search_parameters, headers=auth)
        except requests.exceptions.TooManyRedirects as e:
            # The URL is wrong.
            print (f'Too many redirects: {e}')
            break
        except requests.exceptions.RequestException as e:
            # Request failed, try again later.
            now = datetime.now()
            print (f'{now}: Request Exception: {e}')
            print (f'Token expires: {token["expire_at"]}')
            time.sleep(60)
            request = requests.post(url, json=search_parameters, headers=auth)
        break

    # Test for specific status codes
    if request.status_code == 429:
        # Rate limit has been reached.
        retry_time = datetime.now() + timedelta(seconds=float(request.headers["Retry-After"]))
        print (f'Limit of {request.headers["X-RateLimit-VCallRate"]} has been reached.')
        print (f'Will re-try at approx {retry_time}.')
        print (f'Access token expires at {token["expire_at"]}')

        time.sleep(float(request.headers['Retry-After'])*1.01)
        print (access_token)

        # Get a new token
        token = get_access_token(client_id=client_id, client_secret=client_secret)
        access_token = token['access_token']

        auth = {"Authorization":"Bearer "+access_token}
        request = requests.post(url, json=search_parameters, headers=auth)
        print (token['access_token'])

        if request.status_code != 200:
            raise Exception(request.json()['errors'], request.json()['message'], 
                            'Raised after getting a new access token')

    if request.status_code != 200:
        # status code: 500
        raise Exception(request.json()['errors'], request.json()['message'])

    return token, request.json(), int(request.headers['X-RateLimit-Remaining'])
    


def search_builder(searchForm):
    """
    Build the search parameters list.
    """
    
    assert searchForm['listingType'] in ['Sale', 'Rent', 'Share', 'Sold', 'NewHomes'], \
        'listingType must be one of [Sale, Rent, Share, Sold, NewHomes]'
    
    # Prepare the search parameters
    #searchForm = {
    #    'listingType': listingType,
    #    #'propertyTypes': None,
    #    #'propertyFeatures': None,
    #    #'listingAttributes': None,
    #    'minBedrooms': minBeds,
    #    'maxBedrooms': maxBeds,
    #    'minBathrooms': minBath,
    #    'maxBathrooms': maxBath,
    #    #'minCarspaces': None,
    #    #'maxCarspaces': None,
    #    'minPrice': minPrice,
    #    'maxPrice': maxPrice,
    #    #'minLandArea': None,
    #    #'maxLandArea': None,
    #    #'advertiserIds': None,
    #    #'adIds': None,
    #    #'excludeAdIds': None,
    #    'locations': locations,
    #    #'locationTerms': None,
    #    'keywords': keywords,
    #    #'inspectionFrom': None,
    #    #'inspectionTo': None,
    #    #'auctionFrom': None,
    #    #'auctionTo': None,
    #    #'sort': None,
    #    'page': page,
    #    'pageSize': pageSize,
    #    #'geoWindow':None
    #    'sort': {'sortKey': 'Price',
    #             'direction': 'Ascending'}
    #    }
    
    # Build the search parameters with the locations
    locations = searchForm['locations']
    SearchParameters = []
    SearchQueue = queue.Queue()
    for suburb in locations.keys():
        searchForm['locations'] = [locations[suburb]]
        SearchParameters.append(searchForm.copy())
        SearchQueue.put(searchForm.copy())

    
    # The price range can be adjusted later, to reduce the number of listings returned (max 1000 per search)
    ''' 
    The choice to make the price adjustments later is because, when there is a list of locations, 
    the price ranges neccessary will depend on the number of locations included. If only one location 
    is included in the search, this limits the number of ranges that will be required to search through.
    '''

    return SearchParameters, SearchQueue


def build_search_locations(suburbs=['Balgowlah']):
    """
    build the location parameters for the search parameters
    """

    postcode_file = os.path.join(os.path.join(os.path.dirname(os.path.realpath(__file__)),'..'),'postcodes.csv')
    postcodes = pd.read_csv(postcode_file)
    
    if 'NSW' in suburbs:
        postcodes = postcodes[postcodes['State'] == 'NSW']
    if 'QLD' in suburbs:
        postcodes = postcodes[postcodes['State'] == 'QLD']
    if 'SA' in suburbs:
        postcodes = postcodes[postcodes['State'] == 'SA']
    if 'NT' in suburbs:
        postcodes = postcodes[postcodes['State'] == 'NT']
    if 'ACT' in suburbs:
        postcodes = postcodes[postcodes['State'] == 'ACT']
    if 'WA' in suburbs:
        postcodes = postcodes[postcodes['State'] == 'WA']
    if 'TAS' in suburbs:
        postcodes = postcodes[postcodes['State'] == 'TAS']

    if set(suburbs).issubset(['All', 'NSW', 'QLD', 'SA', 'NT', 'ACT', 'WA', 'TAS']):
        suburbs = postcodes['Suburb']

    # buld the locations with additional parameters
    searchLocations = {}
    for suburb in suburbs:
        location_df = postcodes[postcodes['Suburb'] == suburb]

        if location_df.shape[0] > 0:
            location = {'state': location_df['State'].values[0], 
                        'suburb': location_df['Suburb'].values[0], 
                        'postcode': location_df['Postcode'].values[0],
                        'includeSurroundingSuburbs': True}
            searchLocations[suburb] = location
        else:
            print (f'{suburb} is not in the list.')

    return searchLocations


def extract_price(lastPrice):
    """
    Extract the value of the price
    """
    
    price = 0
    priceDetails = lastPrice.replace('$','').replace(',','').replace('+','').replace('s','').split()
    for item in priceDetails:
        if item.isdigit():
            price = int(int(item)/100000)*100000
            break
        
    return price


def add_dates(listings, df):
    """
    Add the first and last seen dates to the dataframe
    """

    listing_df = json_normalize(listings)

    if df.shape[0] > 0:
        new_listings = listing_df['listing.id']
        mask = df['listing.id'].isin(new_listings)
        # Update the dates already in df
        df['last_seen'].loc[mask] = datetime.utcnow().date().strftime('%d/%m/%Y')
    
    listing_df['first_seen'] = datetime.utcnow().date().strftime('%d/%m/%Y')
    listing_df['last_seen'] = datetime.utcnow().date().strftime('%d/%m/%Y')
    
    listing_df = df.append(listing_df)
    
    # Sort the listings by date, so the last one is most recent
    listing_df = listing_df.sort_values(by=['last_seen'])
    listing_df = listing_df.drop_duplicates(subset='listing.id') # keep the last one

    listing_df = function(listing_df)

    return listing_df


def function(listing_df):

    # Todo: Make sure all listings have a real price
    # extract prices where available.
    # identify listings with no price and use price range function
    
    ## Find prices where there is none.
    #id_list = listing_df[(listing_df['listing.priceDetails.price'].isnull()) & 
    #                     (listing_df['listing.priceDetails.displayPrice'].isnull())]
    #for idx, row in id_list.iterrows():
    #    min_price, max_price, middle_price = find_price_range(access_token, row['listing.id'], 500000, 2000000, 25000)
    #    listing_df['listing.priceDetails.displayPrice'].iloc[idx] = middle_price
    #
    return listing_df


def calling_function(client_id=None, client_secret=None, filename=None, suburbs=[], searchForm={}):
    
    #client_id = credentials['client_id']
    #client_secret = credentials['client_secret']
    
    #suburbs = ['Balgowlah'] #, 'Manly Vale', 'Dee Why', 'Brookvale', 'Cremorne']
    #suburbs = ['NSW']
    access_token = get_access_token(client_id=client_id, client_secret=client_secret)

    # Limits of the post request enforced by Domain.com.au
    #maxPageSize = 200
    maxPages = 5

    # Read the realestate file, if it exists
    #file = 'local_listings.csv'
    #filename = os.path.join(os.path.join(os.path.dirname(os.path.realpath(__file__)),'..'),file)

    try:
        df = pd.read_csv(filename, sep=',')
        df.drop(['Unnamed: 0'], axis=1, inplace=True)
        
    except FileNotFoundError:
        df = pd.DataFrame()

    # Build search paramters
    searchParameters, searchQueue = search_builder(searchForm)
    
    listings = []
    length = -1

    while searchQueue.qsize() > 0:
        print(f'Searching queue {searchQueue.qsize()}...')

        # Get the first item from the search queue
        search = searchQueue.get()
        access_token, search_results, remaining_calls = search_domain(access_token, search)
        listings.extend(search_results)
     
        # Check if new listings can be added
        search['page'] += 1 
        access_token, new_listings, remaining_calls = search_domain(access_token, search)

        while new_listings and search['page'] < maxPages:
            print (f'page: {search["page"]}')
            listings.extend(new_listings)

            # Add the next page of search results
            search['page'] += 1
            access_token, new_listings, remaining_calls = search_domain(access_token, search)
 
        # Add the last page of search results
        listings.extend(new_listings)
    

        if ((len(new_listings) > 0) & (search['page'] >= maxPages)):
            # Update the search parameters when the maximum page count has been reached.
            # This uses the last price as the mimumum price in the next search criterea
            lastPrice = new_listings[-1]['listing']['priceDetails']['displayPrice']
            new_minPrice = extract_price(lastPrice)
            search['minPrice'] = new_minPrice
            search['page'] = 1
            searchQueue.put(search.copy())

        if remaining_calls <= 5:
            # Update the dates and save to file if we are getting close to the call limit
            listing_df =  add_dates(listings, df)
            listing_df.to_csv(filename)

    # Update the dates and save to file
    listing_df = add_dates(listings, df)
    listing_df = listing_df[listing_df['type'] != 'Project']

    # find missing prices where available.

    listing_df.to_csv(filename)

if __name__ == '__main__':

    # Todo: set up initialisation for new calling function
    # Some properties will be sold after a while, the price search may not work in this case....
    #    set a maximum before breaking out


    client_id = credentials['client_id']
    client_secret = credentials['client_secret']

    file = 'local_listings.csv'
    filename = os.path.join(os.path.join(os.path.dirname(os.path.realpath(__file__)),'..'),file)
    
    suburbs = ['Balgowlah'] #, 'Manly Vale', 'Dee Why', 'Brookvale', 'Cremorne']
    ##suburbs = ['NSW']

    # Limits of the post request enforced by Domain.com.au
    maxPages = 5

    # Build the first item for the serach queue
    locations = build_search_locations(suburbs)
    page=1
    pageSize=200
    searchForm = {
        'listingType': 'Sale',
        #'propertyTypes': None,
        #'propertyFeatures': None,
        #'listingAttributes': None,
        'minBedrooms': None,
        'maxBedrooms': None,
        'minBathrooms': None,
        'maxBathrooms': None,
        #'minCarspaces': None,
        #'maxCarspaces': None,
        'minPrice': None,
        'maxPrice': None,
        #'minLandArea': None,
        #'maxLandArea': None,
        #'advertiserIds': None,
        #'adIds': None,
        #'excludeAdIds': None,
        'locations': locations,
        #'locationTerms': None,
        'keywords': [],
        #'inspectionFrom': None,
        #'inspectionTo': None,
        #'auctionFrom': None,
        #'auctionTo': None,
        #'sort': None,
        'page': page,
        'pageSize': pageSize,
        #'geoWindow':None
        'sort': {'sortKey': 'Price',
                 'direction': 'Ascending'}
        }

    calling_function(client_id=client_id, client_secret=client_secret, filename=filename, suburbs=suburbs, searchForm=searchForm)

