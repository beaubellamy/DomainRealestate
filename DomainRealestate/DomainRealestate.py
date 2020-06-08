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
from extract_prices import listing_prices


'''
Code has been developed based of this post:
https://medium.com/@alexdambra/how-to-get-aussie-property-price-guides-using-python-the-domain-api-afe871efac96
''' 

def get_access_token(credentials={}):
    """
    Get the access token for the project.
    """
    client_id = credentials['client_id']
    client_secret = credentials['client_secret']

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


def validate_post_request(url,  headers, post_payload, credentials):

    request = requests.post(url, headers=headers, json=post_payload)
    
    #token=request.json()

    # check for status.
    if request.status_code == 429:
        # Rate limit has been reached.
        retry_time = datetime.now() + timedelta(seconds=float(request.headers["Retry-After"]))
        quota = quota_limit(request)
        print (f'Limit of {quota} has been reached.')
        print (f'Will re-try at approx {retry_time}.')
        #print (f'Access token expires at {token["expire_at"]}')

        time.sleep(float(request.headers['Retry-After'])*1.01)
        print (access_token)

        # Get a new token
        token = get_access_token(credentials)
        access_token = token['access_token']

        auth = {"Authorization":"Bearer "+access_token}
        request = requests.post(url, json=post_payload, headers=auth)
        print (token['access_token'])

        if request.status_code != 200:
            raise Exception(request.json()['errors'], request.json()['message'], 
                            'Raised after getting a new access token')

    return request


def validate_get_request(url, headers, credentials):

    request = requests.get(url,headers=headers)
    
    # check for status.
    if request.status_code == 429:
        # Rate limit has been reached.
        retry_time = datetime.now() + timedelta(seconds=float(request.headers["Retry-After"]))
        quota = quota_limit(request)
        print (f'Limit of {quota} has been reached.')
        print (f'Will re-try at approx {retry_time}.')
        #print (f'Access token expires at {token["expire_at"]}')

        time.sleep(float(request.headers['Retry-After'])*1.01)
        #print (access_token)

        # Get a new token
        token = get_access_token(credentials)
        access_token = token['access_token']

        auth = {"Authorization":"Bearer "+access_token}
        request = requests.get(url,headers=headers)
        
        if request.status_code != 200:
            raise Exception(request.json()['errors'], request.json()['message'], 
                            'Raised after getting a new access token')

    return request

def remaining_calls(request):

    if 'X-RateLimit-Remaining' in request.headers.keys():
        remaining = request.headers['X-RateLimit-Remaining']
    elif 'X-Quota-PerDay-Remaining' in request.headers.keys():
        remaining = request.headers['X-Quota-PerDay-Remaining']
    else:
        remaining = -1

    return remaining


def quota_limit(request):

    if 'x-ratelimit-vcallrate' in request.headers.keys():
        quota = request.headers['x-ratelimit-vcallrate']
    elif 'X-Quota-PerDay-Limit' in request.headers.keys():
        quota = request.headers['X-Quota-PerDay-Limit']
    else:
        quota = -1

    return quota


def check_for_listing(request, property_id, price, increment, increase_price=True):

    continue_searching = True

    if increase_price == True:
        prefix = 'Lower'
        search_prefix = 'increasing'
    else:
        prefix = 'Upper'
        search_prefix = 'decreasing'

    listing_request=request.json()
    listings = []
    for listing in listing_request:
        listings.append(listing["listing"]["id"])
        
    if increase_price == True:
        if int(property_id) in listings:
            print(f"{prefix} bound found: ", price)
            #min_price=min_price-increment
            continue_searching=False
        else:
            price=price+increment
    else:
        if int(property_id) not in listings:
            price+=increment
            print(f"{prefix} bound found: ", price)
            #min_price=min_price-increment
            continue_searching=False
        else:
            price=price-increment
        
    if continue_searching:
        print(f"Not found. {search_prefix} price to {price}")
    
    time.sleep(0.1)  # sleep a bit so you don't make too many API calls too quickly   

    return continue_searching, price

def search_for_price(property_fields, auth, request, increment):
    #property_fields = {'price': min_price, 
    #                   'property_type': property_type, 
    #                   'bedrooms': bedrooms, 
    #                   'bathrooms': bathrooms, 
    #                   'suburb': suburb, 
    #                   'postcode': postcode}

    price = property_fields['price']

    url = "https://api.domain.com.au/v1/listings/residential/_search" # Set destination URL here
        
    while searching_for_price:
    
        post_fields = build_post_fields(property_fields)
        
        #request = requests.post(url,headers=auth,json=post_fields)
        request = validate_post_request(url, auth, post_fields, credentials)

        searching_for_price, min_price = check_for_listing(request, property_id, min_price, increment, True)
        #listing_request=request.json()
        #listings = []
        #for listing in listing_request:
        #    listings.append(listing["listing"]["id"])
        
        #if int(property_id) in listings:
        #    print("Lower bound found: ", min_price)
        #    #min_price=min_price-increment
        #    searching_for_price=False
        #else:
        #    min_price=min_price+increment
        #    print("Not found. Increasing max price to ",min_price)
        #    time.sleep(0.1)  # sleep a bit so you don't make too many API calls too quickly   

        if min_price >= UpperBoundPrice:
            upper = 2
            break

    #searching_for_price=True
    if UpperBoundPrice>0:
        max_price=UpperBoundPrice
    else:  
        max_price=min_price+400000  

    return max_price


def build_post_fields(property_fields):
        
    post_fields ={
        "listingType":"Sale",
        "maxPrice":property_fields['price'],
        "pageSize":100,
        "propertyTypes":property_fields['propertyTypes'],
        "minBedrooms":property_fields['bedrooms'],
        "maxBedrooms":property_fields['bedrooms'],
        "minBathrooms":property_fields['bathrooms'],
        "maxBathrooms":property_fields['bathrooms'],
        "locations":[
        {
            "state":"",
            "region":"",
            "area":"",
            "suburb":property_fields['suburb'],
            "postCode":property_fields['postcode'],
            "includeSurroundingSuburbs":False
        }
        ]
    }

    return post_fields


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
    request = validate_get_request(url, auth, credentials)
    #request = requests.get(url,headers=auth)

    details=request.json()

    if details['status'] == 'sold':
        date = details['saleDetails']['soldDetails']['soldDate']
        price = details['saleDetails']['soldDetails']['soldPrice']

        remaining = remaining_calls(request)

        return int(remaining), date, price, price

    # Get the property details
    address=details['addressParts']
    postcode=address['postcode']
    suburb=address['suburb']
    bathrooms=details['bathrooms']
    bedrooms=details['bedrooms']
    property_type=details['propertyTypes']
    print(f'Property: {property_type} \nAddress: {suburb}, {postcode} \n'
          f'Bedrooms:{str(bedrooms)}, \nBathrooms:{str(bathrooms)}')

    # The below puts all relevant property types into a single string. eg. a property listing 
    # can be a 'house' and a 'townhouse'
    #property_type_str=""
    #for p in details['propertyTypes']:
    #    property_type_str=property_type_str+(p)

    min_price=lowerBoundPrice
    continue_searching=True
    details['postcode'] = address['postcode']
    details['suburb'] = address['suburb']

    # Start your loop
    while continue_searching:
        
        details['price'] = min_price

        url = "https://api.domain.com.au/v1/listings/residential/_search" # Set destination URL here
        post_fields = build_post_fields(details)
        
        #request = requests.post(url,headers=auth,json=post_fields)
        request = validate_post_request(url, auth, post_fields, credentials)

        continue_searching, min_price = check_for_listing(request, property_id, min_price, increment, True)
               
        #if continue_searching and min_price == UpperBoundPrice:
        #    UpperBoundPrice *= 2
        #    print ('stop')

        #if min_price >= UpperBoundPrice:
        #    upper = 2
        #    break

    continue_searching=True
    UpperBoundPrice = min_price*1.2
    max_price = UpperBoundPrice
    #if UpperBoundPrice>0:
    #    max_price=UpperBoundPrice
    #else:  
    #    max_price=min_price+400000  


    while continue_searching:
    
        details['price'] = max_price

        url = "https://api.domain.com.au/v1/listings/residential/_search" # Set destination URL here
        post_fields = build_post_fields(details)
                
        request = validate_post_request(url,  auth, post_fields, credentials)

        continue_searching, max_price = check_for_listing(request, property_id, max_price, increment, False)

        # If the maximum price is greater than the upper bound, the real price was not
        # found. Increase the upper bound and continue searching
        if not continue_searching and max_price >= UpperBoundPrice:
            UpperBoundPrice *= 2
            #max_price = UpperBoundPrice
            continue_searching = True

        #if max_price <= lowerBoundPrice:
        #    lower = 1
        #    break
    
    # Print the results
    print(address['displayAddress'])
    print(details['headline'])
    print("Property Type:",details['propertyTypes'])
    print("Details: ",int(bedrooms),"bedroom,",int(bathrooms),"bathroom")
    print("Display price:",details['priceDetails']['displayPrice'])      
    if min_price==max_price:
        print(f'Price guide: ${min_price}')
    else:
        print(f'Price range: ${min_price} - ${max_price}')
    print("URL:",details['seoUrl'])

    remaining = remaining_calls(request)

    return int(remaining), None, min_price, max_price


def search_domain(token, search_parameters):
    """
    Return a list of all the listings that are relevant to the search parameters.
    """
    access_token = token['access_token']
    time.sleep(60)

    url = "https://api.domain.com.au/v1/listings/residential/_search"
    if datetime.now() > token['expire_at']:
        token = get_access_token(credentials)
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
        quota = quota_limit(request)
        print (f'Limit of {quota} has been reached.')
        print (f'Will re-try at approx {retry_time}.')
        print (f'Access token expires at {token["expire_at"]}')

        time.sleep(float(request.headers['Retry-After'])*1.01)
        print (access_token)

        # Get a new token
        token = get_access_token(credentials)
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

    remaining = remaining_calls(request)

    return token, request.json(), int(remaining)
    


def search_builder(searchForm):
    """
    Build the search parameters list.
    """
    
    assert searchForm['listingType'] in ['Sale', 'Rent', 'Share', 'Sold', 'NewHomes'], \
        'listingType must be one of [Sale, Rent, Share, Sold, NewHomes]'
    
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
    
    return listing_df


def function(listing_df):

    # Todo: Make sure all listings have a real price
    # extract prices where available.
    # identify listings with no price and use price range function
    listing_df['listing.priceDetails.displayPrice'] = listing_df['listing.priceDetails.displayPrice'].fillna('none')
    null_price = listing_df['listing.priceDetails.price'].isnull()

    # Get the ones that are just numbers
    display_is_number = listing_df['listing.priceDetails.displayPrice'].str.isdigit()
    listing_df.loc[(null_price & display_is_number), 'listing.priceDetails.price'] = listing_df[null_price & display_is_number]['listing.priceDetails.displayPrice']

    # filter the dataframe that have numbers in the display price
    

    # extract the price

    print (f'{test.shape}')


    ## Find prices where there is none.
    #id_list = listing_df[(listing_df['listing.priceDetails.price'].isnull()) & 
    #                     (listing_df['listing.priceDetails.displayPrice'].isnull())]
    #for idx, row in id_list.iterrows():
    #    min_price, max_price, middle_price = find_price_range(access_token, row['listing.id'], 500000, 2000000, 25000)
    #    listing_df['listing.priceDetails.displayPrice'].iloc[idx] = middle_price
    #
    return listing_df


def Domain(filename=None, searchForm={}):
    

    access_token = get_access_token(credentials)

    # Limits of the post request enforced by Domain.com.au
    maxPages = 5

    # Read the realestate file, if it exists
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
        access_token, search_results, remaining = search_domain(access_token, search)
        listings.extend(search_results)
     
        # Check if new listings can be added
        search['page'] += 1 
        access_token, new_listings, remaining = search_domain(access_token, search)

        while new_listings and search['page'] < maxPages:
            print (f'page: {search["page"]}')
            listings.extend(new_listings)

            # Add the next page of search results
            search['page'] += 1
            access_token, new_listings, remaining = search_domain(access_token, search)
 
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

        if remaining <= 5:
            # Update the dates and save to file if we are getting close to the call limit
            listing_df = add_dates(listings, df)
            listing_df.to_csv(filename)

    # Update the dates and save to file
    listing_df = add_dates(listings, df)
    listing_df = listing_df[listing_df['type'] != 'Project']

    print (f'You have {remaining} api calls left.')
    listing_df.to_csv(filename)

    return access_token, listing_df

def setup(file):
    
    # Todo: set up initialisation for new calling function
    # Some properties will be sold after a while, the price search may not work in this case....
    #    set a maximum before breaking out

    #client_id = credentials['client_id']
    #client_secret = credentials['client_secret']

    
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

    return filename, searchForm

    #Domain(filename=filename, suburbs=suburbs, searchForm=searchForm)


if __name__ == '__main__':


    file = 'local_listings.csv'
    filename, searchForm = setup(file)

    #access_token, df = Domain(filename=filename, searchForm=searchForm)
    access_token = get_access_token(credentials) # only required when not calling Domain

    ## extract the prices in tom something usefull
    df = listing_prices(filename)
    #df['Sold Date'] = None
    
    # find prices by calling the search api
    # Find prices where there is none.
    id_list = df[(df['fromPrice'].isnull()) | (df['toPrice'].isnull())]
    for idx, row in id_list.iterrows():
     
        remaining, date, min_price, max_price = find_price_range(access_token, row['listing.id'], 500000, 2000000, 25000)

        df.loc[idx, 'listing.priceDetails.price'] = 'price search'
        df.loc[idx, 'listing.priceDetails.priceFrom'] = min_price
        df.loc[idx, 'fromPrice'] = min_price
        df.loc[idx, 'listing.priceDetails.priceTo'] = max_price
        df.loc[idx, 'toPrice'] = max_price

        if date is not None:
            df.loc[idx, 'Sold Date'] = date

        # Update the file before we run out of api calls.
        if remaining < 100:
            df.to_csv(filename)

    missing_prices = df[(df['fromPrice'].isnull()) | (df['fromPrice'].isnull())].shape[0]
    print (f'There are {missing_prices} listings that are missing price information.')

    

