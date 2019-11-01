import json, csv
from pandas.io.json import json_normalize
import requests
import re, string, timeit
import time
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
    return token["access_token"]


def find_price_range(access_token, property_id, lowerBoundPrice, UpperBoundPrice, increment):
    """
    Find the price range of a property listing
    access_token: Must be a valid access token for the project.
    property_id: The unique property id at the end of the url 
    """
   
    # Function prints the property details and whether each price guess has the property listed.

    # Get the property details
    url = "https://api.domain.com.au/v1/listings/"+property_id
    auth = {"Authorization":"Bearer "+access_token}
    request = requests.get(url,headers=auth)
    details=request.json()

    # get the property details
    address=details['addressParts']
    postcode=address['postcode']
    suburb=address['suburb']
    bathrooms=details['bathrooms']
    bedrooms=details['bedrooms']
    carspaces=details['carspaces']
    property_type=details['propertyTypes']
    print(f'Property: {property_type} \nAddress: {suburb}, {postcode} \n'
          f'Bedrooms:{str(bedrooms)}, \nBathrooms:{str(bathrooms)},  \nCarspace:{str(carspaces)}')

    # the below puts all relevant property types into a single string. eg. a property listing can be a 'house' and a 'townhouse'
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
        listings

        if int(property_id) in listings:
            max_price=max_price-increment
            print("Lower bound found: ", max_price)
            searching_for_price=False
        else:
            max_price=max_price+increment
            print("Not found. Increasing max price to ",max_price)
            time.sleep(0.1)  # sleep a bit so you don't make too many API calls too quickly   


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
       
    if max_price<1000000:
        lower=max_price/1000
        upper=min_price/1000
        denom="K"
    else: 
        lower=max_price/1000000
        upper=min_price/1000000
        denom="M"

    # Print the results
    print(address['displayAddress'])
    print(details['headline'])
    print("Property Type:",property_type_str)
    print("Details: ",int(bedrooms),"bedroom,",int(bathrooms),"bathroom,",int(carspaces),"carspace")
    print("Display price:",details['priceDetails']['displayPrice'])      
    if max_price==min_price:
        print("Price guide:","$",lower,denom)
    else:
        print("Price range:","$",lower,"-","$",upper,denom)
    print("URL:",details['seoUrl'])


def search_domain(access_token, search_parameters):
    """
    Return a list of all the listings that are relevant to the search parameters.
    """

    url = "https://api.domain.com.au/v1/listings/residential/_search"
    auth = {"Authorization":"Bearer "+access_token}
    request = requests.post(url, json=search_parameters, headers=auth)
    
    if request.status_code != 200:
        raise Exception(request.json()['errors'], request.json()['message'])

    return request.json()


def search_builder(listingType='Sale',
                   minBeds=None, maxBeds=None,
                   minBath=None, maxBath=None,
                   minPrice=None, maxPrice=None, 
                   locations={}, 
                   keywords=[],
                   page=1, pageSize=200):
    """
    Build the search parameters list.
    """

    
    assert listingType in ['Sale', 'Rent', 'Share', 'Sold', 'NewHomes'], \
        f'listingType must be on ene of [Sale, Rent, Share, Sold, NewHomes]'
    
    # Prepare the search parameters
    searchFrom = {
        'listingType': listingType,
        #'propertyTypes': None,
        #'propertyFeatures': None,
        #'listingAttributes': None,
        'minBedrooms': minBeds,
        'maxBedrooms': maxBeds,
        'minBathrooms': minBath,
        'maxBathrooms': maxBath,
        #'minCarspaces': None,
        #'maxCarspaces': None,
        'minPrice': minPrice,
        'maxPrice': maxPrice,
        #'minLandArea': None,
        #'maxLandArea': None,
        #'advertiserIds': None,
        #'adIds': None,
        #'excludeAdIds': None,
        'locations': locations,
        #'locationTerms': None,
        'keywords': keywords,
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
    


    # Build the search parameters with the locations
    SearchParameters = []
    SearchQueue = queue.Queue()
    for suburb in locations.keys():
        searchFrom['locations'] = [locations[suburb]]
        SearchParameters.append(searchFrom.copy())
        SearchQueue.put(searchFrom.copy())

    
    # The price range can be adusted later, to reduce the number of listings returned (max 1000 per search)
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
    
    locationLibrary = {
        'Balgowlah': {'state': 'NSW',
                        'suburb': 'Balgowlah', 
                        'postcode': 2093,
                        'includeSurroundingSuburbs': True
                        },
        'Westmead':  {'state': 'NSW', 
                        'suburb': 'Westmead', 
                        'postcode': 2145,
                        'includeSurroundingSuburbs': True
                        },
        'Crestmead': {'state': 'QLD', 
                        'suburb': 'Crestmead', 
                        'postcode': 4132,
                        'includeSurroundingSuburbs': True
                        },
        'Spare':     {'state': 'NSW', 
                        'suburb': 'spare', 
                        'postcode': 9999,
                        'includeSurroundingSuburbs': True
                        }
        }
        
    searchLocations = {}
    for suburb in suburbs:
        if suburb in locationLibrary.keys():
            searchLocations[suburb] = locationLibrary[suburb]
        else:
            print (f'{suburb} is not in the location library.')
        
    return searchLocations


def extract_price(lastPrice):
    priceDetails = lastPrice.replace('$','').replace(',','').replace('+','').replace('s','').split()
    for item in priceDetails:
        if item.isdigit():
            price = int(int(item)/100000)*100000
            break
        
    return price

def get_search_parameters(maxPageSize=100):
    """
    Set up the search parameters (Originally used for testing).
    """

    # For a list of possible search parameters and their values:
    # https://developer.domain.com.au/docs/apis/pkg_agents_listings/references/listings_detailedresidentialsearch
 
    search_parameters = [
        {"listingType": "Sale", 
         "page": 1,
         "pageSize": maxPageSize,
         "locations": [ 
             {"state": "NSW", 
              "suburb": "Balgowlah", 
              "postcode": 2093,
              "includeSurroundingSuburbs": True
              } ]
            },
        {"listingType": "Sale", 
         "page": 1,
         "pageSize": maxPageSize,
         "locations": [ 
             {"state": "NSW", 
              "suburb": "Westmead", 
              "postcode": 2145,
              "includeSurroundingSuburbs": True
              } ]
         } ]
        

    return search_parameters


if __name__ == '__main__':
    
    client_id = credentials['client_id']
    client_secret = credentials['client_secret']
    access_token = get_access_token(client_id=client_id, client_secret=client_secret)

    maxPageSize = 200
    maxPages = 5
    #find_price_range(access_token, '132223042', 600000, 1200000, 5000)

    suburbs = ['Balgowlah', 'Westmead', 'Crestmead']
    #suburbs = ['Balgowlah']
    locations = build_search_locations(suburbs)
    searchList, searchQueue = search_builder(listingType='Sale', minBeds=None, maxBeds=None, minBath=None, maxBath=None, 
                                 minPrice=None, maxPrice=None, locations=locations, page=1, pageSize=200)
    # Todo: Sale response gets different keys, but maybe the property id is the same. So 
    # the property id can be used to check for sold price

    listings = []
    length = -1

    while searchQueue.qsize() > 0:
        search = searchQueue.get()
        listings.extend(search_domain(access_token, search))
     
        # Check if new listings can be added
        search['page'] += 1 
        new_listings = search_domain(access_token, search)

        while new_listings and search['page'] < maxPages:
            listings.extend(new_listings)
            search['page'] += 1
            new_listings = search_domain(access_token, search)
 
        listings.extend(new_listings)
    
        if ((len(new_listings) > 0) & (search['page'] >= maxPages)):
            lastPrice = new_listings[-1]['listing']['priceDetails']['displayPrice']
            new_minPrice = extract_price(lastPrice)
            search['minPrice'] = new_minPrice
            search['page'] = 1
            searchQueue.put(search.copy())

    print ('while complete')

    df = json_normalize(listings)
    df = df.drop_duplicates(subset='listing.id')
    df.to_csv('C:\\Users\\Beau\\Documents\\DataScience\\DomainRealestate\\test.csv')
    





