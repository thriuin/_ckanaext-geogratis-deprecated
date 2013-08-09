from ckan.lib.cli import CkanCommand
from time import sleep
import dateutil.parser
import datetime
import simplejson as json
import urllib2
import os.path
import sys

class GeogratisCommand(CkanCommand):
    """
    CKAN Geogratis Extension
    
    Usage::

        paster geogratis print-one  <uuid>
                         updated-since <date-time> [<file-name>]
                         
        <date-time> is datetime string in ISO 8601 format e.g. "2013-01-30T01:30:00"
        <file-name> is the name of a text file to write out the updated records in JSON Lines format
    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    
    def command(self):
        '''
        Parse command line arguments and call appropriate method.
        '''
        if not self.args or self.args[0] in ['--help', '-h', 'help']:
            print self.__doc__
            return

        cmd = self.args[0]
        self._load_config()
        
        if cmd == 'print-one':

            json_data = None
            try:
                nrcan_ds_id = self.args[1] 
                json_obj = _get_geogratis_item(nrcan_ds_id)
                print(json.dumps(json_obj, indent = 2 * ' '))
                
            except urllib2.URLError, e:
                print e.reason


        elif cmd == 'updated-since':
            try:
                dt = dateutil.parser.parse(self.args[1])
            except ValueError, e:
                print '"%s" is an invalid date' % self.args[1]
                return
                
            try:
                # We need to switch to ATOM for the list of data
                # Remember, with the ATOM feed, the start-index must come from the 'next' value in the ATOM feed
                # Investigate the user of the feedparser library
                # http://geogratis.gc.ca/api/en/nrcan-rncan/ess-sst/?edited-min=2013-06-14T06:00:00.000Z&alt=atom&start-index=1437924
                json_obj = _get_feed_json_obj('http://geogratis.gc.ca/api/en/nrcan-rncan/ess-sst?edited-min=%s&alt=json' % dt.isoformat())
            except urllib2.URLError, e:
                print e.reason
                return
            
            dt = datetime.date.today()
          
            i = 0  # artificial limit while developing
            
            jl_file = sys.stdout
            jl_pretty_format = True
            if len(self.args) > 2:
                jl_file = open(os.path.normpath(self.args[2]), 'wt')
                jl_pretty_format = False
                   
            while True:
                i = i + 1
                products = json_obj['products']
                for product in products:
                    sleep(0.5)  #Not to overwhelm geogratis
                    geoproduct_en = _get_geogratis_item(product['id'], 'en')
                    # Do not process Canadian Digital Elevation Data
                    if _get_product_type(geoproduct_en) == "canadian-digital-elevation-data":
                        continue
                    geoproduct_fr = _get_geogratis_item(product['id'], 'fr')
                    odproduct = _convert_to_od_dataset(geoproduct_en, geoproduct_fr)
                    if jl_pretty_format:
                        print  >>  jl_file, (json.dumps(odproduct, indent = 2 * ' '))
                    else:
                        print  >>  jl_file, (json.dumps(odproduct))
                json_obj = _get_feed_json_obj(_get_next_link(json_obj))
                if json_obj['count'] == 0 or i == 1:
                    break

def _convert_to_od_dataset(geoproduct_en, geoproduct_fr):
    odproduct = {}
    
    odproduct['id'] = geoproduct_en['id']
    odproduct['author_email'] = "open-ouvert@tbs-sct.gc.ca"
    odproduct['language'] = "eng; CAN | fra; CAN"
    odproduct['owner_org'] = "nrcan-rncan"
    odproduct['department_number'] = "115"
    odproduct['title'] = geoproduct_en['title']
    odproduct['title_fra'] = geoproduct_fr['title']
    odproduct['name'] = ""
    odproduct['notes'] = geoproduct_en['summary']
    odproduct['notes_fra'] = geoproduct_fr['summary']
    odproduct['catalog_type'] = "Geo Data | G\u00e9o"
    odproduct['subject'] = _get_gc_subject(geoproduct_en, geoproduct_fr)
    odproduct['topic_category'] = geoproduct_en['topicCategories']
    odproduct['keywords'] = _extract_keywords(geoproduct_en['keywords'])
    odproduct['keywords_fra'] = _extract_keywords(geoproduct_fr['keywords'])
    odproduct['license_id'] = "ca-ogl-lgo"
    odproduct['attribution'] = "Contains information licensed under the Open Government Licence \u2013 Canada."
    odproduct['attribution_fra'] = "Contient des informations autoris\u00e9es sous la Licence du gouvernement ouvert- Canada"
    odproduct['geographic_region'] = _get_places(geoproduct_en, geoproduct_fr)
    odproduct['spatial'] = geoproduct_en['geometry']
    odproduct['date_published'] = ""
    odproduct['spatial_representation_type'] = "Vector | Vecteur"
    odproduct['presentation_form'] = "documentDigital"
    if geoproduct_en['browseImages'] and len(geoproduct_en['browseImages']) > 0:
        odproduct['browse_graphic_url'] = geoproduct_en['browseImages'][0]['link']
    odproduct['date_modified'] = geoproduct_en['updatedDate']
    odproduct['maintenance_and_update_frequency'] = "As Needed | Au besoin"
    odproduct['data_series_name'] = geoproduct_en['citation']['series']
    odproduct['data_series_name_fra'] = geoproduct_fr['citation']['series']
    odproduct['data_series_issue_identification'] = geoproduct_en['citation']['seriesIssue']
    odproduct['data_series_issue_identification_fra'] = geoproduct_fr['citation']['seriesIssue']
    odproduct['digital_object_identifier'] = ""
    odproduct['time_period_coverage_start'] = ""
    odproduct['time_period_coverage_end'] = ""
    odproduct['url'] = geoproduct_en['url']
    odproduct['url_fra'] = geoproduct_fr['url']
    odproduct['endpoint_url'] = "http://geogratis.gc.ca/api/en"
    odproduct['endpoint_url_fra'] = "http://geogratis.gc.ca/api/fr"
    odproduct['ready_to_publish'] = True
    odproduct['portal_release_date'] = ""
 
    return odproduct
                
def _get_next_link(json_obj):
    links = json_obj['links']
    for link in links:
        if link['rel'] == 'next':
            return link['href']
    
def _get_feed_json_obj(link):
    response = urllib2.urlopen(link)
    json_data = response.read()
    json_obj = json.loads(json_data)
    json_obj['url'] = link
    return json_obj

def _get_geogratis_item(geo_id, lang):
    json_obj = _get_feed_json_obj('http://geogratis.gc.ca/api/%s/nrcan-rncan/ess-sst/%s.json' % (lang, geo_id))
    return json_obj

def _extract_keywords(keywords):
    simple_keywords = []
    for keyword in keywords:
        words = keyword.split('>')
        if len(words) > 0:
            last_word = words.pop()
            simple_keywords.append(last_word.strip())
    return simple_keywords
        
def _get_product_type(geoproduct):
    product_type = ""
    categories = geoproduct['categories']
    for category in categories:
      if category['type'] == 'urn:iso:series':
          product_type = category['terms'][0]['term']
  
def _get_places(geoproduct_en, geoproduct_fr):
    places = ""
    # For this, not likely to match against current schema - needs more discussion
    return places
    
def _get_gc_subject(geoproduct_en, geoproduct_fr):
    subject = ""
    
    # Get the English subjects
    categories_en = geoproduct_en['categories']
    subjects_en = []
    for category in categories_en:
        if category['type'] == 'urn:gc:subject':
            subjects_en = category['terms']
    
    # Get the French subjects
    categories_fr = geoproduct_fr['categories']
    subjects_fr = []
    for category in categories_fr:
        if category['type'] == 'urn:gc:subject':
            subjects_en = category['terms']
    
    # Merge
    
    return subject
  