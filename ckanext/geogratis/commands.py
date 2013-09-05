from ckan.lib.cli import CkanCommand
from ckanext.canada.metadata_schema import schema_description
from paste.script import command
from time import sleep
import csv
import dateutil.parser
import datetime
import logging
import os.path
import paste.script
import re 
import simplejson as json
import urllib2
import sys

class GeogratisCommand(CkanCommand):
    """
    CKAN Geogratis Extension
    
    Usage:
        paster geogratis print_one -u <uuid> [-f <file-name>] 
                         updated -d <date-time> [-f <file-name>] [-r <report_file>] [-n]
                         get_all [-f <file-name>] [-r <report_file>] [-n]
                         [-h | --help]
                         
    Arguments:
        <uuid>        is the Geogratis dataset ID number
        <date-time>   is datetime string in ISO 8601 format e.g. "2013-01-30T01:30:00"
        <file-name>   is the name of a text file to write out the updated records in JSON Lines format
        <report_file> is the name of a text to write out a import records report in .csv format
        
    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    
    parser = command.Command.standard_parser(verbose=True)           
    parser.add_option('-u', '--uuid', dest='uuid', help='Geogratis dataset ID number')
    parser.add_option('-d', '--date', dest='date', help='datetime string in ISO 8601 format')
    parser.add_option('-r', '--report-file', dest='report_file', help='text to write out a import records report')
    parser.add_option('-f', '--json-file', dest='jl_file', help='imported Geogratis records in JSON lines format')
    parser.add_option('-n', '--no-print', dest='noprint', action='store_true', help='Do not print out the JSON records')
    parser.add_option('-c', '--config', dest='config',
        default='development.ini', help='Configuration file to use.')
                
    def command(self):
        '''
        Parse command line arguments and call appropriate method.
        '''
        if not self.args or self.args[0] in ['--help', '-h', 'help']:
            print self.__doc__
            return
        
        cmd = self.args[0]

        self._load_config()
        
        self.logger = logging.getLogger('ckanext')
           
        jl_file = sys.stdout
        jl_pretty_format = True
        if self.options.jl_file:
            jl_file = open(os.path.normpath(self.options.jl_file), 'wt')
            jl_pretty_format = False
                        
        if cmd == 'print_one':

            json_data = None
            try:
                if not self.options.uuid:
                    print self.__doc__
                    return
                json_obj = self._get_geogratis_item(self.options.uuid, 'en')
                print  >>  jl_file, json.dumps(json_obj, indent = 2 * ' ')
                
            except urllib2.URLError, e:
                logger.error(e.reason)


        elif cmd == 'updated' or cmd == 'get_all':
            
            query_string = 'http://geogratis.gc.ca/api/en/nrcan-rncan/ess-sst?alt=json'
            if cmd == 'updated':
                if not self.options.date:
                    print self.__doc__
                    return
                try:
                    dt = dateutil.parser.parse(self.options.date)
                    query_string = 'http://geogratis.gc.ca/api/en/nrcan-rncan/ess-sst?edited-min=%s&alt=json' % dt.isoformat()
                except ValueError, e:
                    logger.error('"%s" is an invalid date' % self.options.date)
                    return
      
            try:
                json_obj = self._get_feed_json_obj(query_string)
            except urllib2.URLError, e:
                logger.error(e.reason)
                return
            
            dt = datetime.date.today()
          
            i = 0  # artificial limit while developing
            
            # Log all exports to a CSV file
            
            if self.options.report_file:
                reportf = open(self.options.report_file, 'wt') # use 'at' for appending
                fieldnames = ('ID', 'Pass or Fail', 'Title (EN)', 'Title (FR)', 'Summary (EN)', 'Summary (FR)', 
                              'Topic Categories', 'Keywords', 'Published Date', 'Browse Images',
                              'Series (EN)', 'Series (FR)', 'Series Issue (EN)', 'Series Issue (FR)', 'Reason for Failure')
                self.report = csv.DictWriter(reportf, dialect='excel',fieldnames=fieldnames)
                self.report.writerow(dict(zip(fieldnames, fieldnames)))
                   
            # Create a look-up dict of the choices for the Canada OD Schema topic categories choices
            
            self.topic_choices = dict((c['eng'], c)
                for c in schema_description.dataset_field_by_id['topic_category']['choices'] if 'eng' in c)
            
            while True:
                i = i + 1
                products = json_obj['products']
                for product in products:
                    sleep(0.2)  #Not to overwhelm Geogratis
                    self.reasons = ''
                    
                    # Retrieve and test for English record
                    geoproduct_en = self._get_geogratis_item(product['id'], 'en')
                    if not geoproduct_en:
                        self.logger.warn('Unable to retrieve English record for %s' % product['id'])
                        continue
                    
                    # Do not process Canadian Digital Elevation Data
                    if self._get_product_type(geoproduct_en) == "canadian-digital-elevation-data":
                        continue
                    
                    # Retrieve and test for French record
                    geoproduct_fr = self._get_geogratis_item(product['id'], 'fr')
                    if not geoproduct_fr:
                        self.logger.warn('Unable to retrieve French record for %s' % product['id'])
                        self.reasons = "Unable to retrieve French record"
                        continue
                        
                    odproduct = self._convert_to_od_dataset(geoproduct_en, geoproduct_fr)
                    if odproduct and not self.options.noprint:
                        if jl_pretty_format:
                            print  >>  jl_file, (json.dumps(odproduct, indent = 2 * ' '))
                        else:
                            print  >>  jl_file, (json.dumps(odproduct))

                json_obj = self._get_feed_json_obj(self._get_next_link(json_obj))
                if json_obj['count'] == 0 or i == 1:
                    break
                
    """
    The following NRCAN fields are mandatory for the Open Data schema:
    * id
    * title (English and French)
    * summary (English and French)
    * subject
    * topicCategories
    * keywords
    * spatial
    * date_published 
    * browse_graphic_url
    * 
    """
    def _convert_to_od_dataset(self, geoproduct_en, geoproduct_fr):
        odproduct = {}
        valid = True
        
        odproduct['id'] = geoproduct_en['id']
        odproduct['author_email'] = "open-ouvert@tbs-sct.gc.ca"
        odproduct['language'] = "eng; CAN | fra; CAN"
        odproduct['owner_org'] = "nrcan-rncan"
        odproduct['department_number'] = "115"
        odproduct['title'] = geoproduct_en['title']
        odproduct['title_fra'] = geoproduct_fr['title']
        odproduct['name'] = ""
        odproduct['notes'] = geoproduct_en.get('summary', 'No title provided')
        odproduct['notes_fra'] = geoproduct_fr.get('summary', 'Pas de titre pr\u00e9vu')
        odproduct['catalog_type'] = "Geo Data | G\u00e9o"
        # The subject and category fields are derived from the topicCategories field in Geogratis.
        # In the CKAN Canada metadata_schema intergace, there is a mapping that determine which GoC subject
        # to use based on the topicCategories being used.
        topics_subjects = self._get_gc_subject_category(geoproduct_en)
        odproduct['subject'] = topics_subjects['subjects']
        if len(odproduct['subject']) == 0:
            valid = False
            self.reasons = '%s No GC Subjects;' % self.reasons
        odproduct['topic_category'] = topics_subjects['topics']
        if len(odproduct['topic_category']) == 0:
            valid = False
            self.reasons = '%s No GC Topics;' % self.reasons
                    
        # Keywords
        odproduct['keywords'] = self._extract_keywords(geoproduct_en.get('keywords', []))
        if len(odproduct['keywords']) == 0:
            valid = False
            self.reasons = '%s Missing English Keywords;' % self.reasons
        odproduct['keywords_fra'] = self._extract_keywords(geoproduct_fr.get('keywords', []))
        if len(odproduct['keywords_fra']) == 0:
            valid = False        
            self.reasons = '%s Missing English Keywords;' % self.reasons
        
        odproduct['license_id'] = "ca-ogl-lgo"
        odproduct['attribution'] = "Contains information licensed under the Open Government Licence \u2013 Canada."
        odproduct['attribution_fra'] = "Contient des informations autoris\u00e9es sous la Licence du gouvernement ouvert- Canada"
        odproduct['geographic_region'] = self._get_places(geoproduct_en, geoproduct_fr)
        odproduct['spatial'] = geoproduct_en['geometry']
        try:
            odproduct['date_published'] = geoproduct_en['citation']['publicationDate']
        except:
            odproduct['date_published'] = ''
            valid = False
            self.reasons = '%s Missing Date Published;' % self.reasons
            
        odproduct['spatial_representation_type'] = "Vector | Vecteur"
        odproduct['presentation_form'] = "documentDigital"
        try:
            odproduct['browse_graphic_url'] =  geoproduct_en['browseImages'][0]['link']
        except:
            odproduct['browse_graphic_url'] =  "/static/img/canada_default.png"
        odproduct['date_modified'] = geoproduct_en.get('updatedDate', '2000-01-01')
        odproduct['maintenance_and_update_frequency'] = "As Needed | Au besoin"
        try:
            odproduct['data_series_name'] = geoproduct_en['citation']['series']
        except:
            odproduct['data_series_name'] = ''
        try:
            odproduct['data_series_name_fra'] = geoproduct_fr['citation']['series']
        except:
            odproduct['data_series_name_fra'] = '' 
        try:
            odproduct['data_series_issue_identification'] = geoproduct_en['citation']['seriesIssue']
        except:
            odproduct['data_series_issue_identification'] = ''
        try:
            odproduct['data_series_issue_identification_fra'] = geoproduct_fr['citation']['seriesIssue']
        except:
            odproduct['data_series_issue_identification_fra'] = ''
        odproduct['digital_object_identifier'] = ""
        odproduct['time_period_coverage_start'] = ""
        odproduct['time_period_coverage_end'] = ""
        odproduct['url'] = geoproduct_en['url']
        odproduct['url_fra'] = geoproduct_fr['url']
        odproduct['endpoint_url'] = "http://geogratis.gc.ca/api/en"
        odproduct['endpoint_url_fra'] = "http://geogratis.gc.ca/api/fr"
        odproduct['ready_to_publish'] = True
        odproduct['portal_release_date'] = ""
     
        # Keep a report of the results
        if self.options.report_file:
            self.report.writerow({'ID' : odproduct['id'], 
                            'Pass or Fail' : valid, 
                            'Title (EN)' : odproduct['title'].encode('utf-8'), 
                            'Title (FR)' : odproduct['title_fra'].encode('utf-8'), 
                            'Summary (EN)' : 'Y' if odproduct['notes'] <> 'No title provided' else 'N', 
                            'Summary (FR)' : 'Y' if odproduct['notes_fra'] <> 'Pas de titre pr\u00e9vu' else 'N', 
                            'Topic Categories' : 'Y' if len(odproduct['topic_category']) > 0 else 'N', 
                            'Keywords' : 'Y' if len(odproduct['topic_category']) > 0 else 'N', 
                            'Published Date' : 'Y' if odproduct['date_published'] <> '' else 'N', 
                            'Browse Images' : 'Y' if odproduct['browse_graphic_url'] <> "/static/img/canada_default.png" else 'N',
                            'Series (EN)' : 'Y' if odproduct['data_series_name'] <> '' else 'N', 
                            'Series (FR)' : 'Y' if odproduct['data_series_name_fra'] <> '' else 'N', 
                            'Series Issue (EN)' : 'Y' if odproduct['data_series_issue_identification'] <> '' else 'N', 
                            'Series Issue (FR)' : 'Y' if odproduct['data_series_issue_identification_fra'] <> '' else 'N',
                            'Reason for Failure' : self.reasons})
        if not valid:
            odproduct = None
        return odproduct

    def _get_feed_json_obj(self, link):
        try:
            response = urllib2.urlopen(link)
            json_data = response.read()
            json_obj = json.loads(json_data)
            json_obj['url'] = link
            return json_obj
        except urllib2.HTTPError, e:
            self.logger.error(e.msg)
        return None

    def _get_geogratis_item(self, geo_id, lang):
        json_obj = self._get_feed_json_obj('http://geogratis.gc.ca/api/%s/nrcan-rncan/ess-sst/%s.json' % (lang, geo_id))
        return json_obj

    def _get_next_link(self, json_obj):
        links = json_obj['links']
        for link in links:
            if link['rel'] == 'next':
                return link['href']
    
    
    def _extract_keywords(self, keywords):
        simple_keywords = []
        for keyword in keywords:
            words = keyword.split('>')
            if len(words) > 0:
                last_word = words.pop()
                simple_keywords.append(last_word.strip())
        return simple_keywords
            
    def _get_product_type(self, geoproduct):
        product_type = ""
        categories = geoproduct['categories']
        for category in categories:
          if category['type'] == 'urn:iso:series':
              product_type = category['terms'][0]['term']
      
    def _get_places(self, geoproduct_en, geoproduct_fr):
        places = ""
        # For this, not likely to match against current schema - needs more discussion
        return places
        
    def _get_gc_subject_category(self, geoproduct_en):
        topics = []
        subjects = []
        
        # These could be key words
        # Get the English subjects
#        categories_en = geoproduct_en['categories']
#        subjects_en = []
#        for category in categories_en:
#            if category['type'] == 'urn:gc:subjects':
#                subjects_en = category['terms']
        
        # Get the French subjects
#        categories_fr = geoproduct_fr['categories']
#        subjects_fr = []
#        for category in categories_fr:
#            if category['type'] == 'urn:gc:subjects':
#                subjects_en = category['terms']

        schema_categories = schema_description.dataset_field_by_id['topic_category']['choices']
        topic_categories = geoproduct_en.get('topicCategories', [])
        for topic in topic_categories:
            topic_key = re.sub("([a-z])([A-Z])","\g<1> \g<2>", topic).title()
            topics.append(self.topic_choices[topic_key]['key'])
            topic_subject_keys = self.topic_choices[topic_key]['subject_ids']
            for topic_subject_key in topic_subject_keys:
                subjects.append(schema_description.dataset_field_by_id['subject']['choices_by_id'][topic_subject_key]['key'])
#         print topics
#         print subjects
        return { 'topics' : topics, 'subjects' : subjects}
  